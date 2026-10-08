package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Writes to a hash-sharded table whose shards are the same real engine (a shard group is homogeneous): a multi-row INSERT spanning shards,
 * atomic failure, broadcast UPDATE/DELETE and transactions, through a running Warp. Opt-in: WARP_TEST_SHARDED_WRITES=mssql|oracle and
 * WARP_TEST_BROWNOUT_PG_BIN (the config database), with the engine reachable as set up by the comments on each case.
 */
class ShardedWriteEnginesLiveTest {

    private record Engine(String name, String[] urls, String[] users, String[] passwords) {
    }

    private static Engine mssql() {
        String opts = ";encrypt=true;trustServerCertificate=true";
        return new Engine("mssql",
                new String[] {"jdbc:sqlserver://127.0.0.1:14341;databaseName=shard1" + opts, "jdbc:sqlserver://127.0.0.1:14341;databaseName=shard2" + opts},
                new String[] {"sa", "sa"}, new String[] {"Warp_Test_1234!", "Warp_Test_1234!"});
    }

    private static Engine oracle() {
        return new Engine("oracle",
                new String[] {"jdbc:oracle:thin:@//127.0.0.1:15211/FREEPDB1", "jdbc:oracle:thin:@//127.0.0.1:15211/FREEPDB1"},
                new String[] {"shard1", "shard2"}, new String[] {"shardpw1", "shardpw2"});
    }

    private static long count(Engine e, int shard) throws Exception {
        return scalar(e, shard, "select count(*) from orders");
    }

    private static long scalar(Engine e, int shard, String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(e.urls()[shard], e.users()[shard], e.passwords()[shard]);
                Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void resetShards(Engine e) throws Exception {
        for (int i = 0; i < 2; i++) {
            try (Connection c = DriverManager.getConnection(e.urls()[i], e.users()[i], e.passwords()[i]); Statement st = c.createStatement()) {
                try {
                    st.execute("drop table orders");
                } catch (SQLException absent) {
                    // first run
                }
                st.execute("create table orders (id int primary key, customer_id int, amount int)");
            }
        }
    }

    private void run(Engine e) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank() && e.name().equals(System.getenv("WARP_TEST_SHARDED_WRITES")),
                "set WARP_TEST_SHARDED_WRITES=" + e.name() + " and WARP_TEST_BROWNOUT_PG_BIN");
        resetShards(e);
        Path dir = Files.createTempDirectory("shardwrites");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        try {
            String spec = "default=" + cfg.url() + "|warp|secret;s1=" + e.urls()[0].replace(";", "%3B") + "|" + e.users()[0] + "|" + e.passwords()[0]
                    + ";s2=" + e.urls()[1].replace(";", "%3B") + "|" + e.users()[1] + "|" + e.passwords()[1];
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", spec)
                    .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:s1,s2")
                    .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    // a multi-row INSERT spanning shards is cut per shard
                    StringBuilder values = new StringBuilder();
                    for (int i = 0; i < 20; i++) {
                        values.append(i == 0 ? "" : ", ").append("(").append(100 + i).append(", ").append(100 + i).append(", 1)");
                    }
                    assertEquals(20, st.executeUpdate("insert into orders (id, customer_id, amount) values " + values));
                    assertEquals(20, count(e, 0) + count(e, 1));
                    assertTrue(count(e, 0) > 0 && count(e, 1) > 0, "both shards received part of it: " + count(e, 0) + " / " + count(e, 1));
                    for (int i = 0; i < 20; i++) {
                        try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = " + (100 + i))) {
                            rs.next();
                            assertEquals(1, rs.getInt(1), "keyed read finds customer " + (100 + i));
                        }
                    }
                    // atomic: a duplicate key on one shard leaves nothing behind on the other
                    var dup = assertThrows(SQLException.class, () -> st.executeUpdate(
                            "insert into orders (id, customer_id, amount) values (900, 900, 1), (100, 100, 1), (901, 901, 1), (902, 902, 1), (903, 903, 1)"));
                    System.out.println("SHARDWRITE-NOTE " + e.name() + " duplicate: " + dup.getMessage().replaceAll("\\s+", " "));
                    assertEquals(20, count(e, 0) + count(e, 1), "the failed statement left no rows on any shard");
                    // broadcast UPDATE / DELETE with exact counts
                    assertEquals(20, st.executeUpdate("update orders set amount = amount + 1"));
                    assertEquals(40, scalar(e, 0, "select coalesce(sum(amount), 0) from orders") + scalar(e, 1, "select coalesce(sum(amount), 0) from orders"));
                    // a client transaction spanning both shards commits or rolls back as one
                    if (!e.name().equals("mssql")) { // client transactions need XA, which this SQL Server container does not have
                    c.setAutoCommit(false);
                    assertEquals(20, st.executeUpdate("update orders set amount = amount + 1000"));
                    c.rollback();
                    assertEquals(40, scalar(e, 0, "select coalesce(sum(amount), 0) from orders") + scalar(e, 1, "select coalesce(sum(amount), 0) from orders"));
                    assertEquals(20, st.executeUpdate("update orders set amount = amount + 1000"));
                    c.commit();
                    assertEquals(20040, scalar(e, 0, "select coalesce(sum(amount), 0) from orders") + scalar(e, 1, "select coalesce(sum(amount), 0) from orders"));
                    c.setAutoCommit(true);
                    } else {
                        assertEquals(20, st.executeUpdate("update orders set amount = amount + 1000"));
                    }
                    assertEquals(20, st.executeUpdate("delete from orders where id >= 100"));
                    assertEquals(0, count(e, 0) + count(e, 1));
                }
            }
        } finally {
            cfg.stop("immediate");
        }
    }

    /** Needs the SQL Server container from the setup notes (port 14341, databases shard1 and shard2). */
    @Test
    void sqlServerShardsTakeSplitInsertsAndBroadcastWritesAtomically() throws Exception {
        run(mssql());
    }

    /** Needs Oracle Free on port 15211 with users shard1 / shard2 (passwords shardpw1 / shardpw2). */
    @Test
    void oracleShardsTakeSplitInsertsAndBroadcastWritesAtomically() throws Exception {
        run(oracle());
    }
}
