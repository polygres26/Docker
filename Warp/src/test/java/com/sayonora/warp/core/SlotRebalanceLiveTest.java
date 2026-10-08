package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalMySql;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Adding a third shard to a slot-sharded table while writes continue: slots move to the new shard through the admin API, every acknowledged
 * write survives exactly once on the shard that owns its slot, and the table can be rebalanced again (back). Opt-in: WARP_TEST_BROWNOUT_PG_BIN
 * (config database and the Postgres case) and WARP_TEST_MYSQL_BIN (the MySQL case).
 */
class SlotRebalanceLiveTest {

    private interface Shard {
        Connection conn() throws Exception;

        String backendSpec(String name);
    }

    private static long count(Shard s) throws Exception {
        try (Connection c = s.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static Set<Long> ids(Shard s) throws Exception {
        Set<Long> out = new HashSet<>();
        try (Connection c = s.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from orders")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    private void run(String engine, LocalPostgres cfg, Shard s1, Shard s2, Shard s3) throws Exception {
        String spec = "default=" + cfg.url() + "|warp|secret;" + s1.backendSpec("s1") + ";" + s2.backendSpec("s2") + ";" + s3.backendSpec("s3");
        try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63")
                .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
            String admin = "http://localhost:" + warp.metricsPort();
            Set<Long> acked = ConcurrentHashMap.newKeySet();
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                for (int from = 1; from <= 20_000; from += 500) { // multi-row inserts spanning both shards, split per shard by Warp
                    StringBuilder values = new StringBuilder();
                    for (int i = from; i < from + 500; i++) {
                        values.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", ").append(i % 100).append(")");
                        acked.add((long) i);
                    }
                    st.executeUpdate("insert into orders (id, customer_id, amount) values " + values);
                }
            }
            assertEquals(20_000, count(s1) + count(s2));
            assertEquals(0, count(s3));

            // writes keep coming while 21 of the 64 slots move to s3
            AtomicBoolean stop = new AtomicBoolean();
            AtomicLong next = new AtomicLong(100_000);
            AtomicLong failed = new AtomicLong();
            List<Thread> writers = new ArrayList<>();
            for (int w = 0; w < 3; w++) {
                Thread t = new Thread(() -> {
                    try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                        while (!stop.get()) {
                            long id = next.incrementAndGet();
                            try {
                                st.executeUpdate("insert into orders (id, customer_id, amount) values (" + id + ", " + id + ", 1)");
                                acked.add(id);
                            } catch (SQLException e) {
                                failed.incrementAndGet();
                            }
                            Thread.sleep(5);
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                });
                t.start();
                writers.add(t);
            }
            // the new backend already holds the (empty) table, so it joins the group before anything is asked of the table
            assertTrue(BrownoutHarness.http("POST", admin + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}").contains("s3="));
            // a scatter read of the rows that never change must always see each of them exactly once, also while rows are being copied
            AtomicLong wrongCounts = new AtomicLong();
            AtomicLong countChecks = new AtomicLong();
            Thread counter = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    while (!stop.get()) {
                        try (ResultSet rs = st.executeQuery("select count(*) from orders where id <= 20000")) {
                            rs.next();
                            countChecks.incrementAndGet();
                            if (rs.getLong(1) != 20_000) {
                                wrongCounts.incrementAndGet();
                                System.out.println("SLOT-NOTE scatter count saw " + rs.getLong(1));
                            }
                        }
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    wrongCounts.incrementAndGet();
                    System.out.println("SLOT-NOTE counter failed: " + e);
                }
            });
            counter.start();
            writers.add(counter);
            Thread.sleep(1500);
            String dry = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21,\"dryRun\":true}");
            assertTrue(dry.contains("\"dryRun\":true") && dry.contains("\"slotsToMove\":21"), dry);
            assertEquals(0, count(s3), "a dry run moves nothing");
            long t0 = System.currentTimeMillis();
            String res = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}");
            System.out.println("SLOT-NOTE " + engine + " rebalance to s3 took " + (System.currentTimeMillis() - t0) + " ms: " + res);
            JsonObject r = JsonParser.parseString(res).getAsJsonObject();
            assertTrue(r.has("rowsCopied") && r.get("rowsCopied").getAsLong() > 0, res);
            assertTrue(!r.has("warning"), res);
            Thread.sleep(1500);

            // back again: everything s3 got goes home, still under write load
            res = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s1\",\"slots\":[21,22,23,24,25]}");
            System.out.println("SLOT-NOTE " + engine + " second rebalance: " + res);
            JsonObject second = JsonParser.parseString(res).getAsJsonObject();
            if (engine.equals("postgres") || engine.equals("mysql")) { // on a small table the publish and the cleanup can outweigh the copy elsewhere
                assertTrue(second.get("scatterReadsHeldMillis").getAsLong() <= second.get("writesHeldMillis").getAsLong(),
                        "scatter reads wait for less than the writes do (the copy is staged): " + res);
            }
            Thread.sleep(1500);
            stop.set(true);
            for (Thread t : writers) {
                t.join(20_000);
            }

            // every acknowledged id exists exactly once, on the shard that owns its slot in the final map
            String mapText = JsonParser.parseString(BrownoutHarness.http("GET", admin + "/api/sharding", null)).getAsJsonObject()
                    .getAsJsonArray("tables").get(0).getAsJsonObject().get("map").getAsString();
            ShardingStrategy.SlotStrategy finalMap = ShardingStrategy.SlotStrategy.parse(mapText);
            Map<String, Set<Long>> held = new HashMap<>();
            held.put("s1", ids(s1));
            held.put("s2", ids(s2));
            held.put("s3", ids(s3));
            Set<Long> everything = new HashSet<>();
            long rows = 0;
            for (Map.Entry<String, Set<Long>> e : held.entrySet()) {
                rows += e.getValue().size();
                everything.addAll(e.getValue());
                for (long id : e.getValue()) {
                    assertEquals(e.getKey(), finalMap.resolve(String.valueOf(id)), "id " + id + " sits on the shard that owns its slot");
                }
            }
            assertEquals(rows, everything.size(), "no id exists on two shards");
            Set<Long> lost = new HashSet<>(acked);
            lost.removeAll(everything);
            System.out.println("SLOT-RESULT " + engine + " | acked " + acked.size() + " | rows " + rows + " | lost " + lost.size() + " | failed writes " + failed.get()
                    + " | map " + mapText + " | s1/s2/s3 = " + held.get("s1").size() + "/" + held.get("s2").size() + "/" + held.get("s3").size());
            assertEquals(Set.of(), lost, "no acknowledged write is missing");
            System.out.println("SLOT-NOTE " + engine + " scatter counts checked " + countChecks.get() + " times during the moves, wrong " + wrongCounts.get());
            assertTrue(countChecks.get() > 20, "the counter ran");
            assertEquals(0, wrongCounts.get(), "a scatter read never saw a moved row twice or missed one");
            assertEquals(0, failed.get(), "writes waited for the slots they needed instead of failing");
            assertTrue(held.get("s3").size() > 0, "s3 keeps part of the table");
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("select count(*) from orders")) {
                rs.next();
                assertEquals(rows, rs.getLong(1), "a scatter count through Warp sees every row once");
            }
        }
    }

    @Test
    void postgresAThirdShardTakesSlotsWhileWritesContinue() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("slotpg");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres[] pg = new LocalPostgres[3];
        try {
            Shard[] shards = new Shard[3];
            for (int i = 0; i < 3; i++) {
                pg[i] = LocalPostgres.primary(bin, dir, "s" + i, LocalPostgres.freePort());
                try (Connection c = pg[i].conn(); Statement st = c.createStatement()) {
                    st.execute("create table orders (id int primary key, customer_id int, amount int)");
                }
                LocalPostgres p = pg[i];
                shards[i] = new Shard() {
                    public Connection conn() throws Exception {
                        return p.conn();
                    }

                    public String backendSpec(String name) {
                        return name + "=" + p.url() + "|warp|secret";
                    }
                };
            }
            run("postgres", cfg, shards[0], shards[1], shards[2]);
        } finally {
            for (LocalPostgres p : pg) {
                if (p != null) {
                    p.stop("immediate");
                }
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void mysqlAThirdShardTakesSlotsWhileWritesContinue() throws Exception {
        String pgBin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        String myBin = System.getenv("WARP_TEST_MYSQL_BIN");
        Assumptions.assumeTrue(pgBin != null && !pgBin.isBlank() && myBin != null && !myBin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN and WARP_TEST_MYSQL_BIN");
        Path dir = Files.createTempDirectory("slotmy");
        LocalPostgres cfg = LocalPostgres.primary(pgBin, dir, "cfg", LocalPostgres.freePort());
        LocalMySql[] my = new LocalMySql[3];
        try {
            Shard[] shards = new Shard[3];
            for (int i = 0; i < 3; i++) {
                my[i] = LocalMySql.create(myBin, dir, "s" + i, LocalPostgres.freePort(), 31 + i);
                my[i].exec("create database shard", "create table shard.orders (id int primary key, customer_id int, amount int)");
                LocalMySql m = my[i];
                shards[i] = new Shard() {
                    public Connection conn() throws Exception {
                        return DriverManager.getConnection(m.url("shard"), "root", "");
                    }

                    public String backendSpec(String name) {
                        return name + "=" + m.url("shard") + "|root|";
                    }
                };
            }
            run("mysql", cfg, shards[0], shards[1], shards[2]);
        } finally {
            for (LocalMySql m : my) {
                if (m != null) {
                    m.stop();
                }
            }
            cfg.stop("immediate");
        }
    }

    private static Shard jdbcShard(String url, String user, String password) {
        return new Shard() {
            public Connection conn() throws Exception {
                return DriverManager.getConnection(url, user, password);
            }

            public String backendSpec(String name) {
                return name + "=" + url.replace(";", "%3B") + "|" + user + "|" + password;
            }
        };
    }

    private void realEngine(String engine) throws Exception {
        String pgBin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(pgBin != null && !pgBin.isBlank() && engine.equals(System.getenv("WARP_TEST_SHARD_ENGINE")),
                "set WARP_TEST_SHARD_ENGINE=" + engine + " and WARP_TEST_BROWNOUT_PG_BIN");
        Shard[] shards = new Shard[3];
        for (int i = 0; i < 3; i++) {
            shards[i] = engine.equals("mssql")
                    ? jdbcShard("jdbc:sqlserver://127.0.0.1:14341;databaseName=shard" + (i + 1) + ";encrypt=true;trustServerCertificate=true", "sa", "Warp_Test_1234!")
                    : jdbcShard("jdbc:oracle:thin:@//127.0.0.1:15211/FREEPDB1", "shard" + (i + 1), "shardpw" + (i + 1));
            try (Connection c = shards[i].conn(); Statement st = c.createStatement()) {
                for (String drop : new String[] {"drop table orders", "drop table orders__rebal"}) {
                    try {
                        st.execute(drop);
                    } catch (SQLException absent) {
                        // first run
                    }
                }
                st.execute("create table orders (id int primary key, customer_id int, amount int)");
            }
        }
        Path dir = Files.createTempDirectory("slot" + engine);
        LocalPostgres cfg = LocalPostgres.primary(pgBin, dir, "cfg", LocalPostgres.freePort());
        try {
            run(engine, cfg, shards[0], shards[1], shards[2]);
        } finally {
            cfg.stop("immediate");
        }
    }

    /** Needs the SQL Server container of the setup notes (port 14341, databases shard1..shard3, sa / Warp_Test_1234!). */
    @Test
    void sqlServerAThirdShardTakesSlotsWhileWritesContinue() throws Exception {
        realEngine("mssql");
    }

    /** Needs Oracle Free on port 15211 with users shard1..shard3 (passwords shardpw1..3). */
    @Test
    void oracleAThirdShardTakesSlotsWhileWritesContinue() throws Exception {
        realEngine("oracle");
    }
}
