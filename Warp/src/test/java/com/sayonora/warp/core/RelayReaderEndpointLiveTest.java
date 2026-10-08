package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * MySQL in RELAY mode (a raw-byte proxy) with a reader port: the main port reaches the primary, the reader port a replica, and the replica
 * refuses writes. Opt-in: WARP_TEST_MYSQL_BIN and WARP_TEST_BROWNOUT_PG_BIN (the config database).
 */
class RelayReaderEndpointLiveTest {

    private static long serverId(String url) throws Exception {
        try (Connection c = DriverManager.getConnection(url, "root", ""); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select @@server_id")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void mysqlRelayMainPortGoesToThePrimaryAndTheReaderPortToAReplica() throws Exception {
        String myBin = System.getenv("WARP_TEST_MYSQL_BIN");
        String pgBin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(myBin != null && !myBin.isBlank() && pgBin != null && !pgBin.isBlank(), "set WARP_TEST_MYSQL_BIN and WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("relayreader");
        LocalPostgres cfg = LocalPostgres.primary(pgBin, dir, "cfg", LocalPostgres.freePort());
        LocalMySql p = LocalMySql.create(myBin, dir, "p", LocalPostgres.freePort(), 11);
        LocalMySql r = LocalMySql.create(myBin, dir, "r", LocalPostgres.freePort(), 12);
        try {
            r.replicateFrom(p, 2);
            p.exec("create database shard", "create table shard.t (id int primary key)", "insert into shard.t values (1)");
            long deadline = System.currentTimeMillis() + 30_000;
            while (r.scalar("select count(*) from information_schema.tables where table_schema='shard'") < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            int readPort = LocalPostgres.freePort();
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("mywire", "WARP_MYWIRE_PORT")
                    .env("WARP_BACKENDS", "my=" + p.url("shard") + "|root|||" + r.url("shard") + "~30")
                    .env("WARP_MYWIRE_BACKEND_MODE", "relay")
                    .env("WARP_MYSQL_HOST", "127.0.0.1").env("WARP_MYSQL_PORT", String.valueOf(p.port()))
                    .env("WARP_MYWIRE_RELAY_BACKEND", "my").env("WARP_MYWIRE_RELAY_READ_PORT", String.valueOf(readPort))
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                Thread.sleep(4000); // the replica is sampled
                String main = "jdbc:mysql://127.0.0.1:" + warp.port("mywire") + "/shard?allowPublicKeyRetrieval=true&useSSL=false";
                String reader = "jdbc:mysql://127.0.0.1:" + readPort + "/shard?allowPublicKeyRetrieval=true&useSSL=false";
                assertEquals(11, serverId(main), "the main relay port reaches the primary");
                assertEquals(12, serverId(reader), "the reader port reaches the replica");
                try (Connection c = DriverManager.getConnection(reader, "root", ""); Statement st = c.createStatement()) {
                    try (ResultSet rs = st.executeQuery("select count(*) from t")) {
                        rs.next();
                        assertEquals(1, rs.getInt(1));
                    }
                    SQLException refused = assertThrows(SQLException.class, () -> st.executeUpdate("insert into t values (2)"));
                    assertTrue(refused.getMessage().toLowerCase().contains("read-only") || refused.getMessage().toLowerCase().contains("read only"), refused.getMessage());
                }
                try (Connection c = DriverManager.getConnection(main, "root", ""); Statement st = c.createStatement()) {
                    assertEquals(1, st.executeUpdate("insert into t values (3)"));
                }
                long until = System.currentTimeMillis() + 20_000;
                int seen = 0;
                while (seen < 2 && System.currentTimeMillis() < until) {
                    try (Connection c = DriverManager.getConnection(reader, "root", ""); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from t")) {
                        rs.next();
                        seen = rs.getInt(1);
                    }
                    Thread.sleep(300);
                }
                assertEquals(2, seen, "a write through the main port reaches the replica read through the reader port");

                // the replica goes away: the reader port falls back to the primary rather than failing
                r.stop();
                Thread.sleep(12_000);
                assertEquals(11, serverId(reader), "with the replica down the reader port falls back to the primary");
            }
        } finally {
            r.stop();
            p.stop();
            cfg.stop("immediate");
        }
    }
}
