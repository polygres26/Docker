package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Sharding, read replicas and failover together, on Postgres and on MySQL: {@code orders} is hash-sharded over two shard backends that each have a
 * replica and {@code promote} mode. Checks that writes spread over both shards, that reads (including a scatter-gather aggregate) are
 * served by the shard replicas, that crashing one shard's primary fails over that shard only while the other keeps serving, that a planned
 * switchover on the other shard works under load, and that no acknowledged write is lost or duplicated. Opt-in: WARP_TEST_BROWNOUT_PG_BIN (Postgres) and WARP_TEST_MYSQL_BIN (MySQL).
 */
class ShardedReplicaFailoverLiveTest {

    /** One database server of either engine. */
    private interface Node {
        int port();

        String url();

        Connection conn() throws Exception;

        boolean writable();

        void crash();

        void stop();

        /** The backend-spec credentials: {@code user|password}. */
        String credentials();
    }

    private static Node pgNode(LocalPostgres pg) {
        return new Node() {
            public int port() {
                return pg.port();
            }

            public String url() {
                return pg.url();
            }

            public Connection conn() throws Exception {
                return pg.conn();
            }

            public boolean writable() {
                return pg.writable();
            }

            public void crash() {
                pg.stop("immediate");
            }

            public void stop() {
                pg.stop("immediate");
            }

            public String credentials() {
                return "warp|secret";
            }
        };
    }

    private static Node myNode(com.sayonora.warp.testsupport.LocalMySql my) {
        return new Node() {
            public int port() {
                return my.port();
            }

            public String url() {
                return my.url("shard");
            }

            public Connection conn() throws Exception {
                return java.sql.DriverManager.getConnection(my.url("shard"), "root", "");
            }

            public boolean writable() {
                return my.writable();
            }

            public void crash() {
                try {
                    my.crash();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            public void stop() {
                my.stop();
            }

            public String credentials() {
                return "root|";
            }
        };
    }

    private static final String DDL = "create table orders (id int primary key, customer_id int, amount int)";

    private static long count(Node n) throws Exception {
        try (Connection c = n.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long sumAmount(Node n) throws Exception {
        return sumWhere(n, "1 = 1");
    }

    private static long sumWhere(Node n, String where) throws Exception {
        try (Connection c = n.conn(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("select count(*), coalesce(sum(amount), 0) from orders where " + where)) {
            rs.next();
            return where.equals("1 = 1") ? rs.getLong(2) : rs.getLong(1);
        }
    }

    private static Set<Long> ids(Node n) throws Exception {
        Set<Long> out = new HashSet<>();
        try (Connection c = n.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from orders")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    private static void waitFor(String what, long seconds, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.currentTimeMillis() + seconds * 1000;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (cond.call()) {
                    return;
                }
            } catch (Exception ignored) {
                // not yet
            }
            Thread.sleep(500);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static long routedReads(WarpProcess warp, Node... replicas) throws Exception {
        String json = BrownoutHarness.http("GET", "http://localhost:" + warp.metricsPort() + "/api/replicas", null);
        long total = 0;
        for (var p : com.google.gson.JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("primaries")) {
            for (var r : p.getAsJsonObject().getAsJsonArray("replicas")) {
                for (Node rep : replicas) {
                    if (r.getAsJsonObject().get("url").getAsString().contains(":" + rep.port() + "/")) {
                        total += r.getAsJsonObject().get("routedReads").getAsLong();
                    }
                }
            }
        }
        return total;
    }

    @Test
    void postgresShardedTableWithReplicasSurvivesAShardFailoverAndASwitchover() throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("shardedha");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres p1 = LocalPostgres.primary(bin, dir, "p1", LocalPostgres.freePort());
        LocalPostgres p2 = LocalPostgres.primary(bin, dir, "p2", LocalPostgres.freePort());
        LocalPostgres r1 = null;
        LocalPostgres r2 = null;
        try {
            for (LocalPostgres p : new LocalPostgres[] {p1, p2}) {
                try (Connection c = p.conn(); Statement st = c.createStatement()) {
                    st.execute(DDL);
                }
            }
            r1 = LocalPostgres.replicaOf(bin, dir, "r1", p1, LocalPostgres.freePort());
            r2 = LocalPostgres.replicaOf(bin, dir, "r2", p2, LocalPostgres.freePort());
            run("postgres", cfg, pgNode(p1), pgNode(r1), pgNode(p2), pgNode(r2));
        } finally {
            for (LocalPostgres p : new LocalPostgres[] {r1, r2, p1, p2, cfg}) {
                if (p != null) {
                    p.stop("immediate");
                }
            }
        }
    }

    @Test
    void mysqlShardedTableWithReplicasSurvivesAShardFailoverAndASwitchover() throws Exception {
        String pgBin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        String myBin = System.getenv("WARP_TEST_MYSQL_BIN");
        Assumptions.assumeTrue(pgBin != null && !pgBin.isBlank() && myBin != null && !myBin.isBlank(),
                "set WARP_TEST_BROWNOUT_PG_BIN (the config database) and WARP_TEST_MYSQL_BIN");
        Path dir = Files.createTempDirectory("shardedhamy");
        LocalPostgres cfg = LocalPostgres.primary(pgBin, dir, "cfg", LocalPostgres.freePort());
        var p1 = com.sayonora.warp.testsupport.LocalMySql.create(myBin, dir, "p1", LocalPostgres.freePort(), 11);
        var r1 = com.sayonora.warp.testsupport.LocalMySql.create(myBin, dir, "r1", LocalPostgres.freePort(), 12);
        var p2 = com.sayonora.warp.testsupport.LocalMySql.create(myBin, dir, "p2", LocalPostgres.freePort(), 21);
        var r2 = com.sayonora.warp.testsupport.LocalMySql.create(myBin, dir, "r2", LocalPostgres.freePort(), 22);
        try {
            r1.replicateFrom(p1, 2);
            r2.replicateFrom(p2, 2);
            for (var p : new com.sayonora.warp.testsupport.LocalMySql[] {p1, p2}) {
                p.exec("create database shard", "create table shard.orders (id int primary key, customer_id int, amount int)");
            }
            waitFor("the replicas to hold the table", 30, () -> r1.scalar("select count(*) from information_schema.tables where table_schema='shard'") == 1
                    && r2.scalar("select count(*) from information_schema.tables where table_schema='shard'") == 1);
            run("mysql", cfg, myNode(p1), myNode(r1), myNode(p2), myNode(r2));
        } finally {
            r1.stop();
            r2.stop();
            p1.stop();
            p2.stop();
            cfg.stop("immediate");
        }
    }

    private void run(String engine, LocalPostgres cfg, Node p1, Node r1, Node p2, Node r2) throws Exception {
        String spec = "default=" + cfg.url() + "|warp|secret;s1=" + p1.url() + "|" + p1.credentials() + "||" + r1.url() + "~10|promote;s2="
                + p2.url() + "|" + p2.credentials() + "||" + r2.url() + "~10|promote";
        try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:s1,s2")
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
            String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
            Set<Long> acked = new HashSet<>();
            // 1. writes spread over both shards, each landing on the shard a keyed read will look at
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                for (int i = 1; i <= 100; i++) {
                    st.executeUpdate("insert into orders (id, customer_id, amount) values (" + i + ", " + i + ", " + i * 10 + ")");
                    acked.add((long) i);
                }
                for (int i = 1; i <= 100; i += 7) {
                    try (ResultSet rs = st.executeQuery("select amount from orders where customer_id = " + i)) {
                        assertTrue(rs.next(), "a keyed read finds the row written for customer " + i);
                        assertEquals(i * 10, rs.getInt(1));
                    }
                }
            }
            assertTrue(count(p1) > 10 && count(p2) > 10, "both shards got rows: " + count(p1) + " / " + count(p2));
            assertEquals(100, count(p1) + count(p2));

            // 1b. UPDATE / DELETE: keyed ones go to one shard, the rest to every shard, with exact row counts
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                assertEquals(1, st.executeUpdate("update orders set amount = 99999 where customer_id = 7"), "a keyed update touches the one row");
                assertEquals(1, sumWhere(p1, "amount = 99999") + sumWhere(p2, "amount = 99999"), "...on exactly one shard");
                assertEquals(1, st.executeUpdate("update orders set amount = 70 where customer_id = 7"));
                assertEquals(100, st.executeUpdate("update orders set amount = amount + 1"), "an unkeyed update runs on every shard and the counts add up");
                assertEquals(50600, sumAmount(p1) + sumAmount(p2), "both shards were updated");
                assertEquals(100, st.executeUpdate("update orders set amount = amount - 1"));
                assertEquals(10, st.executeUpdate("delete from orders where id > 90"), "an unkeyed delete runs on every shard");
                for (long id = 91; id <= 100; id++) {
                    acked.remove(id);
                }
                assertEquals(90, count(p1) + count(p2));
                var refused = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                        () -> st.executeUpdate("update orders set customer_id = 5 where id = 1"));
                assertTrue(refused.getMessage().contains("cannot UPDATE the shard key"), refused.getMessage());
            }
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                var refused = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                        () -> st.executeUpdate("delete from orders where amount = -1"));
                System.out.println("SHARDED-NOTE " + engine + " broadcast inside a transaction: " + refused.getMessage().replaceAll("\\s+", " "));
                c.rollback();
            }

            // 2. reads, including a scatter-gather aggregate, are served by the shard replicas
            Thread.sleep(5000); // replicas sampled, read-after-write window passed
            long before = routedReads(warp, r1, r2);
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                for (int i = 0; i < 6; i++) {
                    try (ResultSet rs = st.executeQuery("select sum(amount) from orders")) {
                        rs.next();
                        assertEquals(40950, rs.getLong(1), "scatter-gather SUM across both shards (rows 1..90)");
                    }
                }
            }
            long after = routedReads(warp, r1, r2);
            assertTrue(after > before, "reads were served by the shard replicas (routed reads " + before + " -> " + after + ")");

            // 3. crash shard 1's primary under write load: only shard 1 fails over
            AtomicBoolean stop = new AtomicBoolean();
            Set<Long> loadAcked = java.util.concurrent.ConcurrentHashMap.newKeySet();
            long[] failures = {0};
            Thread load = new Thread(() -> {
                long id = 1000;
                while (!stop.get()) {
                    id++;
                    try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                        st.executeUpdate("insert into orders (id, customer_id, amount) values (" + id + ", " + id + ", 1)");
                        loadAcked.add(id);
                    } catch (Exception e) {
                        failures[0]++;
                    }
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }, "shard-load");
            load.start();
            Thread.sleep(3000);
            p1.crash();
            waitFor("shard 1's replica to be promoted", 60, r1::writable);
            assertTrue(p2.writable(), "shard 2's primary is untouched");
            Thread.sleep(6000);

            // 4. planned switchover of shard 2 under the same load
            String res = BrownoutHarness.http("POST", "http://localhost:" + warp.metricsPort() + "/api/failover/s2/switchover",
                    "{\"target\":\"" + r2.url() + "\"}");
            assertTrue(res.contains("\"ok\":true"), res);
            Thread.sleep(6000);
            stop.set(true);
            load.join(20_000);

            // 5. nothing acknowledged was lost or duplicated
            acked.addAll(loadAcked);
            Set<Long> present = new HashSet<>(ids(r1));
            present.addAll(ids(r2));
            Set<Long> lost = new HashSet<>(acked);
            lost.removeAll(present);
            long rows = count(r1) + count(r2);
            System.out.println("SHARDED-RESULT " + engine + " | acked " + acked.size() + " | present rows " + rows + " | lost " + lost.size()
                    + " | failed writes during the events " + failures[0] + " | shard1 now " + (r1.writable() ? "former replica" : "?")
                    + " | shard2 now " + (r2.writable() ? "former replica" : "?"));
            assertEquals(Set.of(), lost, "no acknowledged write is missing from the shards' current primaries");
            assertEquals(present.size(), rows, "no id exists twice");
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement();
                    ResultSet rs = st.executeQuery("select count(*) from orders")) {
                rs.next();
                assertEquals(rows, rs.getLong(1), "count through Warp equals what the shards hold");
            }
            // 6. a broadcast write that fails on one shard after another applied it says so (shard 2 has no usable primary left now)
            r2.crash();
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                var partial = org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                        () -> st.executeUpdate("update orders set amount = amount"));
                System.out.println("SHARDED-NOTE " + engine + " partial broadcast failure: " + partial.getMessage().replaceAll("\\s+", " "));
                assertTrue(partial.getMessage().contains("had already applied this statement when shard \"s2\" failed"), partial.getMessage());
            }
        }
    }
}
