package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.ShardCluster;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** An existing hash-sharded table is converted to slots without moving a row, then takes a third shard. Opt-in: WARP_TEST_BROWNOUT_PG_BIN. */
class HashToSlotsLiveTest {

    private void run(String engine) throws Exception {
        Assumptions.assumeTrue(ShardCluster.available(engine), "engine " + engine + " is not available here");
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("hash2slots");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        try (ShardCluster cluster = ShardCluster.start(engine, dir)) {
            cluster.createOrders(0); // the third shard gets its table only when it is about to join
            cluster.createOrders(1);
            String spec = "default=" + cfg.url() + "|warp|secret;" + cluster.shard(0).backendSpec("s1") + ";" + cluster.shard(1).backendSpec("s2") + ";"
                    + cluster.shard(2).backendSpec("s3");
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", spec)
                    .env("WARP_TABLE_SHARDS", "orders:hash:customer_id:s1,s2")
                    .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                String admin = "http://localhost:" + warp.metricsPort();
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    for (int from = 1; from <= 5000; from += 500) {
                        StringBuilder v = new StringBuilder();
                        for (int i = from; i < from + 500; i++) {
                            v.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", 1)");
                        }
                        st.executeUpdate("insert into orders (id, customer_id, amount) values " + v);
                    }
                }
                long before1 = cluster.count(0);
                long before2 = cluster.count(1);
                assertEquals(5000, before1 + before2);

                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = 5")) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "keyed read works on the hash table before the conversion");
                }
                String refusedRebalance = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":10}");
                assertTrue(refusedRebalance.contains("slots strategy"), "a hash table cannot be rebalanced yet: " + refusedRebalance);

                String res = BrownoutHarness.http("POST", admin + "/api/sharding/convert", "{\"table\":\"orders\"}");
                System.out.println("HASH2SLOTS-NOTE " + res);
                assertTrue(res.contains("\"rowsMoved\":0") && res.contains("stripe:s1,s2"), res);
                assertEquals(before1, cluster.count(0), "no row moved");
                assertEquals(before2, cluster.count(1));
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    for (int i = 1; i <= 5000; i += 37) {
                        try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = " + i)) {
                            rs.next();
                            assertEquals(1, rs.getInt(1), "customer " + i + " is still found by its key");
                        }
                    }
                    assertEquals(1, st.executeUpdate("insert into orders (id, customer_id, amount) values (900001, 900001, 1)"));
                }

                cluster.createOrders(2);
                // the table now exists on a backend that is not part of the group: until it joins, queries on it are ambiguous
                res = BrownoutHarness.http("POST", admin + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}");
                assertTrue(res.contains("s3="), res);
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement();
                        ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = 5")) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "keyed reads work with an empty member in the group");
                }
                res = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":300}");
                System.out.println("HASH2SLOTS-NOTE " + res);
                assertTrue(res.contains("\"rowsCopied\""), res);
                assertEquals(5001, cluster.count(0) + cluster.count(1) + cluster.count(2));
                assertTrue(cluster.count(2) > 0);
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    for (int i = 1; i <= 5000; i += 41) {
                        try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = " + i)) {
                            rs.next();
                            assertEquals(1, rs.getInt(1), "customer " + i + " found after the rebalance");
                        }
                    }
                    try (ResultSet rs = st.executeQuery("select count(*) from orders")) {
                        rs.next();
                        assertEquals(5001, rs.getInt(1));
                    }
                }
            }
        } finally {
            cfg.stop("immediate");
        }
    }

    @Test
    void postgresAHashTableConvertsInPlaceAndThenRebalances() throws Exception {
        run("postgres");
    }

    @Test
    void mysqlAHashTableConvertsInPlaceAndThenRebalances() throws Exception {
        run("mysql");
    }

    @Test
    void sqlServerAHashTableConvertsInPlaceAndThenRebalances() throws Exception {
        run("mssql");
    }

    @Test
    void oracleAHashTableConvertsInPlaceAndThenRebalances() throws Exception {
        run("oracle");
    }
}
