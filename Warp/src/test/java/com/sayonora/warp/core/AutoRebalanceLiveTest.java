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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A new empty shard fills by itself: the balancer notices the imbalance, moves slots in passes while writes continue, stops once the shards are
 * within the threshold, and does nothing when the threshold is not exceeded or when paused. Opt-in: WARP_TEST_BROWNOUT_PG_BIN.
 */
class AutoRebalanceLiveTest {

    private static double spread(ShardCluster c) throws Exception {
        long min = Long.MAX_VALUE;
        long max = 0;
        long total = 0;
        for (int i = 0; i < 3; i++) {
            long n = c.count(i);
            min = Math.min(min, n);
            max = Math.max(max, n);
            total += n;
        }
        return (max - min) / ((double) total / 3);
    }

    private void run(String engine, String threshold, boolean expectBalanced, boolean pause) throws Exception {
        Assumptions.assumeTrue(ShardCluster.available(engine), "engine " + engine + " is not available here");
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("autoreb");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        try (ShardCluster cluster = ShardCluster.start(engine, dir)) {
            for (int i = 0; i < 3; i++) {
                cluster.createOrders(i);
            }
            String spec = "default=" + cfg.url() + "|warp|secret;" + cluster.shard(0).backendSpec("s1") + ";" + cluster.shard(1).backendSpec("s2") + ";"
                    + cluster.shard(2).backendSpec("s3");
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", spec)
                    .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63;s3=")
                    .env("WARP_AUTO_REBALANCE", "true")
                    .env("WARP_AUTO_REBALANCE_INTERVAL_SECONDS", "3")
                    .env("WARP_AUTO_REBALANCE_THRESHOLD", threshold)
                    .env("WARP_AUTO_REBALANCE_COOLDOWN_SECONDS", "1")
                    .env("WARP_AUTO_REBALANCE_MAX_SLOTS_PER_RUN", "12")
                    .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                String admin = "http://localhost:" + warp.metricsPort();
                if (pause) {
                    assertTrue(BrownoutHarness.http("POST", admin + "/api/sharding/balancer", "{\"enabled\":false}").contains("\"enabled\":false"));
                }
                Set<Long> acked = ConcurrentHashMap.newKeySet();
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    for (int from = 1; from <= 12_000; from += 500) {
                        StringBuilder v = new StringBuilder();
                        for (int i = from; i < from + 500; i++) {
                            v.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", 1)");
                            acked.add((long) i);
                        }
                        st.executeUpdate("insert into orders (id, customer_id, amount) values " + v);
                    }
                }
                assertEquals(0, cluster.count(2));
                AtomicBoolean stop = new AtomicBoolean();
                AtomicLong next = new AtomicLong(100_000);
                AtomicLong failed = new AtomicLong();
                List<Thread> writers = new ArrayList<>();
                for (int w = 0; w < 2; w++) {
                    Thread t = new Thread(() -> {
                        try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                            while (!stop.get()) {
                                long id = next.incrementAndGet();
                                try {
                                    st.executeUpdate("insert into orders (id, customer_id, amount) values (" + id + ", " + id + ", 1)");
                                    acked.add(id);
                                } catch (java.sql.SQLException e) {
                                    failed.incrementAndGet();
                                }
                                Thread.sleep(10);
                            }
                        } catch (Exception e) {
                            failed.incrementAndGet();
                        }
                    });
                    t.start();
                    writers.add(t);
                }
                long deadline = System.currentTimeMillis() + (expectBalanced ? 90_000 : 12_000);
                while (System.currentTimeMillis() < deadline) {
                    Thread.sleep(1000);
                    if (expectBalanced && spread(cluster) <= 0.25) {
                        break;
                    }
                }
                String status = BrownoutHarness.http("GET", admin + "/api/sharding", null);
                System.out.println("AUTO-NOTE threshold=" + threshold + " pause=" + pause + " rows " + cluster.count(0) + "/" + cluster.count(1) + "/" + cluster.count(2)
                        + " status " + status.substring(status.indexOf("\"balancer\""), Math.min(status.length(), status.indexOf("\"balancer\"") + 330)));
                if (expectBalanced) {
                    assertTrue(spread(cluster) <= 0.25, "the shards are within the threshold: spread " + spread(cluster));
                    assertTrue(cluster.count(2) > 2000, "the new shard was filled");
                } else {
                    assertEquals(0, cluster.count(2), "nothing moved");
                }
                Thread.sleep(8000); // and it stays put once balanced
                String mapAfter = BrownoutHarness.http("GET", admin + "/api/sharding", null);
                stop.set(true);
                for (Thread t : writers) {
                    t.join(20_000);
                }
                Set<Long> everything = new HashSet<>();
                long rows = 0;
                for (int i = 0; i < 3; i++) {
                    Set<Long> here = cluster.ids(i);
                    rows += here.size();
                    everything.addAll(here);
                }
                Set<Long> lost = new HashSet<>(acked);
                lost.removeAll(everything);
                System.out.println("AUTO-RESULT threshold=" + threshold + " | acked " + acked.size() + " | rows " + rows + " | lost " + lost.size() + " | failed " + failed.get());
                assertEquals(rows, everything.size());
                assertEquals(Set.of(), lost);
                assertEquals(0, failed.get());
                if (expectBalanced) {
                    assertTrue(spread(cluster) <= 0.30, "still balanced at the end");
                }
                assertTrue(mapAfter.contains("slotsPerShard"));
            }
        } finally {
            cfg.stop("immediate");
        }
    }

    @Test
    void postgresANewEmptyShardFillsByItselfWhileWritesContinue() throws Exception {
        run("postgres", "0.2", true, false);
    }

    @Test
    void mysqlANewEmptyShardFillsByItselfWhileWritesContinue() throws Exception {
        run("mysql", "0.2", true, false);
    }

    @Test
    void sqlServerANewEmptyShardFillsByItselfWhileWritesContinue() throws Exception {
        run("mssql", "0.2", true, false);
    }

    @Test
    void oracleANewEmptyShardFillsByItselfWhileWritesContinue() throws Exception {
        run("oracle", "0.2", true, false);
    }

    @Test
    void postgresNothingMovesWhenTheImbalanceIsWithinTheThresholdOrWhilePaused() throws Exception {
        run("postgres", "5.0", false, false);
        run("postgres", "0.2", false, true);
    }
}
