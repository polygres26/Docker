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

    private static long count(LocalPostgres p) throws Exception {
        try (Connection c = p.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static Set<Long> ids(LocalPostgres p) throws Exception {
        Set<Long> out = new HashSet<>();
        try (Connection c = p.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from orders")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    private static double spread(LocalPostgres[] pg) throws Exception {
        long min = Long.MAX_VALUE;
        long max = 0;
        long total = 0;
        for (LocalPostgres p : pg) {
            long n = count(p);
            min = Math.min(min, n);
            max = Math.max(max, n);
            total += n;
        }
        return (max - min) / ((double) total / pg.length);
    }

    private void run(String threshold, boolean expectBalanced, boolean pause) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("autoreb");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres[] pg = new LocalPostgres[3];
        try {
            for (int i = 0; i < 3; i++) {
                pg[i] = LocalPostgres.primary(bin, dir, "s" + i, LocalPostgres.freePort());
                try (Connection c = pg[i].conn(); Statement st = c.createStatement()) {
                    st.execute("create table orders (id int primary key, customer_id int, amount int)");
                }
            }
            String spec = "default=" + cfg.url() + "|warp|secret;s1=" + pg[0].url() + "|warp|secret;s2=" + pg[1].url() + "|warp|secret;s3=" + pg[2].url() + "|warp|secret";
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
                assertEquals(0, count(pg[2]));
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
                    if (expectBalanced && spread(pg) <= 0.25) {
                        break;
                    }
                }
                String status = BrownoutHarness.http("GET", admin + "/api/sharding", null);
                System.out.println("AUTO-NOTE threshold=" + threshold + " pause=" + pause + " rows " + count(pg[0]) + "/" + count(pg[1]) + "/" + count(pg[2])
                        + " status " + status.substring(status.indexOf("\"balancer\""), Math.min(status.length(), status.indexOf("\"balancer\"") + 330)));
                if (expectBalanced) {
                    assertTrue(spread(pg) <= 0.25, "the shards are within the threshold: spread " + spread(pg));
                    assertTrue(count(pg[2]) > 2000, "the new shard was filled");
                } else {
                    assertEquals(0, count(pg[2]), "nothing moved");
                }
                Thread.sleep(8000); // and it stays put once balanced
                String mapAfter = BrownoutHarness.http("GET", admin + "/api/sharding", null);
                stop.set(true);
                for (Thread t : writers) {
                    t.join(20_000);
                }
                Set<Long> everything = new HashSet<>();
                long rows = 0;
                for (LocalPostgres p : pg) {
                    Set<Long> here = ids(p);
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
                    assertTrue(spread(pg) <= 0.30, "still balanced at the end");
                }
                assertTrue(mapAfter.contains("slotsPerShard"));
            }
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
    void aNewEmptyShardFillsByItselfWhileWritesContinue() throws Exception {
        run("0.2", true, false);
    }

    @Test
    void nothingMovesWhenTheImbalanceIsWithinTheThreshold() throws Exception {
        run("5.0", false, false);
    }

    @Test
    void nothingMovesWhileTheBalancerIsPaused() throws Exception {
        run("0.2", false, true);
    }
}
