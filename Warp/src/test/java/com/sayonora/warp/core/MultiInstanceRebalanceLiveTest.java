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
 * Rebalancing with two Warp instances sharing one control plane and one set of shards: writers and a scatter counter run through BOTH instances
 * while slots move, and the cluster-wide hold keeps every acknowledged write exactly once. A second case pauses one instance so it cannot
 * acknowledge: the move is abandoned, nothing moves, and writes resume. Opt-in: WARP_TEST_BROWNOUT_PG_BIN.
 */
class MultiInstanceRebalanceLiveTest {

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

    private static WarpProcess warp(LocalPostgres cfg, LocalPostgres[] pg) throws Exception {
        String spec = "default=" + cfg.url() + "|warp|secret;s1=" + pg[0].url() + "|warp|secret;s2=" + pg[1].url() + "|warp|secret;s3=" + pg[2].url() + "|warp|secret";
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret");
        BrownoutHarness.separatePorts(b); // first, so the pgwire port chosen below is the one that stays
        b.frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63")
                .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                .env("WARP_RESHARD_ACK_SECONDS", "8")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled");
        return b.start();
    }

    private void run(boolean pauseOne) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("multireshard");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres[] pg = new LocalPostgres[3];
        WarpProcess a = null;
        WarpProcess b = null;
        try {
            for (int i = 0; i < 3; i++) {
                pg[i] = LocalPostgres.primary(bin, dir, "s" + i, LocalPostgres.freePort());
                try (Connection c = pg[i].conn(); Statement st = c.createStatement()) {
                    st.execute("create table orders (id int primary key, customer_id int, amount int)");
                }
            }
            a = warp(cfg, pg);
            b = warp(cfg, pg);
            Thread.sleep(3000); // both registered in warp_nodes
            String[] urls = {"jdbc:postgresql://localhost:" + a.port("pgwire") + "/postgres", "jdbc:postgresql://localhost:" + b.port("pgwire") + "/postgres"};
            String adminA = "http://localhost:" + a.metricsPort();
            Set<Long> acked = ConcurrentHashMap.newKeySet();
            try (Connection c = DriverManager.getConnection(urls[0], "warp", "secret"); Statement st = c.createStatement()) {
                for (int from = 1; from <= 10_000; from += 500) {
                    StringBuilder v = new StringBuilder();
                    for (int i = from; i < from + 500; i++) {
                        v.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", 1)");
                        acked.add((long) i);
                    }
                    st.executeUpdate("insert into orders (id, customer_id, amount) values " + v);
                }
            }
            assertTrue(BrownoutHarness.http("POST", adminA + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}").contains("s3="));
            Thread.sleep(1500); // the other instance applies the config change

            AtomicBoolean stop = new AtomicBoolean();
            AtomicLong next = new AtomicLong(100_000);
            AtomicLong failed = new AtomicLong();
            AtomicLong wrongCounts = new AtomicLong();
            AtomicLong countChecks = new AtomicLong();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < 4; w++) {
                String url = urls[w % 2];
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
                            Thread.sleep(5);
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                });
                t.start();
                threads.add(t);
            }
            for (String url : urls) {
                Thread t = new Thread(() -> {
                    try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                        while (!stop.get()) {
                            try (ResultSet rs = st.executeQuery("select count(*) from orders where id <= 10000")) {
                                rs.next();
                                countChecks.incrementAndGet();
                                if (rs.getLong(1) != 10_000) {
                                    wrongCounts.incrementAndGet();
                                    System.out.println("MULTI-NOTE scatter count saw " + rs.getLong(1));
                                }
                            }
                            Thread.sleep(10);
                        }
                    } catch (Exception e) {
                        wrongCounts.incrementAndGet();
                        System.out.println("MULTI-NOTE counter failed: " + e);
                    }
                });
                t.start();
                threads.add(t);
            }
            Thread.sleep(1500);
            long beforeS3 = count(pg[2]);
            if (pauseOne) {
                b.pause();
            }
            long t0 = System.currentTimeMillis();
            String res = BrownoutHarness.http("POST", adminA + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}");
            System.out.println("MULTI-NOTE pauseOne=" + pauseOne + " rebalance took " + (System.currentTimeMillis() - t0) + " ms: " + res.substring(0, Math.min(400, res.length())));
            if (pauseOne) {
                assertTrue(res.contains("did not acknowledge"), res);
                assertEquals(beforeS3, count(pg[2]), "nothing was moved");
                b.resume();
                Thread.sleep(3000);
                assertTrue(res.contains("nothing was moved"));
            } else {
                assertTrue(res.contains("\"rowsCopied\""), res);
                assertTrue(count(pg[2]) > 0);
                // a second move from the other instance's admin port, now with the target already an owner
                res = BrownoutHarness.http("POST", "http://localhost:" + b.metricsPort() + "/api/sharding/rebalance",
                        "{\"table\":\"orders\",\"to\":\"s1\",\"slots\":[21,22,23,24,25]}");
                System.out.println("MULTI-NOTE second move via instance B: " + res.substring(0, Math.min(300, res.length())));
                assertTrue(res.contains("\"rowsCopied\""), res);
            }
            Thread.sleep(1500);
            stop.set(true);
            for (Thread t : threads) {
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
            System.out.println("MULTI-RESULT pauseOne=" + pauseOne + " | acked " + acked.size() + " | rows " + rows + " | lost " + lost.size() + " | failed writes " + failed.get()
                    + " | scatter checks " + countChecks.get() + " wrong " + wrongCounts.get());
            assertEquals(rows, everything.size(), "no id on two shards");
            assertEquals(Set.of(), lost, "no acknowledged write is missing");
            assertEquals(0, wrongCounts.get(), "scatter reads through either instance were always exact");
            if (!pauseOne) {
                assertEquals(0, failed.get(), "writes through either instance waited instead of failing");
            }
        } finally {
            if (b != null) {
                b.close();
            }
            if (a != null) {
                a.close();
            }
            for (LocalPostgres p : pg) {
                if (p != null) {
                    p.stop("immediate");
                }
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void twoInstancesRebalanceWhileBothTakeWrites() throws Exception {
        run(false);
    }

    @Test
    void anInstanceThatCannotAcknowledgeAbandonsTheMoveAndNothingMoves() throws Exception {
        run(true);
    }
}
