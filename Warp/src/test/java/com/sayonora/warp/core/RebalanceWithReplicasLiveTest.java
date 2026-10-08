package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonParser;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Slot moves on shards that have read replicas. Reads are served by the replicas; the target shard's replica is held back (replay paused) for a
 * while right before the move. The rebalance must wait for it before the map switches, so keyed and scatter reads served by replicas never miss or
 * double a moved row. A second case starts a move while one shard's primary is down and expects a refusal. Opt-in: WARP_TEST_BROWNOUT_PG_BIN.
 */
class RebalanceWithReplicasLiveTest {

    private static void exec(LocalPostgres pg, String sql) throws Exception {
        try (Connection c = pg.conn(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long routedReads(WarpProcess warp, LocalPostgres... replicas) throws Exception {
        String json = BrownoutHarness.http("GET", "http://localhost:" + warp.metricsPort() + "/api/replicas", null);
        long total = 0;
        for (var p : JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("primaries")) {
            for (var r : p.getAsJsonObject().getAsJsonArray("replicas")) {
                for (LocalPostgres rep : replicas) {
                    if (r.getAsJsonObject().get("url").getAsString().contains(":" + rep.port() + "/")) {
                        total += r.getAsJsonObject().get("routedReads").getAsLong();
                    }
                }
            }
        }
        return total;
    }

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

    private void run(boolean primaryDown) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("rebreplicas");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres[] p = new LocalPostgres[3];
        LocalPostgres[] r = new LocalPostgres[3];
        try {
            for (int i = 0; i < 3; i++) {
                p[i] = LocalPostgres.primary(bin, dir, "p" + i, LocalPostgres.freePort());
                exec(p[i], "create table orders (id int primary key, customer_id int, amount int)");
                r[i] = LocalPostgres.replicaOf(bin, dir, "r" + i, p[i], LocalPostgres.freePort());
            }
            String spec = "default=" + cfg.url() + "|warp|secret";
            for (int i = 0; i < 3; i++) {
                spec += ";s" + (i + 1) + "=" + p[i].url() + "|warp|secret||" + r[i].url() + "~120";
            }
            try (WarpProcess warp = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", spec)
                    .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63;s3=")
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_READ_AFTER_WRITE_WINDOW_MS", "200")
                    .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                String admin = "http://localhost:" + warp.metricsPort();
                Set<Long> acked = ConcurrentHashMap.newKeySet();
                try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                    for (int from = 1; from <= 10_000; from += 500) {
                        StringBuilder v = new StringBuilder();
                        for (int i = from; i < from + 500; i++) {
                            v.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", 1)");
                            acked.add((long) i);
                        }
                        st.executeUpdate("insert into orders (id, customer_id, amount) values " + v);
                    }
                }
                assertTrue(BrownoutHarness.http("POST", admin + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}").contains("s3="));
                Thread.sleep(6000); // replicas have the rows and are sampled eligible

                if (primaryDown) {
                    p[0].stop("immediate");
                    String res = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":10}");
                    System.out.println("REBREP-NOTE primary down: " + res);
                    assertTrue(res.contains("not accepting writes") || res.contains("error"), res);
                    assertEquals(0, count(p[2]), "nothing moved");
                    return;
                }

                AtomicBoolean stop = new AtomicBoolean();
                AtomicLong next = new AtomicLong(100_000);
                AtomicLong failed = new AtomicLong();
                AtomicLong wrongKeyed = new AtomicLong();
                AtomicLong wrongScatter = new AtomicLong();
                AtomicLong reads = new AtomicLong();
                long routedBefore = routedReads(warp, r);
                List<Thread> threads = new ArrayList<>();
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
                    threads.add(t);
                }
                for (int rd = 0; rd < 3; rd++) {
                    Thread t = new Thread(() -> {
                        try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                            while (!stop.get()) {
                                int key = 1 + ThreadLocalRandom.current().nextInt(10_000);
                                try (ResultSet rs = st.executeQuery("select count(*) from orders where customer_id = " + key)) {
                                    rs.next();
                                    reads.incrementAndGet();
                                    if (rs.getInt(1) != 1) {
                                        wrongKeyed.incrementAndGet();
                                        System.out.println("REBREP-NOTE keyed read of " + key + " returned " + rs.getInt(1));
                                    }
                                }
                                if (reads.get() % 20 == 0) {
                                    try (ResultSet rs = st.executeQuery("select count(*) from orders where id <= 10000")) {
                                        rs.next();
                                        if (rs.getLong(1) != 10_000) {
                                            wrongScatter.incrementAndGet();
                                            System.out.println("REBREP-NOTE scatter count saw " + rs.getLong(1));
                                        }
                                    }
                                }
                                Thread.sleep(3);
                            }
                        } catch (Exception e) {
                            wrongKeyed.incrementAndGet();
                            System.out.println("REBREP-NOTE reader failed: " + e);
                        }
                    });
                    t.start();
                    threads.add(t);
                }
                Thread.sleep(2000);
                assertTrue(routedReads(warp, r) > routedBefore, "reads are being served by the replicas");

                // the target's replica falls behind for 6 s right before the move
                exec(r[2], "select pg_wal_replay_pause()");
                Thread resume = new Thread(() -> {
                    try {
                        Thread.sleep(6000);
                        exec(r[2], "select pg_wal_replay_resume()");
                    } catch (Exception ignored) {
                        // the test fails on the numbers below
                    }
                });
                resume.start();
                long t0 = System.currentTimeMillis();
                String res = BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}");
                long took = System.currentTimeMillis() - t0;
                System.out.println("REBREP-NOTE rebalance took " + took + " ms: " + res.substring(0, Math.min(500, res.length())));
                assertTrue(res.contains("\"rowsCopied\""), res);
                assertTrue(took > 4000, "the move waited for the target's lagging replica (" + took + " ms)");
                Thread.sleep(2000);
                stop.set(true);
                for (Thread t : threads) {
                    t.join(20_000);
                }
                resume.join(10_000);
                Set<Long> everything = new HashSet<>();
                long rows = 0;
                for (LocalPostgres pg : p) {
                    Set<Long> here = ids(pg);
                    rows += here.size();
                    everything.addAll(here);
                }
                Set<Long> lost = new HashSet<>(acked);
                lost.removeAll(everything);
                System.out.println("REBREP-RESULT acked " + acked.size() + " | rows " + rows + " | lost " + lost.size() + " | failed " + failed.get() + " | reads " + reads.get()
                        + " | wrong keyed " + wrongKeyed.get() + " | wrong scatter " + wrongScatter.get() + " | replica reads +" + (routedReads(warp, r) - routedBefore));
                assertEquals(rows, everything.size());
                assertEquals(Set.of(), lost);
                assertEquals(0, failed.get());
                assertEquals(0, wrongKeyed.get(), "a keyed read served by a replica never missed or doubled a moved row");
                assertEquals(0, wrongScatter.get());
            }
        } finally {
            for (LocalPostgres x : r) {
                if (x != null) {
                    x.stop("immediate");
                }
            }
            for (LocalPostgres x : p) {
                if (x != null) {
                    x.stop("immediate");
                }
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void readsServedByReplicasStayCorrectWhileSlotsMoveAndTheTargetsReplicaLags() throws Exception {
        run(false);
    }

    @Test
    void aMoveIsRefusedWhileAShardsPrimaryIsDown() throws Exception {
        run(true);
    }
}
