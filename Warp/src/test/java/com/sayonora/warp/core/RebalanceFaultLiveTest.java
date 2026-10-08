package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonParser;
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
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A slot move that fails, or whose Warp is killed, at each stage leaves the table consistent: every row on the shard that owns its slot under the
 * map in force, no staging table, nothing lost, nothing doubled -- immediately for a failure Warp handles itself, within about a minute for a Warp
 * that was halted (a restarted instance finishes the abandoned move), and the table can be moved again afterwards. Faults come from
 * WARP_FAULT_AT / WARP_FAULT_MODE (see FaultPoints). Opt-in like the other sharding live tests.
 */
class RebalanceFaultLiveTest {

    private static final int ROWS = 6000;

    private static WarpProcess warp(LocalPostgres cfg, ShardCluster cluster, String faultAt, String faultMode) throws Exception {
        String spec = "default=" + cfg.url() + "|warp|secret;" + cluster.shard(0).backendSpec("s1") + ";" + cluster.shard(1).backendSpec("s2") + ";"
                + cluster.shard(2).backendSpec("s3");
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret");
        BrownoutHarness.separatePorts(b);
        b.frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec)
                .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63;s3=")
                .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                .env("WARP_RESHARD_LEASE_SECONDS", "6")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled");
        if (faultAt != null) {
            b.env("WARP_FAULT_AT", faultAt).env("WARP_FAULT_MODE", faultMode);
        }
        return b.start();
    }

    /** Every row sits on the shard that owns its slot in the map Warp reports, no staging table is left, and exactly the expected ids exist. */
    private static String inconsistency(WarpProcess w, ShardCluster cluster, Set<Long> expected) throws Exception {
        String map = JsonParser.parseString(BrownoutHarness.http("GET", "http://localhost:" + w.metricsPort() + "/api/sharding", null)).getAsJsonObject()
                .getAsJsonArray("tables").get(0).getAsJsonObject().get("map").getAsString();
        ShardingStrategy.SlotStrategy m = ShardingStrategy.SlotStrategy.parse(map);
        String[] names = {"s1", "s2", "s3"};
        Set<Long> all = new HashSet<>();
        long rows = 0;
        for (int i = 0; i < 3; i++) {
            if (cluster.stagingExists(i)) {
                return "staging table left on " + names[i];
            }
            for (long id : cluster.ids(i)) {
                rows++;
                all.add(id);
                if (!names[i].equals(m.resolve(String.valueOf(id)))) {
                    return "id " + id + " is on " + names[i] + " but the map (" + map + ") gives it to " + m.resolve(String.valueOf(id));
                }
            }
        }
        if (rows != all.size()) {
            return (rows - all.size()) + " ids exist twice";
        }
        if (!all.equals(expected)) {
            return "rows differ from what was acknowledged: " + all.size() + " present, " + expected.size() + " expected";
        }
        return null;
    }

    private void run(String engine, String point, String mode) throws Exception {
        Assumptions.assumeTrue(ShardCluster.available(engine), "engine " + engine + " is not available here");
        Path dir = Files.createTempDirectory("rebfault");
        LocalPostgres cfg = LocalPostgres.primary(System.getenv("WARP_TEST_BROWNOUT_PG_BIN"), dir, "cfg", LocalPostgres.freePort());
        WarpProcess w = null;
        try (ShardCluster cluster = ShardCluster.start(engine, dir)) {
            for (int i = 0; i < 3; i++) {
                cluster.createOrders(i);
            }
            w = warp(cfg, cluster, point, mode);
            String url = "jdbc:postgresql://localhost:" + w.port("pgwire") + "/postgres";
            Set<Long> expected = new HashSet<>();
            try (Connection c = DriverManager.getConnection(url, "warp", "secret"); Statement st = c.createStatement()) {
                for (int from = 1; from <= ROWS; from += 500) {
                    StringBuilder v = new StringBuilder();
                    for (int i = from; i < from + 500; i++) {
                        v.append(i == from ? "" : ", ").append("(").append(i).append(", ").append(i).append(", 1)");
                        expected.add((long) i);
                    }
                    st.executeUpdate("insert into orders (id, customer_id, amount) values " + v);
                }
            }
            assertTrue(BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}").contains("s3="));
            String res;
            try {
                res = BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}");
            } catch (Exception connectionLost) {
                res = "connection lost: " + connectionLost;
            }
            System.out.println("FAULT-NOTE " + engine + " " + point + "/" + mode + ": " + res.substring(0, Math.min(260, res.length())));
            long t0 = System.currentTimeMillis();
            if (mode.equals("halt")) {
                assertTrue(res.startsWith("connection lost"), "the instance died: " + res);
                w.close();
                w = warp(cfg, cluster, null, null); // a fresh instance, no fault; it finds the abandoned hold and finishes the job
                String problem = inconsistency(w, cluster, expected);
                long deadline = System.currentTimeMillis() + 120_000;
                while (problem != null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(2000);
                    problem = inconsistency(w, cluster, expected);
                }
                System.out.println("FAULT-NOTE " + engine + " " + point + "/halt: consistent again " + (System.currentTimeMillis() - t0) / 1000 + " s after the kill, problem=" + problem);
                assertEquals(null, problem, "the restarted instance repaired the table");
            } else {
                assertTrue(res.contains("injected fault") || res.contains("warning"), res);
                boolean afterSwitch = point.equals("after-switch") || point.equals("mid-purge");
                String problem = inconsistency(w, cluster, expected);
                if (afterSwitch) {
                    assertTrue(problem != null, "after the switch the old copies are still there until they are reconciled");
                    w.close(); // the same fault would fire again inside this instance's reconcile
                    w = warp(cfg, cluster, null, null);
                    // the failed move left its hold for the recovery loop, so a manual reconcile is refused until that lapses; either way the table gets repaired
                    long until = System.currentTimeMillis() + 90_000;
                    while (problem != null && System.currentTimeMillis() < until) {
                        BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/sharding/reconcile", "{\"table\":\"orders\"}");
                        Thread.sleep(3000);
                        problem = inconsistency(w, cluster, expected);
                    }
                }
                assertEquals(null, problem, "consistent after " + point);
            }
            // and the table can be moved again
            if (!mode.equals("halt") && !(point.equals("after-switch") || point.equals("mid-purge"))) {
                w.close(); // the faulty instance would fault again
                w = warp(cfg, cluster, null, null);
            }
            String again = BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}");
            assertTrue(again.contains("\"rowsCopied\""), again);
            assertEquals(null, inconsistency(w, cluster, expected), "consistent after a clean move");
            try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:" + w.port("pgwire") + "/postgres", "warp", "secret");
                    Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
                rs.next();
                assertEquals(ROWS, rs.getInt(1), "a scatter count through Warp");
            }
            assertFalse(cluster.stagingExists(2));
        } finally {
            if (w != null) {
                w.close();
            }
            cfg.stop("immediate");
        }
    }

    private static final String[] POINTS = {"copy-mid", "after-verify", "after-publish", "before-switch", "after-switch", "mid-purge"};

    @Test
    void postgresAFailureAtEachStageLeavesTheTableConsistent() throws Exception {
        for (String point : POINTS) {
            run("postgres", point, "throw");
        }
    }

    @Test
    void postgresAKilledWarpIsRepairedByARestartedInstance() throws Exception {
        for (String point : POINTS) {
            run("postgres", point, "halt");
        }
    }

    @Test
    void postgresKilledMidPurge() throws Exception {
        run("postgres", "mid-purge", "halt");
    }

    @Test
    void mysqlFailuresAndKills() throws Exception {
        run("mysql", "after-publish", "throw");
        run("mysql", "after-publish", "halt");
        run("mysql", "mid-purge", "halt");
    }

    @Test
    void sqlServerKilledAfterThePublish() throws Exception {
        run("mssql", "after-publish", "halt");
    }

    @Test
    void oracleKilledAfterThePublish() throws Exception {
        run("oracle", "after-publish", "halt");
    }
}
