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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A shard's primary dies while a slot move is stopped at a chosen point (WARP_FAULT_MODE=sleep, see FaultPoints) and its replica is promoted by
 * Warp. Whatever the move does -- fail on the changed primary, fail on the dead target, or finish its cleanup on the promoted node -- the table
 * ends consistent (every row on the shard that owns its slot, nothing lost or doubled, no staging table) and can be moved again. Postgres, each
 * shard with a replica in promote mode. Opt-in: WARP_TEST_BROWNOUT_PG_BIN.
 */
class RebalanceFailoverLiveTest {

    private static final int ROWS = 6000;

    private static WarpProcess warp(LocalPostgres cfg, LocalPostgres[] p, LocalPostgres[] r, String faultAt) throws Exception {
        StringBuilder spec = new StringBuilder("default=" + cfg.url() + "|warp|secret");
        for (int i = 0; i < 3; i++) {
            spec.append(";s").append(i + 1).append("=").append(p[i].url()).append("|warp|secret||").append(r[i].url()).append("~120|promote");
        }
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret");
        BrownoutHarness.separatePorts(b);
        b.frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", spec.toString())
                .env("WARP_TABLE_SHARDS", "orders:slots:customer_id:64/s1=0-31;s2=32-63;s3=")
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                .env("WARP_RESHARD_LEASE_SECONDS", "8")
                .env("WARP_QOS_RATE_PER_SEC", "1000000").env("WARP_QOS_BURST", "1000000")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled");
        if (faultAt != null) {
            b.env("WARP_FAULT_AT", faultAt).env("WARP_FAULT_MODE", "sleep").env("WARP_FAULT_SLEEP_MS", "40000");
        }
        return b.start();
    }

    private static Set<Long> ids(LocalPostgres n) throws Exception {
        Set<Long> out = new HashSet<>();
        try (Connection c = n.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select id from orders")) {
            while (rs.next()) {
                out.add(rs.getLong(1));
            }
        }
        return out;
    }

    private static boolean stagingExists(LocalPostgres n) {
        try (Connection c = n.conn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders__rebal")) {
            return rs.next();
        } catch (Exception absent) {
            return false;
        }
    }

    private static String inconsistency(WarpProcess w, LocalPostgres[] live, Set<Long> expected) throws Exception {
        String map = JsonParser.parseString(BrownoutHarness.http("GET", "http://localhost:" + w.metricsPort() + "/api/sharding", null)).getAsJsonObject()
                .getAsJsonArray("tables").get(0).getAsJsonObject().get("map").getAsString();
        ShardingStrategy.SlotStrategy m = ShardingStrategy.SlotStrategy.parse(map);
        Set<Long> all = new HashSet<>();
        long rows = 0;
        for (int i = 0; i < 3; i++) {
            if (stagingExists(live[i])) {
                return "staging table left on s" + (i + 1);
            }
            for (long id : ids(live[i])) {
                rows++;
                all.add(id);
                if (!("s" + (i + 1)).equals(m.resolve(String.valueOf(id)))) {
                    return "id " + id + " is on s" + (i + 1) + " but the map (" + map + ") gives it to " + m.resolve(String.valueOf(id));
                }
            }
        }
        if (rows != all.size()) {
            return (rows - all.size()) + " ids exist twice";
        }
        return all.equals(expected) ? null : "rows differ from what was acknowledged: " + all.size() + " present, " + expected.size() + " expected";
    }

    private void run(String point, int crashShard) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("rebfail");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres[] p = new LocalPostgres[3];
        LocalPostgres[] r = new LocalPostgres[3];
        WarpProcess w = null;
        try {
            for (int i = 0; i < 3; i++) {
                p[i] = LocalPostgres.primary(bin, dir, "p" + i, LocalPostgres.freePort());
                try (Connection c = p[i].conn(); Statement st = c.createStatement()) {
                    st.execute("create table orders (id int primary key, customer_id int, amount int)");
                }
                r[i] = LocalPostgres.replicaOf(bin, dir, "r" + i, p[i], LocalPostgres.freePort());
            }
            w = warp(cfg, p, r, point);
            String url = "jdbc:postgresql://localhost:" + w.port("pgwire") + "/postgres";
            String admin = "http://localhost:" + w.metricsPort();
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
            assertTrue(BrownoutHarness.http("POST", admin + "/api/sharding/add-shard", "{\"table\":\"orders\",\"shard\":\"s3\"}").contains("s3="));
            Thread.sleep(6000); // the replicas hold the rows

            AtomicReference<String> outcome = new AtomicReference<>();
            Thread mover = new Thread(() -> {
                try {
                    outcome.set(BrownoutHarness.http("POST", admin + "/api/sharding/rebalance", "{\"table\":\"orders\",\"to\":\"s3\",\"count\":21}"));
                } catch (Exception e) {
                    outcome.set("call failed: " + e);
                }
            });
            mover.start();
            Thread.sleep(5000); // the move is standing at the fault point
            p[crashShard].stop("immediate");
            long killedAt = System.currentTimeMillis();
            mover.join(180_000);
            String result = outcome.get();
            System.out.println("FAILOVER-NOTE " + point + " / s" + (crashShard + 1) + " primary killed: move ended " + (System.currentTimeMillis() - killedAt) / 1000
                    + " s later: " + (result == null ? "still running" : result.substring(0, Math.min(300, result.length()))));
            long deadline = System.currentTimeMillis() + 90_000;
            while (!r[crashShard].writable() && System.currentTimeMillis() < deadline) {
                Thread.sleep(1000);
            }
            assertTrue(r[crashShard].writable(), "Warp promoted the replica of s" + (crashShard + 1));
            LocalPostgres[] live = {p[0], p[1], p[2]};
            live[crashShard] = r[crashShard];
            String problem = inconsistency(w, live, expected);
            deadline = System.currentTimeMillis() + 120_000;
            while (problem != null && System.currentTimeMillis() < deadline) {
                Thread.sleep(2000);
                problem = inconsistency(w, live, expected);
            }
            System.out.println("FAILOVER-NOTE " + point + ": consistent " + (System.currentTimeMillis() - killedAt) / 1000 + " s after the kill, problem=" + problem);
            assertEquals(null, problem, "the table is consistent after the failover");

            // a fresh instance (no fault) moves the table again; the config already names the promoted node
            w.close();
            StringBuilder spec = new StringBuilder();
            w = warp(cfg, p, r, null);
            String again = BrownoutHarness.http("POST", "http://localhost:" + w.metricsPort() + "/api/sharding/rebalance",
                    "{\"table\":\"orders\",\"to\":\"s3\",\"count\":10}");
            System.out.println("FAILOVER-NOTE " + point + ": move again: " + again.substring(0, Math.min(200, again.length())));
            assertTrue(again.contains("\"rowsCopied\"") || again.contains("nothing to move"), again);
            assertEquals(null, inconsistency(w, live, expected), "consistent after moving again");
        } finally {
            if (w != null) {
                w.close();
            }
            for (int i = 0; i < 3; i++) {
                if (r[i] != null) {
                    r[i].stop("immediate");
                }
                if (p[i] != null) {
                    p[i].stop("immediate");
                }
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void aSourcesPrimaryDiesBeforeTheMapSwitches() throws Exception {
        run("before-switch", 0);
    }

    @Test
    void theTargetsPrimaryDiesRightAfterThePublish() throws Exception {
        run("after-publish", 2);
    }

    @Test
    void aSourcesPrimaryDiesRightAfterTheMapSwitched() throws Exception {
        run("after-switch", 0);
    }
}
