package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * A Warp instance must not stay on an old config because a change notification reached it while its LISTEN connection was down. Two Warp
 * instances share a config database; both LISTEN connections are cut, a switchover is run through the first in that gap (its notification
 * reaches nobody), and the second must still end up pointing at the new primary. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class ConfigFreshnessLiveTest {

    private static String primaryOf(WarpProcess warp) throws Exception {
        var root = com.google.gson.JsonParser.parseString(BrownoutHarness.http("GET",
                "http://localhost:" + warp.metricsPort() + "/api/failover", null)).getAsJsonObject();
        for (var g : root.getAsJsonArray("groups")) {
            for (var n : g.getAsJsonObject().getAsJsonArray("nodes")) {
                if ("primary".equals(n.getAsJsonObject().get("configuredRole").getAsString())) {
                    return n.getAsJsonObject().get("url").getAsString();
                }
            }
        }
        return "";
    }

    private void missedChange(boolean cutListeners) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("cfgfresh");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        WarpProcess w0 = null;
        WarpProcess w1 = null;
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            String spec = "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote";
            // reconnect path: the poll is off, so only the re-read on reconnect can save the instance; poll path: LISTEN stays up, the
            // notification is suppressed at the source (the trigger is disabled) and only the 3 s poll can find the new version
            w0 = start(cfg, spec, cutListeners ? "0" : "3");
            w1 = start(cfg, spec, cutListeners ? "0" : "3");
            Thread.sleep(3000);

            try (var c = cfg.conn(); var st = c.createStatement()) {
                if (cutListeners) { // cut every LISTEN connection, then switch over before they are back (they retry every 2 s)
                    st.execute("select pg_terminate_backend(pid) from pg_stat_activity where query ilike 'LISTEN%'");
                } else {
                    st.execute("alter table warp_config disable trigger warp_config_notify_trigger");
                }
            }
            String res = BrownoutHarness.http("POST", "http://localhost:" + w0.metricsPort() + "/api/failover/pg/switchover",
                    "{\"target\":\"" + replica.url() + "\"}");
            assertTrue(res.contains("\"ok\":true"), res);

            long deadline = System.currentTimeMillis() + 40_000;
            String seen = "";
            while (System.currentTimeMillis() < deadline) {
                seen = primaryOf(w1);
                if (seen.contains(":" + replica.port() + "/")) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue(seen.contains(":" + replica.port() + "/"),
                    "instance 1 still lists the old primary after 40 s -- it missed the change and never caught up: " + seen);
        } finally {
            if (w1 != null) {
                w1.close();
            }
            if (w0 != null) {
                w0.close();
            }
            primary.stop("immediate");
            if (replica != null) {
                replica.stop("immediate");
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void anInstanceThatMissedAConfigChangeWhileItsListenConnectionWasDownCatchesUp() throws Exception {
        missedChange(true);
    }

    @Test
    void anInstanceThatNeverGotTheNotificationAtAllCatchesUpOnThePeriodicPoll() throws Exception {
        missedChange(false);
    }

    private static WarpProcess start(LocalPostgres cfg, String spec, String pollSeconds) throws Exception {
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                .env("WARP_CONFIG_POLL_SECONDS", pollSeconds)
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled");
        BrownoutHarness.separatePorts(b);
        return b.start();
    }
}
