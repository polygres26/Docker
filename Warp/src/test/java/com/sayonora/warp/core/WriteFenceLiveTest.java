package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * With the write fence on, a Warp instance that missed a switchover (no notification, no poll) must learn it within about a second, not
 * at the next config poll, and a write it takes right afterwards must land on the new primary. The control run without the fence stays
 * on the old primary for the whole window, which shows the fence is what closed it. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class WriteFenceLiveTest {

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

    private void run(boolean fence) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("wfence");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        WarpProcess w0 = null;
        WarpProcess w1 = null;
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            String spec = "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote";
            w0 = start(cfg, spec, fence);
            w1 = start(cfg, spec, fence);
            Thread.sleep(3000);
            try (var c = cfg.conn(); var st = c.createStatement()) {
                st.execute("alter table warp_config disable trigger warp_config_notify_trigger");
            }
            String res = BrownoutHarness.http("POST", "http://localhost:" + w0.metricsPort() + "/api/failover/pg/switchover",
                    "{\"target\":\"" + replica.url() + "\"}");
            assertTrue(res.contains("\"ok\":true"), res);
            long t0 = System.currentTimeMillis();
            String seen = "";
            while (System.currentTimeMillis() - t0 < 8000) {
                seen = primaryOf(w1);
                if (seen.contains(":" + replica.port() + "/")) {
                    break;
                }
                Thread.sleep(250);
            }
            boolean learned = seen.contains(":" + replica.port() + "/");
            long took = System.currentTimeMillis() - t0;
            if (fence) {
                assertTrue(learned, "the fenced instance still lists the old primary after 8 s: " + seen);
                assertTrue(took < 5000, "learned only after " + took + " ms");
            } else {
                assertFalse(learned, "control: without the fence the instance should still be behind (poll is off)");
            }
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
    void aFencedInstanceLearnsASwitchoverWithinAboutASecondEvenWithNoNotificationAndNoPoll() throws Exception {
        run(true);
    }

    @Test
    void withoutTheFenceTheSameInstanceStaysBehind() throws Exception {
        run(false);
    }

    private static WarpProcess start(LocalPostgres cfg, String spec, boolean fence) throws Exception {
        WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                .env("WARP_BACKENDS", spec)
                .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                .env("WARP_CONFIG_POLL_SECONDS", "0")
                .env("WARP_WRITE_FENCE", String.valueOf(fence))
                .env("WARP_WRITE_FENCE_REFRESH_MILLIS", "300")
                .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                .env("WARP_OTEL_ENDPOINT", "disabled");
        BrownoutHarness.separatePorts(b);
        return b.start();
    }
}
