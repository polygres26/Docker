package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.BrownoutHarness;
import com.sayonora.warp.testsupport.LocalPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * WARP_FAILOVER_REQUIRE_FENCE: Warp promotes only after a fence succeeded. A crashed Postgres primary, promote mode, one Warp instance.
 * Opt-in: set WARP_TEST_BROWNOUT_PG_BIN.
 */
class RequireFenceLiveTest {

    private enum Case { WEBHOOK_OK, WEBHOOK_REFUSES, NO_FENCER }

    private void run(Case c) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("reqfence");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        List<String> hooks = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fence", ex -> {
            hooks.add(new String(ex.getRequestBody().readAllBytes()));
            ex.sendResponseHeaders(c == Case.WEBHOOK_OK ? 200 : 503, -1);
            ex.close();
        });
        server.start();
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            WarpProcess.Builder b = WarpProcess.builder().pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .env("WARP_BACKENDS", "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote")
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                    .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                    .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                    .env("WARP_FAILOVER_REQUIRE_FENCE", "true")
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                    .env("WARP_OTEL_ENDPOINT", "disabled");
            if (c != Case.NO_FENCER) {
                b.env("WARP_FAILOVER_FENCE_WEBHOOK", "http://127.0.0.1:" + server.getAddress().getPort() + "/fence");
            }
            try (WarpProcess warp = b.start()) {
                Thread.sleep(3000);
                primary.stop("immediate");
                long deadline = System.currentTimeMillis() + 45_000;
                while (c == Case.WEBHOOK_OK && !replica.writable() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(500);
                }
                if (c != Case.WEBHOOK_OK) {
                    Thread.sleep(20_000); // long past the confirmation window
                }
                String events = BrownoutHarness.http("GET", "http://localhost:" + warp.metricsPort() + "/api/failover", null);
                if (c == Case.WEBHOOK_OK) {
                    assertTrue(replica.writable(), "promoted after the fence succeeded");
                    assertEquals(1, hooks.size(), "the fence was asked exactly once");
                    assertTrue(hooks.get(0).contains("\"event\":\"failover-fence\"") && hooks.get(0).contains("\"port\":" + primary.port()), hooks.get(0));
                } else {
                    assertFalse(replica.writable(), "no promotion: " + c);
                    assertTrue(events.contains("promote-blocked"), events);
                    if (c == Case.NO_FENCER) {
                        assertTrue(hooks.isEmpty());
                        assertTrue(events.contains("WARP_FAILOVER_REQUIRE_FENCE"), events);
                    } else {
                        assertFalse(hooks.isEmpty(), "the fence was attempted");
                        assertTrue(events.contains("fencing the failed primary"), events);
                    }
                }
            }
        } finally {
            server.stop(0);
            if (replica != null) {
                replica.stop("immediate");
            }
            primary.stop("immediate");
            cfg.stop("immediate");
        }
    }

    @Test
    void promotesOnlyAfterTheWebhookFenceSucceeds() throws Exception {
        run(Case.WEBHOOK_OK);
    }

    @Test
    void doesNotPromoteWhenTheFenceIsRefused() throws Exception {
        run(Case.WEBHOOK_REFUSES);
    }

    @Test
    void doesNotPromoteWithoutAnyFencerWhenOneIsRequired() throws Exception {
        run(Case.NO_FENCER);
    }
}
