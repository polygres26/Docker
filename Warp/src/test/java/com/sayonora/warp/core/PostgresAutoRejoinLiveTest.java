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
 * End to end through Warp: with {@code WARP_FAILOVER_REJOIN_COMMAND} set to {@code scripts/pg-rebuild-standby.sh}, the old Postgres
 * primary comes back as a streaming standby on its own after a planned switchover and after a crash failover, and Warp lists it as
 * an eligible replica again (so reads can be routed to it). Opt-in: set WARP_TEST_BROWNOUT_PG_BIN to a Postgres bin directory.
 */
class PostgresAutoRejoinLiveTest {

    private void run(boolean crash) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("pgautorejoin");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            String script = Path.of("scripts/pg-rebuild-standby.sh").toAbsolutePath().toString();
            String command = "LC_ALL=en_US.UTF-8 '" + script + "' '" + bin + "' '" + primary.dir() + "'";
            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .env("WARP_BACKENDS", "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote")
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                    .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                    .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                    .env("WARP_FAILOVER_REJOIN_COMMAND", command)
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                Thread.sleep(3000);
                String api = "http://localhost:" + warp.metricsPort();
                if (crash) {
                    primary.stop("immediate");
                    long deadline = System.currentTimeMillis() + 60_000;
                    while (!replica.writable() && System.currentTimeMillis() < deadline) {
                        Thread.sleep(500);
                    }
                    // wait until Warp has switched over before the old primary returns on its own
                    while (!BrownoutHarness.http("GET", api + "/api/replicas", null).contains(primary.url().substring(5))
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(500);
                    }
                    primary.start();
                } else {
                    String res = BrownoutHarness.http("POST", api + "/api/failover/pg/switchover",
                            "{\"target\":\"" + replica.url() + "\"}");
                    assertTrue(res.contains("\"ok\":true"), res);
                }
                long deadline = System.currentTimeMillis() + 90_000;
                String last = "";
                boolean back = false;
                while (!back && System.currentTimeMillis() < deadline) {
                    Thread.sleep(1000);
                    last = BrownoutHarness.http("GET", api + "/api/replicas", null);
                    // the old primary is listed as a replica of the new one, measured as a replica and eligible
                    back = !primary.writable() && last.contains("\"isReplica\":true") && last.contains("\"eligible\":true")
                            && last.contains(String.valueOf(primary.port()));
                }
                assertTrue(back, "old primary did not come back as an eligible replica: " + last);
            }
        } finally {
            primary.stop("immediate");
            if (replica != null) {
                replica.stop("immediate");
            }
            cfg.stop("immediate");
        }
    }

    @Test
    void oldPrimaryRejoinsAfterASwitchover() throws Exception {
        run(false);
    }

    @Test
    void oldPrimaryRejoinsAfterACrashFailover() throws Exception {
        run(true);
    }
}
