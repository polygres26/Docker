package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * The two split-brain guards through a running Warp in promote mode, each with a control run that switches the guard off to show the
 * hazard it prevents. Opt-in: set WARP_TEST_BROWNOUT_PG_BIN to a Postgres bin directory.
 * <ul>
 *   <li>Partition: the primary refuses Warp's connections but is alive and still streams to its standby. With the replica check Warp
 *       must NOT promote; without it, it does, and there are two writers.</li>
 *   <li>Stale writer: the primary crashes, the replica is promoted, the old primary is restarted on its own. With the freeze it stops
 *       accepting writes within seconds; without it, it takes writes the new primary never sees.</li>
 * </ul>
 */
class PostgresSplitBrainGuardLiveTest {

    private interface Scenario {
        void run(WarpProcess warp, LocalPostgres primary, LocalPostgres replica, String api) throws Exception;
    }

    private void withWarp(String guardEnv, String guardValue, Scenario scenario) throws Exception {
        String bin = System.getenv("WARP_TEST_BROWNOUT_PG_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(), "set WARP_TEST_BROWNOUT_PG_BIN");
        Path dir = Files.createTempDirectory("pgsplit");
        LocalPostgres cfg = LocalPostgres.primary(bin, dir, "cfg", LocalPostgres.freePort());
        LocalPostgres primary = LocalPostgres.primary(bin, dir, "primary", LocalPostgres.freePort());
        LocalPostgres replica = null;
        try {
            replica = LocalPostgres.replicaOf(bin, dir, "replica", primary, LocalPostgres.freePort());
            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend("127.0.0.1", cfg.port(), "postgres", "warp", "secret")
                    .env("WARP_BACKENDS", "pg=" + primary.url() + "|warp|secret||" + replica.url() + "~10|promote")
                    .env("WARP_REPLICA_LAG_CHECK_SECONDS", "1")
                    .env("WARP_FAILOVER_PROBE_SECONDS", "1")
                    .env("WARP_FAILOVER_CONFIRM_PROBES", "3")
                    .env("WARP_FAILOVER_COOLDOWN_SECONDS", "5")
                    .env(guardEnv, guardValue)
                    .env("WARP_ADMIN_TOKEN", BrownoutHarness.TOKEN)
                    .env("WARP_GRPC_PORT", String.valueOf(LocalPostgres.freePort()))
                    .env("WARP_OTEL_ENDPOINT", "disabled").start()) {
                Thread.sleep(4000); // a few healthy probes, so the replica has a lag reading from while the primary is up
                scenario.run(warp, primary, replica, "http://localhost:" + warp.metricsPort());
            }
        } finally {
            primary.stop("immediate");
            if (replica != null) {
                replica.stop("immediate");
            }
            cfg.stop("immediate");
        }
    }

    private static String failoverStatus(String api) throws Exception {
        return BrownoutHarness.http("GET", api + "/api/failover", null);
    }

    private void partition(boolean guardOn) throws Exception {
        withWarp("WARP_FAILOVER_STANDBY_CONFIRM", String.valueOf(guardOn), (warp, primary, replica, api) -> {
            primary.refuseNewClientConnections(); // alive, still streaming, but Warp cannot reach it
            long deadline = System.currentTimeMillis() + (guardOn ? 25_000 : 45_000);
            if (guardOn) {
                String status = "";
                while (!status.contains("heard from the primary") && System.currentTimeMillis() < deadline) {
                    Thread.sleep(1000);
                    status = failoverStatus(api);
                }
                assertTrue(status.contains("heard from the primary"), "Warp explains why it is not promoting: " + status);
                Thread.sleep(8000); // well past the confirmation window: still nothing
                assertFalse(replica.writable(), "the replica was not promoted while the primary is alive");
            } else {
                while (!replica.writable() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(1000);
                }
                assertTrue(replica.writable(), "control: without the replica check Warp promotes a replica of a live primary");
            }
        });
    }

    @Test
    void aPrimaryWarpCannotReachButItsStandbyStillHearsFromIsNotReplaced() throws Exception {
        partition(true);
    }

    @Test
    void controlWithoutTheReplicaCheckThePartitionedPrimaryIsReplacedAndThereAreTwoWriters() throws Exception {
        partition(false);
    }

    private void staleWriter(boolean guardOn) throws Exception {
        withWarp("WARP_FAILOVER_FREEZE_STALE_WRITER", String.valueOf(guardOn), (warp, primary, replica, api) -> {
            primary.stop("immediate");
            long deadline = System.currentTimeMillis() + 60_000;
            while (!replica.writable() && System.currentTimeMillis() < deadline) {
                Thread.sleep(500);
            }
            assertTrue(replica.writable(), "Warp promoted the replica");
            Thread.sleep(3000);
            primary.start(); // the old primary returns on its own, writable (no standby.signal)
            assertTrue(primary.writable());
            deadline = System.currentTimeMillis() + 40_000;
            String refused = null;
            while (System.currentTimeMillis() < deadline) {
                try (var c = primary.conn(); var st = c.createStatement()) {
                    st.execute("create table if not exists stale(id serial primary key, v text)");
                    st.execute("insert into stale(v) values ('written to the stale primary')");
                    if (!guardOn) {
                        break; // the control only needs one write to get through
                    }
                } catch (Exception e) {
                    refused = e.getMessage();
                    break;
                }
                Thread.sleep(1000);
            }
            if (guardOn) {
                assertTrue(refused != null && refused.toLowerCase().contains("read-only"),
                        "the stale primary refuses writes once Warp freezes it: " + refused);
                String status = failoverStatus(api);
                assertTrue(status.contains("stale-writer-frozen"), status);
            } else {
                assertEquals(null, refused, "control: without the freeze the stale primary takes writes the new primary never sees");
            }
        });
    }

    @Test
    void aStaleOldPrimaryThatComesBackWritableIsFrozen() throws Exception {
        staleWriter(true);
    }

    @Test
    void controlWithoutTheFreezeTheStaleOldPrimaryTakesWrites() throws Exception {
        staleWriter(false);
    }
}
