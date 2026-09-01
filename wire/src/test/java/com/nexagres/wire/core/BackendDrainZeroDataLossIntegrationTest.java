package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Proves the admin drain route's "wait for zero data loss" behavior end-to-end -- a planned
 * switchover should wait for its fallback to genuinely catch up before reporting drain complete,
 * unlike the (advisory-only, never-blocking) lag check {@code BackendHealthChecker} does for an
 * unplanned failure. Real subprocess, real HTTP calls, real Postgres -- the fallback's actual
 * replication state is shadowed the same deterministic way {@link ReplicationLagIntegrationTest}
 * does (see its class javadoc for why), so this test controls exactly how far behind it is without
 * needing real streaming replication infrastructure; everything downstream of that -- the drain
 * route calling {@link ReplicationLag#awaitLagBelow}, actually blocking on it, and reporting the
 * real outcome in its response -- is exercised for real.
 */
class BackendDrainZeroDataLossIntegrationTest {

    @Test
    void drainReportsTimeoutWhileLaggingAndConfirmsOnceCaughtUp() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres primary = RealPostgres.start();
                RealPostgres fallback = RealPostgres.start()) {

            installShadowLagFunctions(fallback, 999);

            String adminToken = "test-admin-token-" + System.nanoTime();
            String backends = "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password()
                    + ";primary=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password() + "|fallback"
                    + ";fallback=" + fallback.jdbcUrl() + "|" + fallback.username() + "|" + fallback.password();

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", backends)
                    .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("WARP_ADMIN_TOKEN", adminToken)
                    .env("WARP_BACKEND_HEALTH_CHECK_SECONDS", "0")
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String stillLaggingBody = adminPost(warp.metricsPort(), adminToken,
                        "/api/backends/primary/drain?graceMs=800");
                assertTrue(stillLaggingBody.contains("\"zeroDataLoss\":\"TIMED OUT"),
                        "drain must report the wait timed out while the fallback is still lagging: " + stillLaggingBody);

                // Undrain so the second drain call below re-exercises the full path (drain is
                // otherwise idempotent on an already-DRAINING backend, which would be fine too,
                // but this is closer to what an operator's real retry looks like).
                adminPost(warp.metricsPort(), adminToken, "/api/backends/primary/undrain");

                installShadowLagFunctions(fallback, 0);
                String caughtUpBody = adminPost(warp.metricsPort(), adminToken,
                        "/api/backends/primary/drain?graceMs=5000");
                assertTrue(caughtUpBody.contains("\"zeroDataLoss\":\"confirmed zero lag\""),
                        "drain must confirm zero data loss once the fallback has genuinely caught up: " + caughtUpBody);
            }
        }
    }

    private static void installShadowLagFunctions(RealPostgres pg, double lagSeconds) throws Exception {
        try (Connection conn = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = conn.createStatement()) {
            st.execute("CREATE OR REPLACE FUNCTION pg_is_in_recovery() RETURNS boolean AS "
                    + "$$ SELECT true $$ LANGUAGE sql");
            st.execute("CREATE OR REPLACE FUNCTION pg_last_xact_replay_timestamp() RETURNS timestamptz AS "
                    + "$$ SELECT now() - interval '" + lagSeconds + " seconds' $$ LANGUAGE sql");
            st.execute("ALTER DATABASE " + pg.database() + " SET search_path = public, pg_catalog");
        }
    }

    private static String adminPost(int metricsPort, String adminToken, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + metricsPort + path)
                .toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + adminToken);
        int status = conn.getResponseCode();
        try (var in = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
            String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(200, status, "admin call to " + path + " must succeed: " + body);
            return body;
        }
    }
}
