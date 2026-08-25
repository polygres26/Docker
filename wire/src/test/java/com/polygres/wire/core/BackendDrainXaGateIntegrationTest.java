package com.polygres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.testsupport.PolyWireProcess;
import com.polygres.wire.testsupport.RealPostgres;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.junit.jupiter.api.Test;

/**
 * Proves the Phase 4a switchover safety gate: {@code POST /api/backends/{name}/drain} must refuse
 * (409) to drain a backend with an unresolved {@code polywire_xa_log} row against it -- draining
 * closes that backend's connection pool, and {@code XaRecovery} would otherwise reconnect to it BY
 * NAME on the next crash-recovery pass, so closing the pool out from under a branch that's still
 * genuinely prepared-but-undecided would make a recoverable in-doubt transaction unrecoverable.
 *
 * <p>Writes directly into {@code polywire_xa_log} (the exact table/row shape {@link
 * com.polygres.wire.xa.XaRecoveryLog#logDecided} would have written) rather than orchestrating a
 * real prepared transaction end-to-end -- {@code XaRecoveryIntegrationTest} already covers that the
 * log gets written and recovered correctly; this test is purely about the drain gate's own read of
 * that log, which only cares that an unresolved row exists.
 */
class BackendDrainXaGateIntegrationTest {

    @Test
    void drainRefusesWhileAnUnresolvedXaBranchExistsAndSucceedsOnceItsResolved() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start(); RealPostgres primary = RealPostgres.start()) {

            String adminToken = "test-admin-token-" + System.nanoTime();
            String backends = "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password()
                    + ";primary=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password();

            try (PolyWireProcess polywire = PolyWireProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                    .env("POLYWIRE_BACKENDS", backends)
                    .env("POLYWIRE_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("POLYWIRE_ADMIN_TOKEN", adminToken)
                    .env("POLYWIRE_BACKEND_HEALTH_CHECK_SECONDS", "0")
                    .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                    .start()) {

                // Main's startup path already called XaRecoveryLog.ensureSchema() against this
                // same control-plane database, so polywire_xa_log exists by the time PolyWire is
                // up and accepting connections.
                try (Connection conn = DriverManager.getConnection(controlPlane.jdbcUrl(),
                        controlPlane.username(), controlPlane.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO polywire_xa_log (gtrid_hex, branch_index, backend_name) "
                                        + "VALUES (?, ?, ?)")) {
                    ps.setString(1, "deadbeef");
                    ps.setInt(2, 0);
                    ps.setString(3, "primary");
                    ps.executeUpdate();
                }

                int status = adminPost(polywire.metricsPort(), adminToken, "/api/backends/primary/drain?local=true");
                assertEquals(409, status,
                        "drain must refuse while polywire_xa_log has an unresolved row for this backend");

                try (Connection conn = DriverManager.getConnection(controlPlane.jdbcUrl(),
                        controlPlane.username(), controlPlane.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "UPDATE polywire_xa_log SET resolved_at = now() "
                                        + "WHERE gtrid_hex = ? AND branch_index = ?")) {
                    ps.setString(1, "deadbeef");
                    ps.setInt(2, 0);
                    ps.executeUpdate();
                }

                status = adminPost(polywire.metricsPort(), adminToken, "/api/backends/primary/drain?local=true");
                assertEquals(200, status, "once resolved, drain must succeed normally");
            }
        }
    }

    private static int adminPost(int metricsPort, String adminToken, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + metricsPort + path)
                .toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + adminToken);
        int status = conn.getResponseCode();
        try (var in = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
            if (in != null) {
                assertTrue(new String(in.readAllBytes(), StandardCharsets.UTF_8).length() >= 0);
            }
        }
        return status;
    }
}
