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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the planned-switchover primitive: {@code POST /api/backends/{name}/drain}
 * stops routing NEW statements to a named backend in favor of its configured {@code fallbackName}
 * (see {@code BackendRegistry.resolveForRouting}), and {@code /undrain} reverses it -- real
 * subprocess, two real Postgres containers standing in for "primary" and "its replica/cross-region
 * fallback", no mocks.
 *
 * <p>This deliberately does NOT exercise mid-drain XA behavior -- {@code XaRecovery} resolves a
 * backend by its exact name via {@code BackendRegistry.get} (unaffected by drain state, see its
 * javadoc), so an in-doubt branch against a draining backend is a separate, not-yet-built concern
 * (see the switchover plan's Phase 4) that this test doesn't claim to cover.
 */
class BackendDrainSwitchoverIntegrationTest {

    @Test
    void drainRedirectsNewStatementsToTheConfiguredFallbackAndUndrainReversesIt() throws Exception {
        try (RealPostgres controlPlane = RealPostgres.start();
                RealPostgres primary = RealPostgres.start();
                RealPostgres fallback = RealPostgres.start()) {

            for (RealPostgres pg : new RealPostgres[] {primary, fallback}) {
                try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                        Statement st = c.createStatement()) {
                    st.execute("CREATE SCHEMA shopx");
                    st.execute("CREATE TABLE shopx.orders (id int)");
                }
            }

            String adminToken = "test-admin-token-" + System.nanoTime();
            // The 4th, pipe-delimited field on "primary" is its fallback backend's NAME within
            // this same POLYWIRE_BACKENDS spec -- see BackendRegistry.fromConfig's javadoc on that
            // field. "fallback" itself carries no fallback of its own (3 fields only).
            String backends = "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password()
                    + ";primary=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password() + "|fallback"
                    + ";fallback=" + fallback.jdbcUrl() + "|" + fallback.username() + "|" + fallback.password();

            try (PolyWireProcess polywire = PolyWireProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                    .env("POLYWIRE_BACKENDS", backends)
                    .env("POLYWIRE_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("POLYWIRE_ROUTER_SCHEMA_RULES", "shopx:primary")
                    .env("POLYWIRE_ADMIN_TOKEN", adminToken)
                    .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres";
                try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password());
                        Statement st = conn.createStatement()) {

                    st.execute("INSERT INTO shopx.orders (id) VALUES (1)");
                    assertEquals(1, countOrders(primary), "before any drain, routing must land on primary");
                    assertEquals(0, countOrders(fallback));

                    int drainStatus = adminPost(polywire.metricsPort(), adminToken, "/api/backends/primary/drain?graceMs=1000");
                    assertEquals(200, drainStatus, "drain must succeed with the correct admin token");

                    st.execute("INSERT INTO shopx.orders (id) VALUES (2)");
                    assertEquals(1, countOrders(primary),
                            "while primary is DRAINING, a new statement must NOT still land on it");
                    assertEquals(1, countOrders(fallback),
                            "while primary is DRAINING, resolveForRouting must redirect new statements to its "
                                    + "configured fallback instead");

                    int undrainStatus = adminPost(polywire.metricsPort(), adminToken, "/api/backends/primary/undrain");
                    assertEquals(200, undrainStatus);

                    st.execute("INSERT INTO shopx.orders (id) VALUES (3)");
                    assertEquals(2, countOrders(primary),
                            "after undrain, routing must land back on primary");
                    assertEquals(1, countOrders(fallback), "and stop landing on the fallback");
                }
            }
        }
    }

    private static int countOrders(RealPostgres pg) throws SQLException {
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM shopx.orders")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static int adminPost(int metricsPort, String adminToken, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + metricsPort + path)
                .toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + adminToken);
        int status = conn.getResponseCode();
        // Drain the response body so the connection is cleanly reusable/closable either way.
        try (var in = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
            if (in != null) {
                new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return status;
    }
}
