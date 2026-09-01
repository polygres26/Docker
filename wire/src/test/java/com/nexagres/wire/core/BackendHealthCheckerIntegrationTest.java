package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the unplanned-failure half of the switchover design ({@link
 * BackendHealthChecker}): a real outage (the "primary" container genuinely stopped, not a config
 * flag) gets detected by the periodic connectivity probe and routing redirects to the configured
 * fallback automatically -- no operator, no admin API call. Complements
 * {@code BackendDrainSwitchoverIntegrationTest}, which covers the planned/admin-driven half of the
 * same routing mechanism.
 */
class BackendHealthCheckerIntegrationTest {

    @Test
    void anUnreachablePrimaryIsAutoDetectedAndNewStatementsRedirectToItsFallback() throws Exception {
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

            String backends = "default=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password()
                    + ";primary=" + primary.jdbcUrl() + "|" + primary.username() + "|" + primary.password() + "|fallback"
                    + ";fallback=" + fallback.jdbcUrl() + "|" + fallback.username() + "|" + fallback.password();

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(controlPlane.host(), controlPlane.port(), controlPlane.database(),
                            controlPlane.username(), controlPlane.password())
                    .frontend("pgwire", "WARP_PGWIRE_PORT")
                    .env("WARP_BACKENDS", backends)
                    .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                    .env("WARP_ROUTER_SCHEMA_RULES", "shopx:primary")
                    // Fast enough for a test to wait out without dragging real-time out of it too
                    // far -- production would use something like 15-30s (see Main's default).
                    .env("WARP_BACKEND_HEALTH_CHECK_SECONDS", "1")
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
                try (Connection conn = DriverManager.getConnection(url, primary.username(), primary.password());
                        Statement st = conn.createStatement()) {

                    st.execute("INSERT INTO shopx.orders (id) VALUES (1)");
                    assertEquals(1, countOrders(primary), "before any outage, routing must land on primary");

                    primary.stop();
                    try {
                        // The health checker's first probe fires at t=1s (initialDelay ==
                        // period, see BackendHealthChecker.start's javadoc) and then every 1s
                        // after -- poll for up to 10s of real wall-clock for it to notice and
                        // flip state, rather than assuming a fixed sleep is long enough.
                        awaitCondition(java.time.Duration.ofSeconds(10), () -> {
                            try {
                                st.execute("INSERT INTO shopx.orders (id) VALUES (2)");
                                return countOrders(fallback) == 1;
                            } catch (SQLException e) {
                                return false;
                            }
                        });
                        // Can't also assert on countOrders(primary) here -- primary is genuinely
                        // stopped, so a direct connection to it fails too, by design; the fact
                        // that awaitCondition above only succeeded once fallback's count went to
                        // 1 is already the proof the new row didn't (and couldn't have) landed on
                        // primary.
                        assertEquals(1, countOrders(fallback),
                                "once the health checker marks primary DOWN, new statements must "
                                        + "redirect to its configured fallback");
                    } finally {
                        primary.resume();
                    }
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

    private interface Condition {
        boolean check();
    }

    private static void awaitCondition(java.time.Duration timeout, Condition condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.check()) {
                return;
            }
            Thread.sleep(300);
        }
        assertTrue(condition.check(), "condition did not become true within " + timeout);
    }
}
