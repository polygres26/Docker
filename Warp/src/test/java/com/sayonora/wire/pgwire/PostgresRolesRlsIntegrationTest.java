package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@code WARP_AUTH_MODE=postgres_roles} genuinely wires an
 * authenticated identity into native Postgres row-level security -- real subprocess, real
 * Postgres container, no mocks. Two real Postgres roles ({@code alice}, {@code bob}) each connect
 * through pgwire with their own real password; a real RLS policy on the backend, keyed off
 * {@code current_setting('warp.user_id')} (set by {@code PostgresRlsSessionInitializer} --
 * see {@code JdbcBackendExecutor}), determines what each one can see. This is the exact gap the
 * Warp docs site used to flag as "built, correct, and not yet load-bearing" -- this test is
 * what makes that no longer true, for the identity-propagation half of that claim.
 *
 * <p>Also proves the second half: a real login is recorded to the audit log and readable back via
 * {@code GET /api/audit}.
 */
class PostgresRolesRlsIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;
    private static final String ADMIN_TOKEN = "test-admin-token-" + System.nanoTime();

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();

        // The container's default `postgres` role is a real superuser -- used here ONLY to set up
        // the fixture, never as Warp's own backend credential. That distinction matters: a
        // superuser (or a table's owner, by default) unconditionally BYPASSES every RLS policy in
        // Postgres, regardless of what current_setting('warp.user_id') is set to -- confirmed
        // live, this test's first draft used postgres/postgres as Warp's backend credential
        // and every row leaked through unfiltered. Warp's own backend connection here is a
        // dedicated, ordinary (non-superuser, non-table-owner) role instead, exactly as a real
        // deployment needs to be configured for RLS to actually do anything: it needs an explicit
        // GRANT to read pg_authid (normally superuser-only) and an explicit GRANT on the
        // RLS-protected table -- neither comes from ambient superuser privilege.
        try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE ROLE warp_admin LOGIN PASSWORD 'warp-admin-pw'");
            st.execute("GRANT SELECT ON pg_authid TO warp_admin");
            // Needed for Warp's own control-plane tables (warp_config, etc), which it
            // creates itself on startup -- ownership of ITS OWN tables doesn't grant RLS bypass on
            // orders below, since RLS bypass is per-table-owned, not schema-wide.
            st.execute("GRANT ALL ON SCHEMA public TO warp_admin");
            st.execute("CREATE ROLE alice LOGIN PASSWORD 'alice-pw'");
            st.execute("CREATE ROLE bob LOGIN PASSWORD 'bob-pw'");
            st.execute("CREATE TABLE orders (id serial PRIMARY KEY, owner_user text, item text)");
            st.execute("INSERT INTO orders (owner_user, item) VALUES "
                    + "('alice', 'alice-order-1'), ('alice', 'alice-order-2'), ('bob', 'bob-order-1')");
            // Warp never opens its own Postgres connection AS alice/bob -- every statement
            // actually runs on warp_admin's one connection, with warp_admin's own SELECT
            // grant, filtered only by the RLS policy's USING clause against the session GUC. alice/
            // bob are LOGIN roles purely so PgRoleAuthCache has a real password hash to verify their
            // pgwire-frontend login against; they need no table grants of their own.
            st.execute("GRANT SELECT ON orders TO warp_admin");
            st.execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY");
            st.execute("CREATE POLICY orders_isolation ON orders "
                    + "USING (owner_user = current_setting('warp.user_id', true))");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), "warp_admin", "warp-admin-pw")
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_AUTH_MODE", "postgres_roles")
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private Connection connectAs(String username, String password) throws SQLException {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, username, password);
    }

    @Test
    void aliceOnlySeesHerOwnRowsThroughRealPostgresRls() throws SQLException {
        try (Connection conn = connectAs("alice", "alice-pw"); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT item FROM orders ORDER BY item")) {
            java.util.List<String> items = new java.util.ArrayList<>();
            while (rs.next()) {
                items.add(rs.getString(1));
            }
            assertEquals(java.util.List.of("alice-order-1", "alice-order-2"), items,
                    "alice must see only her own rows -- warp.user_id must have been set to "
                            + "'alice' on this connection for Postgres's own RLS policy to filter correctly");
        }
    }

    @Test
    void bobOnlySeesHisOwnRowsNotAlices() throws SQLException {
        try (Connection conn = connectAs("bob", "bob-pw"); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT item FROM orders")) {
            assertTrue(rs.next());
            assertEquals("bob-order-1", rs.getString(1));
            assertFalse(rs.next(), "bob must never see alice's rows, proving this isn't just "
                    + "returning everything unfiltered");
        }
    }

    @Test
    void wrongPasswordIsRejectedAndRecordedAsALoginFailure() throws Exception {
        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                () -> connectAs("alice", "wrong-password"));
        assertTrue(e.getMessage() != null);

        String events = fetchAudit();
        assertTrue(events.contains("\"DB_LOGIN_FAILED\"") && events.contains("\"alice\""),
                "a failed real-role login must be recorded, not silently dropped: " + events);
    }

    @Test
    void successfulLoginIsRecordedInTheAuditLogAndReadableViaTheAdminApi() throws Exception {
        try (Connection conn = connectAs("bob", "bob-pw")) {
            // connection alone is enough to trigger the login event
            assertTrue(conn.isValid(2));
        }

        String events = fetchAudit();
        assertTrue(events.contains("\"DB_LOGIN_SUCCEEDED\"") && events.contains("\"bob\"")
                        && events.contains("real Postgres role"),
                "a successful postgres_roles login must be recorded with the real role identity: " + events);
    }

    private String fetchAudit() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                        "http://localhost:" + warp.metricsPort() + "/api/audit?limit=50")
                .toURL().openConnection();
        conn.setRequestProperty("Authorization", "Bearer " + ADMIN_TOKEN);
        int status = conn.getResponseCode();
        assertEquals(200, status, "GET /api/audit must succeed with the correct admin token");
        return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
