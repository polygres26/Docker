package com.nexagres.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
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
 * End-to-end proof that a real Oracle wire (orawire) login carries a genuine, distinguishable
 * identity into native Postgres row-level security -- real subprocess, real Postgres container,
 * a real ojdbc11 client speaking O5LOGON, no mocks.
 *
 * <p>orawire's SQL-translation path always executes against Postgres, not real Oracle (see
 * RequestLoop's javadoc on {@code terminalExecutor}): every reachable {@code SessionHandler.run()}
 * construction site passes {@code oracleConnection = null}, so the real-Oracle-backend "dual"
 * path never reaches {@code JdbcBackendExecutor} at all -- it's intercepted earlier by a raw TNS
 * byte relay. That means the correct native-RLS mechanism here is {@code
 * PostgresRlsSessionInitializer} (the same one pgwire/mssqlwire use), not the Oracle-VPD-shaped
 * {@code OracleVpdSessionInitializer} -- SYS_CONTEXT/{@code DBMS_SESSION.SET_CONTEXT} would simply
 * fail against a Postgres JDBC connection.
 *
 * <p>Unlike pgwire, O5LOGON needs the real plaintext password server-side to verify the client's
 * encrypted challenge response, so it can never be satisfied from Postgres's own hashed {@code
 * pg_authid} verifiers the way {@code PgRoleAuthCache} is -- {@code WARP_AUTH_CREDENTIALS}
 * (a real, distinguishable per-user credential list, structurally mirroring {@code
 * AwsIamCredentialStore}) is what makes orawire logins into real identities instead of one shared
 * username everyone presents.
 */
class OracleRolesRlsIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;
    private static final String ADMIN_TOKEN = "test-admin-token-" + System.nanoTime();

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();

        // postgres (the container default superuser) sets up the fixture only -- never Warp's
        // own backend credential, since a Postgres superuser (or a table's owner, by default)
        // unconditionally bypasses every RLS policy regardless of any session GUC. warp_admin
        // is what Warp actually connects to Postgres as: an ordinary role with just the grants
        // it needs.
        try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = admin.createStatement()) {
            st.execute("CREATE ROLE warp_admin LOGIN PASSWORD 'warp-admin-pw'");
            st.execute("GRANT ALL ON SCHEMA public TO warp_admin");
            st.execute("CREATE TABLE orders (id serial PRIMARY KEY, owner_user text, item text)");
            // owner_user is uppercase because a real Oracle client (ojdbc here) uppercases an
            // unquoted username before O5LOGON ever puts it on the wire -- CredentialStore's
            // multi-user lookup is case-insensitive to match that Oracle convention, but the
            // AccessContext/session GUC value that actually reaches Postgres carries whatever case
            // the wire sent, so the RLS policy has to be written against that, same as it would be
            // for a real deployment.
            st.execute("INSERT INTO orders (owner_user, item) VALUES "
                    + "('ALICE', 'alice-order-1'), ('ALICE', 'alice-order-2'), ('BOB', 'bob-order-1')");
            st.execute("GRANT SELECT ON orders TO warp_admin");
            st.execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY");
            st.execute("CREATE POLICY orders_isolation ON orders "
                    + "USING (owner_user = current_setting('warp.user_id', true))");
        }

        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), "warp_admin", "warp-admin-pw")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                // orawire's O5LOGON needs the real plaintext password server-side (not a Postgres
                // role hash), so alice/bob's credentials are configured here directly, unrelated to
                // any Postgres role of the same name.
                .env("WARP_AUTH_CREDENTIALS", "alice=alice-pw;bob=bob-pw")
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
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
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
        assertTrue(events.contains("\"DB_LOGIN_FAILED\"") && events.contains("\"ALICE\""),
                "a failed orawire login must be recorded, not silently dropped: " + events);
    }

    @Test
    void successfulLoginIsRecordedInTheAuditLogAndReadableViaTheAdminApi() throws Exception {
        // A real statement, not conn.isValid() -- ojdbc's isValid() issues an Oracle ping
        // (TTC function code 147) that this narrow slice of the wire protocol doesn't implement,
        // unrelated to whether the O5LOGON login itself (already complete by the time a
        // Connection is returned) succeeded and was recorded.
        try (Connection conn = connectAs("bob", "bob-pw"); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1 FROM DUAL")) {
            assertTrue(rs.next());
        }

        String events = fetchAudit();
        assertTrue(events.contains("\"DB_LOGIN_SUCCEEDED\"") && events.contains("\"BOB\"")
                        && events.contains("real per-user credential"),
                "a successful orawire login under WARP_AUTH_CREDENTIALS must be recorded "
                        + "with the real identity: " + events);
    }

    /** A real Oracle client presenting a wrong password over O5LOGON must see Oracle's own real
     * ORA-01017, not a generic rejection. This was a genuine gap, root-caused and fixed against a
     * REAL Oracle Database (23c Free, gvenzl/oracle-free), not guessed: a TCP-proxy capture of an
     * actual ojdbc11 client failing a login against real Oracle showed the server sends a pair of
     * TNS MARKER packets BEFORE the error DATA packet -- {@code O5LogonHandler.sendRejection} used
     * to skip that handshake entirely and send the error frame directly, which left a client
     * mid-rich/12c-auth exchange unable to decode it (it fell back to its own generic driver-side
     * error, observed as 18745, instead of ORA-01017). Fixed by reproducing the real marker
     * handshake (see {@code O5LogonHandler}'s own javadoc for why the server doesn't actually wait
     * for the client's echo before proceeding); the message text below ("invalid credential or not
     * authorized", not the older "invalid username/password" wording) is also exactly what the
     * real captured Oracle 23c server sent, not assumed. */
    @Test
    void aWrongPasswordReturnsARealOra01017() {
        SQLException rejected = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                () -> connectAs("alice", "not-alices-real-password"),
                "a wrong password over O5LOGON must fail the login");
        assertEquals(1017, rejected.getErrorCode(), "must be a real ORA-01017, not a generic default");
        assertTrue(rejected.getMessage() != null && rejected.getMessage().startsWith("ORA-01017"),
                "client should see Oracle's own real wording for a failed logon -- got: " + rejected.getMessage());
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
