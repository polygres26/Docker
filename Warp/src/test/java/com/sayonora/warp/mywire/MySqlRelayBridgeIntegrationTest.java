package com.sayonora.warp.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.warp.testsupport.RealMySql;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * First-ever live test of mywire's new three-way {@code WARP_MYWIRE_BACKEND_MODE} split -- EMULATE
 * (pre-existing dialect-translation-to-Postgres default, exercised here for parity with the two new
 * modes rather than left implicitly "already covered elsewhere"), RELAY and BRIDGE (both new -- see
 * {@code ServerOptions.MySqlBackendMode}) -- mirroring orawire's own {@code
 * OracleRelayPlsqlIntegrationTest}/{@code OracleBridgePlsqlCallIntegrationTest} pattern. Unlike
 * Oracle, MySQL has no PL/SQL-equivalent procedural gap to exercise here -- this proves all three
 * modes actually reach and round-trip against their real target end-to-end (a real MySQL backend
 * for RELAY/BRIDGE, the configured Postgres backend via dialect translation for EMULATE), which is
 * the genuinely new, previously-unverified capability for RELAY/BRIDGE.
 */
class MySqlRelayBridgeIntegrationTest {

    private static WarpProcess.Builder mywireWarp(RealMySql mysql, RealPostgres postgres, String backendMode) {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(),
                        postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .env("WARP_MYWIRE_BACKEND_MODE", backendMode)
                .env("WARP_MYSQL_HOST", mysql.host())
                .env("WARP_MYSQL_PORT", String.valueOf(mysql.port()))
                .env("WARP_MYSQL_DATABASE", mysql.database())
                .env("WARP_MYSQL_USER", mysql.username())
                .env("WARP_MYSQL_PASSWORD", mysql.password())
                .env("WARP_OTEL_ENDPOINT", "disabled");
    }

    @Test
    @Timeout(180)
    void adaptModeDialectTranslatesToTheConfiguredPostgresBackend() throws Exception {
        try (RealMySql mysql = RealMySql.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (WarpProcess warp = mywireWarp(mysql, postgres, "emulate").start()) {
                String url = "jdbc:mysql://localhost:" + warp.port("mywire")
                        + "/" + postgres.database() + "?useSSL=false&allowPublicKeyRetrieval=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {
                    st.execute("CREATE TABLE adapt_it (id INT PRIMARY KEY, val VARCHAR(50))");
                    st.execute("INSERT INTO adapt_it VALUES (1, 'via-emulate')");
                    try (ResultSet rs = st.executeQuery("SELECT val FROM adapt_it WHERE id = 1")) {
                        assertEquals(true, rs.next(),
                                "EMULATE mode must dialect-translate the client's MySQL-wire SQL and execute "
                                        + "it against the configured Postgres backend -- the pre-existing "
                                        + "default mode, exercised here for parity with RELAY/BRIDGE below");
                        assertEquals("via-emulate", rs.getString(1));
                    }
                }
            }

            // EMULATE never touches the real MySQL container at all -- confirm the table this test
            // just wrote through Warp does NOT exist there, proving EMULATE really did route to
            // Postgres and not silently fall through to a real MySQL connection.
            try (Connection direct = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement st = direct.createStatement()) {
                org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class,
                        () -> st.executeQuery("SELECT * FROM adapt_it"),
                        "EMULATE mode must not have created this table on the real MySQL backend");
            }
        }
    }

    @Test
    @Timeout(180)
    void relayModeProxiesRawBytesToARealMySqlBackend() throws Exception {
        try (RealMySql mysql = RealMySql.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE relay_it (id INT PRIMARY KEY, val VARCHAR(50))");
                stmt.execute("INSERT INTO relay_it VALUES (1, 'via-relay')");
            }

            try (WarpProcess warp = mywireWarp(mysql, postgres, "relay").start()) {
                String url = "jdbc:mysql://localhost:" + warp.port("mywire")
                        + "/" + mysql.database() + "?useSSL=false&allowPublicKeyRetrieval=true";
                try (Connection conn = DriverManager.getConnection(url, mysql.username(), mysql.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT val FROM relay_it WHERE id = 1")) {
                    assertEquals(true, rs.next(), "relay must reach the real MySQL backend and return its row");
                    assertEquals("via-relay", rs.getString(1));
                }
            }
        }
    }

    @Test
    @Timeout(180)
    void bridgeModeExecutesVerbatimSqlAgainstAPooledMySqlBackend() throws Exception {
        try (RealMySql mysql = RealMySql.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(mysql.jdbcUrl(), mysql.username(), mysql.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE bridge_it (id INT PRIMARY KEY, val VARCHAR(50))");
                stmt.execute("INSERT INTO bridge_it VALUES (1, 'via-bridge')");
            }

            try (WarpProcess warp = mywireWarp(mysql, postgres, "bridge").start()) {
                String url = "jdbc:mysql://localhost:" + warp.port("mywire")
                        + "/" + mysql.database() + "?useSSL=false&allowPublicKeyRetrieval=true";
                // Bridge mode's client-facing login is a SEPARATE credential from its backend pool
                // (mirrors orawire's Bridge -- see MySqlBridgePool's own javadoc): the client
                // authenticates against Warp's own CredentialStore, which WarpProcess.Builder#pgBackend
                // seeds from WARP_AUTH_USER/PASSWORD = the Postgres creds passed to it above, NOT the
                // real MySQL WARP_MYSQL_USER/PASSWORD the pool itself uses to reach the real backend.
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT val FROM bridge_it WHERE id = 1")) {
                    assertEquals(true, rs.next(),
                            "bridge mode must parse the client's real MySQL wire protocol and execute the "
                                    + "verbatim SQL against a real, pooled MySQL backend connection");
                    assertEquals("via-bridge", rs.getString(1));
                }
            }
        }
    }
}
