package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real Postgres JDBC client, talking pgwire's native protocol (no dialect
 * translation involved), gets correct results through the full pipeline (real
 * StatementPipeline: firewall/router/QoS/translation/rollup/cache/stats) into a real Postgres
 * backend -- a real subprocess of {@code Main}, a real disposable Postgres container, no mocks.
 */
class PgWireIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) {
            warp.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void simpleSelectReturnsRealResult() throws SQLException {
        try (Connection conn = connect();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 21 * 2 AS answer")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("answer"));
            assertFalse(rs.next());
        }
    }

    @Test
    void createInsertSelectRoundTrip() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS pgwire_it (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("DELETE FROM pgwire_it");
            stmt.executeUpdate("INSERT INTO pgwire_it (id, name) VALUES (1, 'alpha'), (2, 'beta')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM pgwire_it ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alpha", rs.getString("name"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("beta", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void transactionRollbackDiscardsUncommittedWrites() throws SQLException {
        try (Connection conn = connect()) {
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE IF NOT EXISTS pgwire_it_txn (id INT PRIMARY KEY)");
                setup.execute("DELETE FROM pgwire_it_txn");
            }

            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("INSERT INTO pgwire_it_txn (id) VALUES (1)");
            }
            conn.rollback();
            conn.setAutoCommit(true);

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pgwire_it_txn")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void metricsEndpointReportsStatementsAfterQueries() throws Exception {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
        }
        java.net.HttpURLConnection metricsConn = (java.net.HttpURLConnection) java.net.URI
                .create("http://localhost:" + warp.metricsPort() + "/metrics").toURL().openConnection();
        assertEquals(200, metricsConn.getResponseCode());
        String body;
        try (var in = metricsConn.getInputStream()) {
            body = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        assertTrue(body.contains("warp_statements_total"),
                "expected /metrics to report warp_statements_total, got:\n" + body);
    }
}
