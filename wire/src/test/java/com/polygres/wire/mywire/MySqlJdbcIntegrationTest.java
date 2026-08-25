package com.polygres.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.wire.testsupport.PolyWireProcess;
import com.polygres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real MySQL JDBC client (MySQL Connector/J, real
 * {@code COM_STMT_PREPARE}/{@code COM_STMT_EXECUTE} wire protocol) gets correct results through
 * mywire into a real Postgres backend -- real subprocess, real Postgres container, no mocks.
 * Complements {@code MySqlBinaryProtocolTest}'s golden-byte unit coverage the same way
 * {@code OracleJdbcIntegrationTest}/{@code MssqlJdbcIntegrationTest} complement their own
 * decoders' unit tests: a hand-written unit test only proves the decoder matches its author's own
 * understanding of the spec, not that a real driver's actual wire encoding matches it.
 */
class MySqlJdbcIntegrationTest {

    private static RealPostgres postgres;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        polywire = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "POLYWIRE_MYWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) {
            polywire.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:mysql://localhost:" + polywire.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void plainComQueryStillWorksUnaffectedByPrepareSupportBeingAdded() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 21 * 2")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void preparedStatementWithBindParametersRoutesThroughRealComStmtPrepareExecute() throws SQLException {
        // useServerPrepStmts=true forces Connector/J to actually use COM_STMT_PREPARE/EXECUTE
        // (its default for many versions is client-side prepare, which would silently bypass this
        // feature entirely and still pass by sending literal SQL through COM_QUERY -- explicit
        // opt-in makes sure this test proves what it claims to).
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mysql_jdbc_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
                setup.executeUpdate("INSERT INTO mysql_jdbc_it (id, name) VALUES (1, 'alpha')");
                setup.executeUpdate("INSERT INTO mysql_jdbc_it (id, name) VALUES (2, 'beta')");
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM mysql_jdbc_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("alpha", rs.getString(1));
                    assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("UPDATE mysql_jdbc_it SET name = ? WHERE id = ?")) {
                ps.setString(1, "alpha-updated");
                ps.setInt(2, 1);
                int updated = ps.executeUpdate();
                conn.commit();
                assertEquals(1, updated);
            }

            // Re-executing the SAME PreparedStatement a second time exercises
            // new_params_bound_flag=0 (Connector/J only resends type info on the first EXECUTE for
            // a given prepared statement) -- proves cached-type reuse in decodeExecuteParams works
            // against a real client, not just the hand-constructed unit test for that path.
            try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM mysql_jdbc_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("alpha-updated", rs.getString(1));
                }
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("beta", rs.getString(1));
                }
            }

            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE mysql_jdbc_it");
                conn.commit();
            }
        }
    }

    @Test
    void preparedStatementWithNullBindValueIsHandledCorrectly() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mysql_jdbc_null_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO mysql_jdbc_null_it (id, name) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.executeUpdate();
                conn.commit();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM mysql_jdbc_null_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(null, rs.getString(1), "a NULL bind value must decode as SQL NULL, not the string \"null\"");
                }
            }
            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE mysql_jdbc_null_it");
                conn.commit();
            }
        }
    }
}
