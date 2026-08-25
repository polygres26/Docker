package com.polygres.wire.mssqlwire;

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
 * End-to-end proof that a real SQL Server JDBC client (mssql-jdbc, real TDS wire protocol) gets
 * correct results through mssqlwire into a real Postgres backend -- real subprocess, real Postgres
 * container, no mocks. Specifically exercises {@code PreparedStatement} with bind parameters,
 * which mssql-jdbc sends as an RPC {@code sp_executesql} call -- the exact shape
 * {@link com.polygres.wire.mssqlwire.frontend.RpcRequestReader} decodes. Complements
 * {@code RpcRequestReaderTest}'s golden-byte unit coverage: a hand-written unit test only proves
 * the decoder matches the author's own understanding of the TDS spec, not that a real driver's
 * actual wire encoding matches it -- this is the check that does.
 */
class MssqlJdbcIntegrationTest {

    private static RealPostgres postgres;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        polywire = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mssqlwire", "POLYWIRE_MSSQLWIRE_PORT")
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
        String url = "jdbc:sqlserver://localhost:" + polywire.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void plainSqlBatchStillWorksUnaffectedByRpcSupportBeingAdded() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 21 * 2")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void preparedStatementWithBindParametersRoutesThroughRealSpExecutesql() throws SQLException {
        // mssql-jdbc sends this as an RPC sp_executesql call with @P0 bound to 42 -- this only
        // passes if RpcRequestReader correctly decodes a REAL driver's actual wire bytes, not just
        // bytes this project itself constructed to match its own understanding of the spec.
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_jdbc_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
                setup.executeUpdate("INSERT INTO mssql_jdbc_it (id, name) VALUES (1, 'alpha')");
                setup.executeUpdate("INSERT INTO mssql_jdbc_it (id, name) VALUES (2, 'beta')");
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM mssql_jdbc_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("alpha", rs.getString(1));
                    assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE mssql_jdbc_it SET name = ? WHERE id = ?")) {
                ps.setString(1, "alpha-updated");
                ps.setInt(2, 1);
                int updated = ps.executeUpdate();
                conn.commit();
                assertEquals(1, updated);
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM mssql_jdbc_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("alpha-updated", rs.getString(1),
                            "the bind value must have actually reached the backend, not been dropped/misdecoded");
                }
            }

            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE mssql_jdbc_it");
                conn.commit();
            }
        }
    }

    @Test
    void preparedStatementWithNullBindValueIsHandledCorrectly() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_jdbc_null_it (id INTEGER PRIMARY KEY, name VARCHAR(50))");
                conn.commit();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO mssql_jdbc_null_it (id, name) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.executeUpdate();
                conn.commit();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM mssql_jdbc_null_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(null, rs.getString(1), "a NULL bind value must decode as SQL NULL, not the string \"null\"");
                }
            }
            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE mssql_jdbc_null_it");
                conn.commit();
            }
        }
    }
}
