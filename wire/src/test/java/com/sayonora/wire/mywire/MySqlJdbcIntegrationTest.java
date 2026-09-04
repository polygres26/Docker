package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
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
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
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
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
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

    /** Phase 4 of the error-code plan: proves {@code SqlStateErrorMapper}/{@code
     * DialectErrorMessages} work correctly over the real mywire wire protocol too, not just
     * orawire -- a real MySQL client creating a table that already exists must see MySQL's own
     * real error number and wording, not Postgres's. */
    @Test
    void creatingATableThatAlreadyExistsReturnsARealMySqlError1050() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mysql_jdbc_dup_it (id INTEGER PRIMARY KEY)");
                conn.commit();

                SQLException duplicate = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> stmt.execute("CREATE TABLE mysql_jdbc_dup_it (id INTEGER PRIMARY KEY)"),
                        "creating a table under a name that already exists must fail");
                assertEquals(1050, duplicate.getErrorCode(), "must be real MySQL 1050 (ER_TABLE_EXISTS_ERROR)");
                assertTrue(duplicate.getMessage() != null && duplicate.getMessage().contains("already exists"),
                        "client should see MySQL's own real wording, not Postgres's -- got: " + duplicate.getMessage());
            } finally {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE mysql_jdbc_dup_it");
                    conn.commit();
                } catch (SQLException ignoredCleanupFailure) {
                    // best-effort
                }
            }
        }
    }

    /** Real proof that {@code COM_STMT_SEND_LONG_DATA} (streamed BLOB parameters) works -- a real
     * gap found and fixed: {@code setBinaryStream}/{@code setBlob} with an {@code InputStream}
     * makes Connector/J stream the value via one or more {@code COM_STMT_SEND_LONG_DATA} packets
     * ahead of {@code COM_STMT_EXECUTE} rather than inlining it in EXECUTE's own value section --
     * a shape {@code MySqlWireSessionHandler} used to detect and refuse outright on the next
     * EXECUTE. Large enough (64KB, well past Connector/J's internal buffer) to make it likely
     * real chunking across multiple SEND_LONG_DATA packets occurs, not just one.
     */
    @Test
    void streamedBlobParameterViaSendLongDataRoundTripsCorrectly() throws Exception {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mysql_jdbc_blob_it (id INTEGER PRIMARY KEY, payload BYTEA)");
                conn.commit();
            }

            byte[] payload = new byte[64 * 1024];
            new java.util.Random(42).nextBytes(payload);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO mysql_jdbc_blob_it (id, payload) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBinaryStream(2, new java.io.ByteArrayInputStream(payload), payload.length);
                assertEquals(1, ps.executeUpdate());
                conn.commit();
            }

            // Verify directly against the real Postgres backend rather than reading the value back
            // through mywire's own result-row path: MySqlBinaryProtocol.encodeRow deliberately
            // declares/encodes every column as VAR_STRING (see its class doc), so a raw byte[]
            // result would come back as Java's String.valueOf(byte[]) rather than real bytes --
            // a separate, already-disclosed trade-off unrelated to SEND_LONG_DATA's bind-side
            // correctness, which is what this test is actually proving. encode(...,'hex') sidesteps
            // that entirely by comparing as text on both sides.
            try (Connection direct = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    PreparedStatement check = direct.prepareStatement(
                            "SELECT encode(payload, 'hex') FROM mysql_jdbc_blob_it WHERE id = 1")) {
                try (ResultSet rs = check.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(hex(payload), rs.getString(1),
                            "streamed BLOB bind value must round-trip byte-for-byte through "
                                    + "COM_STMT_SEND_LONG_DATA");
                }
            }

            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE mysql_jdbc_blob_it");
                conn.commit();
            }
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Test
    void aNotNullViolationReturnsARealMySqlError1048() throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE mysql_jdbc_notnull_it (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL)");
                conn.commit();

                SQLException violation = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                        () -> stmt.executeUpdate("INSERT INTO mysql_jdbc_notnull_it (id, name) VALUES (1, NULL)"),
                        "inserting NULL into a NOT NULL column must fail");
                assertEquals(1048, violation.getErrorCode(), "must be real MySQL 1048 (ER_BAD_NULL_ERROR)");
                assertEquals("Column 'name' cannot be null", violation.getMessage(),
                        "client should see MySQL's own real wording, with the real column name");
            } finally {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE mysql_jdbc_notnull_it");
                    conn.commit();
                } catch (SQLException ignoredCleanupFailure) {
                    // best-effort
                }
            }
        }
    }
}
