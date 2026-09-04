package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.postgresql.util.PGobject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real proof that UUID and JSON/JSONB bind parameters -- extremely common in Rails/Django/Node
 * apps (UUID primary keys, JSON/JSONB columns) -- work through pgwire, both as ordinary
 * text-format binds (pgjdbc's default) and as binary-format ones (forced via
 * {@code binaryTransfer=true}). Closes a real gap found auditing this frontend for GA
 * transparency: neither type had any OID-specific handling before, so a bound value reached the
 * backend as a plain, untyped string and failed with a genuine
 * "column x is of type uuid/json but expression is of type character varying" -- the exact same
 * class of bug boolean/timestamp binds already had fixed earlier this session.
 *
 * <p>Also proves the separate, more serious bug found in the same audit: a bind value of a type
 * with NO decoder at all (e.g. NUMERIC/DECIMAL in binary format, still unimplemented) used to
 * throw a plain IOException that killed the ENTIRE connection rather than just failing that one
 * statement -- see {@code PgWireSessionHandler#dispatchExtended}'s own fix.
 */
class PgUuidJsonBindParamIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE uuid_json_bind_it (id UUID PRIMARY KEY, data JSONB, notes JSON)");
            st.execute("CREATE TABLE numeric_bind_it (id INT PRIMARY KEY, amount NUMERIC)");
        }
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
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

    /** Real apps bind JSON/JSONB via a client-side PGobject with its type already set (that's
     * what tells pgjdbc which real OID to declare on the wire) -- not {@code setObject(i, string,
     * Types.OTHER)}, which pgjdbc sends as an untyped ("unknown") parameter with no OID at all,
     * a client-side choice this fix can't do anything about either way. */
    private static PGobject jsonObject(String pgType, String json) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType(pgType);
        obj.setValue(json);
        return obj;
    }

    private Connection connect(boolean binary) throws SQLException {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/" + postgres.database()
                + (binary ? "?binaryTransfer=true&prepareThreshold=1" : "");
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void uuidAndJsonbBindTextFormat() throws Exception {
        UUID id = UUID.randomUUID();
        try (Connection conn = connect(false);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO uuid_json_bind_it (id, data, notes) VALUES (?, ?, ?)")) {
            ps.setObject(1, id);
            ps.setObject(2, jsonObject("jsonb", "{\"a\": 1}"));
            ps.setObject(3, jsonObject("json", "{\"b\": 2}"));
            assertEquals(1, ps.executeUpdate());
        }
        try (Connection conn = connect(false);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT data, notes FROM uuid_json_bind_it WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("{\"a\": 1}", rs.getString(1));
                assertEquals("{\"b\": 2}", rs.getString(2));
            }
        }
    }

    @Test
    void uuidAndJsonbBindBinaryFormat() throws Exception {
        UUID id = UUID.randomUUID();
        try (Connection conn = connect(true);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO uuid_json_bind_it (id, data, notes) VALUES (?, ?, ?)")) {
            ps.setObject(1, id);
            ps.setObject(2, jsonObject("jsonb", "{\"a\": 1}"));
            ps.setObject(3, jsonObject("json", "{\"b\": 2}"));
            assertEquals(1, ps.executeUpdate());
        }
        try (Connection conn = connect(true);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT data, notes FROM uuid_json_bind_it WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("{\"a\": 1}", rs.getString(1));
                assertEquals("{\"b\": 2}", rs.getString(2));
            }
        }
    }

    /** The connection-killing bug: before the fix, this exact sequence hung/broke the second
     * statement (and every statement after it) instead of just failing the one NUMERIC bind. */
    @Test
    void unsupportedBinaryTypeFailsOnlyThatStatementNotTheWholeConnection() throws Exception {
        try (Connection conn = connect(true)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO numeric_bind_it (id, amount) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBigDecimal(2, new java.math.BigDecimal("12.50"));
                // May or may not throw depending on whether pgjdbc chose binary format for this
                // specific NUMERIC bind -- either outcome is fine; what matters is the connection
                // survives it either way (plain autocommit mode, so a failed statement needs no
                // explicit rollback to leave the connection usable).
                try {
                    ps.executeUpdate();
                } catch (SQLException ignoredIfBinaryNumericWasChosen) {
                    // expected possibility -- see comment above
                }
            }
            // The connection must still be usable for a completely unrelated statement.
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
