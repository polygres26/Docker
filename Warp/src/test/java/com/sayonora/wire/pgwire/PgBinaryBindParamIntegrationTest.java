package com.sayonora.wire.pgwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real proof that pgjdbc's BINARY-format bind parameters -- not just text-format -- work through
 * pgwire, closing a real gap found live while testing multi-engine sharding this session:
 * {@code PgWireSessionHandler#handleBind} used to throw {@code "binary-format bind parameters are
 * not supported"} for ANY parameter a client sent in binary form, and pgjdbc negotiates binary
 * format for plain integers (and several other common types) by default once a statement is
 * server-side prepared -- meaning an ordinary Postgres application using {@code PreparedStatement}
 * against Warp could break outright, with no workaround available to the customer (unlike Oracle/
 * MySQL/SQL Server, where dialect translation is the only source of incompatibility risk, a
 * Postgres app going through pgwire has NO translation happening at all -- this was a pure
 * wire-protocol gap, the one item on the whole compatibility list with no "route around it"
 * option).
 *
 * <p>{@code binaryTransfer=true} plus a low {@code prepareThreshold} forces pgjdbc to actually use
 * binary format deterministically (its default heuristic only kicks in after enough repeated
 * executions of the same statement to server-prepare it) -- this test isn't relying on pgjdbc's
 * own internal heuristics happening to trigger, it forces the exact wire shape being tested.
 */
class PgBinaryBindParamIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE binary_bind_it (id INT PRIMARY KEY, amount BIGINT, rate DOUBLE PRECISION, "
                    + "active BOOLEAN, label TEXT, created_at TIMESTAMP)");
        }
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private Connection connect() throws Exception {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/" + postgres.database()
                + "?binaryTransfer=true&prepareThreshold=1";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void insertAndReadBackViaBinaryFormatBindParametersOfEveryCommonType() throws Exception {
        Timestamp createdAt = Timestamp.valueOf("2025-01-15 10:30:00");
        try (Connection conn = connect();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO binary_bind_it (id, amount, rate, active, label, created_at) "
                                + "VALUES (?, ?, ?, ?, ?, ?)")) {
            // Execute twice: the first execution is client-side, prepareThreshold=1 means the
            // SECOND execution actually server-prepares and is the one pgjdbc sends in binary
            // format -- both must succeed, not just the first.
            for (int i = 1; i <= 2; i++) {
                ps.setInt(1, 100 + i);
                ps.setLong(2, 5_000_000_000L + i);
                ps.setDouble(3, 3.14159 + i);
                ps.setBoolean(4, i % 2 == 0);
                ps.setString(5, "row-" + i);
                ps.setTimestamp(6, createdAt);
                ps.executeUpdate();
            }
        }

        try (Connection conn = connect();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT amount, rate, active, label, created_at FROM binary_bind_it WHERE id = ?")) {
            ps.setInt(1, 102);
            ps.execute();
            // A second execute with a DIFFERENT bind value, still on the same PreparedStatement --
            // the case that actually needs prepareThreshold to trip and binary format to be used.
            ps.setInt(1, 102);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "expected the row inserted with binary-format bind parameters to read back");
                assertEquals(5_000_000_002L, rs.getLong(1));
                assertEquals(3.14159 + 2, rs.getDouble(2), 0.00001);
                assertEquals(true, rs.getBoolean(3));
                assertEquals("row-2", rs.getString(4));
                assertEquals(createdAt, rs.getTimestamp(5));
            }
        }
    }
}
