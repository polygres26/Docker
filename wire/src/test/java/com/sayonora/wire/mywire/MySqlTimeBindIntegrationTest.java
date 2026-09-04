package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real MySQL Connector/J client can bind a {@code java.sql.Time} parameter
 * through a real {@code COM_STMT_PREPARE}/{@code COM_STMT_EXECUTE} round trip -- a gap found live
 * (TIME's own binary-protocol type code, 0x0b, wasn't in {@link MySqlBinaryProtocol}'s supported
 * set, so any client binding TIME was refused outright). Complements
 * {@code MySqlJdbcIntegrationTest}'s coverage of DATE/DATETIME/TIMESTAMP binds.
 */
class MySqlTimeBindIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @org.junit.jupiter.api.BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private Connection connect() throws Exception {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void timeBindParameterRoundTripsThroughAnInsertAndAWhereClause() throws Exception {
        try (Connection conn = connect(); Statement setup = conn.createStatement()) {
            setup.execute("CREATE TABLE mysql_time_bind_it (id INTEGER PRIMARY KEY, t TIME)");

            Time value = Time.valueOf("13:45:31");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO mysql_time_bind_it (id, t) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setTime(2, value);
                assertEquals(1, ps.executeUpdate());
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM mysql_time_bind_it WHERE t = ?")) {
                ps.setTime(1, value);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "the bound Time value must actually match the stored row");
                    assertEquals(1, rs.getInt(1));
                }
            }

            setup.execute("DROP TABLE mysql_time_bind_it");
        }
    }
}
