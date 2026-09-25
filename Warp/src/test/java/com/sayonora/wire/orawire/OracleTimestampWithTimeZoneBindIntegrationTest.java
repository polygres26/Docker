package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real Oracle client can bind a TIMESTAMP WITH TIME ZONE (java.time.
 * OffsetDateTime) parameter through orawire -- a gap found in the latest GA-transparency audit:
 * modern JPA/Hibernate apps commonly use java.time types, and this was refused outright before
 * this fix.
 */
class OracleTimestampWithTimeZoneBindIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @org.junit.jupiter.api.BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private Connection connect() throws Exception {
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void offsetDateTimeBindParameterRoundTripsThroughAnInsertAndAWhereClause() throws Exception {
        try (Connection conn = connect(); Statement setup = conn.createStatement()) {
            setup.execute("CREATE TABLE ora_tstz_bind_it (id INTEGER PRIMARY KEY, created_at TIMESTAMPTZ)");

            OffsetDateTime value = OffsetDateTime.of(2026, 9, 4, 21, 0, 31, 735436160, ZoneOffset.ofHours(-7));
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ora_tstz_bind_it (id, created_at) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, value);
                assertEquals(1, ps.executeUpdate());
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM ora_tstz_bind_it WHERE created_at = ?")) {
                ps.setObject(1, value);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "the bound OffsetDateTime value must actually match the stored row");
                    assertEquals(1, rs.getInt(1));
                }
            }

            setup.execute("DROP TABLE ora_tstz_bind_it");
        }
    }
}
