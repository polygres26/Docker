package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real Oracle client can SELECT a RAW-family (bytea) column through orawire --
 * a real gap found in the latest GA-transparency audit: GUID primary keys and hash columns stored
 * as RAW(n) are common, and any SELECT touching one threw {@code UnsupportedOperationException}
 * outright before this fix.
 */
class OracleRawColumnIntegrationTest {

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
    void selectingARawColumnReturnsTheRealBytesNotAnError() throws Exception {
        try (Connection conn = connect(); Statement setup = conn.createStatement()) {
            setup.execute("CREATE TABLE ora_raw_col_it (id INTEGER PRIMARY KEY, payload BYTEA)");

            byte[] payload = {0x00, 0x01, 0x02, (byte) 0xFF, 0x7F, (byte) 0x80, 'A', 'B'};
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO ora_raw_col_it (id, payload) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, payload);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT payload FROM ora_raw_col_it WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertArrayEquals(payload, rs.getBytes(1));
                }
            }

            setup.execute("DROP TABLE ora_raw_col_it");
        }
    }
}
