package com.sayonora.wire.mssqlwire;

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
 * Real proof that a VARBINARY column's bytes actually survive a round trip through mssqlwire --
 * a genuine, serious gap found auditing this frontend for GA transparency: every result column
 * used to be declared NVARCHAR(8000) and every value encoded via {@code String.valueOf(v)}
 * regardless of its real type, so a {@code byte[]} value got Java's default {@code
 * Object.toString()} treatment (e.g. {@code "[B@1a2b3c4d"}) instead of its actual bytes -- silent
 * data corruption, not merely a wrong declared type, for any binary/varbinary/blob column.
 */
class MssqlBinaryColumnIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void varbinaryColumnRoundTripsRealBytesNotAJavaToStringOfTheArray() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_binary_col_it (id INTEGER PRIMARY KEY, payload BYTEA)");

                byte[] payload = {0x00, 0x01, 0x02, (byte) 0xFF, 0x7F, (byte) 0x80, 'A', 'B'};
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO mssql_binary_col_it (id, payload) VALUES (?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setBytes(2, payload);
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT payload FROM mssql_binary_col_it WHERE id = ?")) {
                    ps.setInt(1, 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        byte[] roundTripped = rs.getBytes(1);
                        assertArrayEquals(payload, roundTripped,
                                "the exact bytes must come back, not a stringified Java array reference");
                    }
                }

                setup.execute("DROP TABLE mssql_binary_col_it");
            }
        }
    }
}
