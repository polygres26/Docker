package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real proof that a real Oracle CLOB/BLOB column round-trips through orawire against a REAL
 * Oracle backend (dual-exec, Oracle as authority) -- see {@code
 * JdbcBackendExecutor#materializeLob}'s own javadoc for why: a CLOB/BLOB column's real value is
 * materialized (whole content, not a LOB locator) at read time, so it reaches orawire's own
 * VARCHAR2/RAW encoding as a plain String/byte[], no locator protocol needed. Deliberately
 * scoped to LOBs that comfortably fit in memory -- real LOB streaming is a separate, larger piece
 * of work, not attempted here.
 */
class OracleClobBlobIntegrationTest {

    @Test
    @Timeout(120)
    void aClobAndBlobColumnRoundTripThroughARealOracleBackend() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                try {
                    stmt.execute("DROP TABLE clob_blob_it");
                } catch (SQLException ignored) {
                }
                stmt.execute("CREATE TABLE clob_blob_it (id NUMBER PRIMARY KEY, notes CLOB, payload BLOB)");
            }

            byte[] blobBytes = { 0x01, 0x02, 0x03, (byte) 0xFF, 0x00, 0x7A };
            String clobText = "a real CLOB value, long enough to not be confused with VARCHAR2's own inline limit";

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("orawire", "WARP_ORAWIRE_PORT")
                    .env("WARP_DUAL_EXEC_ENABLED", "true")
                    .env("WARP_DUAL_EXEC_AUTHORITY", "oracle")
                    .env("WARP_DUAL_EXEC_SHADOW_ENABLED", "false")
                    .env("WARP_ORACLE_HOST", oracle.host())
                    .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                    .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                    .env("WARP_ORACLE_USER", oracle.sysUsername())
                    .env("WARP_ORACLE_PASSWORD", oracle.sysPassword())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                // Insert directly against the real Oracle backend -- orawire's own bind-value
                // decoding doesn't yet support CLOB/BLOB *parameters* (see ExecuteRequestReader's
                // own scope), only CLOB/BLOB *result columns* (this test's actual subject).
                try (Connection direct = DriverManager.getConnection(
                        oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                        PreparedStatement ps = direct.prepareStatement(
                                "INSERT INTO clob_blob_it (id, notes, payload) VALUES (?, ?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setString(2, clobText);
                    ps.setBytes(3, blobBytes);
                    ps.executeUpdate();
                }

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT notes, payload FROM clob_blob_it WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(clobText, rs.getString(1), "CLOB column must round-trip as its real text");
                    assertArrayEquals(blobBytes, rs.getBytes(2), "BLOB column must round-trip as its real bytes");
                }
            } finally {
                try (Connection cleanup = DriverManager.getConnection(
                        oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                        Statement stmt = cleanup.createStatement()) {
                    stmt.execute("DROP TABLE clob_blob_it");
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
