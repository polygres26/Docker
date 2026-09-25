package com.sayonora.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that {@code SET NAMES} (UTF8-family) and {@code SET TRANSACTION ISOLATION LEVEL}
 * are actually handled, not silently swallowed like every other SET before this fix. {@code SET
 * NAMES} is sent by essentially every real MySQL client at connection time -- this now correctly
 * ACKs it (client_encoding is already UTF8 on the real backend connection either way) instead of
 * the client having no idea whether its request was honored. A non-UTF8 charset is deliberately
 * left as a no-op (see MySqlWireSessionHandler#UTF8_FAMILY_CHARSETS's own javadoc: actually
 * issuing "SET client_encoding" to a non-UTF8 value against the shared backend connection breaks
 * pgjdbc itself, since that's the SAME connection Warp's own backend driver depends on).
 */
class MySqlSetNamesAndIsolationIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void setNamesUtf8mb4IsAckedAndTheRealBackendConnectionStaysUsable() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("SET NAMES 'utf8mb4'");
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('client_encoding')")) {
                    assertTrue(rs.next());
                    assertEquals("UTF8", rs.getString(1));
                }
            }
        }
    }

    @Test
    void setIsolationLevelChangesTheRealBackendSession() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                stmt.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('transaction_isolation')")) {
                    assertTrue(rs.next());
                    assertEquals("serializable", rs.getString(1));
                }
            }
        }
    }
}
