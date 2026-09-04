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
 * Real proof that {@code SET time_zone = '...'} actually changes how the session renders
 * date/time values -- a genuine gap found auditing this frontend for GA transparency: every
 * {@code SET} statement other than autocommit was a silent no-op, so an app relying on session
 * timezone (session-local date/time rendering, any timezone-sensitive comparison) got whatever
 * timezone the backend Postgres session happened to default to, with no error or indication
 * anything was ignored.
 */
class MySqlSetTimeZoneIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres"
                + "?useSSL=false&allowPublicKeyRetrieval=true";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    void setTimeZoneChangesHowATimestampWithTimeZoneValueRenders() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement stmt = conn.createStatement()) {
                // current_setting('TimeZone') reads the REAL backend session's own GUC directly --
                // unlike rendering a timestamptz value through the JDBC driver (which applies its
                // own client-side local-timezone conversion independent of the server session),
                // this is unambiguous proof the SET statement actually reached and changed the
                // real backend connection, not a no-op.
                stmt.execute("SET time_zone = '+05:00'");
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('TimeZone')")) {
                    assertTrue(rs.next());
                    assertEquals("+05:00", rs.getString(1),
                            "SET time_zone must actually change the real backend session's own timezone GUC");
                }

                stmt.execute("SET time_zone = '-08:00'");
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('TimeZone')")) {
                    assertTrue(rs.next());
                    assertEquals("-08:00", rs.getString(1), "a second SET must actually change it again");
                }
            }
        }
    }
}
