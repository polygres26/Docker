package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real SQL Server client can bind DATE and DATETIME2 (java.sql.Timestamp)
 * parameters through mssqlwire -- the single most impactful gap found auditing this frontend for
 * GA transparency: virtually every CRUD app binds a timestamp somewhere (audit columns, date-range
 * filters), and this was refused outright before this fix.
 */
class MssqlDateTimeBindIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void dateAndTimestampBindParametersRoundTripThroughAnInsertAndAWhereClause() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_datetime_bind_it (id INTEGER PRIMARY KEY, "
                        + "created_on DATE, created_at TIMESTAMP)");

                Date day = Date.valueOf("2026-09-04");
                Timestamp instant = Timestamp.valueOf("2026-09-04 13:45:30.1234567");

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO mssql_datetime_bind_it (id, created_on, created_at) VALUES (?, ?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setDate(2, day);
                    ps.setTimestamp(3, instant);
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id FROM mssql_datetime_bind_it WHERE created_on = ? AND created_at = ?")) {
                    ps.setDate(1, day);
                    ps.setTimestamp(2, instant);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "the bound DATE/TIMESTAMP values must actually match the stored row");
                        assertEquals(1, rs.getInt(1));
                    }
                }

                setup.execute("DROP TABLE mssql_datetime_bind_it");
            }
        }
    }
}
