package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real SQL Server client can bind a UNIQUEIDENTIFIER (GUID) parameter through
 * mssqlwire -- a real gap found in the latest GA-transparency audit: EF Core's own default primary
 * key strategy is a GUID, so this broke essentially any EF Core-generated schema out of the box.
 */
class MssqlGuidBindIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void guidBindParameterRoundTripsThroughAnInsertAndAWhereClause() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_guid_bind_it (id uuid PRIMARY KEY, val VARCHAR(20))");

                UUID id = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef");
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO mssql_guid_bind_it (id, val) VALUES (?, ?)")) {
                    ps.setObject(1, id);
                    ps.setString(2, "hello");
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT val FROM mssql_guid_bind_it WHERE id = ?")) {
                    ps.setObject(1, id);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "the bound GUID value must actually match the stored row");
                        assertEquals("hello", rs.getString(1));
                    }
                }

                setup.execute("DROP TABLE mssql_guid_bind_it");
            }
        }
    }
}
