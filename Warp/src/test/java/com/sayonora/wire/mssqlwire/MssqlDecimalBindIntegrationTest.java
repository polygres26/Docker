package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real SQL Server client can bind a DECIMAL/NUMERIC parameter through mssqlwire
 * -- a genuine gap found auditing this frontend for GA transparency: any parameterized query
 * binding a monetary or precise-fraction value (extremely common -- prices, balances, quantities)
 * was rejected outright by {@link com.sayonora.wire.mssqlwire.frontend.RpcRequestReader}, which
 * refused DECIMALN/NUMERICN rather than risk a wrong guess at its precision-dependent encoding.
 */
class MssqlDecimalBindIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void decimalBindParameterRoundTripsThroughAnInsertAndAWhereClause() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                setup.execute("CREATE TABLE mssql_decimal_bind_it (id INTEGER PRIMARY KEY, price NUMERIC(10,2))");

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO mssql_decimal_bind_it (id, price) VALUES (?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setBigDecimal(2, new BigDecimal("12.34"));
                    assertEquals(1, ps.executeUpdate());

                    ps.setInt(1, 2);
                    ps.setBigDecimal(2, new BigDecimal("-99.90"));
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id FROM mssql_decimal_bind_it WHERE price = ?")) {
                    ps.setBigDecimal(1, new BigDecimal("12.34"));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "the bound DECIMAL value must actually match the stored row");
                        assertEquals(1, rs.getInt(1));
                    }
                }

                setup.execute("DROP TABLE mssql_decimal_bind_it");
            }
        }
    }
}
