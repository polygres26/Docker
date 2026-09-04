package com.sayonora.wire.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real SQL Server client can call a user-defined stored procedure through
 * mssqlwire -- a genuine, common gap found auditing this frontend for GA transparency: any RPC
 * to a named procedure (not one of the {@code sp_executesql}/{@code sp_prepare}/{@code sp_execute}
 * family) was refused outright with a clean error, so an app that calls its own stored procedures
 * -- not just ad-hoc parameterized SQL -- got a hard failure for every one of those calls.
 * Scoped to the common case this frontend can actually support without a data-dictionary lookup
 * for parameter directions: an equivalently-named Postgres FUNCTION with IN-only parameters,
 * called via {@code CallableStatement}'s {@code {call name(?, ?)}} escape syntax.
 */
class MssqlStoredProcedureCallIntegrationTest {

    private Connection connect(WarpProcess warp, RealPostgres postgres) throws Exception {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + postgres.username() + ";password=" + postgres.password() + ";";
        return DriverManager.getConnection(url);
    }

    @Test
    void callableStatementInvokesARealPostgresFunctionAndReturnsItsRows() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .start()) {

            try (Connection conn = connect(warp, postgres); Statement setup = conn.createStatement()) {
                // BIGINT, not INTEGER -- confirmed live: mssql-jdbc's CallableStatement binds a
                // plain setInt() value as BIGINT when it has no real catalog metadata for the
                // procedure's declared parameter types (which it can't get from this backend), and
                // Postgres's own function overload resolution is strict about the exact numeric
                // width, so the backend function's signature has to match what the driver actually
                // sends, not just the "same value" loosely.
                setup.execute("CREATE FUNCTION double_it(n BIGINT) RETURNS BIGINT AS "
                        + "'SELECT n * 2' LANGUAGE SQL");

                try (CallableStatement cs = conn.prepareCall("{call double_it(?)}")) {
                    cs.setInt(1, 21);
                    try (ResultSet rs = cs.executeQuery()) {
                        assertTrue(rs.next(), "the function's own result row must come back");
                        assertEquals(42, rs.getInt(1));
                    }
                }

                setup.execute("DROP FUNCTION double_it");
            }
        }
    }
}
