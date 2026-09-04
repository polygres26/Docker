package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real proof that a real ojdbc {@code CallableStatement} call to a PL/SQL procedure with only IN
 * parameters (a real, common EBS-style shape: a side-effecting call with no return value) works
 * through orawire against a REAL Oracle backend (dual-exec, Oracle as authority) -- see {@code
 * RequestLoop#handlePlSqlExecute}'s own javadoc for the full root-cause/scope writeup this
 * implements, including why OUT/IN OUT parameters (including REF CURSOR) are refused rather than
 * guessed at: a first attempt at scalar OUT params was tried and found wrong live (a real
 * ORA-17401 protocol violation), so this narrow slice covers only what's actually been verified.
 */
class OraclePlsqlCallIntegrationTest {

    @Test
    @Timeout(120)
    void callableStatementWithOnlyInParametersWorks() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                try {
                    stmt.execute("DROP PROCEDURE plsql_call_it_proc");
                } catch (SQLException ignored) {
                }
                try {
                    stmt.execute("DROP TABLE plsql_call_it");
                } catch (SQLException ignored) {
                }
                stmt.execute("CREATE TABLE plsql_call_it (id NUMBER PRIMARY KEY, val NUMBER)");
                stmt.execute("CREATE PROCEDURE plsql_call_it_proc(p_id IN NUMBER, p_val IN NUMBER) AS "
                        + "BEGIN INSERT INTO plsql_call_it (id, val) VALUES (p_id, p_val); END;");
            }

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

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        CallableStatement cs = conn.prepareCall("{call plsql_call_it_proc(?, ?)}")) {
                    cs.setInt(1, 1);
                    cs.setInt(2, 42);
                    cs.execute();
                }
            }

            // Verify directly against the real Oracle backend that the INSERT the procedure ran
            // actually landed -- not just that the call returned without error.
            try (Connection check = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = check.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT val FROM plsql_call_it WHERE id = 1")) {
                org.junit.jupiter.api.Assertions.assertTrue(rs.next());
                assertEquals(42, rs.getInt(1), "the procedure's real INSERT must have landed on the real backend");
            } finally {
                try (Connection cleanup = DriverManager.getConnection(
                        oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                        Statement stmt = cleanup.createStatement()) {
                    stmt.execute("DROP PROCEDURE plsql_call_it_proc");
                    stmt.execute("DROP TABLE plsql_call_it");
                } catch (SQLException ignored) {
                }
            }
        }
    }

    @Test
    @Timeout(120)
    void outParameterIsRefusedNotSilentlyWrong() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                try {
                    stmt.execute("DROP PROCEDURE plsql_call_out_it_proc");
                } catch (SQLException ignored) {
                }
                stmt.execute("CREATE PROCEDURE plsql_call_out_it_proc(p_in IN NUMBER, p_out OUT NUMBER) AS "
                        + "BEGIN p_out := p_in * 2; END;");
            }

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

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        CallableStatement cs = conn.prepareCall("{call plsql_call_out_it_proc(?, ?)}")) {
                    cs.setInt(1, 21);
                    cs.registerOutParameter(2, Types.NUMERIC);
                    assertThrows(SQLException.class, cs::execute,
                            "an OUT parameter must be refused with a clean error, not silently "
                                    + "mishandled, hung, or produce a corrupted response");
                }
            } finally {
                try (Connection cleanup = DriverManager.getConnection(
                        oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                        Statement stmt = cleanup.createStatement()) {
                    stmt.execute("DROP PROCEDURE plsql_call_out_it_proc");
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
