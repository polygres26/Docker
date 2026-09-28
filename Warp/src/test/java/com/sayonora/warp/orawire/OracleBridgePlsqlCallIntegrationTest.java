package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealOracle;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import oracle.jdbc.OracleTypes;
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
 * Real proof that a real ojdbc {@code CallableStatement} call to a PL/SQL procedure works through
 * orawire's BRIDGE mode specifically (verbatim execution against a real Oracle backend, via
 * {@code OracleBridgePool} -- see {@code ServerOptions.OracleBackendMode.BRIDGE}) -- not just under
 * dual-exec/Adapt, which {@link OraclePlsqlCallIntegrationTest} already covers. Both modes populate
 * {@code RequestLoop#oracleConnection} and so share the exact same {@code handlePlSqlExecute} code
 * path, but that had never actually been exercised live under Bridge mode specifically until this
 * class -- confirmed live (2026-09-28) via a real ad hoc JDBC client against a real running Bridge
 * instance before this test was written, then locked in here as a permanent regression test.
 *
 * <p>Covers everything {@link OraclePlsqlCallIntegrationTest} covers (IN-only call, single scalar
 * OUT parameter, REF CURSOR clean refusal) plus three shapes that class doesn't test at all:
 * multiple OUT parameters (clean refusal), a package-qualified procedure call (clean refusal --
 * {@code OracleProcedureCatalog}'s name-resolution regex doesn't match a dotted name at all), and a
 * function call via the {@code {? = call func(...)}} JDBC shape (also refused -- the RETURN-value
 * binding shape doesn't match the same regex either, a distinct gap from the package-qualified one
 * even though both currently produce the same class of refusal).
 */
class OracleBridgePlsqlCallIntegrationTest {

    private static WarpProcess.Builder bridgeWarp(RealOracle oracle, RealPostgres postgres) {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(),
                        postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_ORACLE_BACKEND_MODE", "bridge")
                .env("WARP_ORACLE_HOST", oracle.host())
                .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                .env("WARP_ORACLE_USER", oracle.sysUsername())
                .env("WARP_ORACLE_PASSWORD", oracle.sysPassword())
                .env("WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS", "app_user1=" + oracle.sysPassword())
                .env("WARP_OTEL_ENDPOINT", "disabled");
    }

    @Test
    @Timeout(180)
    void bridgeModeInOnlyAndSingleOutParameterWork() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                try {
                    stmt.execute("DROP PROCEDURE bridge_it_in_proc");
                } catch (SQLException ignored) {
                }
                try {
                    stmt.execute("DROP PROCEDURE bridge_it_out_proc");
                } catch (SQLException ignored) {
                }
                try {
                    stmt.execute("DROP TABLE bridge_it");
                } catch (SQLException ignored) {
                }
                stmt.execute("CREATE TABLE bridge_it (id NUMBER PRIMARY KEY, val NUMBER)");
                stmt.execute("CREATE PROCEDURE bridge_it_in_proc(p_id IN NUMBER, p_val IN NUMBER) AS "
                        + "BEGIN INSERT INTO bridge_it (id, val) VALUES (p_id, p_val); END;");
                stmt.execute("CREATE PROCEDURE bridge_it_out_proc(p_in IN NUMBER, p_out OUT NUMBER) AS "
                        + "BEGIN p_out := p_in * 2; END;");
                // Real ojdbc client credential -- Bridge mode logs the client in as a real Oracle
                // user (see WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS above), distinct from Adapt/dual-exec's
                // Postgres-side credential. Uses the same SYS password for simplicity in this test only.
                try {
                    stmt.execute("CREATE USER app_user1 IDENTIFIED BY \"" + oracle.sysPassword() + "\"");
                } catch (SQLException ignored) {
                }
                stmt.execute("GRANT CREATE SESSION TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_it_in_proc TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_it_out_proc TO app_user1");
                stmt.execute("GRANT SELECT, INSERT ON bridge_it TO app_user1");
            }

            try (WarpProcess warp = bridgeWarp(oracle, postgres).start()) {
                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call bridge_it_in_proc(?, ?)}")) {
                    cs.setInt(1, 1);
                    cs.setInt(2, 42);
                    cs.execute();
                }

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call bridge_it_out_proc(?, ?)}")) {
                    cs.setInt(1, 21);
                    cs.registerOutParameter(2, Types.NUMERIC);
                    cs.execute();
                    assertEquals(42, cs.getInt(2), "OUT parameter must carry the procedure's real computed value");
                }
            }

            try (Connection check = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = check.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT val FROM bridge_it WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1), "the IN-only procedure's real INSERT must have landed on the real backend");
            }
        }
    }

    @Test
    @Timeout(180)
    void bridgeModeRefCursorAndMultiOutAreRefusedCleanly() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE PROCEDURE bridge_refcursor_proc(p_cur OUT SYS_REFCURSOR) AS "
                        + "BEGIN OPEN p_cur FOR SELECT 1 FROM DUAL; END;");
                stmt.execute("CREATE PROCEDURE bridge_multiout_proc(p_in IN NUMBER, p_out1 OUT NUMBER, "
                        + "p_out2 OUT NUMBER) AS BEGIN p_out1 := p_in; p_out2 := p_in; END;");
                try {
                    stmt.execute("CREATE USER app_user1 IDENTIFIED BY \"" + oracle.sysPassword() + "\"");
                } catch (SQLException ignored) {
                }
                stmt.execute("GRANT CREATE SESSION TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_refcursor_proc TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_multiout_proc TO app_user1");
            }

            try (WarpProcess warp = bridgeWarp(oracle, postgres).start()) {
                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call bridge_refcursor_proc(?)}")) {
                    cs.registerOutParameter(1, OracleTypes.CURSOR);
                    assertThrows(SQLException.class, cs::execute,
                            "a REF CURSOR OUT parameter must be refused with a clean error under Bridge mode too");
                }

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call bridge_multiout_proc(?, ?, ?)}")) {
                    cs.setInt(1, 5);
                    cs.registerOutParameter(2, Types.NUMERIC);
                    cs.registerOutParameter(3, Types.NUMERIC);
                    assertThrows(SQLException.class, cs::execute,
                            "multiple OUT parameters must be refused with a clean error under Bridge mode too");
                }
            }
        }
    }

    /** Package-qualified calls and function-return-value calls are both refused today -- neither
     * shape matches {@code OracleProcedureCatalog}'s name-resolution regex (a bare, non-dotted
     * procedure name only). This test locks in that both fail CLEANLY (a real SQLException) rather
     * than hanging or corrupting the session -- not that either capability works. See
     * {@code OracleProcedureCatalog}'s own javadoc for the disclosed scope. */
    @Test
    @Timeout(180)
    void bridgeModePackageQualifiedAndFunctionCallsAreRefusedCleanly() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE PACKAGE bridge_pkg AS PROCEDURE pkg_proc(p_in IN NUMBER); END bridge_pkg;");
                stmt.execute("CREATE PACKAGE BODY bridge_pkg AS "
                        + "PROCEDURE pkg_proc(p_in IN NUMBER) AS BEGIN NULL; END; END bridge_pkg;");
                stmt.execute("CREATE FUNCTION bridge_func(p_in IN NUMBER) RETURN NUMBER AS "
                        + "BEGIN RETURN p_in * 2; END;");
                try {
                    stmt.execute("CREATE USER app_user1 IDENTIFIED BY \"" + oracle.sysPassword() + "\"");
                } catch (SQLException ignored) {
                }
                stmt.execute("GRANT CREATE SESSION TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_pkg TO app_user1");
                stmt.execute("GRANT EXECUTE ON bridge_func TO app_user1");
            }

            try (WarpProcess warp = bridgeWarp(oracle, postgres).start()) {
                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call bridge_pkg.pkg_proc(?)}")) {
                    cs.setInt(1, 1);
                    assertThrows(SQLException.class, cs::execute,
                            "a package-qualified procedure call must be refused cleanly, not silently wrong");
                }

                try (Connection conn = DriverManager.getConnection(url, "app_user1", oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{? = call bridge_func(?)}")) {
                    cs.registerOutParameter(1, Types.NUMERIC);
                    cs.setInt(2, 10);
                    assertThrows(SQLException.class, cs::execute,
                            "a function-return-value call must be refused cleanly, not silently wrong");
                }
            }
        }
    }
}
