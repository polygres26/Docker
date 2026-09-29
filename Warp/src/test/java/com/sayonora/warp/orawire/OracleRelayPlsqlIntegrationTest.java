package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.warp.testsupport.RealOracle;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import oracle.jdbc.OracleTypes;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Sanity-check regression test for Relay mode ({@code WARP_ORACLE_BACKEND_MODE=relay}) -- the one
 * mode that is architecturally NOT expected to need any PL/SQL-specific handling at all, since
 * {@code SessionHandler.run} routes it through {@code NativeSessionRelay}'s raw byte pump straight
 * to a real Oracle instance, bypassing {@code RequestLoop}/{@code handlePlSqlExecute} entirely (see
 * [[warp-plsql-support-matrix]]). This test proves that assumption holds for exactly the PL/SQL
 * shapes that are REFUSED (not silently broken, but genuinely unsupported) in both Bridge mode
 * ({@link OracleBridgePlsqlCallIntegrationTest}) and Adapt+dual-exec
 * ({@link OraclePlsqlCallIntegrationTest}) -- a real multi-statement anonymous block with a loop, a
 * package-qualified procedure/function call, a REF CURSOR OUT parameter returning multiple rows,
 * and multiple OUT parameters in one call -- all of which must simply work end-to-end through
 * Relay, confirming it really is a dumb, transparent proxy with none of the other two modes' own
 * wire-protocol-reimplementation gaps.
 */
class OracleRelayPlsqlIntegrationTest {

    private static WarpProcess.Builder relayWarp(RealOracle oracle, RealPostgres postgres) {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(),
                        postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .env("WARP_ORACLE_BACKEND_MODE", "relay")
                .env("WARP_ORACLE_HOST", oracle.host())
                .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                .env("WARP_OTEL_ENDPOINT", "disabled");
    }

    @Test
    @Timeout(180)
    void relayModePassesThroughEverythingBridgeAndAdaptRefuse() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(
                    oracle.sysJdbcUrl(), oracle.sysUsername(), oracle.sysPassword());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE PACKAGE relay_it_pkg AS "
                        + "v_counter NUMBER := 0; "
                        + "PROCEDURE increment; "
                        + "FUNCTION get_counter RETURN NUMBER; "
                        + "END relay_it_pkg;");
                stmt.execute("CREATE PACKAGE BODY relay_it_pkg AS "
                        + "PROCEDURE increment AS BEGIN v_counter := v_counter + 1; END; "
                        + "FUNCTION get_counter RETURN NUMBER AS BEGIN RETURN v_counter; END; "
                        + "END relay_it_pkg;");
                stmt.execute("CREATE PROCEDURE relay_it_refcursor_proc(p_cur OUT SYS_REFCURSOR) AS "
                        + "BEGIN OPEN p_cur FOR SELECT 1 AS x FROM DUAL UNION ALL SELECT 2 FROM DUAL; END;");
                stmt.execute("CREATE PROCEDURE relay_it_multiout_proc(p_in IN NUMBER, p_out1 OUT NUMBER, "
                        + "p_out2 OUT NUMBER) AS BEGIN p_out1 := p_in * 2; p_out2 := p_in * 3; END;");
            }

            try (WarpProcess warp = relayWarp(oracle, postgres).start()) {
                // Unlike Bridge/Adapt mode (which don't forward the service name to a real
                // listener at all, so "anything" works as a placeholder there), Relay is a raw
                // byte proxy that forwards the CONNECT packet's service name VERBATIM to real
                // Oracle -- it must be the real, registered service name.
                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/" + oracle.serviceName();

                // A genuine multi-statement anonymous block with real control-flow logic -- refused
                // outright in Bridge/Adapt (only a single procedure/function CALL is supported
                // there); Relay must just run it against real Oracle. Package state (v_counter)
                // is real Oracle session state -- like real Oracle itself, it only persists within
                // the SAME physical session/connection, not across separate ones, so the increments
                // and the read-back below share one connection rather than using a fresh one each
                // time (the earlier version of this test wrongly expected cross-connection
                // persistence, which isn't how Oracle package state works even with no Warp
                // involved at all).
                try (Connection conn = DriverManager.getConnection(url, oracle.sysUsername(), oracle.sysPassword())) {
                    try (Statement st = conn.createStatement()) {
                        st.execute("DECLARE v_total NUMBER := 0; BEGIN "
                                + "FOR i IN 1..5 LOOP v_total := v_total + i; END LOOP; "
                                + "relay_it_pkg.increment; relay_it_pkg.increment; relay_it_pkg.increment; "
                                + "END;");
                    }

                    // Package-qualified function call -- refused in Bridge/Adapt (the
                    // procedure-name resolver's regex doesn't recognize a dotted name at all).
                    try (CallableStatement cs = conn.prepareCall("{? = call relay_it_pkg.get_counter}")) {
                        cs.registerOutParameter(1, Types.NUMERIC);
                        cs.execute();
                        assertEquals(3, cs.getInt(1),
                                "the package's own counter must reflect the 3 real increments made above, "
                                        + "within the same real Oracle session");
                    }
                }

                // REF CURSOR OUT parameter returning multiple rows -- refused in Bridge/Adapt (both
                // real attempts at this response shape crashed a real JDBC client).
                try (Connection conn = DriverManager.getConnection(url, oracle.sysUsername(), oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call relay_it_refcursor_proc(?)}")) {
                    cs.registerOutParameter(1, OracleTypes.CURSOR);
                    cs.execute();
                    try (ResultSet rs = (ResultSet) cs.getObject(1)) {
                        assertEquals(1, firstRow(rs));
                        assertEquals(2, nextRow(rs));
                    }
                }

                // Multiple OUT parameters in one call -- refused in Bridge/Adapt ("only a single
                // scalar OUT parameter per call has been verified").
                try (Connection conn = DriverManager.getConnection(url, oracle.sysUsername(), oracle.sysPassword());
                        CallableStatement cs = conn.prepareCall("{call relay_it_multiout_proc(?, ?, ?)}")) {
                    cs.setInt(1, 5);
                    cs.registerOutParameter(2, Types.NUMERIC);
                    cs.registerOutParameter(3, Types.NUMERIC);
                    cs.execute();
                    assertEquals(10, cs.getInt(2), "first OUT parameter must carry the real computed value");
                    assertEquals(15, cs.getInt(3), "second OUT parameter must carry the real computed value");
                }
            }
        }
    }

    private static int firstRow(ResultSet rs) throws java.sql.SQLException {
        org.junit.jupiter.api.Assertions.assertTrue(rs.next());
        return rs.getInt(1);
    }

    private static int nextRow(ResultSet rs) throws java.sql.SQLException {
        org.junit.jupiter.api.Assertions.assertTrue(rs.next());
        return rs.getInt(1);
    }
}
