package com.sayonora.wire.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real regression guard for three bugs found live only through a STRICT real Oracle JDBC thin
 * client (ojdbc11) talking to orawire in TRANSLATED mode (WARP_ORACLE_BACKEND_MODE unset, i.e.
 * NOT native) with dual-exec enabled and Oracle as the authoritative backend:
 *
 * <ol>
 *   <li>the authoritative-Oracle {@code Statement} used to keep {@code sourceDialect=ORACLE} even
 *       though {@code RouterStage} always resolves orawire's unregistered native-default backend
 *       name back to "default" (Postgres) for dialect-translation purposes -- silently
 *       mistranslating genuine Oracle SQL into Postgres syntax before it ran against the REAL
 *       Oracle connection ({@code RequestLoop#handleExecute}'s own javadoc on {@code
 *       pipelineDialect} has the full root-cause writeup);
 *   <li>a column's real Oracle-reported unconstrained-NUMBER scale sentinel (-127, Oracle's own
 *       convention, as opposed to Postgres's equivalent 0) reached the wire encoder unclamped,
 *       producing a malformed DESCRIBE column packet ({@code RequestLoop#toColumnMetadata}'s own
 *       javadoc);
 *   <li>{@code executeShadow} used the wire's own {@code EXEC_OPTION_FETCH} bit (unreliable for a
 *       real ojdbc thin driver's combined describe/execute RPC) instead of {@code
 *       PreparedStatement.execute()}'s own return value to decide query vs. update against the
 *       shadow connection.
 * </ol>
 *
 * <p>Confirmed live before the fix: real SQLcl and real ojdbc11 both failed outright against this
 * exact setup running {@code select 1 from dual} (client-side crash/error -- ojdbc11 specifically
 * threw {@code ArrayIndexOutOfBoundsException: Index 8 out of bounds for length 8} inside {@code
 * T4CMAREngineNIO.buffer2Value}/{@code T4CTTIdcb.receive}), while a looser client (python-oracledb
 * thin mode) tolerated the malformed response and happened to succeed anyway -- which is exactly
 * why a strict OCI-family client (ojdbc11, used here) is required to catch this class of bug; a
 * looser client is not a valid regression guard for it.
 */
class OracleDualExecTranslatedModeDualIntegrationTest {

    @Test
    // RealOracle.start() alone can legitimately take up to 180s on a cold first-boot (creating the
    // whole DB) -- a 180s test timeout leaves zero headroom for the rest of the test body once that
    // happens (confirmed live: a run that hit a slow boot timed out here with the container still
    // mid-startup). 300s gives real headroom beyond the documented worst case.
    @Timeout(300)
    void realOjdbcThinClientSelectOneFromDualSucceedsInTranslatedModeWithOracleAuthority() throws Exception {
        try (RealOracle oracle = RealOracle.start();
                RealPostgres postgres = RealPostgres.start()) {

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(),
                            postgres.password())
                    .frontend("orawire", "WARP_ORAWIRE_PORT")
                    // Deliberately NOT setting WARP_ORACLE_BACKEND_MODE=native -- this is exactly
                    // the TRANSLATED-mode + dual-exec-authority=oracle combination the bugs above
                    // were only reachable through.
                    .env("WARP_DUAL_EXEC_ENABLED", "true")
                    .env("WARP_DUAL_EXEC_AUTHORITY", "oracle")
                    // Shadow execution left ON (unlike OraclePlsqlCallIntegrationTest, which turns
                    // it off) specifically to also exercise executeShadow's real-ResultSet-based
                    // query/update detection (bug #3) against the real Postgres shadow connection.
                    .env("WARP_DUAL_EXEC_SHADOW_ENABLED", "true")
                    // Avoids colliding with any other Warp instance (this dev-env demo server
                    // included) already bound to the default gRPC port 7070 -- same pattern
                    // WorkloadCaptureIntegrationTest already uses.
                    .env("WARP_GRPC_PORT", String.valueOf(findFreePort()))
                    .env("WARP_ORACLE_HOST", oracle.host())
                    .env("WARP_ORACLE_PORT", String.valueOf(oracle.port()))
                    .env("WARP_ORACLE_SERVICE", oracle.serviceName())
                    .env("WARP_ORACLE_USER", oracle.sysUsername())
                    .env("WARP_ORACLE_PASSWORD", oracle.sysPassword())
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("select 1 from dual")) {
                    assertTrue(rs.next(), "the real ojdbc11 client must receive a row, not crash or hang "
                            + "on a malformed DESCRIBE response");
                    assertEquals(1, rs.getInt(1));
                    assertFalse(rs.next(), "select 1 from dual must return exactly one row");
                }

                // A second, differently-shaped real query on a fresh statement/cursor -- proves the
                // fix isn't narrowly special-cased to the exact "select 1 from dual" text.
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("select 2+2 from dual")) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
