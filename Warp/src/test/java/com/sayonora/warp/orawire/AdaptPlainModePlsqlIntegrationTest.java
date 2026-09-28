package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.SqlStateErrorMapper;
import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Confirms the exact, currently-undocumented-by-any-test failure shape for a PL/SQL block sent to
 * PLAIN Adapt mode -- the actual default configuration (no dual-exec, no real Oracle backend at
 * all, {@code oracleConnection == null} in {@code RequestLoop}) -- as distinct from
 * {@link OraclePlsqlCallIntegrationTest} (dual-exec + Oracle authority, a real Oracle connection
 * IS present) and {@link com.sayonora.warp.orawire.backend.OracleBridgePoolTest}/
 * {@link OracleBridgePlsqlCallIntegrationTest} (Bridge mode).
 *
 * <p>Confirmed live (2026-09-28): {@code isPlSqlBlock} is a purely textual {@code BEGIN}/{@code
 * DECLARE} prefix check, run unconditionally in {@code handleExecute} regardless of backend mode
 * -- so EVERY PL/SQL-shaped statement (a genuine anonymous block, or the {@code BEGIN
 * proc(:1); END;} shape a JDBC {@code CallableStatement} produces) reaches {@code
 * handlePlSqlExecute}, which immediately throws a clean {@code IllegalStateException} the moment it
 * sees {@code oracleConnection == null} -- there is no dialect-translation pass-through attempt at
 * all for plain Adapt mode (an earlier working theory that PL/SQL might fall through to the
 * generic, non-PL/SQL-aware translator and get forwarded to Postgres as literal text was WRONG --
 * {@code isPlSqlBlock} always intercepts first). The refusal is clean and immediate regardless of
 * whether the named procedure exists at all.
 *
 * <p>Real, separate, pre-existing usability bug found while confirming this (NOT fixed here --
 * out of scope, far broader than PL/SQL): {@code RequestLoop}'s generic {@code catch
 * (RuntimeException e)} handler hardcodes Oracle error code 942 (ORA-00942, "table or view does
 * not exist") for EVERY uncaught {@code RuntimeException} anywhere in the request-handling path,
 * not just this one. The real, correct, helpful message text DOES come through intact via {@code
 * SQLException.getMessage()}, but any application branching on {@code SQLException.getErrorCode()
 * == 942} (a common real-world pattern for "handle missing table") would misfire on this
 * completely unrelated refusal. See the follow-up task filed for widening this beyond a hardcoded
 * generic code.
 */
class AdaptPlainModePlsqlIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) {
            warp.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    private Connection connect() throws SQLException {
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        return DriverManager.getConnection(url, postgres.username(), postgres.password());
    }

    @Test
    @Timeout(60)
    void anonymousBlockIsRefusedCleanlyWithAHelpfulMessage() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> st.execute("BEGIN NULL; END;"),
                    "a genuine anonymous PL/SQL block must be refused cleanly in plain Adapt mode, "
                            + "not silently pass through to Postgres as literal text");
            assertTrue(e.getMessage().contains("PL/SQL execution requires a real Oracle backend"),
                    "the refusal message must clearly explain WHY, not just fail generically: " + e.getMessage());
        }
    }

    @Test
    @Timeout(60)
    void procedureCallShapeIsRefusedCleanlyEvenWhenTheProcedureDoesNotExist() throws Exception {
        try (Connection conn = connect();
                CallableStatement cs = conn.prepareCall("{call nonexistent_proc(?)}")) {
            cs.setInt(1, 1);
            SQLException e = assertThrows(SQLException.class, cs::execute,
                    "a JDBC CallableStatement's own BEGIN proc(:1); END; shape must also be refused "
                            + "cleanly -- the refusal happens before any procedure-name resolution, "
                            + "so it must not depend on the procedure actually existing");
            assertTrue(e.getMessage().contains("PL/SQL execution requires a real Oracle backend"),
                    "the refusal message must clearly explain WHY, not just fail generically: " + e.getMessage());
        }
    }

    /** Locks in the fix for a real, separate, pre-existing usability bug found while confirming
     * the refusal shape above: {@code RequestLoop}'s generic {@code catch (RuntimeException e)}
     * handler used to hardcode Oracle error code 942 (ORA-00942, "table or view does not exist")
     * for EVERY uncaught {@code RuntimeException} anywhere in the request-handling path, not just
     * this one -- so any application branching on {@code SQLException.getErrorCode() == 942} (a
     * common real-world pattern for "handle missing table") would misfire on this completely
     * unrelated refusal. It now reports {@link SqlStateErrorMapper#ORACLE_INTERNAL_ERROR}
     * (ORA-00600, Oracle's own convention for an internal error) instead, while the real, correct,
     * helpful message text still comes through intact via {@code SQLException.getMessage()}. */
    @Test
    @Timeout(60)
    void plsqlRefusalReportsInternalErrorCodeNotTheUnrelatedMissingTableCode() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> st.execute("BEGIN NULL; END;"));
            assertNotEquals(SqlStateErrorMapper.ORACLE_DEFAULT, e.getErrorCode(),
                    "an unrelated internal RuntimeException (here, the PL/SQL refusal) must not be "
                            + "reported as ORA-00942 'table or view does not exist' -- real "
                            + "application code commonly branches on getErrorCode() == 942 "
                            + "specifically to detect a missing table, and would misfire on this");
            assertEquals(SqlStateErrorMapper.ORACLE_INTERNAL_ERROR, e.getErrorCode(),
                    "an uncaught RuntimeException with no mapped SQLSTATE should report Oracle's "
                            + "own real 'internal error' convention (ORA-00600)");
            assertTrue(e.getMessage().contains("PL/SQL execution requires a real Oracle backend"),
                    "the real, helpful message text must still come through unchanged: " + e.getMessage());
        }
    }
}
