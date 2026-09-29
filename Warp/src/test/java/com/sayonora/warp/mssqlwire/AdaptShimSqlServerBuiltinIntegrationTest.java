package com.sayonora.warp.mssqlwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * mssqlwire's version of {@code com.sayonora.warp.orawire.AdaptShimPlsqlBuiltinIntegrationTest} /
 * {@code com.sayonora.warp.mywire.AdaptShimMysqlBuiltinIntegrationTest} -- real, live proof that
 * plain Adapt mode's always-on {@code SET db_emulation = 'sqlserver'} (see {@code
 * core.access.MssqlPgEmulationSessionInitializer}) actually reaches Shim/pg_sqlserver's real
 * T-SQL-compatible {@code sys.*} builtin functions.
 *
 * <p>Requires a real, locally built {@code pg_sqlserver} (and its {@code pg_oracle} dependency)
 * Postgres extension and {@code WARP_TEST_PG_LOCAL=1} -- see {@code
 * AdaptShimPlsqlBuiltinIntegrationTest}'s own javadoc for the exact env vars. Skips cleanly via
 * {@code assumeTrue} if unavailable.
 */
class AdaptShimSqlServerBuiltinIntegrationTest {

    @Test
    @Timeout(120)
    void adaptModeReachesRealSqlServerCompatibleBuiltinsViaShim() throws Exception {
        RealPostgres postgres;
        try {
            postgres = RealPostgres.start(java.util.List.of("shared_preload_libraries=pg_oracle"));
        } catch (Exception e) {
            assumeTrue(false, "requires a real pg_sqlserver-capable local Postgres (WARP_TEST_PG_LOCAL=1, "
                    + "WARP_TEST_PG_BIN set, pg_oracle+pg_sqlserver already built+installed) -- skipping: " + e);
            return;
        }
        try (RealPostgres pg = postgres) {
            try (Connection setup = DriverManager.getConnection(
                    "jdbc:postgresql://" + pg.host() + ":" + pg.port() + "/" + pg.database(),
                    pg.username(), pg.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_oracle CASCADE");
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_sqlserver CASCADE");
            } catch (SQLException e) {
                assumeTrue(false, "pg_sqlserver extension not available on this Postgres install -- skipping: " + e);
                return;
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                    .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire")
                        + ";encrypt=false;trustServerCertificate=true";

                try (Connection conn = DriverManager.getConnection(url, pg.username(), pg.password());
                        Statement st = conn.createStatement()) {

                    // CHARINDEX: T-SQL's own 1-based substring search with a start offset --
                    // plain Postgres's POSITION()/STRPOS() has no 3-argument (start-offset) form
                    // at all, so this proves pg_sqlserver's own real implementation, not a
                    // passthrough to an existing Postgres builtin.
                    try (ResultSet rs = st.executeQuery("SELECT CHARINDEX('lo', 'hello world', 4)")) {
                        assertEquals(true, rs.next());
                        assertEquals(4, rs.getInt(1),
                                "CHARINDEX must find 'lo' starting the search at position 4 -- "
                                        + "the real T-SQL 3-argument semantics, not the plain 2-arg search");
                    }

                    // LEN: T-SQL's own trailing-whitespace-trimming length -- distinct from
                    // plain Postgres LENGTH(), which does NOT trim trailing whitespace.
                    try (ResultSet rs = st.executeQuery("SELECT LEN('hello   ')")) {
                        assertEquals(true, rs.next());
                        assertEquals(5, rs.getInt(1),
                                "LEN() must trim trailing whitespace per real T-SQL semantics, unlike "
                                        + "plain Postgres LENGTH()");
                    }

                    // IIF: T-SQL's own inline conditional -- plain Postgres has no IIF() at all
                    // (only the CASE WHEN form).
                    //
                    // Real, live-discovered bug in Shim/pg_sqlserver, NOT a Warp defect, found
                    // while writing this test: sys.iif(p_condition boolean, p_true anyelement,
                    // p_false anyelement)'s generic ANYELEMENT signature cannot resolve two
                    // untyped string literals with no other type hint -- confirmed live via a real
                    // "ERROR: could not determine polymorphic type because input has type unknown"
                    // from Postgres itself on the exact call shape a real migrated app commonly
                    // sends (IIF(cond, 'literal1', 'literal2')). Real SQL Server's own IIF has no
                    // such restriction. Worked around here with an explicit cast on one branch
                    // (Postgres then infers the other literal's type from it, same trick CASE WHEN
                    // needs in the same situation) to prove the underlying dispatch mechanism does
                    // work once the type is resolvable -- but this is a genuine pg_sqlserver
                    // usability gap for the common two-literal call shape, not fixed here (out of
                    // Warp's own scope: this lives in Shim/pg_sqlserver's own SQL source, a
                    // separate repo/deliverable).
                    try (ResultSet rs = st.executeQuery("SELECT IIF(1 > 0, CAST('yes' AS text), 'no')")) {
                        assertEquals(true, rs.next());
                        assertEquals("yes", rs.getString(1));
                    }

                    // REPLICATE: T-SQL's own string-repeat -- plain Postgres has REPEAT(), a
                    // different name pg_sqlserver's sys.replicate exposes under the T-SQL name.
                    try (ResultSet rs = st.executeQuery("SELECT REPLICATE('ab', 3)")) {
                        assertEquals(true, rs.next());
                        assertEquals("ababab", rs.getString(1));
                    }
                }
            }
        }
    }
}
