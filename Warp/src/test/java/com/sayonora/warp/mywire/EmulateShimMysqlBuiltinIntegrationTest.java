package com.sayonora.warp.mywire;

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
 * mywire's version of {@code com.sayonora.warp.orawire.AdaptShimPlsqlBuiltinIntegrationTest} --
 * real, live proof that plain Emulate mode's always-on {@code SET db_emulation = 'mysql'} (see
 * {@code core.access.MySqlPgEmulationSessionInitializer}) actually reaches Shim/pg_mysql's real
 * MySQL-compatible builtin functions, unlike Oracle's Shim reach this has NO syntax gap to close
 * (a plain {@code SELECT LAST_INSERT_ID()} already parses fine against Postgres) -- what's actually
 * being proven here is that pg_mysql's real, unqualified-name-resolving implementations of
 * MySQL-specific builtins (which plain Postgres has no equivalent for at all) genuinely produce
 * MySQL-correct results, not just "the statement didn't error."
 *
 * <p>Requires a real, locally built {@code pg_mysql} (and its {@code pg_oracle} dependency)
 * Postgres extension and {@code WARP_TEST_PG_LOCAL=1} -- see {@code AdaptShimPlsqlBuiltinIntegrationTest}'s
 * own javadoc for the exact env vars. Skips cleanly via {@code assumeTrue} if unavailable.
 */
class EmulateShimMysqlBuiltinIntegrationTest {

    @Test
    @Timeout(120)
    void adaptModeReachesRealMysqlCompatibleBuiltinsViaShim() throws Exception {
        RealPostgres postgres;
        try {
            postgres = RealPostgres.start(java.util.List.of("shared_preload_libraries=pg_oracle"));
        } catch (Exception e) {
            assumeTrue(false, "requires a real pg_mysql-capable local Postgres (WARP_TEST_PG_LOCAL=1, "
                    + "WARP_TEST_PG_BIN set, pg_oracle+pg_mysql already built+installed) -- skipping: " + e);
            return;
        }
        try (RealPostgres pg = postgres) {
            try (Connection setup = DriverManager.getConnection(
                    "jdbc:postgresql://" + pg.host() + ":" + pg.port() + "/" + pg.database(),
                    pg.username(), pg.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_oracle CASCADE");
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_mysql CASCADE");
            } catch (SQLException e) {
                assumeTrue(false, "pg_mysql extension not available on this Postgres install -- skipping: " + e);
                return;
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                    .frontend("mywire", "WARP_MYWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:mysql://localhost:" + warp.port("mywire")
                        + "/" + pg.database() + "?useSSL=false&allowPublicKeyRetrieval=true";

                try (Connection conn = DriverManager.getConnection(url, pg.username(), pg.password());
                        Statement st = conn.createStatement()) {

                    // LAST_INSERT_ID(): plain Postgres has no equivalent concept at all (no
                    // implicit last-generated-value tracking without naming the sequence) -- this
                    // proves pg_mysql's real session-scoped tracking, wired to fire on every
                    // INSERT via the emulation's own hook, actually works end-to-end through
                    // mywire's real MySQL wire protocol.
                    st.execute("CREATE TABLE last_id_it (id SERIAL PRIMARY KEY, val TEXT)");
                    st.execute("INSERT INTO last_id_it (val) VALUES ('a')");
                    long firstId;
                    try (ResultSet rs = st.executeQuery("SELECT LAST_INSERT_ID()")) {
                        assertEquals(true, rs.next());
                        firstId = rs.getLong(1);
                    }
                    st.execute("INSERT INTO last_id_it (val) VALUES ('b')");
                    try (ResultSet rs = st.executeQuery("SELECT LAST_INSERT_ID()")) {
                        assertEquals(true, rs.next());
                        assertEquals(firstId + 1, rs.getLong(1),
                                "LAST_INSERT_ID() must track the most recent INSERT's generated id, "
                                        + "a real MySQL semantic plain Postgres has no equivalent for");
                    }

                    // GROUP_CONCAT: a real CREATE AGGREGATE, not a scalar function -- proves the
                    // emulation's unqualified-name resolution works for aggregates too, not just
                    // plain function calls.
                    st.execute("CREATE TABLE group_concat_it (grp INT, val TEXT)");
                    st.execute("INSERT INTO group_concat_it VALUES (1, 'x'), (1, 'y'), (1, 'z')");
                    try (ResultSet rs = st.executeQuery(
                            "SELECT GROUP_CONCAT(val) FROM group_concat_it WHERE grp = 1")) {
                        assertEquals(true, rs.next());
                        assertEquals("x,y,z", rs.getString(1));
                    }

                    // DATE_FORMAT: MySQL's own format-string dialect (%Y-%m-%d), which plain
                    // Postgres's to_char() does not understand at all -- proves pg_mysql's real
                    // format-string translation, not just a passthrough.
                    try (ResultSet rs = st.executeQuery(
                            "SELECT DATE_FORMAT(TIMESTAMP '2026-03-04 05:06:07', '%Y-%m-%d')")) {
                        assertEquals(true, rs.next());
                        assertEquals("2026-03-04", rs.getString(1));
                    }
                }
            }
        }
    }
}
