package com.sayonora.warp.orawire;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real, live proof that plain Adapt mode (no dual-exec, no real Oracle backend at all) can reach
 * Shim/pg_oracle's genuine DBMS_* builtin implementations for the narrow, first-slice shape
 * described in the {@code warp-adapt-plsql-shim-reach-plan} memory note: a single call to a KNOWN
 * Shim builtin ({@link com.sayonora.warp.core.ShimBuiltinCatalog}), rewritten into a plain,
 * parameterized Postgres function call instead of {@code handlePlSqlExecute}'s previously-universal
 * refusal.
 *
 * <p>Requires a real, locally built {@code pg_oracle} Postgres extension (see
 * {@code Shim/pg_oracle/README.md}'s "Building" section -- {@code make && make install} against
 * this host's own Postgres install) and {@code WARP_TEST_PG_LOCAL=1} (local-process Postgres,
 * {@code WARP_TEST_PG_BIN} pointing at that Postgres's {@code bin} directory) -- there is no ready
 * Docker image with pg_oracle baked in yet (a real, disclosed gap in the test infra, not this
 * feature). Skips cleanly via {@code assumeTrue} if either isn't available, rather than failing a
 * CI run that doesn't have pg_oracle built.
 */
class AdaptShimPlsqlBuiltinIntegrationTest {

    @Test
    @Timeout(120)
    void plainAdaptModeReachesShimBuiltinInsteadOfRefusing() throws Exception {
        RealPostgres postgres;
        try {
            postgres = RealPostgres.start(java.util.List.of("shared_preload_libraries=pg_oracle"));
        } catch (Exception e) {
            assumeTrue(false, "requires a real pg_oracle-capable local Postgres (WARP_TEST_PG_LOCAL=1, "
                    + "WARP_TEST_PG_BIN set, pg_oracle already built+installed) -- skipping: " + e);
            return;
        }
        try (RealPostgres pg = postgres) {
            try (Connection setup = DriverManager.getConnection(
                    "jdbc:postgresql://" + pg.host() + ":" + pg.port() + "/" + pg.database(),
                    pg.username(), pg.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_oracle CASCADE");
            } catch (SQLException e) {
                assumeTrue(false, "pg_oracle extension not available on this Postgres install -- skipping: " + e);
                return;
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                    .frontend("orawire", "WARP_ORAWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";

                // The primary, confirmed-working shape: a void procedure call (no return value),
                // the most commonly-needed Shim builtin for migrated application logging.
                try (Connection conn = DriverManager.getConnection(url, pg.username(), pg.password());
                        CallableStatement cs = conn.prepareCall("{call dbms_output.put_line(?)}")) {
                    cs.setString(1, "hello from AdaptShimPlsqlBuiltinIntegrationTest");
                    cs.execute();
                }

                // A known-unsupported builtin (not in the allowlist) must still fall through to
                // the existing clean refusal, not a raw, ungraceful Postgres "function does not
                // exist" error -- confirms this feature doesn't loosen the refusal for anything
                // outside its own narrow, hand-maintained catalog.
                try (Connection conn = DriverManager.getConnection(url, pg.username(), pg.password());
                        Statement st = conn.createStatement()) {
                    SQLException e = assertThrows(SQLException.class,
                            () -> st.execute("BEGIN dbms_lock.sleep(1); END;"),
                            "an unsupported builtin (not in ShimBuiltinCatalog's allowlist) must still be "
                                    + "refused cleanly, not silently routed to Postgres as a raw function call");
                    org.junit.jupiter.api.Assertions.assertTrue(
                            e.getMessage().contains("PL/SQL execution requires a real Oracle backend"),
                            "the refusal message must be the same clean, helpful one plain Adapt mode "
                                    + "always gives for anything outside this feature's narrow scope: "
                                    + e.getMessage());
                }
            }
        }
    }
}
