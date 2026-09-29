package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Real, live proof that mywire/mssqlwire's Adapt mode does NOT assume a Shim extension
 * (pg_mysql/pg_sqlserver) is installed on the backend Postgres -- the scenario raised directly:
 * "Adapt should not always assume pg_* modules are linked because Warp can be run against Supabase
 * or RDS Postgres etc" (neither of those, nor Cloud SQL or Azure Database for PostgreSQL, allow
 * installing a third-party C extension at all). Uses a completely plain {@link RealPostgres} --
 * no {@code shared_preload_libraries}, nothing installed -- to genuinely simulate that class of
 * managed provider, not just "an unconfigured local Postgres."
 *
 * <p>Before the fix this test locks in: {@code DialectTranslations.normalizeMysql()}'s SHOW
 * COLUMNS/DESCRIBE/SHOW INDEX/SHOW VARIABLES/SHOW CREATE TABLE rewrites, and {@code
 * normalizeSqlServer()}'s {@code @@IDENTITY} rewrite, unconditionally targeted {@code
 * mysql_catalog.*}/{@code sys.*} functions with no availability check at all -- unlike {@code
 * normalizeOracle()}'s already-correctly-gated {@code TO_CHAR}/{@code TO_DATE} rewrite. Against a
 * plain Postgres, those statements used to fail with a raw, confusing "schema mysql_catalog does
 * not exist" error instead of either a clear, actionable refusal (MySQL's SHOW-style statements,
 * which have no plain-Postgres equivalent to fall back to) or a real, working degraded translation
 * (SQL Server's {@code @@IDENTITY}, which DOES have one: plain Postgres's own {@code lastval()}).
 */
class AdaptWithoutShimIntegrationTest {

    @Test
    @Timeout(120)
    void mysqlShowColumnsRefusesCleanlyInsteadOfReferencingAMissingSchema() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            try (Connection setup = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE shim_gap_it (id INT PRIMARY KEY, val TEXT)");
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mywire", "WARP_MYWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {
                String url = "jdbc:mysql://localhost:" + warp.port("mywire") + "/" + postgres.database()
                        + "?useSSL=false&allowPublicKeyRetrieval=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {
                    SQLException e = assertThrows(SQLException.class,
                            () -> st.executeQuery("SHOW COLUMNS FROM shim_gap_it"),
                            "SHOW COLUMNS must be refused cleanly against a plain Postgres backend with "
                                    + "no pg_mysql installed, not silently generate SQL referencing a "
                                    + "schema that doesn't exist");
                    assertTrue(e.getMessage().contains("pg_mysql") && e.getMessage().contains("Supabase"),
                            "the refusal must clearly name the real cause (missing pg_mysql) and the "
                                    + "real-world scenario (managed Postgres providers), not a raw "
                                    + "'schema mysql_catalog does not exist': " + e.getMessage());
                }
            }
        }
    }

    @Test
    @Timeout(120)
    void sqlServerAtAtIdentityFallsBackToRealPostgresLastvalInsteadOfFailing() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            try (Connection setup = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE shim_gap_identity_it (id SERIAL PRIMARY KEY, val TEXT)");
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {
                String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;trustServerCertificate=true";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO shim_gap_identity_it (val) VALUES ('a')");
                    long generatedId;
                    try (ResultSet rs = st.executeQuery("SELECT id FROM shim_gap_identity_it WHERE val = 'a'")) {
                        assertTrue(rs.next());
                        generatedId = rs.getLong(1);
                    }
                    try (ResultSet rs = st.executeQuery("SELECT @@IDENTITY")) {
                        assertTrue(rs.next(),
                                "@@IDENTITY must fall back to plain Postgres's own lastval() and still "
                                        + "work against a backend with no pg_sqlserver installed, not fail "
                                        + "outright referencing a missing sys schema");
                        assertEquals(generatedId, rs.getLong(1),
                                "the degraded lastval()-based @@IDENTITY must still return the real, "
                                        + "correct last-generated id");
                    }
                }
            }
        }
    }
}
