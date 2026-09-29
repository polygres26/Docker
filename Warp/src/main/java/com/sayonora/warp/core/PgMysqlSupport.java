package com.sayonora.warp.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects whether the {@code pg_mysql} extension (Shim/pg_mysql) is installed on a given backend
 * Postgres database -- the mywire equivalent of {@link PgOracleSupport}, same reason: Warp can
 * be deployed against a plain, unmodified Postgres with no pg_mysql extension at all, and without
 * detecting that up front, {@code MySqlPgEmulationSessionInitializer}'s {@code SET db_emulation =
 * 'mysql'} would fail every single statement outright (either because db_emulation itself has no
 * 'mysql' enum value on an older/absent pg_oracle -- see pg_mysql's own control file for why that
 * dependency exists -- or, on a plain Postgres backend, because db_emulation doesn't exist as a
 * GUC at all).
 *
 * <p>Checks for {@code pg_mysql} specifically, not {@code pg_oracle} -- pg_mysql's own {@code
 * requires = 'pg_oracle'} dependency (see its control file) means a successfully created pg_mysql
 * extension already implies a compatible pg_oracle is present too, so this single check is
 * sufficient; the reverse isn't true (pg_oracle can be installed alone, for orawire-only
 * deployments, with no pg_mysql and no 'mysql' emulation support at all).
 */
public final class PgMysqlSupport {

    private PgMysqlSupport() {
    }

    private static final String PROBE_SQL = "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_mysql'";

    private static final ConcurrentHashMap<String, Boolean> AVAILABLE_CACHE = new ConcurrentHashMap<>();

    /** For call sites that only have a {@link BackendTarget}. */
    public static boolean isAvailable(BackendTarget target) throws SQLException {
        Boolean cached = AVAILABLE_CACHE.get(target.jdbcUrl());
        if (cached != null) {
            return cached;
        }
        try (Connection connection = target.open()) {
            return cacheResult(target.jdbcUrl(), probe(connection));
        }
    }

    /** For call sites that already hold an open {@link Connection}. */
    public static boolean isAvailable(Connection connection) throws SQLException {
        String key = connection.getMetaData().getURL();
        Boolean cached = AVAILABLE_CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        return cacheResult(key, probe(connection));
    }

    private static boolean probe(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(PROBE_SQL)) {
            return rs.next();
        }
    }

    private static boolean cacheResult(String key, boolean available) {
        AVAILABLE_CACHE.put(key, available);
        return available;
    }

    // Consulted by DialectTranslations.normalizeMysql(), which has no Connection/BackendTarget of
    // its own -- same pattern as PgOracleSupport.CURRENT_STATEMENT_AVAILABLE, see that class's own
    // javadoc for why this has to be a ThreadLocal set one pipeline stage earlier
    // (DialectTranslationStage.handle()) rather than passed as a parameter. Defaults to true
    // (assume installed) for the same reason: unchanged behavior for the common case and for any
    // call site (tests, other pipelines) that never sets it.
    //
    // Real, live-found gap this closes (2026-09-29, raised directly: "Emulate should not always
    // assume pg_* modules are linked because Warp can be run against Supabase or RDS Postgres"):
    // normalizeMysql()'s SHOW COLUMNS/DESCRIBE/SHOW INDEX/SHOW VARIABLES/SHOW CREATE TABLE rewrites
    // used to unconditionally target mysql_catalog.* functions with NO availability check at all --
    // unlike normalizeOracle()'s TO_CHAR/TO_DATE rewrite, which was already correctly gated behind
    // PgOracleSupport. Against a real managed Postgres with no pg_mysql installed (Supabase, RDS,
    // Cloud SQL, Azure Database for PostgreSQL -- none of which allow installing a third-party C
    // extension), those five statement shapes would generate SQL referencing a schema that doesn't
    // exist, surfacing a raw, confusing "schema mysql_catalog does not exist" error instead of a
    // clear, actionable one.
    private static final ThreadLocal<Boolean> CURRENT_STATEMENT_AVAILABLE = ThreadLocal.withInitial(() -> true);

    public static void setCurrentStatementAvailable(boolean available) {
        CURRENT_STATEMENT_AVAILABLE.set(available);
    }

    public static boolean isCurrentStatementAvailable() {
        return CURRENT_STATEMENT_AVAILABLE.get();
    }
}
