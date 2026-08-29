package com.nexagres.wire.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects whether the {@code pg_oracle} extension (db/pg_oracle) is installed on a given backend
 * Postgres database, so orawire can degrade gracefully instead of failing every single statement
 * when it isn't -- Polywire can be deployed against a plain, unmodified Postgres with no pg_oracle
 * extension at all, and previously that meant every orawire request died on the very first
 * statement (see {@code OraclePgEmulationSessionInitializer}'s old unconditional
 * {@code SET db_emulation = 'oracle'}, and {@code DialectTranslations}'s old unconditional
 * schema-qualification of {@code TO_CHAR}/{@code TO_DATE} to {@code oracle_catalog.*}).
 *
 * <p>Same {@code SELECT 1 FROM pg_extension WHERE extname = '...'} probe, cached by backend
 * identity, that {@code influxwire.PgTimeSeriesStore} already uses for detecting TimescaleDB --
 * this is that exact pattern applied to pg_oracle. Caching is safe per physical backend
 * (jdbc URL), not per logical session: whether an extension is installed is a property of the
 * target database that doesn't change at runtime, unlike session-scoped GUCs such as
 * {@code db_emulation} itself (see {@code LazyPooledConnection}'s docs on physical-backend reuse
 * across logical sessions for why that distinction matters here).
 */
public final class PgOracleSupport {

    private PgOracleSupport() {
    }

    private static final String PROBE_SQL = "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_oracle'";

    private static final ConcurrentHashMap<String, Boolean> AVAILABLE_CACHE = new ConcurrentHashMap<>();

    /** For call sites that only have a {@link BackendTarget} (e.g. the translation pipeline stage,
     * which runs before any per-session connection/initializer exists). */
    public static boolean isAvailable(BackendTarget target) throws SQLException {
        Boolean cached = AVAILABLE_CACHE.get(target.jdbcUrl());
        if (cached != null) {
            return cached;
        }
        try (Connection connection = target.open()) {
            return cacheResult(target.jdbcUrl(), probe(connection));
        }
    }

    /** For call sites that already hold an open {@link Connection} (e.g. the session initializer,
     * which runs once per statement on the actual backend connection it's about to use). */
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

    // Consulted by DialectTranslations.normalizeOracle(), which has no Connection/BackendTarget of
    // its own -- translation is a pure string transform, and it runs one pipeline stage earlier
    // than the executor that actually holds a connection to detect against (see
    // DialectTranslationStage.handle(), which sets this immediately before calling translate() on
    // the same thread, and DialectTranslations.normalizeOracle(), which reads it synchronously in
    // that same call). Defaults to true (assume installed) so behavior is unchanged for the
    // overwhelmingly common case where pg_oracle IS installed, and for any call site (tests,
    // other pipelines) that never sets it at all.
    private static final ThreadLocal<Boolean> CURRENT_STATEMENT_AVAILABLE = ThreadLocal.withInitial(() -> true);

    public static void setCurrentStatementAvailable(boolean available) {
        CURRENT_STATEMENT_AVAILABLE.set(available);
    }

    public static boolean isCurrentStatementAvailable() {
        return CURRENT_STATEMENT_AVAILABLE.get();
    }
}
