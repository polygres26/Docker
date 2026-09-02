package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects whether the {@code pg_mysql} extension (db/pg_mysql) is installed on a given backend
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
}
