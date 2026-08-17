package com.polygres.advisor.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide, per-target connection pool registry. Trimmed adaptation of Omnigate's
 * {@code com.omnigate.core.BackendConnectionPools} -- same lazy-open, shared-per-key pool model,
 * minus the multi-vendor driverClassNameFor() table Omnigate needs for its wider backend list
 * (Snowflake/Redshift/BigQuery/etc.); Advisor only ever pools the four {@link SourceDialect}s it
 * profiles or migrates to, and lets the JDBC 4+ driver auto-register handle driver selection.
 *
 * <p>{@code minimumIdle(0)}: a configured-but-unscanned target holds zero real connections.
 * {@code maximumPoolSize} is kept small -- Advisor does read-only catalog/workload scans, not
 * high-throughput OLTP traffic, so a handful of connections per target is enough headroom for
 * profiler queries to run in parallel without hammering a production source.
 */
public final class BackendConnectionPools {

    private static final ConcurrentHashMap<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    private BackendConnectionPools() {}

    public static Connection borrow(String poolKey, String jdbcUrl, String user, String password) throws SQLException {
        HikariDataSource dataSource = pools.computeIfAbsent(poolKey, k -> create(k, jdbcUrl, user, password));
        return dataSource.getConnection();
    }

    public static void closeAll() {
        pools.values().forEach(HikariDataSource::close);
        pools.clear();
    }

    private static HikariDataSource create(String poolKey, String jdbcUrl, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("polygres-advisor-" + poolKey);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(15_000);
        config.setReadOnly(true);
        return new HikariDataSource(config);
    }
}
