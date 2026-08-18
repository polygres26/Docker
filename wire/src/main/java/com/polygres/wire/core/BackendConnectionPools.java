package com.polygres.wire.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public final class BackendConnectionPools {

    private static final ConcurrentHashMap<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    public static Connection borrow(String poolKey, String jdbcUrl, String user, String password) throws SQLException {
        HikariDataSource dataSource = pools.computeIfAbsent(poolKey, k -> create(k, jdbcUrl, user, password));
        return dataSource.getConnection();
    }

    public static String poolKeyFor(String jdbcUrl, String user) {
        return jdbcUrl + "|" + (user == null ? "" : user);
    }

    public record PoolStats(String poolKey, int activeConnections, int idleConnections, int totalConnections,
            int maxPoolSize, int threadsAwaitingConnection) {
    }

    public static java.util.List<PoolStats> snapshot() {
        return pools.entrySet().stream()
                .map(e -> statsOf(e.getKey(), e.getValue()))
                .toList();
    }

    public static PoolStats statsFor(String poolKey) {
        HikariDataSource dataSource = pools.get(poolKey);
        return dataSource == null ? null : statsOf(poolKey, dataSource);
    }

    private static PoolStats statsOf(String poolKey, HikariDataSource dataSource) {
        var pool = dataSource.getHikariPoolMXBean();
        return new PoolStats(poolKey, pool.getActiveConnections(), pool.getIdleConnections(),
                pool.getTotalConnections(), dataSource.getMaximumPoolSize(), pool.getThreadsAwaitingConnection());
    }

    private static HikariDataSource create(String poolKey, String jdbcUrl, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(poolKey);
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName("org.postgresql.Driver");
        if (user != null) {
            config.setUsername(user);
            config.setPassword(password);
        }
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(intEnv("POLYWIRE_POOL_MAX_SIZE", 20));
        
        config.setConnectionTimeout(longEnv("POLYWIRE_POOL_CONNECT_TIMEOUT_MS", 5_000));
        config.setIdleTimeout(longEnv("POLYWIRE_POOL_IDLE_TIMEOUT_MS", 60_000));
        applyStatementCacheProperties(config, jdbcUrl);
        return new HikariDataSource(config);
    }

    private static void applyStatementCacheProperties(HikariConfig config, String jdbcUrl) {
        int cacheSize = intEnv("POLYWIRE_STMT_CACHE_SIZE", 250);
        if (cacheSize <= 0) {
            return;
        }
        
        config.addDataSourceProperty("prepareThreshold", "1");
        config.addDataSourceProperty("preparedStatementCacheQueries", String.valueOf(cacheSize));
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private static long longEnv(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    private BackendConnectionPools() {
    }
}
