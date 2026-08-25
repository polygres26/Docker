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

    /** Result of {@link #drain}: {@code drainedCleanly} is true when every in-flight connection
     * finished and was returned before {@code graceMillis} elapsed; false means the grace period
     * expired first and the pool was closed anyway (Hikari's own {@code close()} applies a short
     * internal timeout to any connections still checked out at that point -- draining does not
     * wait forever). {@code activeConnectionsAtClose} is what was still checked out at the moment
     * this call gave up waiting (0 when {@code drainedCleanly}), useful for an admin caller/log
     * line to report exactly what got cut short. */
    public record DrainResult(boolean poolExisted, boolean drainedCleanly, int activeConnectionsAtClose) {
    }

    /** Stops a pool from handing out further connections and waits (bounded by {@code
     * graceMillis}) for whatever's already checked out to be returned before closing it. This is
     * NEW-CHECKOUT prevention only, not a hard kill switch -- it relies on the caller (routing,
     * via {@code BackendRegistry.resolveForRouting}) having already stopped sending new work to
     * this backend's name before calling drain, same as a real load balancer's drain: connection
     * refusal at the front door plus a grace period, not mid-flight cancellation.
     *
     * <p>Once drained, the pool is removed from the registry entirely (not just suspended) --  a
     * later {@code borrow()} against the same poolKey (e.g. after an undrain repoints routing back
     * to this backend) transparently creates a fresh pool via {@code computeIfAbsent}, so there's
     * no separate "resume" call needed. */
    public static DrainResult drain(String poolKey, long graceMillis) {
        HikariDataSource dataSource = pools.remove(poolKey);
        if (dataSource == null) {
            return new DrainResult(false, true, 0);
        }
        var pool = dataSource.getHikariPoolMXBean();
        long deadline = System.currentTimeMillis() + graceMillis;
        int active = pool.getActiveConnections();
        while (active > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            active = pool.getActiveConnections();
        }
        boolean drainedCleanly = active == 0;
        dataSource.close();
        return new DrainResult(true, drainedCleanly, active);
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
