package com.sayonora.warp.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

public final class BackendConnectionPools {

    private static final ConcurrentHashMap<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    /** Reserved alias for the default (config-primary Postgres) pool -- what a statement with no
     * {@code targetBackend} runs on. Registered by {@code PgConnections.connect}. */
    public static final String DEFAULT_ALIAS = "default";

    private static final ConcurrentHashMap<String, String> backendAliases = new ConcurrentHashMap<>();

    /** Borrow for a caller with no session-state handling of its own (internal statements, HTTP frontends,
     * MCP, ...): anything a wire session applied to the physical connection -- RLS identity, db_emulation,
     * tenant search_path -- is stripped first ({@link com.sayonora.warp.core.access.SessionStateReconciler#cleanse}). */
    public static Connection borrow(String poolKey, String jdbcUrl, String user, String password) throws SQLException {
        Connection connection = borrowForSession(poolKey, jdbcUrl, user, password);
        try {
            com.sayonora.warp.core.access.SessionStateReconciler.cleanse(connection);
        } catch (SQLException | RuntimeException e) {
            // a connection whose Warp-applied state could not be stripped must not go back to the pool as is
            evict(poolKey, connection);
            try {
                connection.close();
            } catch (SQLException ignored) {
                // already evicted
            }
            throw e;
        }
        return connection;
    }

    /** Borrow for a {@link SessionConnectionLease}: the session reconciles Warp-applied state itself, per
     * statement, so nothing is stripped here (that would cost a round trip on every borrow). */
    public static Connection borrowForSession(String poolKey, String jdbcUrl, String user, String password)
            throws SQLException {
        HikariDataSource dataSource = pools.computeIfAbsent(poolKey, k -> create(k, jdbcUrl, user, password));
        try {
            return dataSource.getConnection();
        } catch (java.sql.SQLTransientConnectionException e) {
            // Hikari reports pure exhaustion (every connection checked out, nothing failed to connect) as
            // a timeout with no cause; a timeout that carries a cause is a real connect failure and must
            // keep its own (08001) error.
            if (e.getCause() == null && e.getMessage() != null && e.getMessage().contains("request timed out")) {
                var pool = dataSource.getHikariPoolMXBean();
                throw new BackendPoolExhaustedException(poolKey, dataSource.getConnectionTimeout(),
                        dataSource.getMaximumPoolSize(), pool.getActiveConnections(),
                        Math.max(0, pool.getThreadsAwaitingConnection()), e);
            }
            throw e;
        }
    }

    /** Removes {@code connection} (currently checked out of pool {@code poolKey}) from the pool instead of
     * returning it -- for a connection whose session state could not be reset. */
    public static void evict(String poolKey, Connection connection) {
        HikariDataSource dataSource = pools.get(poolKey);
        if (dataSource != null) {
            dataSource.evictConnection(connection);
        }
    }

    /** Remembers that backend {@code backendName} (or {@link #DEFAULT_ALIAS}) is served by pool
     * {@code poolKey}, so callers that only know the backend name -- {@code QosControlStage}'s pool-wait
     * threshold -- can find the pool's stats. Idempotent and cheap (called on every borrow). */
    public static void registerBackendAlias(String backendName, String poolKey) {
        if (backendName != null && !poolKey.equals(backendAliases.get(backendName))) {
            backendAliases.put(backendName, poolKey);
        }
    }

    /** Stats of the pool serving {@code backendName} ({@code null} or "default" = the default pool),
     * or {@code null} if no connection has been borrowed for it yet. */
    public static PoolStats statsForBackend(String backendName) {
        String key = backendAliases.get(backendName == null ? DEFAULT_ALIAS : backendName);
        return key == null ? null : statsFor(key);
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
        // Real bug, found live: this used to hardcode "org.postgresql.Driver" unconditionally --
        // harmless while every backend really was Postgres, but a real, hard crash the moment a
        // real Oracle backend's own connection pool got created (HikariCP's own DriverDataSource
        // refuses outright: "Driver org.postgresql.Driver claims to not accept jdbcUrl ..."). Real
        // driver-class lookup instead, matching BackendTarget.dialect()'s own URL-prefix dispatch;
        // an unrecognized prefix leaves driverClassName unset, which HikariCP resolves itself via
        // DriverManager's own URL-matching (its documented fallback) rather than a wrong forced
        // guess.
        String driverClassName = BackendDriverRegistry.driverClassNameFor(jdbcUrl);
        if (driverClassName != null) {
            config.setDriverClassName(driverClassName);
        }
        if (user != null) {
            config.setUsername(user);
            config.setPassword(password);
        }
        config.setMinimumIdle(0);
        // Sizing (see docs/WARP_GUIDE.md "Connection multiplexing"): wire sessions (pgwire, mywire, mssqlwire,
        // orawire, boltwire) no longer hold a backend connection per client -- they borrow per statement and pin
        // one only while a transaction or backend session state is open (SessionConnectionLease), so this pool
        // only has to cover peak CONCURRENT WORK, not the number of connected clients. The default of 30 stays
        // generous on purpose: with WARP_MULTIPLEX_SESSIONS=false every session holds one for its whole life
        // again (then it must exceed License.DEVELOPER_MAX_CONNECTIONS, 25), and Warp's own internal borrowers
        // (schema checks, failed-statement log, node registry) draw from the same pool. An Enterprise
        // deployment with no connection ceiling should size it to its real backend capacity.
        config.setMaximumPoolSize(intEnv("WARP_POOL_MAX_SIZE", 30));

        config.setConnectionTimeout(longEnv("WARP_POOL_CONNECT_TIMEOUT_MS", 5_000));
        config.setIdleTimeout(longEnv("WARP_POOL_IDLE_TIMEOUT_MS", 60_000));
        applyStatementCacheProperties(config, jdbcUrl);
        return new HikariDataSource(config);
    }

    private static void applyStatementCacheProperties(HikariConfig config, String jdbcUrl) {
        int cacheSize = intEnv("WARP_STMT_CACHE_SIZE", 250);
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
