package com.polygres.wire.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide, per-backend connection pool registry — what turns many
 * frontend client connections into a few real backend connections (OJP's
 * model). One {@link HikariDataSource} per distinct backend identity
 * ({@code poolKey}, e.g. "host:port/database" or a {@link BackendTarget}'s
 * name), shared across every session/frontend that talks to that backend;
 * created lazily on first use and cached for the life of the process.
 *
 * <p>{@code minimumIdle(0)} is the load-bearing setting here: Hikari only
 * opens a physical connection when {@link #borrow} is actually called, not
 * when the pool itself is created — so a backend an operator has configured
 * but no client has queried yet holds zero real connections, per the
 * original ask ("can the backend connection be zero until it's really
 * used"). {@code maximumPoolSize} is the "few" in "many-to-few": every
 * client session borrows and returns (via plain {@code Connection.close()},
 * which Hikari intercepts to mean "return to pool" rather than "disconnect")
 * around each unit of work rather than holding a physical connection for
 * its own lifetime — see {@link LazyPooledConnection} for the manual-commit
 * frontends, and each frontend's own per-statement borrow for autocommit
 * ones.
 *
 * <p><b>Postgres-only</b>: unlike Omnigate (which pooled Oracle, MySQL/MariaDB,
 * Snowflake, Redshift, BigQuery, Databricks, SQL Server, AWS JDBC wrapper, and
 * generic-Calcite-REST backends, each with its own driver-class and
 * statement-cache-property branch), PolyWire always routes to a Postgres
 * backend — every URL-scheme branch here has been stripped down to that one
 * case.
 */
public final class BackendConnectionPools {

    private static final ConcurrentHashMap<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    public static Connection borrow(String poolKey, String jdbcUrl, String user, String password) throws SQLException {
        HikariDataSource dataSource = pools.computeIfAbsent(poolKey, k -> create(k, jdbcUrl, user, password));
        return dataSource.getConnection();
    }

    /**
     * The physical identity a pool should be keyed on -- the actual {@code (jdbcUrl, user)} a
     * connection is opened against, never a caller-chosen label like {@link BackendTarget#name}.
     * Found live: {@link BackendTarget#borrow} used to key its pool on {@code name} while
     * {@code PgConnections}' own direct-session path (pgwire's {@code sessionConnection()},
     * mywire/mssqlwire/gRPC's per-statement borrow) keyed on {@code host:port/db/user} -- two
     * different labels for what is, in the default single-backend deployment, the exact same
     * physical Postgres, so it silently split into two independent 20-connection Hikari pools
     * (confirmed via {@code /metrics}: {@code pool="default"} and
     * {@code pool="localhost:5442/postgres/postgres"} both sitting at {@code idle=20}
     * simultaneously) -- doubling the real ceiling on backend connections {@code
     * POLYWIRE_POOL_MAX_SIZE} was supposed to be capping. Every caller now derives its poolKey
     * from this one method so two routes to the same physical backend always collapse into one
     * shared pool, regardless of what label (a configured backend name, a directly-built JDBC URL)
     * got them there.
     */
    public static String poolKeyFor(String jdbcUrl, String user) {
        return jdbcUrl + "|" + (user == null ? "" : user);
    }

    public record PoolStats(String poolKey, int activeConnections, int idleConnections, int totalConnections,
            int maxPoolSize, int threadsAwaitingConnection) {
    }

    /**
     * One entry per backend that's had at least one connection borrowed —
     * surfaced on {@code /metrics} and via {@link
     * com.polygres.wire.telemetry.PolyWireTelemetry}, proof that many client
     * sessions really do share a few physical connections.
     * {@code threadsAwaitingConnection} is what shows the frontend actually
     * waiting once the pool is saturated, not just the cap holding.
     */
    public static java.util.List<PoolStats> snapshot() {
        return pools.entrySet().stream()
                .map(e -> statsOf(e.getKey(), e.getValue()))
                .toList();
    }

    /** Null if this pool has never been borrowed from (see class javadoc) — callers should treat that as "not saturated." Used by {@link QosControlStage} for pool-aware admission. */
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
        // Kept short (not Hikari's 30s default) so RoutingBackendExecutor-style failover callers
        // still fail fast onto their fallback instead of stalling on a dead pool's timeout.
        config.setConnectionTimeout(longEnv("POLYWIRE_POOL_CONNECT_TIMEOUT_MS", 5_000));
        config.setIdleTimeout(longEnv("POLYWIRE_POOL_IDLE_TIMEOUT_MS", 60_000));
        applyStatementCacheProperties(config, jdbcUrl);
        return new HikariDataSource(config);
    }

    /**
     * OJP/ShardingSphere-Proxy precedent: deliberately not a hand-rolled cache of
     * {@code PreparedStatement} objects in {@link JdbcBackendExecutor} — that would need its own
     * eviction, connection-affinity tracking across Hikari borrow/return, and explicit close-on-evict
     * logic, reimplementing what pgJDBC already does well. Instead this turns on pgJDBC's own
     * battle-tested server-side prepared-statement cache via connection properties, sized by
     * {@code POLYWIRE_STMT_CACHE_SIZE} (default 250, matching ShardingSphere-Proxy's own
     * {@code prepStmtCacheSize} default). {@code 0} disables it (falls back to pgJDBC's un-tuned
     * default rather than forcing it off).
     *
     * <p>Live-verified (real container, {@code log_statement=all}, confirmed repeated identical SQL
     * text moves from an unnamed to a named server-side prepared statement on the same physical
     * connection once the threshold is reached — see Omnigate's commit notes, this property carries
     * over unchanged since PolyWire is Postgres-only).
     */
    private static void applyStatementCacheProperties(HikariConfig config, String jdbcUrl) {
        int cacheSize = intEnv("POLYWIRE_STMT_CACHE_SIZE", 250);
        if (cacheSize <= 0) {
            return;
        }
        // prepareThreshold=1: cache starting from the very next execution of the same SQL
        // text on this connection, rather than pgJDBC's default of waiting 5 executions —
        // proxied connections see the same handful of statement shapes over and over, so
        // there's little value in pgJDBC's default caution here.
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
