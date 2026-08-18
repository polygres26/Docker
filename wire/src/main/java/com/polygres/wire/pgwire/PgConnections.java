package com.polygres.wire.pgwire;

import com.polygres.wire.core.BackendConnectionPools;
import com.polygres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared "open a JDBC connection to the configured Postgres backend" logic —
 * used by every frontend that talks to it directly (PgBackendPool,
 * PgWireSessionHandler, QueryServiceImpl, MySqlWireSessionHandler), and by
 * PolyWire's own control-plane stores ({@code ConfigStore}, {@code
 * FirewallRuleStore}, {@code TranslationCacheStore}, {@code
 * FailedStatementLog} — see {@link #openRaw}'s javadoc for why those need a
 * second, non-pooled entry point into this same failover logic).
 *
 * <p>ARCHITECTURE.md gap-analysis item "switchover/failover": opt-in via
 * {@code ORAPG_PG_STANDBY_HOST} (same user/password/database as the
 * primary — a physical-replica HA pair, not a separately configured
 * backend; for routing traffic to a differently-shaped backend, use
 * {@code POLYWIRE_BACKENDS}/{@link com.polygres.wire.core.RouterStage} instead).
 * When set, a failed connect attempt against the currently-active side
 * immediately retries the other side; a successful retry flips the shared
 * {@code onStandby} flag so every subsequent connection (across every
 * frontend/session, process-wide — including the control-plane stores,
 * since they share this same static flag) goes straight to the healthy
 * side without re-paying the failed primary's connect timeout each time. A
 * background probe polls the primary every {@code
 * ORAPG_PG_FAILBACK_CHECK_SECONDS} (default 10s) while on standby and flips
 * back once it recovers — a narrow-slice health check (plain
 * connect-and-close, no replication-lag or read-only-mode awareness), not a
 * full HA controller.
 *
 * <p>Every {@link #open} call is actually a {@link BackendConnectionPools#borrow}
 * — many frontend sessions across pgwire/mywire/gRPC/orawire share the same
 * small pool per (host, port, database, user), rather than each opening its
 * own physical connection. Primary and standby are distinct pool keys, so
 * failing over doesn't reuse — or need to drain — the other side's pool.
 */
public final class PgConnections {

    private static final Logger log = LoggerFactory.getLogger(PgConnections.class);

    private static final AtomicBoolean onStandby = new AtomicBoolean(false);
    private static volatile ScheduledExecutorService failbackProbe;

    @FunctionalInterface
    private interface ConnectionOpener {
        Connection open(String host, int port, ServerOptions options) throws SQLException;
    }

    public static Connection open(ServerOptions options) throws SQLException {
        return openWithFailover(options, PgConnections::connect);
    }

    /**
     * Same primary/standby failover as {@link #open} (shares the same static {@code onStandby}
     * flag and failback probe -- a failover discovered via either entry point is immediately
     * visible to the other), but a genuinely raw, non-pooled {@link DriverManager} connection
     * instead of a {@link BackendConnectionPools#borrow} one.
     *
     * <p>Needed specifically for {@code ConfigStore}/{@code FirewallRuleStore}'s persistent
     * {@code LISTEN} connections: a pooled connection is returned to the pool (not truly closed)
     * on {@code close()}, and Postgres's {@code LISTEN} state is per-session -- handing a
     * listening connection back into a shared pool for some unrelated later borrower to reuse
     * would leak that session's notifications to whoever borrows it next. One-shot reads
     * ({@code readLatest()}, {@code readRules()}, {@code record(...)}) still use {@link #open}
     * (pooled is correct there -- a genuinely one-shot borrow-then-close).
     */
    public static Connection openRaw(ServerOptions options) throws SQLException {
        return openWithFailover(options, PgConnections::connectRaw);
    }

    private static Connection openWithFailover(ServerOptions options, ConnectionOpener opener) throws SQLException {
        if (options.pgStandbyHost() == null || options.pgStandbyHost().isBlank()) {
            return opener.open(options.pgHost(), options.pgPort(), options);
        }
        boolean preferStandby = onStandby.get();
        String primaryHost = preferStandby ? options.pgStandbyHost() : options.pgHost();
        int primaryPort = preferStandby ? options.pgStandbyPort() : options.pgPort();
        String fallbackHost = preferStandby ? options.pgHost() : options.pgStandbyHost();
        int fallbackPort = preferStandby ? options.pgPort() : options.pgStandbyPort();
        try {
            return opener.open(primaryHost, primaryPort, options);
        } catch (SQLException primaryFailure) {
            log.warn("failover: {}:{} unreachable ({}), trying {}:{}",
                    primaryHost, primaryPort, primaryFailure.getMessage(), fallbackHost, fallbackPort);
            Connection connection = opener.open(fallbackHost, fallbackPort, options);
            if (onStandby.compareAndSet(preferStandby, !preferStandby)) {
                log.warn("failover: switched to {}:{}", fallbackHost, fallbackPort);
                if (!preferStandby) {
                    startFailbackProbe(options);
                }
            }
            return connection;
        }
    }

    /** While serving from standby, periodically checks whether the primary has come back so traffic can switch back automatically. */
    private static synchronized void startFailbackProbe(ServerOptions options) {
        if (failbackProbe != null) {
            return;
        }
        int intervalSeconds = Integer.parseInt(System.getenv().getOrDefault("ORAPG_PG_FAILBACK_CHECK_SECONDS", "10"));
        failbackProbe = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pg-failback-probe");
            t.setDaemon(true);
            return t;
        });
        failbackProbe.scheduleWithFixedDelay(() -> {
            if (!onStandby.get()) {
                return; // already switched back (e.g. by a normal request-path retry)
            }
            try (Connection probe = connect(options.pgHost(), options.pgPort(), options)) {
                if (onStandby.compareAndSet(true, false)) {
                    log.warn("failover: primary {}:{} recovered, switching back", options.pgHost(), options.pgPort());
                }
            } catch (SQLException stillDown) {
                // primary still unreachable — keep serving from standby, check again next tick
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private static Connection connect(String host, int port, ServerOptions options) throws SQLException {
        // RTT optimization (ARCHITECTURE.md §11): plain concatenation, not String.formatted — this
        // runs on every single statement (executeSimpleQuery calls PgConnections.open per
        // statement, by design — see that class's javadoc), and Formatter's parsing of the format
        // string is measurably slower than concatenation for a hot path this narrow. No behavior
        // change: same URL/poolKey shape either way.
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + options.pgDatabase();
        // Poolkey derived the same way BackendTarget's own borrow() does -- see
        // BackendConnectionPools#poolKeyFor's javadoc for why this must not be a hand-built string
        // of its own: this project's default single-backend deployment reaches the exact same
        // physical Postgres via both this path and a registered BackendTarget, and the two used to
        // silently end up in separate pools before poolKeyFor unified them.
        String poolKey = BackendConnectionPools.poolKeyFor(url, options.pgUser());
        return BackendConnectionPools.borrow(poolKey, url, options.pgUser(), options.pgPassword());
    }

    private static Connection connectRaw(String host, int port, ServerOptions options) throws SQLException {
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + options.pgDatabase();
        Properties props = new Properties();
        if (options.pgUser() != null) {
            props.setProperty("user", options.pgUser());
        }
        if (options.pgPassword() != null) {
            props.setProperty("password", options.pgPassword());
        }
        return DriverManager.getConnection(url, props);
    }

    private PgConnections() {
    }
}
