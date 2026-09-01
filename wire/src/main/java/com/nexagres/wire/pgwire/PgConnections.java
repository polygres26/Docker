package com.nexagres.wire.pgwire;

import com.nexagres.wire.core.BackendConnectionPools;
import com.nexagres.wire.server.ServerOptions;
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

    public static Connection openRaw(ServerOptions options) throws SQLException {
        return openWithFailover(options, PgConnections::connectRaw);
    }

    /**
     * For read-only statements a caller has already decided are safe to serve from a replica
     * (see {@code WARP_READ_ROUTING_ENABLED} in {@code RoutingBackendExecutor}): tries the
     * standby first, falls back to the primary if the standby is unreachable or none is
     * configured. Deliberately independent of {@link #onStandby}/the failback-probe machinery
     * above -- that state means "the primary is down, we're degraded"; this method's standby
     * preference is a normal, healthy-path load-balancing choice and must never itself flip or
     * be confused with the outage-driven failover state (a read-routing attempt hitting an
     * unreachable standby is not a primary outage and must not be logged/treated as one).
     */
    public static Connection openForRead(ServerOptions options) throws SQLException {
        if (options.pgStandbyHost() == null || options.pgStandbyHost().isBlank()) {
            return open(options);
        }
        try {
            return connect(options.pgStandbyHost(), options.pgStandbyPort(), options);
        } catch (SQLException standbyUnreachable) {
            log.warn("read routing: standby {}:{} unreachable ({}), reading from primary instead",
                    options.pgStandbyHost(), options.pgStandbyPort(), standbyUnreachable.getMessage());
            return open(options);
        }
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

    private static synchronized void startFailbackProbe(ServerOptions options) {
        if (failbackProbe != null) {
            return;
        }
        int intervalSeconds = Integer.parseInt(System.getenv().getOrDefault("WARP_FAILBACK_CHECK_SECONDS", "10"));
        failbackProbe = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pg-failback-probe");
            t.setDaemon(true);
            return t;
        });
        failbackProbe.scheduleWithFixedDelay(() -> {
            if (!onStandby.get()) {
                return;
            }
            try (Connection probe = connect(options.pgHost(), options.pgPort(), options)) {
                if (onStandby.compareAndSet(true, false)) {
                    log.warn("failover: primary {}:{} recovered, switching back", options.pgHost(), options.pgPort());
                }
            } catch (SQLException stillDown) {
                
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private static Connection connect(String host, int port, ServerOptions options) throws SQLException {

        String url = baseUrl(host, port, options);

        String poolKey = BackendConnectionPools.poolKeyFor(url, options.pgUser());
        return BackendConnectionPools.borrow(poolKey, url, options.pgUser(), options.pgPassword());
    }

    private static Connection connectRaw(String host, int port, ServerOptions options) throws SQLException {
        String url = baseUrl(host, port, options);
        Properties props = new Properties();
        if (options.pgUser() != null) {
            props.setProperty("user", options.pgUser());
        }
        if (options.pgPassword() != null) {
            props.setProperty("password", options.pgPassword());
        }
        return DriverManager.getConnection(url, props);
    }

    /** {@code jdbc:postgresql://host:port/database}, plus {@code ?sslmode=...} (and {@code
     * &sslrootcert=...}) when {@code WARP_PG_SSLMODE}/{@code WARP_PG_SSLROOTCERT} are
     * set -- see {@code ServerOptions.parse}'s javadoc on those fields for why this exists
     * (required for Supabase/Azure Database for PostgreSQL, which reject a plaintext connection
     * outright). Both {@link #connect} and {@link #connectRaw} share this so a switch to the
     * standby -- same {@code options}, different host/port -- keeps the same SSL settings. */
    private static String baseUrl(String host, int port, ServerOptions options) {
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + options.pgDatabase();
        if (options.pgSslMode() != null && !options.pgSslMode().isBlank()) {
            url += "?sslmode=" + options.pgSslMode();
            if (options.pgSslRootCert() != null && !options.pgSslRootCert().isBlank()) {
                url += "&sslrootcert=" + options.pgSslRootCert();
            }
        }
        return url;
    }

    private PgConnections() {
    }
}
