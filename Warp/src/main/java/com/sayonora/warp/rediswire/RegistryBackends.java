package com.sayonora.warp.rediswire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ConnectionRoute;
import com.sayonora.warp.core.ConnectionRouter;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** {@link Backends} over Warp's {@link BackendRegistry}: hosts = Postgres backends with the redis store enabled. */
final class RegistryBackends implements Backends {

    private final BackendRegistry registry;
    private final SqlMetricsCollector metrics;

    RegistryBackends(BackendRegistry registry, SqlMetricsCollector metrics) {
        this.registry = registry;
        this.metrics = metrics;
    }

    @Override
    public List<String> defaultHosts() {
        List<String> hosts = registry.storeHosts(StoreType.REDIS);
        return hosts.isEmpty() ? List.of(BackendRegistry.DEFAULT_BACKEND_NAME) : hosts;
    }

    @Override
    public List<String> routeHosts(String username, int db) {
        ConnectionRouter router = registry.connectionRouter();
        // 1) an explicit route on the numeric database ("database": "3" style routes use the name db3)
        ConnectionRoute route = router.resolve(ConnectionRouter.PROTO_REDIS, "db" + db, username);
        if (route.isRejected() && route.description() != null && route.description().contains("does not exist")) {
            throw new RedisError("ERR " + route.description());
        }
        if (!route.isRouted()) {
            // 2) the AUTH/HELLO username plays the role of the database name ("default" = no name)
            String name = username == null || username.equals("default") ? "" : username;
            route = router.resolve(ConnectionRouter.PROTO_REDIS, name, username);
            if (route.isRejected()) {
                throw new RedisError("WRONGPASS invalid username-password pair or user is disabled. (" + route.description() + ")");
            }
        }
        List<String> hosts = router.storeBackends(route);
        if (hosts == null) {
            return null;
        }
        if (hosts.isEmpty()) {
            throw new RedisError("ERR the connection routes to " + route.description()
                    + ", which cannot host the redis store (only Postgres backends can)");
        }
        return hosts;
    }

    private BackendTarget target(String host) {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            t = registry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
        }
        if (t == null) {
            throw new IllegalStateException("rediswire: no backend named \"" + host + "\" is registered");
        }
        return t;
    }

    @Override
    public Connection open(String host) throws SQLException {
        return target(host).open();
    }

    @Override
    public Connection openDedicated(String host) throws SQLException {
        return com.sayonora.warp.cluster.CacheTriggerInstaller.openDedicatedConnection(target(host));
    }

    @Override
    public void ensureSchema(String host) throws SQLException {
        StoreBootstrap.ensure(target(host), StoreType.REDIS);
    }

    @Override
    public void record(String host, boolean write, String command, long nanos) {
        if (metrics != null) {
            metrics.recordOperation("rediswire", host, write ? SqlMetricsCollector.StatementKind.WRITE
                    : SqlMetricsCollector.StatementKind.READ, command, nanos, nanos);
        }
    }
}
