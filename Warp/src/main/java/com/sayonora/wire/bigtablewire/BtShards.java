package com.sayonora.wire.bigtablewire;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ConnectionRoute;
import com.sayonora.wire.core.ShardingStrategy;
import com.sayonora.wire.core.StoreBootstrap;
import com.sayonora.wire.core.StoreType;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shard and connection plumbing of the Bigtable store, same conventions as pubsubwire/gcswire: the hosts are the Postgres backends
 * of the frontend's set that enable the store (declaration order == hash order), the first host is the "home" of the table
 * catalog, and a row lives on {@code hash(table name, row key)}. A project id that names a backend or a backend set (see
 * {@code ConnectionRouter}, protocol "http") pins that project's tables to those hosts. Every unit of work borrows a pooled
 * connection for one short statement or transaction and returns it: nothing is pinned while a client reads a response.
 */
final class BtShards {

    private static final Logger log = LoggerFactory.getLogger(BtShards.class);

    private final BackendRegistry registry;

    BtShards(BackendRegistry registry) {
        this.registry = registry;
    }

    boolean available() {
        return !registry.storeHosts(StoreType.BIGTABLE).isEmpty();
    }

    /** The project id of a table or instance name ("projects/P/..."), or null. */
    static String project(String name) {
        if (name != null && name.startsWith("projects/")) {
            int e = name.indexOf('/', 9);
            return e < 0 ? name.substring(9) : name.substring(9, e);
        }
        return null;
    }

    List<String> hosts(String project) {
        List<String> routed = null;
        try {
            var router = registry.connectionRouter();
            if (router != null && project != null && !project.isEmpty()) {
                ConnectionRoute route = router.resolve("http", project, null);
                routed = router.storeBackends(route);
            }
        } catch (RuntimeException e) {
            log.debug("bigtablewire: project route lookup failed: {}", e.toString());
        }
        List<String> h = routed != null && !routed.isEmpty() ? routed : registry.storeHosts(StoreType.BIGTABLE);
        if (h.isEmpty()) {
            throw BtException.unavailable("No Postgres backend of this set has the bigtable store enabled");
        }
        return h;
    }

    String home(String project) {
        return hosts(project).get(0);
    }

    /** The host owning a row. */
    String owner(List<String> hosts, String table, byte[] rowKey) {
        if (hosts.size() == 1) {
            return hosts.get(0);
        }
        return ShardingStrategy.hash(hosts).resolve(table + "\u0000" + new String(rowKey, StandardCharsets.ISO_8859_1));
    }

    /** Every host that may hold cells of the project's tables. */
    List<String> allHosts() {
        return new ArrayList<>(registry.storeHosts(StoreType.BIGTABLE));
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("bigtablewire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.BIGTABLE);
        return t;
    }

    @FunctionalInterface
    interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    <T> T conn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    <T> T tx(String host, SqlFn<T> fn) {
        try (Connection c = target(host).openManualCommit()) {
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the pool discards the connection
                }
                throw e;
            } finally {
                try {
                    c.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // pool resets on return
                }
            }
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    static BtException storage(SQLException e) {
        if ("23505".equals(e.getSQLState())) {
            return new BtException(io.grpc.Status.Code.ALREADY_EXISTS, "Resource already exists.");
        }
        log.error("bigtablewire: Postgres store error", e);
        return BtException.unavailable("The service is currently unavailable (" + e.getMessage() + ")");
    }
}
