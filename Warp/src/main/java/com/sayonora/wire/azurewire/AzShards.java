package com.sayonora.wire.azurewire;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.ShardingStrategy;
import com.sayonora.wire.core.StoreBootstrap;
import com.sayonora.wire.core.StoreType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shard and connection plumbing shared by the three Azure stores, the same conventions as s3wire/sqswire: the hosts are
 * the Postgres backends of the frontend's set that enable the store (declaration order == hash order), a key maps to a
 * host with {@link ShardingStrategy#hash}, the first host is the "home" of catalogs. Every unit of work borrows a pooled
 * connection for one short statement/transaction and returns it immediately -- nothing is pinned across client I/O.
 * A connect-time route (storage ACCOUNT NAME matching a backend or set name, see {@code ConnectionRouter}) overrides the
 * hosts for the calling thread only ({@link #route}).
 */
final class AzShards {

    private static final Logger log = LoggerFactory.getLogger(AzShards.class);

    private final BackendRegistry registry;
    final StoreType type;
    private static final ThreadLocal<List<String>> ROUTED = new ThreadLocal<>();

    AzShards(BackendRegistry registry, StoreType type) {
        this.registry = registry;
        this.type = type;
    }

    /** Sets (or with null clears) the per-request connect-time route hosts for this thread. */
    static void route(List<String> hosts) {
        if (hosts == null) {
            ROUTED.remove();
        } else {
            ROUTED.set(hosts);
        }
    }

    static List<String> routed() {
        return ROUTED.get();
    }

    List<String> hosts() {
        List<String> r = ROUTED.get();
        List<String> h = r != null ? r : registry.storeHosts(type);
        if (h.isEmpty()) {
            throw new AzureException(503, "ServerBusy", "No Postgres backend of this set has the " + type.id()
                    + " store enabled");
        }
        return h;
    }

    boolean available() {
        return !registry.storeHosts(type).isEmpty();
    }

    String home() {
        return hosts().get(0);
    }

    String owner(String key) {
        List<String> h = hosts();
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(key);
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("azurewire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, type);
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
                    // connection is discarded by the pool anyway
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

    static AzureException storage(SQLException e) {
        log.error("azurewire: Postgres store error", e);
        return new AzureException(500, "InternalError", "The server encountered an internal error. Please retry the request. ("
                + e.getMessage() + ")");
    }

    /** Runs {@code fn} once per host (hash order) and returns the results. */
    <T> List<T> onAll(SqlFn<T> fn) {
        List<T> out = new java.util.ArrayList<>();
        for (String h : hosts()) {
            out.add(conn(h, fn));
        }
        return out;
    }

    BackendRegistry registry() {
        return registry;
    }
}
