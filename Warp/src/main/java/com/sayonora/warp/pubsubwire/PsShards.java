package com.sayonora.warp.pubsubwire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.core.ConnectionRoute;
import com.sayonora.warp.core.ShardingStrategy;
import com.sayonora.warp.core.StoreBootstrap;
import com.sayonora.warp.core.StoreType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shard and connection plumbing of the Pub/Sub store, same conventions as sqswire/gcswire: the hosts are the Postgres backends of
 * the frontend's set that enable the store (declaration order == hash order), the first host is the "home" of every catalog, and a
 * subscription's queue lives on {@code hash(subscription name)}. A project id that names a backend or a backend set (see
 * {@code ConnectionRouter}, protocol "http") pins that project to those hosts. Every unit of work borrows a pooled connection for
 * one short statement or transaction and returns it: nothing is pinned across a long poll or a StreamingPull.
 */
final class PsShards {

    private static final Logger log = LoggerFactory.getLogger(PsShards.class);

    private final BackendRegistry registry;

    PsShards(BackendRegistry registry) {
        this.registry = registry;
    }

    BackendRegistry registry() {
        return registry;
    }

    boolean available() {
        return !registry.storeHosts(StoreType.PUBSUB).isEmpty();
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
            log.debug("pubsubwire: project route lookup failed: {}", e.toString());
        }
        List<String> h = routed != null && !routed.isEmpty() ? routed : registry.storeHosts(StoreType.PUBSUB);
        if (h.isEmpty()) {
            throw PsException.unavailable("No Postgres backend of this set has the pubsub store enabled");
        }
        return h;
    }

    String home(String project) {
        return hosts(project).get(0);
    }

    /** The host owning a subscription's queue. */
    String owner(String project, String subscriptionName) {
        List<String> h = hosts(project);
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(subscriptionName);
    }

    /** Every host that may hold queues, for maintenance sweeps. */
    List<String> allHosts() {
        return new ArrayList<>(registry.storeHosts(StoreType.PUBSUB));
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("pubsubwire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.PUBSUB);
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

    static PsException storage(SQLException e) {
        if ("23505".equals(e.getSQLState())) {
            return new PsException(io.grpc.Status.Code.ALREADY_EXISTS, "Resource already exists in the project.");
        }
        log.error("pubsubwire: Postgres store error", e);
        return PsException.unavailable("The service is currently unavailable (" + e.getMessage() + ")");
    }

    static boolean isUniqueViolation(Throwable t) {
        return t instanceof PsException pe && pe.code == io.grpc.Status.Code.ALREADY_EXISTS;
    }
}
