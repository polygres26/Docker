package com.sayonora.warp.amqpwire;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
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
 * Shard and connection plumbing of the AMQP store, same conventions as pubsubwire/sqswire: the hosts are the Postgres backends of
 * the set that enable the store (declaration order == hash order); the first host is the "home" of exchanges and bindings; a queue
 * (definition and messages) lives on {@code hash(vhost, queue name)}. Every unit of work borrows a pooled connection for one short
 * statement or transaction and returns it: nothing is pinned across a consumer's idle wait.
 */
final class AmqpShards {

    private static final Logger log = LoggerFactory.getLogger(AmqpShards.class);

    private final BackendRegistry registry;

    AmqpShards(BackendRegistry registry) {
        this.registry = registry;
    }

    BackendRegistry registry() {
        return registry;
    }

    boolean available() {
        return !registry.storeHosts(StoreType.AMQP).isEmpty();
    }

    List<String> hosts() {
        List<String> h = registry.storeHosts(StoreType.AMQP);
        if (h.isEmpty()) {
            throw AmqpException.conn(541, "INTERNAL_ERROR - no Postgres backend of this set has the amqp store enabled");
        }
        return h;
    }

    String home() {
        return hosts().get(0);
    }

    String owner(String vhost, String queue) {
        List<String> h = hosts();
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(vhost + "\u0000" + queue);
    }

    List<String> allHosts() {
        return new ArrayList<>(registry.storeHosts(StoreType.AMQP));
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("amqpwire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.AMQP);
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

    static AmqpException storage(SQLException e) {
        log.error("amqpwire: Postgres store error", e);
        return AmqpException.conn(541, "INTERNAL_ERROR - storage error: " + e.getMessage());
    }
}
