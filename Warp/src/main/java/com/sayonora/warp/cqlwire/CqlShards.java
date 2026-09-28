package com.sayonora.warp.cqlwire;

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
 * Shard and connection plumbing of the CQL store: the hosts are the Postgres backends of the frontend's set that enable the
 * {@code cql} store (declaration order == hash order), the first host is the "home" of the schema catalog and a partition lives on
 * {@code hash(serialized partition key)}. Every unit of work borrows a pooled connection for one short statement or transaction
 * and returns it: nothing is pinned while a client reads a response or between pages.
 */
class CqlShards {

    private static final Logger log = LoggerFactory.getLogger(CqlShards.class);

    private final BackendRegistry registry;

    CqlShards(BackendRegistry registry) {
        this.registry = registry;
    }

    boolean available() {
        return !registry.storeHosts(StoreType.CQL).isEmpty();
    }

    List<String> hosts() {
        List<String> h = registry.storeHosts(StoreType.CQL);
        if (h.isEmpty()) {
            throw new CqlError(CqlError.UNAVAILABLE, "No Postgres backend of this set has the cql store enabled");
        }
        return h;
    }

    String home() {
        return hosts().get(0);
    }

    String owner(List<String> hosts, byte[] pk) {
        if (hosts.size() == 1) {
            return hosts.get(0);
        }
        return ShardingStrategy.hash(hosts).resolve(CqlType.hex(pk));
    }

    List<String> allHosts() {
        return new ArrayList<>(registry.storeHosts(StoreType.CQL));
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("cqlwire: backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, StoreType.CQL);
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

    static CqlError storage(SQLException e) {
        log.error("cqlwire: Postgres store error", e);
        return new CqlError(CqlError.SERVER, "The store is currently unavailable (" + e.getMessage() + ")");
    }
}
