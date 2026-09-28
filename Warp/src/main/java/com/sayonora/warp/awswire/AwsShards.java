package com.sayonora.warp.awswire;

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
 * Shard and connection plumbing for the SNS / Kinesis / awsparams stores, the same conventions as s3wire, sqswire and
 * azurewire: the hosts are the Postgres backends of the frontend's backend set that enable the store (declaration order
 * is the hash order), a key maps to a host with {@link ShardingStrategy#hash}, and the first host is the "home" of
 * catalogs. Every unit of work borrows a pooled connection for one short statement or transaction and returns it at
 * once -- nothing is pinned across client I/O, long polls or outbound HTTP.
 */
public final class AwsShards {

    private static final Logger log = LoggerFactory.getLogger(AwsShards.class);

    private final BackendRegistry registry;
    public final StoreType type;

    public AwsShards(BackendRegistry registry, StoreType type) {
        this.registry = registry;
        this.type = type;
    }

    @FunctionalInterface
    public interface SqlFn<T> {
        T apply(Connection c) throws SQLException;
    }

    public boolean available() {
        return !registry.storeHosts(type).isEmpty();
    }

    public List<String> hosts() {
        List<String> h = registry.storeHosts(type);
        if (h.isEmpty()) {
            throw new AwsException(503, "ServiceUnavailable", "No Postgres backend of this set has the " + type.id()
                    + " store enabled (enable it on a backend in the backend set)");
        }
        return h;
    }

    /** First host: home of catalogs and of everything that has no natural shard key. */
    public String home() {
        return hosts().get(0);
    }

    /** The host owning {@code key} (a deterministic hash over the hosts in declaration order). */
    public String owner(String key) {
        List<String> h = hosts();
        return h.size() == 1 ? h.get(0) : ShardingStrategy.hash(h).resolve(key);
    }

    private BackendTarget target(String host) throws SQLException {
        BackendTarget t = registry.resolveForRouting(host);
        if (t == null) {
            throw new SQLException("backend '" + host + "' is not registered");
        }
        StoreBootstrap.ensure(t, type);
        return t;
    }

    public <T> T conn(String host, SqlFn<T> fn) {
        try (Connection c = target(host).open()) {
            return fn.apply(c);
        } catch (SQLException e) {
            throw storage(e);
        }
    }

    public <T> T tx(String host, SqlFn<T> fn) {
        try (Connection c = target(host).openManualCommit()) {
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the pool discards a broken connection
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

    public static AwsException storage(SQLException e) {
        log.error("aws store: Postgres error", e);
        return new AwsException(500, "InternalFailure", "The server encountered an internal error (" + e.getMessage() + ")");
    }

    /** Runs {@code fn} once per host (hash order) and returns the results. */
    public <T> List<T> onAll(SqlFn<T> fn) {
        List<T> out = new ArrayList<>();
        for (String h : hosts()) {
            out.add(conn(h, fn));
        }
        return out;
    }

    public BackendRegistry registry() {
        return registry;
    }
}
