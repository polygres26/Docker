package com.sayonora.warp.rediswire;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * What the Redis frontend needs from the surrounding Warp: the Postgres hosts of its store, pooled and
 * dedicated (LISTEN) connections to them, connect-time routing and metrics. {@link RegistryBackends} adapts a
 * {@code BackendRegistry}; tests and the dev harness use a direct implementation.
 */
interface Backends {

    /** Shard hosts (declaration order = slot-range order) for a session that is not routed. */
    List<String> defaultHosts();

    /**
     * Connect-time routing: hosts for the (username, selected db) pair, {@code null} when no route applies (use
     * {@link #defaultHosts()}). Throws {@link RedisError} when the connection is rejected by strict routing or the route
     * names something that cannot host the store.
     */
    List<String> routeHosts(String username, int db);

    /** A pooled autocommit connection to {@code host}; closing it returns it to the pool. */
    Connection open(String host) throws SQLException;

    /** A dedicated, non-pooled connection (LISTEN); never returned to a pool. */
    Connection openDedicated(String host) throws SQLException;

    /** Idempotent schema bootstrap for the store on {@code host}. */
    void ensureSchema(String host) throws SQLException;

    /** Per-command metrics: RTT in nanoseconds. */
    default void record(String host, boolean write, String command, long nanos) {
    }
}
