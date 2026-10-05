package com.sayonora.warp.core;

import java.util.OptionalLong;

/**
 * The engine-specific half of replica routing and failover: how to measure a replica's lag, tell
 * whether a node currently accepts writes, rank replicas by how much of the primary's log they have
 * received, and promote one. Everything else (eligibility, confirmation windows, leases, config
 * writes) is engine-neutral and lives in {@link ReplicaRouter} / {@link FailoverMonitor}.
 *
 * <p>An engine without an implementation here is never routed to and never failed over -- the
 * callers treat {@code forDialect(...) == null} as "unsupported" rather than guessing.
 */
public interface EngineHa {

    /** Replication lag of {@code replica}. {@code ok=false} when it cannot be measured or the
     * replication threads are not running -- such a replica is ineligible for reads. */
    ReplicaRouter.LagSample lag(BackendTarget replica, long nowMillis);

    /** {@code WRITABLE} only when the node is reachable and accepts writes; {@code READ_ONLY} when it is
     * reachable but is (or acts as) a replica; {@code UNREACHABLE} only when it cannot be talked to. */
    FailoverMonitor.NodeRole role(BackendTarget node);

    /** A monotonic measure of how much of the primary's log this replica has received (higher =
     * less data lost if promoted); empty when it cannot be determined. */
    OptionalLong walPosition(BackendTarget replica) throws Exception;

    /** Promotes {@code replica} to a writable primary, returning only once it accepts writes. Must
     * throw rather than risk losing data it already received. */
    void promote(BackendTarget replica) throws Exception;

    /** False for engines where Warp will follow a promotion made elsewhere but never perform one. */
    default boolean supportsPromote() {
        return true;
    }

    /** The implementation for {@code dialect}, or {@code null} when the engine is not supported. */
    static EngineHa forDialect(SourceDialect dialect) {
        if (dialect == null) {
            return null;
        }
        return switch (dialect) {
            case POSTGRES -> PostgresHa.INSTANCE;
            case MYSQL -> MySqlHa.INSTANCE;
            case ORACLE -> OracleHa.INSTANCE;
            case SQL_SERVER -> SqlServerHa.INSTANCE;
            default -> null;
        };
    }
}
