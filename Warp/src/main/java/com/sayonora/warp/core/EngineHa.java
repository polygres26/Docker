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

    /** True when {@link #repoint} is implemented. */
    default boolean supportsRepoint() {
        return false;
    }

    /**
     * Points {@code replica}'s replication at {@code newPrimary} after a promotion, so it keeps serving
     * reads. Must verify that replication is actually running from the new primary before returning, and
     * throw (with a useful message) when it cannot be repointed or does not start streaming.
     */
    default void repoint(BackendTarget replica, BackendTarget newPrimary) throws Exception {
        throw new UnsupportedOperationException("repointing replicas is not supported for " + replica.dialect());
    }

    enum RejoinOutcome { NOT_NEEDED, REJOINED, NEEDS_REBUILD, UNSUPPORTED }

    /** What {@link #rejoin} did, with a human-readable detail for events. */
    record RejoinResult(RejoinOutcome outcome, String detail) {
        static RejoinResult of(RejoinOutcome o, String detail) {
            return new RejoinResult(o, detail);
        }
    }

    /**
     * A node that used to be the primary (or otherwise is not replicating) is reachable again while
     * {@code currentPrimary} is writable: make it a replica of the current primary if that is provably
     * safe -- it must hold no transactions the current primary lacks -- else say it needs a rebuild.
     * Must never lose data and must leave the node as it found it when it refuses.
     */
    default RejoinResult rejoin(BackendTarget node, BackendTarget currentPrimary) throws Exception {
        return RejoinResult.of(RejoinOutcome.UNSUPPORTED, "automatic rejoin is not supported for " + node.dialect());
    }

    /** True when the planned-switchover operations below are implemented (and promote/repoint work). */
    default boolean supportsSwitchover() {
        return false;
    }

    /** Stops the primary accepting writes (best effort for what it can enforce) without shutting it down. */
    default void freezeWrites(BackendTarget primary) throws Exception {
        throw new UnsupportedOperationException("planned switchover is not supported for " + primary.dialect());
    }

    /** Undoes {@link #freezeWrites}; used when a switchover is aborted. */
    default void unfreezeWrites(BackendTarget primary) throws Exception {
        throw new UnsupportedOperationException("planned switchover is not supported for " + primary.dialect());
    }

    /** With {@code primary} frozen, returns only once {@code replica} has applied everything the primary
     * ever committed; throws if that does not happen within {@code timeoutSeconds}. */
    default void awaitCaughtUp(BackendTarget primary, BackendTarget replica, long timeoutSeconds) throws Exception {
        throw new UnsupportedOperationException("planned switchover is not supported for " + primary.dialect());
    }

    /** Turns the old, frozen primary into a replica of {@code newPrimary}. Returns false when the engine
     * cannot do that over SQL (the old primary is then left read-only, to be rebuilt by hand). */
    default boolean demoteToReplica(BackendTarget oldPrimary, BackendTarget newPrimary) throws Exception {
        return false;
    }

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
