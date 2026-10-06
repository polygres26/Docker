package com.sayonora.warp.core;

import java.util.OptionalDouble;
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

    /**
     * What a planned switchover to {@code target} needs done while the primary is still serving, before writes are stopped (SQL Server:
     * put both replicas in synchronous commit and wait until the target is synchronized, so the demotion that follows cannot lose a
     * commit). Throws, after undoing what it did, when the target is not ready. Most engines need nothing.
     */
    default void prepareSwitchover(BackendTarget primary, BackendTarget target) throws Exception {
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

    /**
     * Evidence that the primary is alive although Warp cannot reach it: how many seconds ago {@code replica} last heard from
     * {@code primary} over its own replication link, or empty when the replica is not connected to that primary (or the engine cannot
     * tell). A recent answer means the primary is up and Warp is probably the one partitioned, so promoting would create a second writer.
     * Empty is "no evidence", never "the primary is dead".
     */
    default OptionalDouble heardFromPrimarySecondsAgo(BackendTarget replica, BackendTarget primary) throws Exception {
        return OptionalDouble.empty();
    }

    /**
     * Stops a node that is writable but must not be (a stale old primary after a failover) from accepting writes, over SQL, without
     * restarting it or touching the host. It is a guard against divergence until the node is rebuilt, not a demotion. The default is
     * {@link #freezeWrites}; engines where that is wrong or unsafe override it or throw {@link UnsupportedOperationException}.
     */
    default void fenceStaleWriter(BackendTarget node) throws Exception {
        freezeWrites(node);
    }

    /**
     * True when {@code node} reports itself writable (it is not a replica) but refuses writes because it was frozen: a primary left
     * read-only by a planned switchover, or a stale old primary Warp froze. Warp applies it to nodes listed as replicas only, so such a node
     * is treated as read-only (not a second writer) while it waits to be rebuilt. It is never applied to the configured primary, so a primary
     * that is read-only for some other reason cannot trigger a failover. The default is false.
     */
    default boolean writesFrozen(BackendTarget node) throws Exception {
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
