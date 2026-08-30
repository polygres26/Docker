package com.nexagres.migration.coordinator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Postgres-backed work distribution across MULTIPLE {@link DistributedCoordinator} processes
 * (separate JVMs/containers, not just separate threads in one JVM -- the actual "massively
 * parallel" scale-out this session's migration plan calls for: Phase 1 parallelized partitions
 * within one process via a local thread pool; this is Phase 2, parallelizing across processes).
 *
 * <p>Deliberately reuses the same "own Postgres table for bookkeeping" pattern as {@code
 * CdcCheckpointStore} rather than standing up a separate coordination system (etcd/ZooKeeper/a
 * message queue) just to hand out work -- a lease row IS the coordination primitive, claimed with
 * one atomic {@code UPDATE ... WHERE ... RETURNING}, same as any other "claim a job" table.
 *
 * <p>Two distinct kinds of lease share this one table:
 * <ul>
 *   <li><b>Partition leases</b> -- one row per {@code (sourceKey, partitionId)}, claimed by
 *   whichever worker process gets there first; {@link #markDone} retires it permanently.
 *   <li><b>The change-feed leadership lease</b> -- a single sentinel row per {@code sourceKey}
 *   (partition id {@link #LEADER_PARTITION_ID}), claimed by exactly one worker process, which is
 *   the only one that calls {@link com.nexagres.migration.core.Source#prepareChangeFeed} and
 *   {@link com.nexagres.migration.core.Source#streamChanges} -- there must only ever be ONE live
 *   change stream per source, never one per worker process.
 * </ul>
 *
 * <p><b>Known, scoped gap</b> (not fixed here): no lease renewal/heartbeat while a partition read
 * or the change feed is in progress -- a lease is claimed once, for {@code leaseTtl}, and never
 * extended. A worker still actively working past its own lease's expiry can have that lease
 * (including change-feed leadership) claimed out from under it by another worker, causing
 * duplicate concurrent work for the remainder of that run. Every write this project produces is
 * idempotent by id, so duplicate work is wasted effort, not corruption -- but it is real wasted
 * effort, and true failover (detecting the old owner is actually dead before reassigning, and a
 * live heartbeat renewing an in-progress lease) is a real, separately scoped follow-up, not
 * pretended-away here. {@code leaseTtl} should be set generously (hours, not minutes) until that
 * follow-up lands.
 */
public final class PartitionLeaseStore {

    /** Reserved partition id for the change-feed leadership sentinel row -- never a real
     * partition id a {@link com.nexagres.migration.core.Source#listPartitions} would produce
     * (every real one is prefixed with the source key and {@code #p}). */
    static final String LEADER_PARTITION_ID = "__leader__";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public PartitionLeaseStore(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    public void ensureSchema() throws SQLException {
        try (Connection conn = open(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS migration_partition_leases ("
                    + "source_key text NOT NULL, "
                    + "partition_id text NOT NULL, "
                    + "status text NOT NULL DEFAULT 'pending', " // pending | done
                    + "worker_id text, "
                    + "leased_until timestamptz, "
                    + "updated_at timestamptz NOT NULL DEFAULT now(), "
                    + "PRIMARY KEY (source_key, partition_id))");
        }
    }

    /** Atomically claims {@code partitionId} for {@code workerId} for {@code leaseTtlSeconds},
     * unless it's already {@code done} or currently leased (unexpired) by someone else. One round
     * trip, one statement -- the {@code INSERT ... ON CONFLICT ... WHERE} clause is what makes
     * this race-safe across concurrent worker processes without a separate SELECT-then-UPDATE. */
    public boolean tryClaim(String sourceKey, String partitionId, String workerId, long leaseTtlSeconds) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO migration_partition_leases (source_key, partition_id, status, worker_id, leased_until, updated_at) "
                                + "VALUES (?, ?, 'pending', ?, now() + (? || ' seconds')::interval, now()) "
                                + "ON CONFLICT (source_key, partition_id) DO UPDATE SET "
                                + "worker_id = EXCLUDED.worker_id, leased_until = EXCLUDED.leased_until, updated_at = now() "
                                + "WHERE migration_partition_leases.status <> 'done' "
                                + "AND (migration_partition_leases.worker_id IS NULL OR migration_partition_leases.leased_until < now())")) {
            ps.setString(1, sourceKey);
            ps.setString(2, partitionId);
            ps.setString(3, workerId);
            ps.setLong(4, leaseTtlSeconds);
            return ps.executeUpdate() == 1;
        }
    }

    /** Retires a partition's lease permanently -- {@link #tryClaim} will never hand it out again,
     * to this worker or any other. */
    public void markDone(String sourceKey, String partitionId) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE migration_partition_leases SET status = 'done', updated_at = now() "
                                + "WHERE source_key = ? AND partition_id = ?")) {
            ps.setString(1, sourceKey);
            ps.setString(2, partitionId);
            ps.executeUpdate();
        }
    }

    /** Releases a lease back to {@code pending} early (immediately claimable by any worker,
     * including this one) -- called when a claimed partition's read fails, so a transient error
     * on one worker doesn't strand that partition unclaimed until its full TTL expires. Only
     * releases if {@code workerId} still owns it, so a worker that's already lost its lease to
     * someone else (past its TTL) can't accidentally release the NEW owner's claim. */
    public void release(String sourceKey, String partitionId, String workerId) throws SQLException {
        try (Connection conn = open();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE migration_partition_leases SET worker_id = NULL, leased_until = now(), updated_at = now() "
                                + "WHERE source_key = ? AND partition_id = ? AND worker_id = ? AND status <> 'done'")) {
            ps.setString(1, sourceKey);
            ps.setString(2, partitionId);
            ps.setString(3, workerId);
            ps.executeUpdate();
        }
    }

    /** Claims the single change-feed leadership sentinel for {@code sourceKey} -- exactly one
     * worker process should ever hold this at a time; see this class's own javadoc for the
     * "no renewal yet" caveat around what happens if that worker dies mid-run. Leadership is
     * never marked {@code done} (there's no terminal state for "owns the live change feed" the
     * way there is for a finished snapshot partition) -- it simply expires and becomes reclaimable
     * like any other lease. */
    public boolean tryAcquireLeadership(String sourceKey, String workerId, long leaseTtlSeconds) throws SQLException {
        return tryClaim(sourceKey, LEADER_PARTITION_ID, workerId, leaseTtlSeconds);
    }
}
