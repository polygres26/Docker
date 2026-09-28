package com.sayonora.dms.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only reporting over the {@code sayonora-migration} module's own bookkeeping tables --
 * {@code warp_cdc_checkpoints} and {@code migration_partition_leases} -- both plain tables in
 * whichever Postgres a running migration was pointed at as its TARGET (the same Postgres Warp
 * itself writes into, per {@code CdcCheckpointStore}/{@code PartitionLeaseStore}'s own design).
 *
 * <p>This is the "Web Progress Report" box from Dsync's own architecture (see this session's own
 * migration-plan discussion): {@code sayonora-migration} has no HTTP surface of its own by design
 * (it's a library + a CLI, not a service) -- Advisor is where a human looks at migration progress,
 * so Advisor reads the SAME bookkeeping tables the migration workers themselves read and write,
 * rather than migration/ growing its own reporting server. Nothing here writes to those tables.
 *
 * <p>Deliberately does NOT go through {@link BackendTarget}/{@link SourceDialect} (unlike every
 * other {@code *Route} in this module) -- {@link ConnectionsRoute#requireTarget} explicitly
 * rejects a {@code POSTGRES}-dialect connection since Postgres is never a valid Advisor migration-
 * assessment SOURCE. Here Postgres is exactly right: it's the migration TARGET, and a
 * {@link ConnectionRecord} pointed at it is just a plain jdbcUrl/user/password Advisor already
 * knows how to borrow a pool for via {@link BackendConnectionPools}.
 *
 * <p>Tolerates the target's migration tables not existing yet (SQLState {@code 42P01}) by
 * returning an empty list -- a perfectly normal state for a connection nobody has pointed a
 * migration worker at yet, not an error.
 */
public final class MigrationStatusStore {

    private static final String UNDEFINED_TABLE = "42P01";

    /** One row of the report -- one migration source (e.g. {@code "mongo:src.orders"}), aggregated
     * across whatever workers have touched it. {@code lagSeconds} is {@code null} when {@code
     * last_event_at} isn't populated yet (a source whose worker predates the {@code
     * last_event_at} column, or hasn't replicated a live change since restarting) -- distinct from
     * {@code 0}, which means "caught up." */
    public record SourceStatus(
            String sourceKey,
            long eventsApplied,
            String lastCheckpointAt,
            Long lagSeconds,
            int partitionsTotal,
            int partitionsDone,
            String leaderWorkerId
    ) {
    }

    public List<SourceStatus> listStatuses(String jdbcUrl, String user, String password, String poolKey) throws SQLException {
        Map<String, Long> eventsApplied = new LinkedHashMap<>();
        Map<String, String> lastCheckpointAt = new LinkedHashMap<>();
        Map<String, Long> lagSeconds = new LinkedHashMap<>();
        Map<String, int[]> partitionCounts = new LinkedHashMap<>(); // [total, done]
        Map<String, String> leaderWorkerId = new LinkedHashMap<>();

        try (Connection conn = BackendConnectionPools.borrow(poolKey, jdbcUrl, user, password)) {
            // The change-feed checkpoint row for a source (key has no "#p" partition suffix).
            // last_event_at (added for Phase 3 of this session's migration plan) is the SOURCE's
            // own timestamp for the last applied event -- EXTRACT(EPOCH FROM (now() -
            // last_event_at)) is a real replication-lag figure, not just "a checkpoint was saved
            // recently" (updated_at alone only proves the worker is alive, not caught up).
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT source_key, events_applied, updated_at, "
                            + "EXTRACT(EPOCH FROM (now() - last_event_at))::bigint AS lag_seconds "
                            + "FROM warp_cdc_checkpoints WHERE source_key NOT LIKE '%#p%' ORDER BY source_key");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("source_key");
                    eventsApplied.put(key, rs.getLong("events_applied"));
                    Instant updatedAt = rs.getTimestamp("updated_at").toInstant();
                    lastCheckpointAt.put(key, updatedAt.toString());
                    long lag = rs.getLong("lag_seconds");
                    if (!rs.wasNull()) {
                        lagSeconds.put(key, lag);
                    }
                }
            } catch (SQLException e) {
                if (!UNDEFINED_TABLE.equals(e.getSQLState())) {
                    throw e;
                }
                // No migration has ever checkpointed against this target -- nothing to report yet.
            }

            // Partition lease counts (excluding the leadership sentinel row) and, separately, who
            // (if anyone) currently holds change-feed leadership for each source.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT source_key, partition_id, status, worker_id FROM migration_partition_leases");
                    ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("source_key");
                    String partitionId = rs.getString("partition_id");
                    if ("__leader__".equals(partitionId)) {
                        leaderWorkerId.put(key, rs.getString("worker_id"));
                        continue;
                    }
                    int[] counts = partitionCounts.computeIfAbsent(key, k -> new int[2]);
                    counts[0]++;
                    if ("done".equals(rs.getString("status"))) {
                        counts[1]++;
                    }
                }
            } catch (SQLException e) {
                if (!UNDEFINED_TABLE.equals(e.getSQLState())) {
                    throw e;
                }
                // No migration has ever leased a partition against this target -- nothing to report yet.
            }
        }

        // Union of every source key seen in either table -- a source can show up in one before the
        // other (e.g. change-feed checkpointed but no partition table rows yet on a source with
        // partitionCount=1, or vice versa on a fresh run that's still snapshotting).
        java.util.LinkedHashSet<String> allKeys = new java.util.LinkedHashSet<>();
        allKeys.addAll(eventsApplied.keySet());
        allKeys.addAll(partitionCounts.keySet());
        allKeys.addAll(leaderWorkerId.keySet());

        List<SourceStatus> result = new ArrayList<>(allKeys.size());
        for (String key : allKeys) {
            int[] counts = partitionCounts.getOrDefault(key, new int[2]);
            result.add(new SourceStatus(
                    key,
                    eventsApplied.getOrDefault(key, 0L),
                    lastCheckpointAt.get(key),
                    lagSeconds.get(key),
                    counts[0],
                    counts[1],
                    leaderWorkerId.get(key)));
        }
        return result;
    }
}
