package com.nexagres.migration.readiness;

import com.nexagres.migration.core.MigrationLicensing;
import com.nexagres.migration.verify.VerificationResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 5 of this session's migration plan. Reads the SAME target-Postgres bookkeeping tables
 * {@code CdcCheckpointStore}, {@code PartitionLeaseStore}, and {@code DeadLetterStore} already
 * write ({@code warp_cdc_checkpoints}, {@code migration_partition_leases},
 * {@code migration_dead_letters}) -- exactly the pattern Advisor's own {@code MigrationStatusStore}
 * uses for its progress report -- and turns them into a small set of pass/fail gates an operator
 * (or an automated cutover script, via {@link CutoverReadinessReport#ready()} as an exit code)
 * checks before pointing a connection at Postgres and retiring the legacy source for good.
 *
 * <p>Deliberately does NOT reimplement row-level verification itself: {@code RowChecksum}/
 * {@link VerificationResult} already exist for that, and computing a fresh count+checksum pair
 * needs a live query against the ACTUAL source system (Mongo, MySQL, DynamoDB, ...), which this
 * class -- generic across every connector -- has no connection to. Callers that have already run
 * that comparison pass the result in via {@link #check(String, boolean, long, VerificationResult)};
 * callers that haven't (or a source where a live row-by-row comparison isn't practical, e.g. a
 * multi-terabyte table where verification only runs periodically, not on every readiness check)
 * use {@link #check(String, boolean, long)}, which simply omits that gate from the report rather
 * than either faking a pass or forcing every readiness check to pay for a full verification pass.
 *
 * <p>Fails CLOSED wherever the underlying data is ambiguous, not just wherever it's explicitly
 * bad -- e.g. zero partition-lease rows for a source reads as "migration may never have run," not
 * as "nothing to check, so pass": a readiness gate that can be tricked into a false green light by
 * simply never having started is worse than useless.
 */
public final class CutoverReadinessChecker {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public CutoverReadinessChecker(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    /** As {@link #check(String, boolean, long, VerificationResult)}, but without a row-level
     * verification gate -- the report simply has one fewer check in it. */
    public CutoverReadinessReport check(String sourceKey, boolean hasLiveChangeFeed, long maxLagSeconds) throws SQLException {
        return check(sourceKey, hasLiveChangeFeed, maxLagSeconds, null);
    }

    /**
     * @param sourceKey the same key used throughout this project's bookkeeping tables (e.g.
     *     {@code "mongo:mydb.orders"})
     * @param hasLiveChangeFeed whether this source has a real live change feed at all -- {@code
     *     false} for a snapshot-only source (Neo4j; SQS's own drain-forward-delete has no separate
     *     "caught up" concept either, see its own connector javadoc), which skips the lag gate
     *     entirely rather than failing a source that was never going to have one
     * @param maxLagSeconds the maximum acceptable {@code now() - last_event_at} for the lag gate
     *     to pass, only consulted when {@code hasLiveChangeFeed} is {@code true}
     * @param verification a freshly computed source/target comparison, or {@code null} to omit
     *     that gate from the report (see this class's own javadoc for why that's the caller's
     *     choice, not a fallback this class makes on its own)
     */
    public CutoverReadinessReport check(String sourceKey, boolean hasLiveChangeFeed, long maxLagSeconds,
            VerificationResult verification) throws SQLException {
        MigrationLicensing.requireEnterpriseForCutoverReadiness();
        List<ReadinessCheck> checks = new ArrayList<>();
        try (Connection conn = open()) {
            checks.add(checkPartitions(conn, sourceKey));
            checks.add(checkChangeFeedLag(conn, sourceKey, hasLiveChangeFeed, maxLagSeconds));
            checks.add(checkDeadLetters(conn));
        }
        if (verification != null) {
            checks.add(checkVerification(verification));
        }
        return new CutoverReadinessReport(sourceKey, checks);
    }

    private ReadinessCheck checkPartitions(Connection conn, String sourceKey) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) AS total, count(*) FILTER (WHERE status = 'done') AS done "
                        + "FROM migration_partition_leases WHERE source_key = ? AND partition_id <> '__leader__'")) {
            ps.setString(1, sourceKey);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long total = rs.getLong("total");
                long done = rs.getLong("done");
                if (total == 0) {
                    return new ReadinessCheck("snapshot partitions",
                            false,
                            "no partition-lease rows recorded for this source -- migration may never have run "
                                    + "against this target, or hasn't reached listPartitions() yet");
                }
                boolean allDone = done == total;
                return new ReadinessCheck("snapshot partitions", allDone,
                        done + "/" + total + " partitions done");
            }
        } catch (SQLException e) {
            if (isUndefinedTable(e)) {
                return new ReadinessCheck("snapshot partitions", false,
                        "migration_partition_leases table does not exist on this target -- no migration has run");
            }
            throw e;
        }
    }

    private ReadinessCheck checkChangeFeedLag(Connection conn, String sourceKey, boolean hasLiveChangeFeed,
            long maxLagSeconds) throws SQLException {
        if (!hasLiveChangeFeed) {
            return new ReadinessCheck("change-feed lag", true,
                    "source has no live change feed (snapshot-only) -- nothing to lag-check");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT last_event_at, EXTRACT(EPOCH FROM (now() - last_event_at))::bigint AS lag_seconds "
                        + "FROM warp_cdc_checkpoints WHERE source_key = ?")) {
            ps.setString(1, sourceKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new ReadinessCheck("change-feed lag", false,
                            "no checkpoint row found for this source -- change-feed leadership may never have "
                                    + "been acquired");
                }
                Timestamp lastEventAt = rs.getTimestamp("last_event_at");
                if (lastEventAt == null) {
                    return new ReadinessCheck("change-feed lag", false,
                            "checkpoint exists but no live change event has been applied yet -- lag unknown");
                }
                long lagSeconds = rs.getLong("lag_seconds");
                Instant lastEventInstant = lastEventAt.toInstant();
                boolean withinThreshold = lagSeconds <= maxLagSeconds;
                return new ReadinessCheck("change-feed lag", withinThreshold,
                        "lag " + lagSeconds + "s (threshold " + maxLagSeconds + "s), last event at "
                                + lastEventInstant + " (" + Duration.ofSeconds(lagSeconds) + " ago)");
            }
        } catch (SQLException e) {
            if (isUndefinedTable(e)) {
                return new ReadinessCheck("change-feed lag", false,
                        "warp_cdc_checkpoints table does not exist on this target -- no migration has run");
            }
            throw e;
        }
    }

    /** Global across every source on this target, not per-{@code sourceKey} -- {@code
     * migration_dead_letters} (see its own javadoc) never recorded which source a failed event
     * came from, only its SQL text and params, so a dead letter from an unrelated migration on
     * the same target Postgres will also block this gate. Documented here rather than silently
     * scoped as if it were per-source: an operator relying on this check for a multi-source target
     * should clear (or at least review) dead letters before treating a zero here as "this specific
     * source has none." */
    private ReadinessCheck checkDeadLetters(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM migration_dead_letters");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            long count = rs.getLong(1);
            return new ReadinessCheck("dead letters", count == 0,
                    count == 0
                            ? "no dead-lettered events on this target"
                            : count + " dead-lettered event(s) on this target (global, not source-scoped -- "
                                    + "review migration_dead_letters before cutting over)");
        } catch (SQLException e) {
            if (isUndefinedTable(e)) {
                // No ResilientSink has ever run against this target -- nothing was ever dead-lettered,
                // a genuine pass, unlike the other two checks' "table missing" cases (which mean "no
                // migration ran at all," not "nothing to report").
                return new ReadinessCheck("dead letters", true,
                        "migration_dead_letters table does not exist -- no failures have ever been recorded");
            }
            throw e;
        }
    }

    private ReadinessCheck checkVerification(VerificationResult verification) {
        return new ReadinessCheck("row-level verification", verification.matches(),
                "source count " + verification.sourceCount() + " vs target count " + verification.targetCount()
                        + ", source checksum " + Long.toHexString(verification.sourceChecksum())
                        + " vs target checksum " + Long.toHexString(verification.targetChecksum()));
    }

    private static boolean isUndefinedTable(SQLException e) {
        return "42P01".equals(e.getSQLState());
    }
}
