package com.nexagres.dms.migration;

import com.nexagres.dms.core.ConnectionRecord;
import com.nexagres.dms.core.ConnectionStore;
import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.checkpoint.DeadLetterStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.core.MigrationLicensing;
import com.nexagres.migration.sink.PolywireGrpcSink;
import com.nexagres.migration.sink.ResilientSink;
import com.nexagres.migration.core.Sink;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What makes Advisor's Data Sync section actually LAUNCH a migration, not just report on one --
 * this is the piece {@code MigrationStatusStore}'s own javadoc explicitly described as out of
 * scope when it was first built ("nexagres-migration has no HTTP surface of its own by design...
 * Advisor is where a human looks at migration progress"). Constructs a real {@link Coordinator}
 * via {@link MigrationSourceFactory} and runs it on a background thread, tracking each run's
 * state in memory so {@link MigrationJobsRoute} has something to report immediately after
 * starting a job, before the FIRST checkpoint row a poll of {@code MigrationStatusStore} would
 * need even exists yet.
 *
 * <p>Deliberately runs a single-process {@link Coordinator}, never {@code DistributedCoordinator}
 * -- launching a FLEET of separate worker processes from one HTTP request handler in one Advisor
 * process doesn't fit "click Start in a browser" the way a single in-process coordinator does,
 * and {@code DistributedCoordinator} is the Enterprise-only feature anyway (see {@code
 * MigrationLicensing}'s own javadoc in nexagres-migration) -- a free-tier Advisor container
 * launching migrations through this class gets exactly the free-tier behavior for free: {@link
 * Coordinator} itself already clamps {@code parallelism} to {@code 1} without a valid
 * {@code POLYWIRE_LICENSE_KEY}, with zero special-casing needed here.
 *
 * <p>In-memory job registry, not persisted -- an Advisor restart loses the run history (though
 * not the migration's own progress: that lives in the target Postgres's own bookkeeping tables,
 * still readable via {@code MigrationStatusStore} regardless of whether Advisor remembers
 * launching it). A real production job queue (surviving restarts, retrying a crashed job) is a
 * further follow-up, not built here -- this is "click Start, watch it run," not a full scheduler.
 */
public final class MigrationJobRunner {

    private static final Logger log = LoggerFactory.getLogger(MigrationJobRunner.class);

    /** One launched run's state, snapshotted for {@link MigrationJobsRoute}'s JSON response --
     * plain fields (like {@link ConnectionRecord}) rather than an immutable record, since a
     * running job's own background thread mutates {@code status}/{@code errorMessage}/{@code
     * finishedAt} in place as it progresses. */
    public static final class JobState {
        public final String id;
        public final String connectorType;
        public final String sourceKeyHint;
        public volatile String status = "RUNNING"; // RUNNING | COMPLETED | FAILED
        public volatile String errorMessage;
        public final String startedAt = Instant.now().toString();
        public volatile String finishedAt;

        JobState(String id, String connectorType, String sourceKeyHint) {
            this.id = id;
            this.connectorType = connectorType;
            this.sourceKeyHint = sourceKeyHint;
        }
    }

    private final ConnectionStore connectionStore = new ConnectionStore();
    private final Map<String, JobState> jobs = new ConcurrentHashMap<>();
    // Tracked separately from JobState (which is what gets serialized to JSON) purely so stop()
    // has something to call close() on -- Source#close() is this project's own documented
    // "ask a running readPartition/streamChanges loop to stop at its next checkpoint" signal (see
    // every connector's own close() javadoc), the same mechanism every migration-module
    // integration test already uses via Thread#interrupt-plus-close in its own teardown.
    private final Map<String, MigrationSourceFactory.Built> runningSources = new ConcurrentHashMap<>();
    // Unbounded cached pool -- each job blocks its own thread for the run's full duration (an
    // initial sync plus, for most connectors, an indefinitely-running live change feed), the same
    // "one thread per long-lived job" shape Coordinator's own worker threads already use.
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread t = new Thread(runnable, "migration-job");
        t.setDaemon(true);
        return t;
    });

    public JobState start(MigrationJobRequest request) {
        MigrationLicensing.requireCapacityForAnotherConcurrentJob((int) countRunningJobs());
        MigrationConnectorType type = parseConnectorType(request.connectorType);
        Optional<ConnectionRecord> target = connectionStore.get(request.targetConnectionId);
        if (target.isEmpty()) {
            throw new IllegalArgumentException("targetConnectionId does not match a saved connection: "
                    + request.targetConnectionId);
        }
        requireNonBlank(request.polywireGrpcHost, "polywireGrpcHost");
        requireNonBlank(request.polywireGrpcUser, "polywireGrpcUser");
        requireNonBlank(request.polywireGrpcPassword, "polywireGrpcPassword");
        if (request.polywireGrpcPort <= 0) {
            throw new IllegalArgumentException("polywireGrpcPort must be a positive port number");
        }

        // Built (and any exception from a malformed sourceConfig) happens on the CALLING thread --
        // a bad request should fail the HTTP call itself with a real error, not silently "start" a
        // job that immediately dies in the background with no feedback to whoever clicked Start.
        MigrationSourceFactory.Built built = MigrationSourceFactory.build(type, request.sourceConfig);

        ConnectionRecord targetConnection = target.get();
        String jobId = UUID.randomUUID().toString();
        JobState state = new JobState(jobId, type.name(), sourceKeyHint(built));
        jobs.put(jobId, state);
        runningSources.put(jobId, built);

        executor.submit(() -> runJob(state, built, targetConnection, request));
        return state;
    }

    /** Count of jobs this registry still considers RUNNING, for {@link
     * MigrationLicensing#requireCapacityForAnotherConcurrentJob} -- deliberately recomputed from
     * {@link JobState#status} rather than tracked as a separate counter, since {@code status} is
     * the single source of truth {@code runJob}'s {@code finally} block already updates. */
    private long countRunningJobs() {
        return jobs.values().stream().filter(j -> "RUNNING".equals(j.status)).count();
    }

    public List<JobState> list() {
        return jobs.values().stream()
                .sorted((a, b) -> b.startedAt.compareTo(a.startedAt))
                .toList();
    }

    /** Asks a running job to stop at its next checkpoint (via {@code Source#close()}) rather than
     * killing its thread outright -- a no-op if the job already finished (its entry was already
     * removed from {@link #runningSources} in {@link #runJob}'s {@code finally}). Returns
     * {@code true} only if a still-running job was actually signaled. */
    public boolean stop(String jobId) {
        MigrationSourceFactory.Built built = runningSources.get(jobId);
        if (built == null) {
            return false;
        }
        try {
            built.source().close();
        } catch (Exception e) {
            log.warn("error signaling migration job {} to stop", jobId, e);
        }
        return true;
    }

    private void runJob(JobState state, MigrationSourceFactory.Built built, ConnectionRecord targetConnection,
            MigrationJobRequest request) {
        try {
            CdcCheckpointStore checkpoints = new CdcCheckpointStore(targetConnection.jdbcUrl, targetConnection.user,
                    targetConnection.password);
            checkpoints.ensureSchema();
            DeadLetterStore deadLetters = new DeadLetterStore(targetConnection.jdbcUrl, targetConnection.user,
                    targetConnection.password);
            deadLetters.ensureSchema();

            try (PolywireGrpcSink grpcSink = new PolywireGrpcSink(request.polywireGrpcHost, request.polywireGrpcPort,
                    request.polywireGrpcUser, request.polywireGrpcPassword)) {
                // Free/Developer tier: write straight to gRPC, no retry/dead-letter wrapper -- a
                // failed write is immediately fatal to the run (see MigrationLicensing's own
                // javadoc on why this is honest, not degraded: the pre-ResilientSink behavior
                // every connector already had). Enterprise: wrap in ResilientSink as before.
                Sink sink = MigrationLicensing.resilientRetryAndDeadLetterAllowed()
                        ? new ResilientSink(grpcSink, deadLetters, 5, 1000)
                        : grpcSink;
                new Coordinator(built.source(), sink, checkpoints, request.parallelism).run();
            }
            state.status = "COMPLETED";
            log.info("migration job {} ({}) completed", state.id, state.connectorType);
        } catch (Exception e) {
            state.status = "FAILED";
            state.errorMessage = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            log.error("migration job {} ({}) failed", state.id, state.connectorType, e);
        } finally {
            state.finishedAt = Instant.now().toString();
            runningSources.remove(state.id);
            closeQuietly(built);
        }
    }

    private void closeQuietly(MigrationSourceFactory.Built built) {
        try {
            built.source().close();
        } catch (Exception e) {
            log.warn("error closing migration source", e);
        }
        for (AutoCloseable resource : built.externalResources()) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("error closing migration source's external resource {}", resource.getClass(), e);
            }
        }
    }

    /** Best-effort label for the job list before any checkpoint row exists yet -- {@code
     * source.toString()} for every connector defaults to {@code Object}'s own (unhelpful) form,
     * so this just reports the connector class's simple name; once the job is running,
     * {@code MigrationStatusStore}'s own {@code sourceKey} (read from the real checkpoint row) is
     * the authoritative label. */
    private static String sourceKeyHint(MigrationSourceFactory.Built built) {
        return built.source().getClass().getSimpleName();
    }

    private static MigrationConnectorType parseConnectorType(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("connectorType is required, one of "
                    + java.util.Arrays.toString(MigrationConnectorType.values()));
        }
        try {
            return MigrationConnectorType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown connectorType '" + raw + "', expected one of "
                    + java.util.Arrays.toString(MigrationConnectorType.values()));
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
    }
}
