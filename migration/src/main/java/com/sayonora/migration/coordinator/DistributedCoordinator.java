package com.sayonora.migration.coordinator;

import com.sayonora.migration.core.MigrationLicensing;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.core.Sink;
import com.sayonora.migration.core.Source;
import com.sayonora.migration.core.StateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2 of this session's own migration plan: {@link Coordinator} parallelizes across LOCAL
 * threads in one process; this parallelizes across SEPARATE WORKER PROCESSES (containers,
 * machines) that each run their own {@code DistributedCoordinator} instance against the same
 * {@link Source} configuration, the same target, and -- the part that actually makes this
 * distributed rather than just N independent copies of the same migration -- the same shared
 * {@link PartitionLeaseStore}, so every partition is done exactly once across the whole fleet
 * (modulo the lease store's own documented no-renewal caveat) instead of N times, once per worker.
 *
 * <p>Every worker process:
 * <ol>
 *   <li>Calls {@link Source#ensureTargetSchema} (idempotent, safe for every worker to call).
 *   <li>Tries to claim the single change-feed leadership lease. At most one worker process wins;
 *   only that one calls {@link Source#prepareChangeFeed} and, later, {@link
 *   Source#streamChanges}. The others skip both entirely -- there must only ever be one live
 *   change stream per source, never one per worker.
 *   <li>Lists this source's partitions and, across a LOCAL thread pool (composing with Phase 1:
 *   each worker process still parallelizes its own claimed share locally), tries to claim each
 *   one via {@link PartitionLeaseStore#tryClaim}. A partition another worker already claimed (or
 *   already finished) is simply skipped -- not an error, just someone else's job.
 *   <li>Once every partition this worker could claim is either read or skipped, the leader (and
 *   only the leader) hands off to {@link Source#streamChanges}; every other worker's {@code run()}
 *   returns -- a non-leader worker process is expected to exit once its share of the initial sync
 *   is done, the same way a real distributed batch job's workers exit after their shard of the
 *   work completes.
 * </ol>
 *
 * <p><b>Paid/free line:</b> this class IS the "real massively parallel way to move data" this
 * session's migration plan gates behind a license -- see {@link
 * MigrationLicensing#requireEnterpriseForDistributedCoordination} for why running MULTIPLE worker
 * processes at all (not just this-process parallelism, see {@link Coordinator}'s own gate) is the
 * paid tier, and why it's enforced by refusing to construct rather than silently degrading.
 * Free/Developer tier: use {@link Coordinator} instead.
 */
public final class DistributedCoordinator {

    private static final Logger log = LoggerFactory.getLogger(DistributedCoordinator.class);

    private final Source source;
    private final Sink sink;
    private final StateStore checkpoints;
    private final PartitionLeaseStore leases;
    private final String sourceKey;
    private final String workerId;
    private final int parallelism;
    private final long leaseTtlSeconds;

    public DistributedCoordinator(Source source, Sink sink, StateStore checkpoints, PartitionLeaseStore leases,
            String sourceKey, String workerId, int parallelism, long leaseTtlSeconds) {
        MigrationLicensing.requireEnterpriseForDistributedCoordination();
        this.source = source;
        this.sink = sink;
        this.checkpoints = checkpoints;
        this.leases = leases;
        this.sourceKey = sourceKey;
        this.workerId = workerId;
        this.parallelism = MigrationLicensing.enforceLocalParallelism(parallelism);
        this.leaseTtlSeconds = leaseTtlSeconds;
    }

    /** Runs this worker process's share of the initial sync, then -- ONLY if this worker won
     * change-feed leadership -- blocks in the live change feed until interrupted or {@link
     * Source#close()} is called from another thread. A non-leader worker returns as soon as its
     * claimable partitions are exhausted. */
    public void run() throws Exception {
        source.ensureTargetSchema(sink);

        boolean isLeader = leases.tryAcquireLeadership(sourceKey, workerId, leaseTtlSeconds);
        log.info("migration[{}]: worker {} {} change-feed leadership", sourceKey, workerId,
                isLeader ? "acquired" : "did NOT acquire (another worker already holds it)");
        if (isLeader) {
            source.prepareChangeFeed(sink, checkpoints);
        }

        List<Partition> partitions = source.listPartitions();
        int workers = Math.min(parallelism, Math.max(1, partitions.size()));
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Future<Void>> futures = new ArrayList<>(partitions.size());
            for (Partition partition : partitions) {
                futures.add(pool.submit((Callable<Void>) () -> {
                    processPartition(partition);
                    return null;
                }));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
        }

        if (!isLeader) {
            log.info("migration[{}]: worker {} finished its share of the initial sync -- not the "
                    + "change-feed leader, exiting", sourceKey, workerId);
            return;
        }
        log.info("migration[{}]: initial sync complete across the fleet -- leader {} starting the live change feed",
                sourceKey, workerId);
        source.streamChanges(sink, checkpoints);
    }

    private void processPartition(Partition partition) throws Exception {
        if (!leases.tryClaim(sourceKey, partition.id(), workerId, leaseTtlSeconds)) {
            log.debug("migration[{}]: partition {} already claimed/done elsewhere -- skipping", sourceKey, partition.id());
            return;
        }
        try {
            source.readPartition(partition, sink, checkpoints);
            leases.markDone(sourceKey, partition.id());
        } catch (Exception e) {
            // Hand the partition back immediately rather than letting it sit claimed-but-stuck
            // until this worker's own lease TTL expires -- a transient failure on one worker
            // shouldn't stall that partition for the rest of the fleet.
            leases.release(sourceKey, partition.id(), workerId);
            throw e;
        }
    }
}
