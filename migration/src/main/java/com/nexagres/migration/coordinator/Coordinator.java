package com.nexagres.migration.coordinator;

import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives one {@link Source} end to end: fan the initial sync out across N parallel workers (one
 * per {@link Partition} -- the "highly parallelized initial sync" a real massively-parallel sync
 * tool needs, see this session's own migration-plan discussion), wait for every one of them to
 * finish, then hand off to the source's own live change feed.
 *
 * <p>The parallelism here is deliberately just a local thread pool for v1 -- the equivalent of
 * Dsync's own {@code runners/local}, not yet its distributed equivalent. The point of keeping
 * {@link Source}/{@link Sink} as narrow interfaces with no reference to threads or transport is
 * exactly so a later, genuinely distributed runner (partitions farmed out to separate
 * machines/processes over a real transport, not just local threads) can replace this class
 * without any connector needing to change at all.
 */
public final class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final Source source;
    private final Sink sink;
    private final StateStore checkpoints;
    private final int parallelism;

    public Coordinator(Source source, Sink sink, StateStore checkpoints, int parallelism) {
        this.source = source;
        this.sink = sink;
        this.checkpoints = checkpoints;
        this.parallelism = Math.max(1, parallelism);
    }

    /** Runs the initial sync, then blocks in the live change feed until interrupted or the
     * source's own {@link Source#close()} is called from another thread. */
    public void run() throws Exception {
        source.ensureTargetSchema(sink);
        List<Partition> partitions = source.listPartitions();
        int workers = Math.min(parallelism, Math.max(1, partitions.size()));
        log.info("migration: {} partition(s), {} parallel worker(s) for the initial sync", partitions.size(), workers);

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Future<Void>> futures = new ArrayList<>(partitions.size());
            for (Partition partition : partitions) {
                futures.add(pool.submit((Callable<Void>) () -> {
                    source.readPartition(partition, sink, checkpoints);
                    return null;
                }));
            }
            // Surfaces the FIRST real failure rather than silently continuing -- a partition that
            // failed to copy means the migration is incomplete, not something to paper over and
            // proceed to CDC as if nothing happened.
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
        }

        log.info("migration: initial sync complete -- starting the live change feed");
        source.streamChanges(sink, checkpoints);
    }
}
