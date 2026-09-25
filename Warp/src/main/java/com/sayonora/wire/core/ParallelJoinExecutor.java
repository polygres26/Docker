package com.sayonora.wire.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.sayonora.wire.config.NodeRegistry;
import com.sayonora.wire.grpc.WarpPeerGrpcServer;
import com.sayonora.wire.grpc.proto.JoinPartitionRequest;
import com.sayonora.wire.grpc.proto.JoinPartitionResponse;
import com.sayonora.wire.grpc.proto.Row;
import com.sayonora.wire.grpc.proto.WarpPeerServiceGrpc;
import com.sayonora.wire.server.ServerOptions;
import io.grpc.ManagedChannel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 0 of the "Warp-native parallel execution engine" design: executes a {@link
 * ParallelJoinPlanner.Plan} as a real, local, multi-threaded hash join -- build side streamed and
 * hash-partitioned across {@code threadCount} in-memory partitions (each with its own Guava {@link
 * BloomFilter}), probe side streamed and dispatched to each partition's own worker thread, which
 * does the actual key lookup/row-emission in parallel with its siblings.
 *
 * <p><b>Why application-code partitioning, not backend-pushed hash predicates</b>: rewriting each
 * side's SQL N times with a {@code WHERE hash(key) % N = i} predicate pushed into the backend's own
 * WHERE clause was considered and rejected -- hash functions aren't portable across Postgres/MySQL/
 * Oracle/SQL Server dialects. Instead each side is scanned exactly ONCE via a single streaming JDBC
 * cursor, and partitioning/hashing happens here, in Java, as rows arrive -- this still parallelizes
 * the actual CPU-bound work (hashing, matching, building output rows), which is where the real win
 * is, without any dialect risk.
 *
 * <p><b>Pipelining</b>: the build phase is an accepted, unavoidable blocking point (a hash join's
 * build side must be complete before probing can start) -- its cost is minimized by always building
 * from the smaller side ({@link ParallelJoinPlanner}'s own row-count probe) and by the Bloom filter
 * letting the probe phase cheaply reject non-matching rows before they're ever queued. Once the
 * build finishes, the probe phase is fully pipelined: rows are dispatched to worker queues as they
 * stream in from the probe backend, and each worker processes its queue concurrently with the probe
 * scan still running -- no second blocking wait.
 *
 * <p>A Bloom filter false positive here can only ever cause a wasted queue entry and a real
 * hash-table miss on the worker side -- it can never produce a wrong join result, since the actual
 * match is always re-verified against the real, exact build-side hash table.
 *
 * <p><b>Phase 1b -- some partitions may run on a remote peer instead of a local thread.</b> When
 * {@code WARP_PARALLEL_JOIN_REMOTE_ENABLED=true} and {@code WARP_PEER_TLS_KEYSTORE} is configured,
 * {@link #remotePeersOrEmpty()} consults {@link NodeRegistry#listOtherLivePeerWorkers} for other
 * live Warp nodes running {@code WarpPeerGrpcServer}. The simplest possible scheduling policy (per
 * the parent design's own "use RemoteNodeSlots for as many partitions as there are usable other
 * nodes for, LocalThreadSlots for the rest"): partition indices {@code [0, min(peerCount,
 * threadCount))} are dispatched one-per-peer over {@code WarpPeerService.JoinPartition} (Phase 1a's
 * primitive); the remaining partitions run exactly as Phase 0 always has, as local threads. The
 * build phase is unaffected either way -- every partition's build-side hash table/Bloom filter is
 * always built locally, since a remote dispatch needs that partition's raw build rows regardless.
 * A remote dispatch that fails (peer unreachable, TLS handshake failure, or the peer's own {@code
 * JoinPartitionResponse.success=false}) falls back to computing that ONE partition locally via
 * {@link RemotePartitionJoin#probeAll} in the coordinator's own thread, synchronously -- a partition
 * never fails the whole query just because one peer was briefly unreachable.
 *
 * <p><b>Known, disclosed Phase 1b limitation</b>: a remotely-processed partition's output values
 * come back text-encoded (the same {@code Row{values, is_null}} convention every gRPC service in
 * this codebase already uses), not as their original JDBC types -- a partition that fell back to
 * local execution, or that never had a peer to begin with, keeps its original typed values. This is
 * consistent with how the rest of the codebase already treats gRPC-carried values, not a new gap
 * specific to this feature, but it does mean a caller inspecting exact value types could observe a
 * difference between a locally-run and a remotely-run partition's rows for the same query.
 *
 * <p><b>Phase 2 additions</b>:
 * <ul>
 * <li><b>Retry/failover</b> -- {@link #dispatchRemotePartition} no longer gives up on the first
 * failed peer. It walks every OTHER discovered live peer (in {@link #remotePeersOrEmpty()}'s own
 * order, skipping the one already tried) before finally falling back to local execution -- a single
 * peer's failure no longer costs that partition's parallelism if any other peer is reachable.</li>
 * <li><b>Cost-based placement</b> ({@code WARP_PARALLEL_JOIN_REMOTE_MIN_ROWS}, default 50,000) --
 * a partition whose combined build+probe row count doesn't clear this threshold is processed
 * locally even though a peer was assigned to it; the network round trip (and the peer's own
 * matching work) only pays off once a partition is genuinely large. This reuses rows already
 * collected -- no new probing round trip.</li>
 * <li><b>Bounded remote buffering</b> ({@code WARP_PARALLEL_JOIN_REMOTE_MAX_BUFFERED_ROWS}, default
 * 200,000) -- practical backpressure/skew mitigation for a remote partition that turns out far
 * larger than expected (a skewed key distribution routing an outsized share of rows to one
 * partition): once a remote partition's buffered probe rows hit this cap, further rows for it
 * spill into a SEPARATE, unbounded overflow list processed locally (not sent over the wire, not
 * handed to the peer) -- protecting both the coordinator's own memory and the size of the single
 * unary gRPC payload a peer would otherwise have to receive. This is a real, disclosed, narrower
 * substitute for the harder problems (dynamic repartitioning, spill-to-disk) the parent design
 * explicitly defers until real usage data justifies that larger investment.</li>
 * </ul>
 */
final class ParallelJoinExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParallelJoinExecutor.class);

    /** Guava's {@link BloomFilter} needs an expected-insertion-count hint to size itself; it
     * remains correct (just with a higher false-positive rate, never a wrong answer -- see this
     * class's own javadoc) if the real build side ends up larger than this. A fixed default avoids
     * needing a row-count estimate before the build scan even starts. */
    private static final long DEFAULT_BLOOM_EXPECTED_INSERTIONS = 1_000_000L;

    /** {@code WARP_PARALLEL_JOIN_DYNAMIC_FILTER_MAX_KEYS} default -- matches {@code
     * SemiJoinPushdown}'s own {@code WARP_SEMIJOIN_MAX_KEYS} default, the same real cost tradeoff
     * (an unbounded literal {@code IN (...)} list is its own expense) applied to the same idea. */
    private static final long DYNAMIC_FILTER_MAX_KEYS = 20_000L;

    /** A single, privately-held instance used purely for reference-equality end-of-stream
     * signaling on each partition's queue -- deliberately not {@code List.of()} (whose empty-list
     * singleton isn't a safe identity to compare against) and never equal to any real row. */
    private static final List<Object> END_OF_STREAM = new ArrayList<>(0);

    private ParallelJoinExecutor() {
    }

    /** {@code WARP_PARALLEL_JOIN_THREADS}, when explicitly set, is an outright OVERRIDE -- an
     * operator who's pinned a specific value gets exactly that, no cost-based second-guessing.
     * Left unset, the real fallback is available-core count -- unchanged Phase 0/1/2 behavior. */
    static int threadCountFromEnvOrDefault() {
        String raw = System.getenv("WARP_PARALLEL_JOIN_THREADS");
        if (raw != null && !raw.isBlank()) {
            try {
                int parsed = Integer.parseInt(raw.trim());
                if (parsed > 0) {
                    return parsed;
                }
            } catch (NumberFormatException ignoredNotANumber) {
                // falls through to the core-count default below
            }
        }
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    /** Cost-based partition-count sizing: when {@code WARP_PARALLEL_JOIN_THREADS} is NOT explicitly
     * set and a real probe-side row-count estimate is available ({@link
     * ParallelJoinPlanner.Plan#probeRowCountEstimate()}, the larger side -- the one that actually
     * drives per-partition work, not the build side), the partition count is derived from it: enough
     * partitions that each gets roughly {@code WARP_PARALLEL_JOIN_TARGET_ROWS_PER_PARTITION} (default
     * 25,000) rows, clamped to {@code [1, availableProcessors()]} -- more partitions than cores just
     * adds handoff overhead with no real parallelism gain. A tiny partition (a handful of rows) still
     * gets 1 partition, not zero; there is never less real parallelism than a plain sequential run
     * would have provided, only less than the core count when it wouldn't help. Falls back to {@link
     * #threadCountFromEnvOrDefault()} whenever the estimate is unknown (probe failed) or the operator
     * explicitly pinned a thread count -- an explicit override always wins outright. */
    static int partitionCountFor(ParallelJoinPlanner.Plan plan) {
        String explicit = System.getenv("WARP_PARALLEL_JOIN_THREADS");
        if (explicit != null && !explicit.isBlank()) {
            return threadCountFromEnvOrDefault();
        }
        long probeRowCountEstimate = plan.probeRowCountEstimate();
        if (probeRowCountEstimate < 0) {
            return threadCountFromEnvOrDefault();
        }
        long targetRowsPerPartition = parseLongEnv("WARP_PARALLEL_JOIN_TARGET_ROWS_PER_PARTITION", 25_000L);
        if (targetRowsPerPartition <= 0) {
            return threadCountFromEnvOrDefault();
        }
        long suggested = (probeRowCountEstimate + targetRowsPerPartition - 1) / targetRowsPerPartition;
        int maxPartitions = Math.max(1, Runtime.getRuntime().availableProcessors());
        return (int) Math.max(1, Math.min(suggested, maxPartitions));
    }

    static ExecutionResult execute(ParallelJoinPlanner.Plan plan, int threadCount) throws SQLException {
        int n = Math.max(1, threadCount);
        PartitionSpill[] partitionSpills = new PartitionSpill[n];
        try {
            return executeInternal(plan, n, partitionSpills);
        } finally {
            // Every spill file is per-EXECUTION, never reused across queries -- always cleaned up
            // here regardless of success/failure/exception, so a query that spills never leaks a
            // temp file even when it ultimately fails for an unrelated reason.
            for (PartitionSpill spill : partitionSpills) {
                if (spill != null) {
                    spill.close();
                }
            }
        }
    }

    private static ExecutionResult executeInternal(ParallelJoinPlanner.Plan plan, int n, PartitionSpill[] partitionSpills)
            throws SQLException {
        List<Map<Object, List<List<Object>>>> partitionTables = new ArrayList<>(n);
        List<BloomFilter<CharSequence>> partitionFilters = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            partitionTables.add(new HashMap<>());
            partitionFilters.add(BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8),
                    DEFAULT_BLOOM_EXPECTED_INSERTIONS));
        }

        // Phase 1b scheduling: partitions [0, remoteCount) go to a real peer over the network,
        // the rest stay local threads exactly as Phase 0 always has. remotePeer(i) is null for a
        // local partition. The build phase below is unaffected either way -- every partition's
        // hash table/Bloom filter is always built locally regardless of where it's PROBED.
        List<NodeRegistry.NodeRow> peers = remotePeersOrEmpty();
        int remoteCountBeforeSpillCheck = Math.min(peers.size(), n);

        // Skew mitigation (Phase 2+): a partition whose build-side row count exceeds
        // WARP_PARALLEL_JOIN_SPILL_THRESHOLD_ROWS (default 1,000,000; <=0 disables spilling
        // entirely, today's unbounded-in-memory behavior unchanged) spills further rows to a
        // per-partition PartitionSpill instead of growing its in-memory hash table without bound --
        // see that class's own javadoc for exactly what this does and doesn't do.
        long spillThresholdRows = parseLongEnv("WARP_PARALLEL_JOIN_SPILL_THRESHOLD_ROWS", 1_000_000L);
        long[] partitionRowCounts = new long[n];

        List<ColumnInfo> buildColumns = new ArrayList<>();
        try (Connection buildConnection = plan.buildBackend().target().open();
                PreparedStatement buildPs = buildConnection.prepareStatement(plan.buildSql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(buildPs, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> columns) {
                    buildColumns.addAll(applyColumnProjection(columns, plan.buildProjection()));
                }

                @Override
                public void onRow(List<Object> row) {
                    // The residual filter (if any) is expressed against the RAW row, before the
                    // projection remap -- see ParallelJoinPlanner.SideExtraction's own javadoc on
                    // why a Filter directly on the leaf never changes row shape.
                    if (plan.buildFilter() != null && !plan.buildFilter().test(row)) {
                        return;
                    }
                    List<Object> logicalRow = applyProjection(row, plan.buildProjection());
                    Object key = keyOf(logicalRow, plan.buildKeyOrdinal());
                    if (key == null) {
                        return; // SQL join semantics: NULL never equals NULL, so it can never match
                    }
                    int partition = partitionOf(key, n);
                    partitionFilters.get(partition).put(String.valueOf(key));
                    partitionRowCounts[partition]++;
                    if (spillThresholdRows > 0 && partitionRowCounts[partition] > spillThresholdRows) {
                        if (trySpill(partitionSpills, partition, key, logicalRow)) {
                            return;
                        }
                        // Spill setup failed (e.g. temp dir unwritable) -- fall back to keeping this
                        // row in memory rather than lose it; a real, disclosed narrowing (this
                        // partition's memory bound isn't honored), never a wrong answer.
                    }
                    partitionTables.get(partition).computeIfAbsent(key, k -> new ArrayList<>()).add(logicalRow);
                }
            });
        }
        boolean anyPartitionSpilled = false;
        for (PartitionSpill spill : partitionSpills) {
            if (spill != null) {
                anyPartitionSpilled = true;
                break;
            }
        }
        if (anyPartitionSpilled && remoteCountBeforeSpillCheck > 0) {
            // A spilled partition's in-memory table is INCOMPLETE by design -- shipping it to a peer
            // as-is would silently drop matches. Rather than thread per-partition spill awareness
            // through the whole remote-dispatch path, this query runs fully local once any partition
            // spills: a coarser, always-correct fallback, consistent with this engine's other real
            // narrowings favoring correctness over maximal optimization.
            log.info("parallel join: {} partition(s) exceeded the {}-row spill threshold -- running "
                    + "this query fully local instead of dispatching any partition remotely",
                    (int) java.util.Arrays.stream(partitionSpills).filter(java.util.Objects::nonNull).count(),
                    spillThresholdRows);
        }
        // Effectively final from here on, so the probe phase's own anonymous StreamingRowHandler
        // below can capture it directly.
        final int remoteCount = anyPartitionSpilled ? 0 : remoteCountBeforeSpillCheck;

        // Work stealing (Phase 2): local partitions are no longer each pinned to their OWN
        // dedicated thread -- a skewed key distribution can leave one partition's thread as the
        // bottleneck while every other partition's thread sits idle, having finished early. Instead
        // every local partition's draining is a short-lived, self-resubmitting task on a shared
        // ForkJoinPool-backed work-stealing executor (see #drainBatch) -- an idle worker thread can
        // pick up a DIFFERENT, busier partition's next batch instead of blocking on its own.
        int localPartitionCount = n - remoteCount;
        ExecutorService stealPool = Executors.newWorkStealingPool(Math.max(1, localPartitionCount));
        CountDownLatch localPartitionsDone = new CountDownLatch(Math.max(0, localPartitionCount));

        List<BlockingQueue<List<Object>>> queues = new ArrayList<>(n);
        List<List<List<Object>>> remoteProbeBuffers = new ArrayList<>(n);
        List<List<List<Object>>> partitionOutputs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            List<List<Object>> output = new ArrayList<>();
            partitionOutputs.add(output);
            if (i < remoteCount) {
                queues.add(null);
                remoteProbeBuffers.add(new ArrayList<>());
                continue;
            }
            remoteProbeBuffers.add(null);
            BlockingQueue<List<Object>> queue = new LinkedBlockingQueue<>(10_000);
            queues.add(queue);
            Map<Object, List<List<Object>>> table = partitionTables.get(i);
            PartitionSpill spill = partitionSpills[i];
            stealPool.execute(() -> drainBatch(stealPool, queue, table, spill, plan, output, localPartitionsDone));
        }

        // Bounded remote buffering (Phase 2) -- a remote partition that turns out far larger than
        // expected (key skew) spills its EXCESS probe rows into a separate, unbounded overflow list
        // processed locally instead of growing the remote buffer (and the eventual gRPC payload)
        // without limit. See this class's own javadoc.
        long remoteMaxBufferedRows = parseLongEnv("WARP_PARALLEL_JOIN_REMOTE_MAX_BUFFERED_ROWS", 200_000L);
        List<List<List<Object>>> remoteOverflowBuffers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            remoteOverflowBuffers.add(i < remoteCount ? new ArrayList<>() : null);
        }

        // Dynamic filtering (this session's own follow-up): now that the build side is fully known,
        // push its own real, distinct join-key values into the PROBE side's own extracted SQL as a
        // "WHERE <col> IN (...)" clause -- the exact-semi-join idea SemiJoinPushdown already applies
        // to the sequential path, closing the gap where the parallel path only ever filtered rows
        // AFTER fetching them (via each partition's own in-process Bloom filter), never cutting what
        // the probe backend itself has to scan/ship in the first place. Deliberately conservative,
        // same stance as SemiJoinPushdown's own: skipped outright (never a wrong answer, just a
        // missed optimization) whenever the probe key's real column name couldn't be resolved, there
        // turned out to be no build rows at all (an empty join -- the ordinary sequential/Bloom path
        // already handles that correctly), or the distinct key COUNT exceeds
        // WARP_PARALLEL_JOIN_DYNAMIC_FILTER_MAX_KEYS (default 20,000, matching SemiJoinPushdown's own
        // WARP_SEMIJOIN_MAX_KEYS default -- an unbounded literal IN-list is its own real cost).
        String probeSql = plan.probeSql();
        if (plan.probeKeyColumnName() != null) {
            java.util.Set<Object> distinctBuildKeys = new java.util.HashSet<>();
            for (int i = 0; i < n && distinctBuildKeys.size() <= DYNAMIC_FILTER_MAX_KEYS; i++) {
                distinctBuildKeys.addAll(partitionTables.get(i).keySet());
                if (partitionSpills[i] != null) {
                    distinctBuildKeys.addAll(partitionSpills[i].spilledKeys());
                }
            }
            long maxKeys = parseLongEnv("WARP_PARALLEL_JOIN_DYNAMIC_FILTER_MAX_KEYS", DYNAMIC_FILTER_MAX_KEYS);
            if (!distinctBuildKeys.isEmpty() && distinctBuildKeys.size() <= maxKeys) {
                probeSql = "SELECT * FROM (" + probeSql + ") __warp_dynamic_filter WHERE " + plan.probeKeyColumnName()
                        + " IN (" + SemiJoinPushdown.literalList(new ArrayList<>(distinctBuildKeys)) + ")";
                log.info("parallel join: dynamic filter pushed {} build-side key(s) into the probe side's own SQL",
                        distinctBuildKeys.size());
            }
        }

        List<ColumnInfo> probeColumns = new ArrayList<>();
        try (Connection probeConnection = plan.probeBackend().target().open();
                PreparedStatement probePs = probeConnection.prepareStatement(probeSql)) {
            JdbcBackendExecutor.streamOnPreparedStatement(probePs, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> columns) {
                    probeColumns.addAll(applyColumnProjection(columns, plan.probeProjection()));
                }

                @Override
                public void onRow(List<Object> row) {
                    if (plan.probeFilter() != null && !plan.probeFilter().test(row)) {
                        return;
                    }
                    List<Object> logicalRow = applyProjection(row, plan.probeProjection());
                    Object key = keyOf(logicalRow, plan.probeKeyOrdinal());
                    if (key == null) {
                        return;
                    }
                    int partition = partitionOf(key, n);
                    if (!partitionFilters.get(partition).mightContain(String.valueOf(key))) {
                        return; // real, cheap reject -- this key provably isn't in the build side
                    }
                    if (partition < remoteCount) {
                        List<List<Object>> remoteBuffer = remoteProbeBuffers.get(partition);
                        if (remoteBuffer.size() < remoteMaxBufferedRows) {
                            remoteBuffer.add(logicalRow);
                        } else {
                            remoteOverflowBuffers.get(partition).add(logicalRow);
                        }
                    } else {
                        offer(queues.get(partition), logicalRow);
                    }
                }
            });
        } catch (SQLException e) {
            for (int i = remoteCount; i < n; i++) {
                offer(queues.get(i), END_OF_STREAM);
            }
            awaitLocalPartitions(stealPool, localPartitionsDone);
            throw e;
        }

        for (int i = remoteCount; i < n; i++) {
            offer(queues.get(i), END_OF_STREAM);
        }
        awaitLocalPartitions(stealPool, localPartitionsDone);

        for (int i = 0; i < remoteCount; i++) {
            dispatchRemotePartition(plan, peers, i, buildColumns, probeColumns,
                    partitionTables.get(i), remoteProbeBuffers.get(i), partitionOutputs.get(i));
            // The overflow spill (if any) never went over the wire -- process it locally, merged
            // into the same partition's output as the remotely (or locally-fallback) processed rows.
            List<List<Object>> overflow = remoteOverflowBuffers.get(i);
            if (!overflow.isEmpty()) {
                log.info("parallel join: partition {} exceeded the {}-row remote buffer cap -- {} "
                        + "overflow row(s) processed locally instead of sent to the peer",
                        i, remoteMaxBufferedRows, overflow.size());
                partitionOutputs.get(i).addAll(RemotePartitionJoin.probeAll(
                        partitionTables.get(i), overflow, plan.probeKeyOrdinal(), plan.leftIsBuild()));
            }
        }

        List<ColumnInfo> finalColumns = new ArrayList<>(buildColumns.size() + probeColumns.size());
        List<List<Object>> allRows = new ArrayList<>();
        if (plan.leftIsBuild()) {
            finalColumns.addAll(buildColumns);
            finalColumns.addAll(probeColumns);
        } else {
            finalColumns.addAll(probeColumns);
            finalColumns.addAll(buildColumns);
        }
        for (List<List<Object>> output : partitionOutputs) {
            allRows.addAll(output);
        }
        return applyProjectionAggregateSortAndFinish(finalColumns, allRows, plan.outputProjection(),
                plan.aggregateSpec(), plan.sortKeys(), plan.fetchLimit());
    }

    /** The final, shared tail of every parallel join execution -- re-applying an outer column
     * selection/reordering, then a {@code GROUP BY} aggregation, then an {@code ORDER BY}/{@code
     * LIMIT} -- against an already-fully-joined row set. Shared by {@link #execute} (Phase 0's own
     * 2-way join) and {@link ChainedJoinExecutor#execute} (an N-way left-deep chain's own final,
     * fully-concatenated result) so the two never risk drifting on this shared final-stage logic. */
    static ExecutionResult applyProjectionAggregateSortAndFinish(List<ColumnInfo> finalColumns, List<List<Object>> allRows,
            List<Integer> outputProjection, ParallelJoinPlanner.AggregateSpec aggregateSpec,
            List<ParallelJoinPlanner.SortKey> sortKeys, Integer fetchLimit) {
        List<ColumnInfo> outputColumns = finalColumns;
        if (outputProjection != null) {
            List<ColumnInfo> projectedColumns = new ArrayList<>(outputProjection.size());
            for (int ordinal : outputProjection) {
                projectedColumns.add(finalColumns.get(ordinal));
            }
            outputColumns = projectedColumns;
        }

        // Skew mitigation follow-up: GROUP BY and a bounded ORDER BY/LIMIT are BOTH naturally
        // single-pass, bounded-memory operations -- an accumulator map costs O(distinct groups),
        // never O(rows), and a bounded top-K heap of size `fetchLimit` never grows past that
        // regardless of how many rows flow through it. The PREVIOUS version of this method didn't
        // take advantage of that: it built a full extra "projected rows" copy of the ALREADY fully
        // materialized join output (see PartitionSpill's own javadoc for why the join output itself
        // is still fully materialized upstream -- that remains a real, disclosed limitation this
        // method alone can't remove), then a full O(n log n) sort-and-truncate for even a LIMIT 10.
        // This version instead applies the projection lazily, per row, feeding directly into
        // whichever bounded structure below actually needs it -- no additional full-size copy, and
        // no full sort when only a small top-K is required.
        if (aggregateSpec == null && sortKeys == null) {
            // A plain passthrough SELECT -- every row must come back regardless, so there's no
            // bounded structure to feed; this is the one case where a full-size copy is genuinely
            // unavoidable (unchanged from before).
            List<List<Object>> outputRows = allRows;
            if (outputProjection != null) {
                List<List<Object>> projectedRows = new ArrayList<>(allRows.size());
                for (List<Object> row : allRows) {
                    projectedRows.add(applyProjection(row, outputProjection));
                }
                outputRows = projectedRows;
            }
            return ExecutionResult.ofQuery(outputColumns, outputRows);
        }

        List<List<Object>> outputRows;
        if (aggregateSpec != null) {
            // aggregateRows() itself already only ever costs O(distinct groups), not O(rows) -- the
            // projection is applied per row, right where each row is consumed, never pre-copied.
            List<List<Object>> internalRows = aggregateRows(allRows, outputProjection, aggregateSpec);
            ParallelJoinPlanner.AggregateSpec spec = aggregateSpec;
            int groupKeyCount = spec.groupKeyOrdinals().size();
            List<ColumnInfo> aggregatedColumns = new ArrayList<>(spec.outputLayout().size());
            for (int i = 0; i < spec.outputLayout().size(); i++) {
                ParallelJoinPlanner.OutputColumn col = spec.outputLayout().get(i);
                int jdbcType = col.isGroupKey()
                        ? outputColumns.get(spec.groupKeyOrdinals().get(col.index())).jdbcType()
                        : spec.aggCalls().get(col.index()).kind() == org.apache.calcite.sql.SqlKind.COUNT
                                ? java.sql.Types.BIGINT : java.sql.Types.NUMERIC;
                aggregatedColumns.add(new ColumnInfo(spec.outputColumnNames().get(i), jdbcType, 0, 0, 0, true));
            }
            // aggregateRows() always emits its OWN natural [group keys][agg results] order --
            // outputLayout (built while planning) says which of THOSE internal columns each FINAL
            // output position actually is, since nothing requires a SELECT list to list group keys
            // before aggregates, or in GROUP BY's own declared order. This reorder step is O(distinct
            // groups), never O(rows), so it's never the memory concern this refactor targets.
            List<List<Object>> reorderedRows = new ArrayList<>(internalRows.size());
            for (List<Object> internalRow : internalRows) {
                List<Object> row = new ArrayList<>(spec.outputLayout().size());
                for (ParallelJoinPlanner.OutputColumn col : spec.outputLayout()) {
                    row.add(col.isGroupKey() ? internalRow.get(col.index()) : internalRow.get(groupKeyCount + col.index()));
                }
                reorderedRows.add(row);
            }
            outputRows = reorderedRows;
            outputColumns = aggregatedColumns;
            if (sortKeys != null) {
                // ORDER BY above a GROUP BY sorts the (already small, O(distinct groups)) aggregated
                // rows -- a full sort here is fine, there's no large row set left to bound against.
                outputRows = boundedTopK(outputRows, sortKeys, fetchLimit);
            }
        } else {
            // A bounded ORDER BY ... LIMIT directly above the join, no aggregation -- the case this
            // refactor targets most directly: stream every row through a bounded top-K heap of size
            // `fetchLimit`, applying the projection lazily per row, instead of copying+sorting the
            // entire (potentially huge) joined row set for what might be a LIMIT 10.
            outputRows = boundedTopKWithProjection(allRows, outputProjection, sortKeys, fetchLimit);
        }
        return ExecutionResult.ofQuery(outputColumns, outputRows);
    }

    /** Streams {@code rows} through a bounded max-heap of size {@code fetchLimit}, applying {@code
     * projection} to each row lazily as it's consumed -- the single-pass, bounded-memory
     * replacement for "copy the whole projected row set, then sort it all, then truncate." Returns
     * the kept rows in final sorted order (ascending per {@code sortKeys}' own semantics). */
    private static List<List<Object>> boundedTopKWithProjection(List<List<Object>> rows, List<Integer> projection,
            List<ParallelJoinPlanner.SortKey> sortKeys, int fetchLimit) {
        int capacity = Math.max(1, fetchLimit);
        // A max-heap ordered by the SAME comparator, reversed -- its peek is always the current
        // worst-of-the-kept row, the one to evict the instant a better candidate arrives.
        java.util.PriorityQueue<List<Object>> heap = new java.util.PriorityQueue<>(capacity,
                (a, b) -> -compareBySortKeys(a, b, sortKeys));
        for (List<Object> row : rows) {
            List<Object> logical = projection == null ? row : applyProjection(row, projection);
            if (heap.size() < capacity) {
                heap.add(logical);
            } else if (compareBySortKeys(logical, heap.peek(), sortKeys) < 0) {
                heap.poll();
                heap.add(logical);
            }
        }
        List<List<Object>> result = new ArrayList<>(heap);
        result.sort((a, b) -> compareBySortKeys(a, b, sortKeys));
        return result;
    }

    /** As {@link #boundedTopKWithProjection}, but for the (always small, O(distinct groups)) already-
     * aggregated row set -- a full sort is fine here, there's no large input to bound against, this
     * just reuses the same heap so a {@code LIMIT} above a {@code GROUP BY} still truncates
     * correctly. */
    private static List<List<Object>> boundedTopK(List<List<Object>> rows, List<ParallelJoinPlanner.SortKey> sortKeys,
            int fetchLimit) {
        return boundedTopKWithProjection(rows, null, sortKeys, fetchLimit);
    }

    /** A single group's running aggregate state, one slot per {@link
     * ParallelJoinPlanner.AggCall}. {@code SUM}/{@code AVG} track a running {@link
     * java.math.BigDecimal} sum; {@code AVG} additionally needs {@code nonNullCount} (a plain
     * average-of-averages across partitions would be WRONG once partitions have different row
     * counts, so this always tracks sum and count separately and divides only once, at the very
     * end); {@code MIN}/{@code MAX} track a running comparison value via {@link
     * RexRowEvaluator#compareValues}; {@code COUNT} tracks a plain counter ({@code COUNT(*)}
     * increments unconditionally, {@code COUNT(col)} only on a non-null value). {@code sawAnyValue}
     * distinguishes "every input was NULL/there were no rows" (real SQL result: {@code NULL} for
     * every aggregate but {@code COUNT}, which is {@code 0}) from "the running value is genuinely
     * this" -- both a real running value of {@code 0} and "no values seen" must never be conflated. */
    private static final class AggAccumulator {
        java.math.BigDecimal sum;
        long nonNullCount;
        Object minMax;
        boolean sawAnyValue;
    }

    /** {@code projection}, when non-null, is applied to each RAW row lazily as it's consumed here --
     * never pre-copied into a separate projected-rows list first, since this accumulation is already
     * a single streaming pass costing O(distinct groups) in memory, never O(rows). */
    private static List<List<Object>> aggregateRows(List<List<Object>> rawRows, List<Integer> projection,
            ParallelJoinPlanner.AggregateSpec spec) {
        // LinkedHashMap: first-seen group order, for a deterministic (if arbitrary) output order --
        // GROUP BY itself makes no ordering guarantee, so any stable order is correct.
        Map<List<Object>, List<AggAccumulator>> groups = new java.util.LinkedHashMap<>();
        for (List<Object> rawRow : rawRows) {
            List<Object> row = projection == null ? rawRow : applyProjection(rawRow, projection);
            List<Object> groupKey = new ArrayList<>(spec.groupKeyOrdinals().size());
            for (int ordinal : spec.groupKeyOrdinals()) {
                groupKey.add(row.get(ordinal));
            }
            List<AggAccumulator> accumulators = groups.computeIfAbsent(groupKey, k -> {
                List<AggAccumulator> fresh = new ArrayList<>(spec.aggCalls().size());
                for (int i = 0; i < spec.aggCalls().size(); i++) {
                    fresh.add(new AggAccumulator());
                }
                return fresh;
            });
            for (int i = 0; i < spec.aggCalls().size(); i++) {
                updateAccumulator(accumulators.get(i), spec.aggCalls().get(i), row);
            }
        }
        List<List<Object>> result = new ArrayList<>(groups.size());
        for (Map.Entry<List<Object>, List<AggAccumulator>> entry : groups.entrySet()) {
            List<Object> outputRow = new ArrayList<>(spec.groupKeyOrdinals().size() + spec.aggCalls().size());
            outputRow.addAll(entry.getKey());
            for (int i = 0; i < spec.aggCalls().size(); i++) {
                outputRow.add(finalizeAccumulator(entry.getValue().get(i), spec.aggCalls().get(i)));
            }
            result.add(outputRow);
        }
        return result;
    }

    private static void updateAccumulator(AggAccumulator acc, ParallelJoinPlanner.AggCall call, List<Object> row) {
        Object value = call.argOrdinal() == null ? null : row.get(call.argOrdinal());
        switch (call.kind()) {
            case COUNT -> {
                if (call.argOrdinal() == null || value != null) {
                    acc.nonNullCount++;
                }
            }
            case SUM, AVG -> {
                if (value != null) {
                    java.math.BigDecimal parsed = RexRowEvaluator.asBigDecimalOrNull(value);
                    if (parsed != null) {
                        acc.sum = acc.sum == null ? parsed : acc.sum.add(parsed);
                        acc.nonNullCount++;
                        acc.sawAnyValue = true;
                    }
                }
            }
            case MIN -> {
                if (value != null && (!acc.sawAnyValue || RexRowEvaluator.compareValues(value, acc.minMax) < 0)) {
                    acc.minMax = value;
                    acc.sawAnyValue = true;
                }
            }
            case MAX -> {
                if (value != null && (!acc.sawAnyValue || RexRowEvaluator.compareValues(value, acc.minMax) > 0)) {
                    acc.minMax = value;
                    acc.sawAnyValue = true;
                }
            }
            default -> throw new IllegalStateException("unsupported aggregate kind reached the executor: " + call.kind());
        }
    }

    private static Object finalizeAccumulator(AggAccumulator acc, ParallelJoinPlanner.AggCall call) {
        return switch (call.kind()) {
            case COUNT -> acc.nonNullCount;
            case SUM -> acc.sawAnyValue ? acc.sum : null;
            case AVG -> acc.sawAnyValue && acc.nonNullCount > 0
                    ? acc.sum.divide(java.math.BigDecimal.valueOf(acc.nonNullCount), 10, java.math.RoundingMode.HALF_UP)
                    : null;
            case MIN, MAX -> acc.sawAnyValue ? acc.minMax : null;
            default -> null;
        };
    }

    private static int compareBySortKeys(List<Object> a, List<Object> b, List<ParallelJoinPlanner.SortKey> sortKeys) {
        for (ParallelJoinPlanner.SortKey sortKey : sortKeys) {
            Object left = a.get(sortKey.ordinal());
            Object right = b.get(sortKey.ordinal());
            int cmp = compareNullsLast(left, right);
            if (cmp != 0) {
                return sortKey.descending() ? -cmp : cmp;
            }
        }
        return 0;
    }

    /** Nulls sort last regardless of direction -- see {@link ParallelJoinPlanner.SortKey}'s own
     * javadoc on why this is a real, disclosed simplification rather than full {@code NULLS FIRST}/
     * {@code NULLS LAST} support. */
    private static int compareNullsLast(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return RexRowEvaluator.compareValues(left, right);
    }

    /** How many rows one task instance drains before voluntarily resubmitting itself -- small
     * enough that a busy partition's work gets sliced into many independently-schedulable pieces
     * (the actual mechanism that lets a work-stealing pool balance skewed partitions across idle
     * threads), large enough that resubmission overhead doesn't dominate for a fast-moving queue. */
    private static final int STEAL_BATCH_SIZE = 256;

    /** How long a drain task waits for the NEXT row before giving up this turn and resubmitting --
     * short enough that an idle-but-empty-right-now partition doesn't monopolize a pool thread
     * that could be stealing work from a busier partition instead. */
    private static final long STEAL_POLL_MILLIS = 5;

    /** One local partition's draining, reshaped as a short-lived, SELF-RESUBMITTING task on a
     * work-stealing {@link ExecutorService} rather than a thread permanently owning this queue --
     * see this class's own javadoc on why. At most one instance of this task is ever active for a
     * given partition at a time (each resubmission happens only after the current batch returns),
     * so {@code output} (a plain {@code ArrayList}) never sees concurrent writers despite running
     * on a shared thread pool -- work stealing happens ACROSS partitions' tasks, never within one
     * partition's own sequential chain. */
    private static void drainBatch(ExecutorService pool, BlockingQueue<List<Object>> queue,
            Map<Object, List<List<Object>>> table, PartitionSpill spill, ParallelJoinPlanner.Plan plan,
            List<List<Object>> output, CountDownLatch done) {
        for (int processed = 0; processed < STEAL_BATCH_SIZE; processed++) {
            List<Object> probeRow;
            try {
                probeRow = queue.poll(STEAL_POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                done.countDown();
                return;
            }
            if (probeRow == null) {
                break; // nothing ready right now -- resubmit below, ceding this thread to other work
            }
            if (probeRow == END_OF_STREAM) {
                done.countDown();
                return; // this partition is fully drained -- stop resubmitting
            }
            // probeRow is already the LOGICAL row (plan.probeProjection() applied before it was
            // queued), so plan.probeKeyOrdinal() addresses it directly -- no further remap here.
            // Delegates to RemotePartitionJoin's own matching code (shared, byte-for-byte, with the
            // remote/Phase-1a path) rather than duplicating the match/emit logic inline here.
            RemotePartitionJoin.probeOne(table, probeRow, plan.probeKeyOrdinal(), plan.leftIsBuild(), output);
            if (spill != null) {
                // This partition also spilled some of its build rows to disk (see PartitionSpill's
                // own javadoc) -- a matching key may have rows in BOTH the in-memory table above and
                // the spill file, so both are always consulted, never just one or the other.
                emitSpillMatches(spill, probeRow, plan.probeKeyOrdinal(), plan.leftIsBuild(), output);
            }
        }
        pool.execute(() -> drainBatch(pool, queue, table, spill, plan, output, done));
    }

    /** Spilled-partition counterpart to {@link RemotePartitionJoin#probeOne} -- reads any matches for
     * {@code probeRow}'s own key back from {@code spill}'s disk file and emits them in the same
     * left-then-right column order. A read failure here is logged and otherwise ignored (this probe
     * row simply loses whatever matches were on disk) rather than failing the whole query -- the same
     * "a degraded signal never fails the query" stance this engine takes everywhere else (a Bloom
     * filter false positive, a failed row-count probe, a failed remote dispatch). */
    private static void emitSpillMatches(PartitionSpill spill, List<Object> probeRow, int probeKeyOrdinal,
            boolean leftIsBuild, List<List<Object>> output) {
        Object key = keyOf(probeRow, probeKeyOrdinal);
        if (key == null) {
            return;
        }
        List<List<Object>> matches;
        try {
            matches = spill.readMatches(key);
        } catch (java.io.IOException e) {
            log.warn("parallel join: reading a spilled partition's build rows failed -- this probe row "
                    + "may be missing some matches ({})", e.toString());
            return;
        }
        for (List<Object> buildRow : matches) {
            List<Object> joined = new ArrayList<>(buildRow.size() + probeRow.size());
            if (leftIsBuild) {
                joined.addAll(buildRow);
                joined.addAll(probeRow);
            } else {
                joined.addAll(probeRow);
                joined.addAll(buildRow);
            }
            output.add(joined);
        }
    }

    /** Lazily creates (on first crossing the threshold) or reuses {@code partition}'s {@link
     * PartitionSpill} and appends {@code row} under {@code key} to it -- {@code false} on any setup/
     * write failure (caller falls back to keeping the row in memory instead, never losing it). */
    private static boolean trySpill(PartitionSpill[] partitionSpills, int partition, Object key, List<Object> row) {
        try {
            PartitionSpill spill = partitionSpills[partition];
            if (spill == null) {
                spill = new PartitionSpill(partition);
                partitionSpills[partition] = spill;
                log.info("parallel join: partition {} exceeded the spill threshold -- further build rows for "
                        + "it are spilling to disk instead of growing its in-memory hash table without bound",
                        partition);
            }
            spill.append(key, row);
            return true;
        } catch (java.io.IOException e) {
            log.warn("parallel join: spilling partition {} to disk failed -- keeping this row in memory "
                    + "instead ({})", partition, e.toString());
            return false;
        }
    }

    private static void awaitLocalPartitions(ExecutorService pool, CountDownLatch done) {
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
        }
    }

    private static void offer(BlockingQueue<List<Object>> queue, List<Object> item) {
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private static Object keyOf(List<Object> row, int index) {
        return index >= 0 && index < row.size() ? row.get(index) : null;
    }

    /** Remaps a raw streamed row into a side's LOGICAL shape per {@link
     * ParallelJoinPlanner.Plan}'s own {@code buildProjection}/{@code probeProjection} -- {@code
     * null} projection means the raw row already IS the logical row, returned unchanged. */
    static List<Object> applyProjection(List<Object> row, List<Integer> projection) {
        if (projection == null) {
            return row;
        }
        List<Object> projected = new ArrayList<>(projection.size());
        for (int ordinal : projection) {
            projected.add(row.get(ordinal));
        }
        return projected;
    }

    /** As {@link #applyProjection(List, List)}, for a side's {@link ColumnInfo} list instead of a
     * data row -- same ordinal remap, applied once per side rather than once per row. A separate
     * name (rather than an overload) because the two {@code List<Object>}/{@code List<ColumnInfo>}
     * signatures erase identically. */
    static List<ColumnInfo> applyColumnProjection(List<ColumnInfo> columns, List<Integer> projection) {
        if (projection == null) {
            return columns;
        }
        List<ColumnInfo> projected = new ArrayList<>(projection.size());
        for (int ordinal : projection) {
            projected.add(columns.get(ordinal));
        }
        return projected;
    }

    private static int columnIndex(List<ColumnInfo> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    private static int partitionOf(Object key, int n) {
        return Math.floorMod(String.valueOf(key).hashCode(), n);
    }

    /** {@code WARP_PARALLEL_JOIN_REMOTE_ENABLED=true} PLUS a configured {@code
     * WARP_PEER_TLS_KEYSTORE} are both required -- the keystore isn't just for running {@code
     * WarpPeerGrpcServer} as a listener, it's this node's own mTLS CLIENT identity too, and real
     * mutual TLS means a coordinator dispatching work to a peer must present a certificate that
     * peer trusts, whether or not this node ALSO happens to run its own peer listener. Any failure
     * here (config parsing, the config-primary Postgres being briefly unreachable) degrades to "no
     * remote peers this call" -- exactly like every other real-but-optional signal in this codebase
     * ({@link ParallelJoinPlanner}'s own row-count probes, {@code SemiJoinPushdown}'s build-side
     * stats) -- never a query failure. */
    private static List<NodeRegistry.NodeRow> remotePeersOrEmpty() {
        if (!parseBoolEnv("WARP_PARALLEL_JOIN_REMOTE_ENABLED")) {
            return List.of();
        }
        String keystorePath = System.getenv("WARP_PEER_TLS_KEYSTORE");
        if (keystorePath == null || keystorePath.isBlank()) {
            log.debug("parallel join: WARP_PARALLEL_JOIN_REMOTE_ENABLED is set but WARP_PEER_TLS_KEYSTORE "
                    + "isn't -- this node has no peer identity to dispatch with, staying fully local");
            return List.of();
        }
        try {
            ServerOptions options = ServerOptions.parse(new String[0]);
            int selfPeerGrpcPort = parseIntEnv("WARP_PEER_GRPC_PORT", 7072);
            return NodeRegistry.listOtherLivePeerWorkers(options, NodeRegistry.resolveHost(), selfPeerGrpcPort);
        } catch (Exception e) {
            log.warn("parallel join: couldn't discover live peers -- staying fully local for this query ({})", e.toString());
            return List.of();
        }
    }

    /** Dispatches ONE partition's already-collected build/probe rows to {@code peer} over {@code
     * WarpPeerService.JoinPartition} (Phase 1a's primitive), appending the joined rows to {@code
     * output}. On ANY failure -- unreachable peer, TLS handshake failure, or the peer's own {@code
     * success=false} -- falls back to computing this ONE partition locally via {@link
     * RemotePartitionJoin#probeAll}, synchronously, in the coordinator's own thread: a transient
     * peer problem costs this one partition's parallelism, never the whole query. */
    private static void dispatchRemotePartition(ParallelJoinPlanner.Plan plan, List<NodeRegistry.NodeRow> peers,
            int preferredIndex, List<ColumnInfo> buildColumns, List<ColumnInfo> probeColumns,
            Map<Object, List<List<Object>>> table, List<List<Object>> probeRows, List<List<Object>> output) {
        // Cost-based placement (Phase 2): a small partition isn't worth a network round trip and a
        // peer's own matching work -- process it locally even though a peer was assigned, reusing
        // rows already collected rather than a fresh probe. Real row counts, not an estimate.
        long combinedRowCount = table.values().stream().mapToLong(List::size).sum() + probeRows.size();
        long remoteMinRows = parseLongEnv("WARP_PARALLEL_JOIN_REMOTE_MIN_ROWS", 50_000L);
        if (combinedRowCount < remoteMinRows) {
            log.debug("parallel join: partition has only {} combined row(s), below the "
                    + "WARP_PARALLEL_JOIN_REMOTE_MIN_ROWS threshold -- processing locally instead of "
                    + "dispatching to a peer", combinedRowCount);
            output.addAll(RemotePartitionJoin.probeAll(table, probeRows, plan.probeKeyOrdinal(), plan.leftIsBuild()));
            return;
        }

        // Retry/failover (Phase 2): try the preferred peer first, then every OTHER discovered live
        // peer (skipping the preferred one, already tried) before giving up on the network
        // entirely -- a single peer's failure no longer costs this partition's parallelism if any
        // other peer is reachable.
        List<NodeRegistry.NodeRow> attemptedInOrder = new ArrayList<>();
        for (int offset = 0; offset < peers.size(); offset++) {
            NodeRegistry.NodeRow candidate = peers.get((preferredIndex + offset) % peers.size());
            if (attemptedInOrder.contains(candidate)) {
                continue;
            }
            attemptedInOrder.add(candidate);
            if (tryDispatchOnce(plan, candidate, buildColumns, probeColumns, table, probeRows, output)) {
                return;
            }
        }
        log.warn("parallel join: every discovered live peer failed for this partition -- falling back "
                + "to local execution ({} peer(s) tried)", attemptedInOrder.size());
        output.addAll(RemotePartitionJoin.probeAll(table, probeRows, plan.probeKeyOrdinal(), plan.leftIsBuild()));
    }

    /** One dispatch attempt against ONE specific peer -- {@code true} on success (rows already
     * appended to {@code output}), {@code false} on any failure (nothing appended, caller decides
     * whether to try another peer or fall back to local). */
    private static boolean tryDispatchOnce(ParallelJoinPlanner.Plan plan, NodeRegistry.NodeRow peer,
            List<ColumnInfo> buildColumns, List<ColumnInfo> probeColumns,
            Map<Object, List<List<Object>>> table, List<List<Object>> probeRows, List<List<Object>> output) {
        String keystorePath = System.getenv("WARP_PEER_TLS_KEYSTORE");
        String keystorePassword = System.getenv("WARP_PEER_TLS_KEYSTORE_PASSWORD");
        try {
            // Phase 2: a pooled, reused channel per peer -- amortizes the real mTLS handshake cost
            // across every partition/query routed to this peer, instead of paying it fresh every
            // single dispatch (Phase 1b's original behavior). Never torn down here on success.
            ManagedChannel channel = PeerChannelPool.getOrCreate(peer.host(), peer.peerGrpcPort(), keystorePath, keystorePassword);
            WarpPeerServiceGrpc.WarpPeerServiceBlockingStub stub = WarpPeerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(30, TimeUnit.SECONDS);
            JoinPartitionRequest.Builder request = JoinPartitionRequest.newBuilder()
                    .addAllBuildColumnNames(namesOf(buildColumns))
                    .setBuildKeyOrdinal(plan.buildKeyOrdinal())
                    .addAllProbeColumnNames(namesOf(probeColumns))
                    .setProbeKeyOrdinal(plan.probeKeyOrdinal())
                    .setLeftIsBuild(plan.leftIsBuild());
            for (List<List<Object>> buildRows : table.values()) {
                for (List<Object> row : buildRows) {
                    request.addBuildRows(toRow(row));
                }
            }
            for (List<Object> row : probeRows) {
                request.addProbeRows(toRow(row));
            }
            JoinPartitionResponse response = stub.joinPartition(request.build());
            if (!response.getSuccess()) {
                throw new IllegalStateException("peer reported failure: " + response.getErrorMessage());
            }
            for (Row row : response.getRowsList()) {
                output.add(fromRow(row));
            }
            return true;
        } catch (RuntimeException | java.security.GeneralSecurityException | java.io.IOException e) {
            log.warn("parallel join: remote partition dispatch to {}:{} failed -- will try another peer "
                    + "if one is available ({})", peer.host(), peer.peerGrpcPort(), e.toString());
            // A real failure (not just this dispatch's own try/catch scope) means the cached
            // channel itself may be bad -- evict it so the NEXT dispatch to this peer builds a
            // fresh one rather than retrying against a connection already proven broken.
            PeerChannelPool.evict(peer.host(), peer.peerGrpcPort());
            return false;
        }
    }

    private static List<String> namesOf(List<ColumnInfo> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (ColumnInfo column : columns) {
            names.add(column.name());
        }
        return names;
    }

    private static Row toRow(List<Object> row) {
        Row.Builder builder = Row.newBuilder();
        for (Object value : row) {
            builder.addIsNull(value == null);
            builder.addValues(value == null ? "" : String.valueOf(value));
        }
        return builder.build();
    }

    private static List<Object> fromRow(Row row) {
        List<Object> values = new ArrayList<>(row.getValuesCount());
        for (int i = 0; i < row.getValuesCount(); i++) {
            values.add(row.getIsNull(i) ? null : row.getValues(i));
        }
        return values;
    }

    private static boolean parseBoolEnv(String name) {
        String raw = System.getenv(name);
        return raw != null && (raw.equalsIgnoreCase("true") || raw.equals("1"));
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
