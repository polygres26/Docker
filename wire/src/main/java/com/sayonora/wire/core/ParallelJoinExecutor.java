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

    /** A single, privately-held instance used purely for reference-equality end-of-stream
     * signaling on each partition's queue -- deliberately not {@code List.of()} (whose empty-list
     * singleton isn't a safe identity to compare against) and never equal to any real row. */
    private static final List<Object> END_OF_STREAM = new ArrayList<>(0);

    private ParallelJoinExecutor() {
    }

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

    static ExecutionResult execute(ParallelJoinPlanner.Plan plan, int threadCount) throws SQLException {
        int n = Math.max(1, threadCount);
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
        int remoteCount = Math.min(peers.size(), n);

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
                    List<Object> logicalRow = applyProjection(row, plan.buildProjection());
                    Object key = keyOf(logicalRow, plan.buildKeyOrdinal());
                    if (key == null) {
                        return; // SQL join semantics: NULL never equals NULL, so it can never match
                    }
                    int partition = partitionOf(key, n);
                    partitionTables.get(partition).computeIfAbsent(key, k -> new ArrayList<>()).add(logicalRow);
                    partitionFilters.get(partition).put(String.valueOf(key));
                }
            });
        }

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
            stealPool.execute(() -> drainBatch(stealPool, queue, table, plan, output, localPartitionsDone));
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

        List<ColumnInfo> probeColumns = new ArrayList<>();
        try (Connection probeConnection = plan.probeBackend().target().open();
                PreparedStatement probePs = probeConnection.prepareStatement(plan.probeSql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(probePs, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> columns) {
                    probeColumns.addAll(applyColumnProjection(columns, plan.probeProjection()));
                }

                @Override
                public void onRow(List<Object> row) {
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
        List<Integer> outputProjection = plan.outputProjection();
        if (outputProjection == null) {
            return ExecutionResult.ofQuery(finalColumns, allRows);
        }
        // A plain column-selection/reordering Project sat directly above the join (e.g. "SELECT
        // c.name, o.amount") -- re-apply it now against the natural left-then-right concatenated
        // row/columns ParallelJoinPlanner already validated it's expressed purely in terms of.
        List<ColumnInfo> projectedColumns = new ArrayList<>(outputProjection.size());
        for (int ordinal : outputProjection) {
            projectedColumns.add(finalColumns.get(ordinal));
        }
        List<List<Object>> projectedRows = new ArrayList<>(allRows.size());
        for (List<Object> row : allRows) {
            List<Object> projectedRow = new ArrayList<>(outputProjection.size());
            for (int ordinal : outputProjection) {
                projectedRow.add(row.get(ordinal));
            }
            projectedRows.add(projectedRow);
        }
        return ExecutionResult.ofQuery(projectedColumns, projectedRows);
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
            Map<Object, List<List<Object>>> table, ParallelJoinPlanner.Plan plan, List<List<Object>> output,
            CountDownLatch done) {
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
        }
        pool.execute(() -> drainBatch(pool, queue, table, plan, output, done));
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
    private static List<Object> applyProjection(List<Object> row, List<Integer> projection) {
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
    private static List<ColumnInfo> applyColumnProjection(List<ColumnInfo> columns, List<Integer> projection) {
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
