package com.sayonora.wire.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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

        List<BlockingQueue<List<Object>>> queues = new ArrayList<>(n);
        List<List<List<Object>>> partitionOutputs = new ArrayList<>(n);
        List<Thread> workers = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            BlockingQueue<List<Object>> queue = new LinkedBlockingQueue<>(10_000);
            queues.add(queue);
            List<List<Object>> output = new ArrayList<>();
            partitionOutputs.add(output);
            Map<Object, List<List<Object>>> table = partitionTables.get(i);
            Thread worker = new Thread(() -> runPartitionWorker(queue, table, plan, output),
                    "warp-parallel-join-" + i);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
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
                    offer(queues.get(partition), logicalRow);
                }
            });
        } catch (SQLException e) {
            for (BlockingQueue<List<Object>> queue : queues) {
                offer(queue, END_OF_STREAM);
            }
            joinQuietly(workers);
            throw e;
        }

        for (BlockingQueue<List<Object>> queue : queues) {
            offer(queue, END_OF_STREAM);
        }
        joinQuietly(workers);

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

    private static void runPartitionWorker(BlockingQueue<List<Object>> queue, Map<Object, List<List<Object>>> table,
            ParallelJoinPlanner.Plan plan, List<List<Object>> output) {
        while (true) {
            List<Object> probeRow;
            try {
                probeRow = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (probeRow == END_OF_STREAM) {
                return;
            }
            // probeRow is already the LOGICAL row (plan.probeProjection() applied before it was
            // queued), so plan.probeKeyOrdinal() addresses it directly -- no further remap here.
            // Delegates to RemotePartitionJoin's own matching code (shared, byte-for-byte, with the
            // remote/Phase-1a path) rather than duplicating the match/emit logic inline here.
            RemotePartitionJoin.probeOne(table, probeRow, plan.probeKeyOrdinal(), plan.leftIsBuild(), output);
        }
    }

    private static void offer(BlockingQueue<List<Object>> queue, List<Object> item) {
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(List<Thread> workers) {
        for (Thread worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
}
