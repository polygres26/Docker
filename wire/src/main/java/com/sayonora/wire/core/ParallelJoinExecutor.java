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
        int[] buildKeyIndex = {-1};
        try (Connection buildConnection = plan.buildBackend().target().open();
                PreparedStatement buildPs = buildConnection.prepareStatement(plan.buildSql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(buildPs, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> columns) {
                    buildColumns.addAll(columns);
                    buildKeyIndex[0] = columnIndex(columns, plan.buildKeyColumn());
                }

                @Override
                public void onRow(List<Object> row) {
                    Object key = keyOf(row, buildKeyIndex[0]);
                    if (key == null) {
                        return; // SQL join semantics: NULL never equals NULL, so it can never match
                    }
                    int partition = partitionOf(key, n);
                    partitionTables.get(partition).computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                    partitionFilters.get(partition).put(String.valueOf(key));
                }
            });
        }
        if (buildKeyIndex[0] < 0) {
            throw new SQLException("parallel join: build-side key column \"" + plan.buildKeyColumn()
                    + "\" wasn't found in its own extracted result set -- this indicates a planner bug, "
                    + "not a real backend/data problem");
        }

        List<BlockingQueue<List<Object>>> queues = new ArrayList<>(n);
        List<List<List<Object>>> partitionOutputs = new ArrayList<>(n);
        List<Thread> workers = new ArrayList<>(n);
        int[] probeKeyIndex = {-1};
        for (int i = 0; i < n; i++) {
            BlockingQueue<List<Object>> queue = new LinkedBlockingQueue<>(10_000);
            queues.add(queue);
            List<List<Object>> output = new ArrayList<>();
            partitionOutputs.add(output);
            Map<Object, List<List<Object>>> table = partitionTables.get(i);
            Thread worker = new Thread(() -> runPartitionWorker(queue, table, probeKeyIndex, plan, output),
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
                    probeColumns.addAll(columns);
                    probeKeyIndex[0] = columnIndex(columns, plan.probeKeyColumn());
                }

                @Override
                public void onRow(List<Object> row) {
                    Object key = keyOf(row, probeKeyIndex[0]);
                    if (key == null) {
                        return;
                    }
                    int partition = partitionOf(key, n);
                    if (!partitionFilters.get(partition).mightContain(String.valueOf(key))) {
                        return; // real, cheap reject -- this key provably isn't in the build side
                    }
                    offer(queues.get(partition), row);
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
            int[] probeKeyIndex, ParallelJoinPlanner.Plan plan, List<List<Object>> output) {
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
            Object key = keyOf(probeRow, probeKeyIndex[0]);
            if (key == null) {
                continue;
            }
            List<List<Object>> matches = table.get(key);
            if (matches == null) {
                continue;
            }
            for (List<Object> buildRow : matches) {
                List<Object> joined = new ArrayList<>(buildRow.size() + probeRow.size());
                if (plan.leftIsBuild()) {
                    joined.addAll(buildRow);
                    joined.addAll(probeRow);
                } else {
                    joined.addAll(probeRow);
                    joined.addAll(buildRow);
                }
                output.add(joined);
            }
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
