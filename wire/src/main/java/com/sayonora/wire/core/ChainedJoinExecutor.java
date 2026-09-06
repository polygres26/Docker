package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * N-way (left-deep chain) counterpart to {@link ParallelJoinExecutor}: executes a {@link
 * ParallelJoinPlanner.ChainPlan}, confirmed via a live diagnostic against a real 3-backend join to
 * match Calcite's own left-deep join-tree shape for {@code A JOIN B JOIN C JOIN ...}.
 *
 * <p><b>Scope, a real, disclosed narrowing</b>: only the chain's FIRST pairwise join (between its
 * two left-most leaves) gets {@link ParallelJoinExecutor}'s full partitioned/parallel/work-stealing/
 * remote-capable treatment, via an unmodified call to {@link ParallelJoinExecutor#execute}. Each
 * SUBSEQUENT leaf is joined against the running, already-materialized result via a single,
 * synchronous, in-memory hash join using {@link RemotePartitionJoin}'s own matching primitive (the
 * same one Phase 1a's remote-peer path uses) -- not yet re-parallelized across partitions or threads.
 * This mirrors how Phase 1a itself shipped its first version of remote dispatch (a real, working
 * primitive first, its own further parallelization a later, separate pass) rather than deferring the
 * whole N-way feature until every leaf could be fully parallelized too.
 *
 * <p>Columns are concatenated {@code [running result columns..., new leaf columns...]} at each step,
 * matching Calcite's own left-deep row-type nesting exactly -- so {@link ParallelJoinPlanner}'s
 * {@code outputProjection}/{@code sortKeys}/{@code aggregateSpec} ordinal math (computed once, up
 * front, against the FULL optimized plan's own row type) applies unchanged to the final result here.
 */
final class ChainedJoinExecutor {

    private ChainedJoinExecutor() {
    }

    static ExecutionResult execute(ParallelJoinPlanner.ChainPlan chainPlan, int partitionCount) throws SQLException {
        ExecutionResult firstStep = ParallelJoinExecutor.execute(chainPlan.firstStepPlan(), partitionCount);
        List<ColumnInfo> runningColumns = firstStep.columns();
        List<List<Object>> runningRows = firstStep.rows();
        for (ParallelJoinPlanner.ChainExtensionStep step : chainPlan.extensionSteps()) {
            Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(runningRows, step.runningResultKeyOrdinal());
            List<ColumnInfo> leafColumns = new ArrayList<>();
            List<List<Object>> leafRows = new ArrayList<>();
            streamLeaf(step, leafColumns, leafRows);
            // leftIsBuild=true: the running result (built into `table`) is always the structurally
            // LEFT side of a left-deep chain's own row-type convention -- matches the
            // [running..., leaf...] concatenation order this class's own javadoc promises.
            List<List<Object>> joined = RemotePartitionJoin.probeAll(table, leafRows, step.leafKeyOrdinal(), true);
            List<ColumnInfo> combinedColumns = new ArrayList<>(runningColumns.size() + leafColumns.size());
            combinedColumns.addAll(runningColumns);
            combinedColumns.addAll(leafColumns);
            runningColumns = combinedColumns;
            runningRows = joined;
        }
        return ParallelJoinExecutor.applyProjectionAggregateSortAndFinish(runningColumns, runningRows,
                chainPlan.outputProjection(), chainPlan.aggregateSpec(), chainPlan.sortKeys(), chainPlan.fetchLimit());
    }

    /** Streams one chain-extension leaf's own extracted SQL directly through its own backend
     * connection (not through Calcite, exactly like {@link ParallelJoinPlanner.Plan}'s build/probe
     * sides), applying its residual filter/projection the same way {@link ParallelJoinExecutor}'s own
     * build/probe streaming does. */
    private static void streamLeaf(ParallelJoinPlanner.ChainExtensionStep step, List<ColumnInfo> outColumns,
            List<List<Object>> outRows) throws SQLException {
        try (Connection connection = step.leafBackend().target().open();
                PreparedStatement ps = connection.prepareStatement(step.leafSql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(ps, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> columns) {
                    outColumns.addAll(ParallelJoinExecutor.applyColumnProjection(columns, step.leafProjection()));
                }

                @Override
                public void onRow(List<Object> row) {
                    if (step.leafFilter() != null && !step.leafFilter().test(row)) {
                        return;
                    }
                    outRows.add(ParallelJoinExecutor.applyProjection(row, step.leafProjection()));
                }
            });
        }
    }
}
