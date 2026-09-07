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
 * The OUTPUT column order is always {@code [running..., leaf...]} regardless of which side the hash
 * table is actually built from (see {@link #joinStep} below) -- {@code
 * RemotePartitionJoin#probeAll}'s own {@code leftIsBuild} flag controls exactly that independently
 * of which side is cheaper to build from.
 *
 * <p><b>Cost-based build-side selection (this session's own follow-up)</b>: unlike the chain's FIRST
 * pairwise join (planned ahead of execution, via a real {@code COUNT(*)} probe against each leaf --
 * see {@link ParallelJoinPlanner#buildTwoWayPlan}), the running result's own size for step 2+ is only
 * known once prior steps have actually run, so this can't be decided at planning time. Instead each
 * extension step streams the new leaf's rows (needed regardless) and then builds its in-memory hash
 * table from whichever of {running result, new leaf} turns out smaller -- a real, if coarser, analog
 * of the same "build from the smaller side" principle {@link ParallelJoinExecutor} already applies to
 * the first pairwise join, closing the gap where a large running result was always hashed even when
 * the new leaf was tiny.
 */
final class ChainedJoinExecutor {

    private ChainedJoinExecutor() {
    }

    static ExecutionResult execute(ParallelJoinPlanner.ChainPlan chainPlan, int partitionCount) throws SQLException {
        ExecutionResult firstStep = ParallelJoinExecutor.execute(chainPlan.firstStepPlan(), partitionCount);
        List<ColumnInfo> runningColumns = firstStep.columns();
        List<List<Object>> runningRows = firstStep.rows();
        for (ParallelJoinPlanner.ChainExtensionStep step : chainPlan.extensionSteps()) {
            List<ColumnInfo> leafColumns = new ArrayList<>();
            List<List<Object>> leafRows = new ArrayList<>();
            streamLeaf(step, leafColumns, leafRows);
            runningRows = joinStep(runningRows, step.runningResultKeyOrdinal(), leafRows, step.leafKeyOrdinal());
            List<ColumnInfo> combinedColumns = new ArrayList<>(runningColumns.size() + leafColumns.size());
            combinedColumns.addAll(runningColumns);
            combinedColumns.addAll(leafColumns);
            runningColumns = combinedColumns;
        }
        return ParallelJoinExecutor.applyProjectionAggregateSortAndFinish(runningColumns, runningRows,
                chainPlan.outputProjection(), chainPlan.aggregateSpec(), chainPlan.sortKeys(), chainPlan.fetchLimit());
    }

    /** Joins {@code runningRows} against {@code leafRows}, always returning rows in {@code
     * [running..., leaf...]} column order -- building the in-memory hash table from whichever side
     * has fewer rows (both are already fully materialized in memory at this point regardless, so
     * there's no additional cost to checking), rather than always hashing the running result. */
    private static List<List<Object>> joinStep(List<List<Object>> runningRows, int runningKeyOrdinal,
            List<List<Object>> leafRows, int leafKeyOrdinal) {
        if (runningRows.size() <= leafRows.size()) {
            Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(runningRows, runningKeyOrdinal);
            // leftIsBuild=true: build (running) then probe (leaf) -- exactly the [running..., leaf...]
            // order this class's own javadoc promises.
            return RemotePartitionJoin.probeAll(table, leafRows, leafKeyOrdinal, true);
        }
        Map<Object, List<List<Object>>> table = RemotePartitionJoin.buildTable(leafRows, leafKeyOrdinal);
        // leftIsBuild=false: probe (running) then build (leaf) -- still [running..., leaf...].
        return RemotePartitionJoin.probeAll(table, runningRows, runningKeyOrdinal, false);
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
