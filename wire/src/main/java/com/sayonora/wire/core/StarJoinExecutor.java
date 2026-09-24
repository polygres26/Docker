package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Executes a {@link ParallelJoinPlanner.StarPlan} -- ported from the sibling ThinkingSense project's
 * own {@code com.omnigate.core.StarJoinExecutor} (see {@link ParallelJoinPlanner}'s own star-topology
 * section header for the full design and provenance).
 *
 * <p>Every spoke's own hash table is built CONCURRENTLY (each spoke is scanned and hashed on its own
 * thread, real parallelism a left-deep chain structurally cannot offer since each of its steps
 * depends on the running result the previous step produced), then the hub is streamed exactly ONCE,
 * probing every spoke's table for each hub row and emitting the full N-way cross product of matches
 * across spokes (correct {@code INNER JOIN} semantics: a hub row with no match on ANY spoke is
 * dropped entirely).
 *
 * <p><b>Real, disclosed scope narrowing carried over from the port</b>: unlike {@link
 * ParallelJoinExecutor}'s own build/probe phases, neither the hub scan nor any spoke's build is
 * itself partitioned across multiple local threads or spilled to disk -- each is one connection, one
 * scan, matching ThinkingSense's own current scope. The real, additional parallelism this class adds
 * over a left-deep chain is spokes building CONCURRENTLY with each other and with nothing else
 * blocking on them, not intra-spoke partitioning.
 */
final class StarJoinExecutor {

    private StarJoinExecutor() {
    }

    static ExecutionResult execute(ParallelJoinPlanner.StarPlan plan) throws SQLException {
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, plan.spokes().size()));
        List<Future<SpokeResult>> futures = new ArrayList<>(plan.spokes().size());
        try {
            for (ParallelJoinPlanner.StarSpoke spoke : plan.spokes()) {
                futures.add(pool.submit(() -> buildSpoke(spoke)));
            }
            List<SpokeResult> spokeResults = new ArrayList<>(futures.size());
            for (Future<SpokeResult> future : futures) {
                spokeResults.add(await(future));
            }
            return probeHub(plan, spokeResults);
        } finally {
            pool.shutdown();
        }
    }

    private static SpokeResult await(Future<SpokeResult> future) throws SQLException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException sqlException) {
                throw sqlException;
            }
            throw new SQLException("star join: building a spoke failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while building a star join spoke", e);
        }
    }

    private record SpokeResult(List<ColumnInfo> columns, Map<Object, List<List<Object>>> table) {
    }

    private static SpokeResult buildSpoke(ParallelJoinPlanner.StarSpoke spoke) throws SQLException {
        ParallelJoinPlanner.LeafInfo leaf = spoke.leaf();
        List<ColumnInfo> columns = new ArrayList<>();
        Map<Object, List<List<Object>>> table = new HashMap<>();
        try (Connection connection = leaf.backend().target().open();
                PreparedStatement ps = connection.prepareStatement(leaf.sql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(ps, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> cols) {
                    columns.addAll(ParallelJoinExecutor.applyColumnProjection(cols, leaf.leafProjection()));
                }

                @Override
                public void onRow(List<Object> row) {
                    if (leaf.leafFilter() != null && !leaf.leafFilter().test(row)) {
                        return;
                    }
                    List<Object> logicalRow = ParallelJoinExecutor.applyProjection(row, leaf.leafProjection());
                    Object key = keyOf(logicalRow, spoke.spokeKeyOrdinal());
                    if (key == null) {
                        return; // SQL join semantics: NULL never equals NULL, so it can never match
                    }
                    table.computeIfAbsent(key, k -> new ArrayList<>()).add(logicalRow);
                }
            });
        }
        return new SpokeResult(columns, table);
    }

    private static ExecutionResult probeHub(ParallelJoinPlanner.StarPlan plan, List<SpokeResult> spokeResults)
            throws SQLException {
        ParallelJoinPlanner.LeafInfo hub = plan.hub();
        List<ColumnInfo> finalColumns = new ArrayList<>();
        List<List<Object>> outputRows = new ArrayList<>();
        try (Connection connection = hub.backend().target().open();
                PreparedStatement ps = connection.prepareStatement(hub.sql())) {
            JdbcBackendExecutor.streamOnPreparedStatement(ps, List.of(), new JdbcBackendExecutor.StreamingRowHandler() {
                @Override
                public void onColumns(List<ColumnInfo> cols) {
                    finalColumns.addAll(ParallelJoinExecutor.applyColumnProjection(cols, hub.leafProjection()));
                    for (SpokeResult spokeResult : spokeResults) {
                        finalColumns.addAll(spokeResult.columns());
                    }
                }

                @Override
                public void onRow(List<Object> row) {
                    if (hub.leafFilter() != null && !hub.leafFilter().test(row)) {
                        return;
                    }
                    List<Object> hubRow = ParallelJoinExecutor.applyProjection(row, hub.leafProjection());
                    // Cross product of every spoke's matches, one spoke at a time -- an empty match
                    // on ANY spoke drops the row entirely (real INNER JOIN semantics), matching what
                    // the equivalent left-deep chain of the same joins would produce.
                    List<List<Object>> combinations = new ArrayList<>();
                    combinations.add(hubRow);
                    for (int i = 0; i < plan.spokes().size(); i++) {
                        ParallelJoinPlanner.StarSpoke spoke = plan.spokes().get(i);
                        Object key = keyOf(hubRow, spoke.hubKeyOrdinal());
                        List<List<Object>> matches = key == null ? List.of()
                                : spokeResults.get(i).table().getOrDefault(key, List.of());
                        if (matches.isEmpty()) {
                            combinations = List.of();
                            break;
                        }
                        List<List<Object>> next = new ArrayList<>(combinations.size() * matches.size());
                        for (List<Object> combination : combinations) {
                            for (List<Object> spokeRow : matches) {
                                List<Object> merged = new ArrayList<>(combination.size() + spokeRow.size());
                                merged.addAll(combination);
                                merged.addAll(spokeRow);
                                next.add(merged);
                            }
                        }
                        combinations = next;
                    }
                    outputRows.addAll(combinations);
                }
            });
        }
        return ParallelJoinExecutor.applyProjectionAggregateSortAndFinish(finalColumns, outputRows,
                plan.outputProjection(), plan.aggregateSpec(), plan.sortKeys(), plan.fetchLimit());
    }

    private static Object keyOf(List<Object> row, int ordinal) {
        return ordinal >= 0 && ordinal < row.size() ? row.get(ordinal) : null;
    }
}
