package com.polygres.wire.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/** Oracle {@code V$SQL_PLAN}-style in-memory history for {@link ShardJoinExecutor}/
 * {@link SchemaFederationStage}'s federated queries -- every real database Oracle/Postgres/MySQL
 * already tracks is visible via {@code EXPLAIN}/query-log tooling; a federated query spanning
 * several of PolyWire's own shards/backends previously had nowhere to show its plan at all, since
 * the plan only ever existed inside the one-shot Calcite connection each stage builds and throws
 * away per statement. Ported from the sibling Omnigate project's own class of the same name/shape
 * -- real, tested, production code there, closing the same real gap this project independently
 * flagged as an open follow-up when {@link ShardJoinExecutor}/{@link SchemaFederationStage} first
 * shipped. */
public interface SqlPlanStore {

    /** One MEASURED (not estimated) leaf table scan -- see {@link LeafScanProfiler}'s own javadoc
     * for exactly how this number is obtained (a real, separate re-execution of just that one
     * leaf's own pushed-down SQL against its own real backend, with real wall-clock timing and a
     * real row count from actually iterating the result) and why it's the honest answer to "can
     * Calcite tell you actual rows/time per plan node" (it can't, out of the box -- this is what
     * closes that gap). {@code errorMessage} is non-null (and {@code rowCount} is 0) if
     * re-executing this one leaf failed; the real federated query above it is unaffected either
     * way -- this is a diagnostic measurement, never load-bearing for the real result. */
    record LeafScanMetric(String backend, String sqlText, long elapsedMillis, long rowCount, String errorMessage)
            implements Serializable {
    }

    /** One captured federated-query plan. {@code planText} is Calcite's own {@code EXPLAIN PLAN
     * FOR} output, verbatim -- never reformatted or reparsed. {@code leafScans} is empty (not
     * {@code null}) when per-leaf profiling didn't run for this statement (bind parameters
     * present -- see {@link LeafScanProfiler}'s own javadoc -- or profiling itself failed
     * entirely), never a sign of an error on its own. */
    record PlanEntry(long planId, Instant capturedAt, String backends, String sqlText,
            String planText, long elapsedMillis, long rowCount, boolean success, String errorMessage,
            List<LeafScanMetric> leafScans)
            implements Serializable {
    }

    long record(String backends, String sqlText, String planText, long elapsedMillis, long rowCount,
            boolean success, String errorMessage, List<LeafScanMetric> leafScans);

    /** Newest first -- matches {@code V$SQL_PLAN}'s typical "what just ran" ordering. */
    List<PlanEntry> snapshot();

    /** {@code null} when plan history is disabled entirely ({@code capacity <= 0}) -- decided by
     * the caller, not this method (see {@code Main}'s own {@code POLYWIRE_FEDERATION_PLAN_HISTORY}
     * wiring). Only the single-process, in-memory implementation is ported here (see {@link
     * InMemorySqlPlanStore}'s own javadoc) -- Omnigate's Ignite-cluster-shared variant is a real
     * follow-up, not needed until PolyWire's own multi-instance federation actually needs a shared
     * plan history, not just a per-instance one. */
    public static SqlPlanStore fromConfig(String historySizeSpec) {
        int capacity = parseIntOrDefault(historySizeSpec, 200);
        return capacity <= 0 ? null : new InMemorySqlPlanStore(capacity);
    }

    private static int parseIntOrDefault(String spec, int defaultValue) {
        if (spec == null || spec.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(spec.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
