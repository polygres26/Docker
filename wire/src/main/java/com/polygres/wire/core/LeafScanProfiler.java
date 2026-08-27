package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real, MEASURED per-shard/per-backend row count and timing for a federated query's own leaf table
 * scans -- the piece Calcite itself doesn't provide (confirmed against a real Calcite dev-list
 * discussion: {@code EXPLAIN PLAN FOR} only ever reports the planner's own pre-execution row-count
 * ESTIMATE, never an actual post-execution measurement per node -- an "InstrumentedRelNode" that
 * wraps generated code to count/time as it runs was proposed there but was never a shipped feature).
 *
 * <p><b>How this gets real numbers without hacking Calcite's code generation</b>: {@link
 * JdbcToEnumerableConverter} is the exact boundary node Calcite's own JDBC adapter already
 * generates -- everything below one of these in the optimized {@link RelNode} tree is ONE real,
 * complete SQL statement Calcite would send to ONE real backend (the pushed-down filter/project/
 * scan chain), and everything above it is in-process Enumerable work (joins, unions, aggregates).
 * This walks the optimized tree, finds every {@code JdbcToEnumerableConverter}, converts each one's
 * own JDBC-side subtree back to real SQL text via {@link RelToSqlConverter} (the same real,
 * tested API {@link RollupStage} already relies on to convert a rewritten plan back to SQL), and
 * re-executes JUST that leaf query against its own real backend -- with real wall-clock timing and
 * a real row count from actually iterating the result. This is a genuinely separate, second
 * execution of each leaf (not shared with the real federated query's own execution above it), the
 * same honest tradeoff a DBA manually running {@code EXPLAIN ANALYZE} on a suspect subquery makes --
 * documented plainly, not hidden.
 *
 * <p><b>Skipped entirely when the statement has bind parameters</b>: {@link RelToSqlConverter}
 * preserves Calcite's own dynamic-parameter markers in the extracted leaf SQL rather than inlining
 * their bound values, and there's no general way from here to know which of the original
 * statement's bind values apply to which extracted leaf subtree -- rather than guess and risk
 * executing a leaf query against the wrong value (or a syntax error), this returns an empty list
 * for that case; the aggregate (whole-query) timing/row-count {@link SqlPlanStore} already captures
 * is unaffected either way.
 */
final class LeafScanProfiler {

    private static final Logger log = LoggerFactory.getLogger(LeafScanProfiler.class);

    private LeafScanProfiler() {
    }

    /** @param mountNameToBackend maps the internal Calcite convention name each leaf's {@code
     *     JdbcConvention} was mounted under (see {@link ShardJoinExecutor}'s {@code __polywire_shardN}
     *     names, or {@link SchemaFederationStage}'s real schema names) back to a
     *     {@link BackendTarget} to actually connect to and a human-readable label for the result. */
    static List<SqlPlanStore.LeafScanMetric> measure(RelNode optimized, SqlDialect dialect,
            Map<String, MountedBackend> mountNameToBackend, boolean hasBindParams) {
        if (hasBindParams) {
            return List.of();
        }
        List<RelNode> leaves = new ArrayList<>();
        collectJdbcLeaves(optimized, leaves);
        log.debug("leaf scan profiling: found {} JdbcToEnumerableConverter leaf(ves) in the optimized plan", leaves.size());
        List<SqlPlanStore.LeafScanMetric> metrics = new ArrayList<>();
        RelToSqlConverter toSql = new RelToSqlConverter(dialect);
        for (RelNode leaf : leaves) {
            JdbcToEnumerableConverter converter = (JdbcToEnumerableConverter) leaf;
            RelNode jdbcSubtree = converter.getInput();
            Convention convention = jdbcSubtree.getTraitSet().getTrait(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
            // Real bug, found live: JdbcConvention.getName() returns "JDBC.<name>", not the bare
            // <name> passed to JdbcConvention.of(...) -- Calcite prefixes it internally. Strip the
            // prefix back off before looking it up against the caller's own mount-name map.
            String conventionName = convention == null ? null : convention.getName();
            String mountName = conventionName != null && conventionName.startsWith("JDBC.")
                    ? conventionName.substring("JDBC.".length()) : conventionName;
            MountedBackend backend = mountName == null ? null : mountNameToBackend.get(mountName);
            if (backend == null) {
                log.warn("leaf scan profiling: couldn't resolve a leaf's own mount name (\"{}\") back to a real "
                        + "backend -- skipping this one leaf's metric, real query is unaffected", mountName);
                continue;
            }
            String sql;
            try {
                SqlNode sqlNode = toSql.visitRoot(jdbcSubtree).asStatement();
                sql = sqlNode.toSqlString(dialect).getSql();
            } catch (RuntimeException e) {
                log.warn("leaf scan profiling: failed to convert a leaf subtree back to SQL for backend \"{}\" "
                        + "-- skipping this one leaf's metric, real query is unaffected ({})", backend.label(), e.toString());
                continue;
            }
            metrics.add(measureOne(backend, sql));
        }
        return metrics;
    }

    private static SqlPlanStore.LeafScanMetric measureOne(MountedBackend backend, String sql) {
        long start = System.nanoTime();
        try (Connection connection = backend.target().open();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            long rows = 0;
            while (rs.next()) {
                rows++;
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            return new SqlPlanStore.LeafScanMetric(backend.label(), sql, elapsedMillis, rows, null);
        } catch (SQLException e) {
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            return new SqlPlanStore.LeafScanMetric(backend.label(), sql, elapsedMillis, 0, e.getMessage());
        }
    }

    /** Stops recursing the instant a {@link JdbcToEnumerableConverter} is found -- everything
     * below it is the JDBC-pushed-down subtree this class converts back to SQL as a whole, not
     * further Enumerable-side nodes to keep walking into. */
    private static void collectJdbcLeaves(RelNode node, List<RelNode> out) {
        if (node instanceof JdbcToEnumerableConverter) {
            out.add(node);
            return;
        }
        for (RelNode input : node.getInputs()) {
            collectJdbcLeaves(input, out);
        }
    }

    record MountedBackend(BackendTarget target, String label) {
    }
}
