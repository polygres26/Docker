package com.sayonora.wire.core;

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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 0 eligibility check + extraction for {@link ParallelJoinExecutor} -- the "Warp-native
 * parallel execution engine" design's coordinator-side {@code FragmentPlanner}, scoped narrowly to
 * its first real shape: a single two-backend equi-join.
 *
 * <p><b>Why this can't just be a hook inside {@link SchemaFederationStage#executeWithMounts}</b>:
 * Calcite's {@code RelRunner.prepareStatement(optimized)} compiles the WHOLE plan -- both leaf
 * scans and the join operator itself -- into one {@code EnumerableConvention}-generated {@code
 * PreparedStatement}, with no seam exposed to caller code between "leaf scans start" and "final
 * rows come back". This class instead reuses {@link LeafScanProfiler}'s own technique (already
 * proven in this codebase) for walking the optimized {@link RelNode} tree and converting a {@code
 * JdbcToEnumerableConverter}'s own subtree back to real SQL via {@link RelToSqlConverter} -- but
 * one level higher, at the {@link Join} node's two INPUTS (which may include a local
 * project/filter Calcite chose not to push down, not just the bare backend scan), so the extracted
 * SQL for each side is everything that side needs, ready to execute as one real, standalone
 * statement against its own single backend.
 *
 * <p><b>Deliberately narrow, exactly like {@link SemiJoinPushdown}</b>: only a single {@link
 * JoinRelType#INNER} join, with a single equi-join key pair, where each side reduces to EXACTLY one
 * {@code JdbcToEnumerableConverter} leaf (so each side is genuinely one backend, not itself a
 * further federated join), and only when the statement has no bind parameters (the same limitation
 * {@link LeafScanProfiler} already accepts, for the same reason: {@link RelToSqlConverter}
 * preserves dynamic-parameter markers rather than inlining bound values, and there's no general way
 * to know which of the original statement's bind values would apply to which extracted side).
 * Any other shape returns {@code null} -- the caller falls back to today's unchanged, sequential
 * {@code RelRunner} execution, exactly as {@link SemiJoinPushdown} degrades to "no pushdown"
 * whenever its own preconditions aren't met.
 */
final class ParallelJoinPlanner {

    private static final Logger log = LoggerFactory.getLogger(ParallelJoinPlanner.class);

    private ParallelJoinPlanner() {
    }

    /** {@code buildSql}/{@code probeSql} are each a complete, standalone SQL statement (no bind
     * parameters) ready to run through {@code buildBackend}/{@code probeBackend}'s own JDBC
     * connection directly -- not through Calcite. {@code leftIsBuild} records which structural side
     * of the original join (left vs. right) turned out to be the build side, purely so {@link
     * ParallelJoinExecutor} can concatenate each output row's columns in the same left-then-right
     * order the original join's own row type uses, regardless of which side was cheaper to build
     * from. {@code outputProjection}, when non-null, is the ordinal (into the natural left-then-right
     * concatenated row) of each output column in order -- from a plain column-selection/reordering
     * {@code Project} Calcite placed directly above the join (e.g. {@code SELECT c.name, o.amount});
     * {@code null} means the natural left-then-right concatenation IS the output, unchanged. */
    record Plan(String buildSql, String buildKeyColumn, LeafScanProfiler.MountedBackend buildBackend,
            String probeSql, String probeKeyColumn, LeafScanProfiler.MountedBackend probeBackend,
            boolean leftIsBuild, List<Integer> outputProjection) {
    }

    /** Returns {@code null} (never throws) whenever the plan isn't eligible for Phase 0's parallel
     * path -- every failure mode here is a real, intentional "fall back to today's behavior", not
     * an error. {@code mountDialects} and {@code mountToBackend} are keyed by the same mount/schema
     * name {@link SchemaFederationStage#executeWithMounts} already builds per backend. */
    static Plan tryPlan(RelNode optimized, Map<String, SqlDialect> mountDialects,
            Map<String, LeafScanProfiler.MountedBackend> mountToBackend, boolean hasBindParams) {
        if (hasBindParams) {
            return null;
        }
        RelNode root = optimized;
        List<Integer> outputProjection = null;
        if (root instanceof Project project) {
            outputProjection = asPlainColumnSelection(project.getProjects());
            if (outputProjection == null) {
                log.debug("parallel join planner: the plan's own top projection isn't a plain column "
                        + "selection/reordering (has a computed expression) -- skipping");
                return null;
            }
            root = project.getInput();
        }
        Join join = findSoleInnerJoin(root);
        if (join == null) {
            log.debug("parallel join planner: no single top-level INNER join found in the optimized plan -- skipping");
            return null;
        }
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.size() != 1) {
            log.debug("parallel join planner: join condition isn't a single equi-join key pair -- skipping");
            return null;
        }
        List<RelNode> leftLeaves = new ArrayList<>();
        collectJdbcLeaves(join.getLeft(), leftLeaves);
        List<RelNode> rightLeaves = new ArrayList<>();
        collectJdbcLeaves(join.getRight(), rightLeaves);
        if (leftLeaves.size() != 1 || rightLeaves.size() != 1) {
            log.debug("parallel join planner: one side of the join isn't exactly one backend -- skipping");
            return null;
        }
        // Each side must BE its own JdbcToEnumerableConverter directly, not a residual local
        // Project/Filter wrapping one -- RelToSqlConverter (below) only knows how to convert the
        // real JDBC-convention subtree BELOW that boundary node (see LeafScanProfiler's own use of
        // it, converting converter.getInput() rather than the converter itself); it has no visitor
        // for the converter node -- or anything above it -- at all. A residual local Enumerable-side
        // operator above the converter is a real, narrower case this Phase 0 implementation doesn't
        // yet handle -- it falls back safely rather than risk converting the wrong subtree.
        if (join.getLeft() != leftLeaves.get(0) || join.getRight() != rightLeaves.get(0)) {
            log.debug("parallel join planner: a side has a residual local operator above its backend scan "
                    + "that Calcite didn't push down -- skipping");
            return null;
        }
        String leftMount = mountNameOf(leftLeaves.get(0));
        String rightMount = mountNameOf(rightLeaves.get(0));
        LeafScanProfiler.MountedBackend leftBackend = leftMount == null ? null : mountToBackend.get(leftMount);
        LeafScanProfiler.MountedBackend rightBackend = rightMount == null ? null : mountToBackend.get(rightMount);
        SqlDialect leftDialect = leftMount == null ? null : mountDialects.get(leftMount);
        SqlDialect rightDialect = rightMount == null ? null : mountDialects.get(rightMount);
        if (leftBackend == null || rightBackend == null || leftDialect == null || rightDialect == null) {
            log.debug("parallel join planner: couldn't resolve both sides' mount names back to a real backend -- skipping");
            return null;
        }
        String leftKeyColumn = join.getLeft().getRowType().getFieldList().get(info.leftKeys.getInt(0)).getName();
        String rightKeyColumn = join.getRight().getRowType().getFieldList().get(info.rightKeys.getInt(0)).getName();
        String leftSql;
        String rightSql;
        try {
            leftSql = toSql(join.getLeft(), leftDialect);
            rightSql = toSql(join.getRight(), rightDialect);
        } catch (RuntimeException e) {
            log.debug("parallel join planner: failed to convert one side back to SQL -- skipping ({})", e.toString());
            return null;
        }
        boolean leftIsBuild = chooseBuildSide(leftBackend, leftSql, rightBackend, rightSql);
        if (leftIsBuild) {
            return new Plan(leftSql, leftKeyColumn, leftBackend, rightSql, rightKeyColumn, rightBackend, true, outputProjection);
        }
        return new Plan(rightSql, rightKeyColumn, rightBackend, leftSql, leftKeyColumn, leftBackend, false, outputProjection);
    }

    /** Returns each expression's own input ordinal, IN ORDER, only when every one of {@code
     * projects} is a plain {@link RexInputRef} (a bare column reference -- selection and/or
     * reordering, never a computed expression like {@code a + b} or a function call) -- {@code
     * null} otherwise. A computed top-level expression is real query semantics this executor has
     * no general way to re-apply outside of Calcite, so it safely falls back to the sequential path
     * instead of ever risking silently dropping it. */
    private static List<Integer> asPlainColumnSelection(List<RexNode> projects) {
        List<Integer> ordinals = new ArrayList<>(projects.size());
        for (RexNode expr : projects) {
            if (!(expr instanceof RexInputRef ref)) {
                return null;
            }
            ordinals.add(ref.getIndex());
        }
        return ordinals;
    }

    /** Real, measured build-side selection -- a lightweight {@code COUNT(*)} against each side's
     * own extracted SQL, run directly through that side's own backend connection (not through
     * Calcite). Deliberately doesn't depend on {@link StatisticsStore} (unlike {@link
     * SemiJoinPushdown}'s own build-side choice) so this path works whether or not statistics are
     * configured -- the two extra round trips are real, small cost against a query already large
     * enough that partitioning is worth attempting. On any failure, defaults to "left is build"
     * (an arbitrary, still-correct choice -- picking the wrong side only costs some efficiency, it
     * never produces a wrong join result). */
    private static boolean chooseBuildSide(LeafScanProfiler.MountedBackend leftBackend, String leftSql,
            LeafScanProfiler.MountedBackend rightBackend, String rightSql) {
        Long leftCount = countRows(leftBackend, leftSql);
        Long rightCount = countRows(rightBackend, rightSql);
        if (leftCount == null || rightCount == null) {
            return true;
        }
        return leftCount <= rightCount;
    }

    private static Long countRows(LeafScanProfiler.MountedBackend backend, String sql) {
        String countSql = "SELECT COUNT(*) FROM (" + sql + ") __warp_parallel_join_count";
        try (Connection connection = backend.target().open();
                PreparedStatement ps = connection.prepareStatement(countSql);
                ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : null;
        } catch (SQLException e) {
            log.debug("parallel join planner: row-count probe failed for backend \"{}\" -- defaulting build-side "
                    + "choice, real query is unaffected ({})", backend.label(), e.toString());
            return null;
        }
    }

    /** {@code leaf} must be the {@code JdbcToEnumerableConverter} itself (see the caller's own
     * check above) -- {@link RelToSqlConverter} converts the real JDBC-convention subtree BELOW
     * that boundary node (exactly {@link LeafScanProfiler#measure}'s own usage), never the
     * converter node itself. */
    private static String toSql(RelNode leaf, SqlDialect dialect) {
        JdbcToEnumerableConverter converter = (JdbcToEnumerableConverter) leaf;
        RelToSqlConverter toSql = new RelToSqlConverter(dialect);
        SqlNode sqlNode = toSql.visitRoot(converter.getInput()).asStatement();
        return sqlNode.toSqlString(dialect).getSql();
    }

    /** The optimized plan's ROOT must be the {@link Join} itself -- deliberately NOT a recursive
     * search through wrapping nodes. A {@code Sort} (from {@code ORDER BY}/{@code LIMIT}) or a
     * {@code Project} (reordering/renaming output columns) above the join encodes real query
     * semantics this executor does not apply -- it only ever runs the join subtree it extracts, so
     * accepting a join found nested under such a node would silently drop that semantics rather
     * than produce a wrong-but-plausible-looking result. Requiring an exact root match means any
     * query with an {@code ORDER BY}/{@code LIMIT}/non-trivial top projection safely falls back to
     * today's unchanged sequential path instead. Also requires {@link JoinRelType#INNER} and that
     * neither input itself contains a further join (Phase 0 handles one two-way join, not an
     * arbitrary join graph). */
    private static Join findSoleInnerJoin(RelNode node) {
        if (!(node instanceof Join join)) {
            return null;
        }
        if (join.getJoinType() != JoinRelType.INNER) {
            return null;
        }
        if (containsJoin(join.getLeft()) || containsJoin(join.getRight())) {
            return null;
        }
        return join;
    }

    private static boolean containsJoin(RelNode node) {
        if (node instanceof Join) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsJoin(input)) {
                return true;
            }
        }
        return false;
    }

    /** As {@link LeafScanProfiler#measure}'s own private {@code collectJdbcLeaves} -- stops
     * recursing the instant a {@code JdbcToEnumerableConverter} is found. Duplicated here (rather
     * than shared) because that method is {@code private} on a class with no shared base -- both
     * copies are small and intentionally identical; a real shared extraction is a reasonable, small
     * follow-up cleanup, not required for Phase 0's correctness. */
    private static void collectJdbcLeaves(RelNode node, List<RelNode> out) {
        if (node instanceof JdbcToEnumerableConverter) {
            out.add(node);
            return;
        }
        for (RelNode input : node.getInputs()) {
            collectJdbcLeaves(input, out);
        }
    }

    /** As {@link LeafScanProfiler#measure}: {@code JdbcConvention.getName()} returns {@code
     * "JDBC.<name>"}, not the bare mount name -- strip the prefix back off. Returns {@code null}
     * when the leaf's own convention can't be resolved at all. */
    private static String mountNameOf(RelNode leaf) {
        JdbcToEnumerableConverter converter = (JdbcToEnumerableConverter) leaf;
        RelNode jdbcSubtree = converter.getInput();
        Convention convention = jdbcSubtree.getTraitSet().getTrait(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        String conventionName = convention == null ? null : convention.getName();
        return conventionName != null && conventionName.startsWith("JDBC.")
                ? conventionName.substring("JDBC.".length()) : conventionName;
    }
}
