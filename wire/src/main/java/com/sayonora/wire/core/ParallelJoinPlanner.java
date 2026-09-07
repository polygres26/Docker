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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
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
     * connection directly -- not through Calcite; their columns are the RAW backend scan's own
     * columns, in that backend's own natural order (unprojected). {@code buildProjection}/{@code
     * probeProjection}, when non-null, remap a raw streamed row into that side's LOGICAL shape --
     * from a plain column-selection/reordering {@code Project} Calcite left un-pushed-down directly
     * above that side's own leaf scan (e.g. the {@code o} side of {@code SELECT o.customer_id,
     * o.amount FROM (SELECT * FROM orders) o ...}); {@code null} means the raw row already IS the
     * logical row. {@code buildKeyOrdinal}/{@code probeKeyOrdinal} are ordinals into that LOGICAL
     * (post-{@code *Projection}) row -- i.e. the same ordinal {@link Join#analyzeCondition()}'s own
     * {@code JoinInfo} already reports relative to that side's row type, usable directly with no
     * further translation once the projection (if any) has been applied. {@code leftIsBuild}
     * records which structural side of the original join (left vs. right) turned out to be the
     * build side, purely so {@link ParallelJoinExecutor} can concatenate each output row's columns
     * in the same left-then-right order the original join's own row type uses, regardless of which
     * side was cheaper to build from. {@code outputProjection}, when non-null, is the ordinal (into
     * the natural left-then-right concatenated LOGICAL row) of each output column in order -- from
     * a plain column-selection/reordering {@code Project} Calcite placed directly above the join
     * itself (e.g. {@code SELECT c.name, o.amount}); {@code null} means that concatenation IS the
     * output, unchanged. */
    record Plan(String buildSql, List<Integer> buildProjection, RexRowEvaluator.RowPredicate buildFilter,
            int buildKeyOrdinal, LeafScanProfiler.MountedBackend buildBackend,
            String probeSql, List<Integer> probeProjection, RexRowEvaluator.RowPredicate probeFilter,
            int probeKeyOrdinal, LeafScanProfiler.MountedBackend probeBackend,
            boolean leftIsBuild, List<Integer> outputProjection, long probeRowCountEstimate,
            List<SortKey> sortKeys, Integer fetchLimit, AggregateSpec aggregateSpec,
            String probeKeyColumnName) {
    }

    /** One supported aggregate call: {@code kind} is one of {@link SqlKind#SUM}, {@code COUNT},
     * {@code MIN}, {@code MAX}, or {@code AVG}; {@code argOrdinal} is the input ordinal (relative
     * to whatever sits directly below the aggregation -- the pre-aggregation projection's output,
     * or the join's own natural row if there's no such projection) it operates over, or {@code
     * null} only for {@code COUNT(*)}. */
    record AggCall(SqlKind kind, Integer argOrdinal) {
    }

    /** A {@code GROUP BY} aggregation directly above the join (or its pre-aggregation projection).
     * Deliberately narrow, in the same spirit as every other eligibility check in this class:
     * exactly one grouping set (no {@code GROUPING SETS}/{@code ROLLUP}/{@code CUBE}), and every
     * aggregate call must be non-{@code DISTINCT}, have no {@code FILTER} clause, take at most one
     * argument, and be one of {@code SUM}/{@code COUNT}/{@code MIN}/{@code MAX}/{@code AVG} -- any
     * other function ({@code STRING_AGG}, {@code ARRAY_AGG}, a user-defined aggregate, ...) refuses
     * outright rather than risk misevaluating it. {@code groupKeyOrdinals} and each call's {@code
     * argOrdinal} are relative to the aggregation's own INPUT row (the pre-aggregation projection's
     * output, or the join's natural row when there's none).
     *
     * <p><b>Real, found-live Calcite decomposition to handle</b>: plain {@code SUM} doesn't compile
     * to {@code EnumerableAggregate} directly -- it needs {@code AggregateReduceFunctionsRule},
     * which rewrites it into {@code $SUM0} (an aggregate that returns 0, not {@code NULL}, for an
     * all-null/empty input) plus a {@code CASE(count = 0, NULL, $SUM0_result)} null-guard in a NEW
     * {@code Project} directly above the {@code Aggregate}; {@code AVG} is similarly rewritten into
     * {@code SUM}/{@code COUNT} plus a division, also in that outer {@code Project}. Rather than
     * leave SUM/AVG (extremely common) entirely unsupported, {@link #tryExtractReducedAggregateSpec}
     * recognizes exactly these two known decomposition shapes and reconstructs the original
     * user-facing {@code SUM}/{@code AVG} call -- {@link ParallelJoinExecutor}'s own accumulator
     * already implements the correct null/empty-group semantics directly, so Calcite's own
     * decomposed form is simply discarded once recognized, never re-executed.
     *
     * <p>{@code outputColumnNames} are Calcite's own real field names for the FINAL output row
     * (preserving a query's real column alias, e.g. {@code SUM(amount) AS total}); {@code
     * outputLayout} says, for each output position in order, which group key or aggregate result
     * (by index into {@code groupKeyOrdinals}/{@code aggCalls}) belongs there -- needed because nothing
     * requires a user's own {@code SELECT} list to list group keys before aggregates, or in the
     * order {@code GROUP BY} declared them (e.g. {@code SELECT SUM(x), name FROM t GROUP BY name}
     * puts the aggregate result first). */
    record AggregateSpec(List<Integer> groupKeyOrdinals, List<AggCall> aggCalls, List<String> outputColumnNames,
            List<OutputColumn> outputLayout) {
    }

    /** One output position: either the {@code groupKeyIndex}-th group key, or the {@code
     * aggCallIndex}-th aggregate result -- see {@link AggregateSpec#outputLayout()}. */
    record OutputColumn(boolean isGroupKey, int index) {
    }

    /** One {@code ORDER BY} key: {@code ordinal} into the FINAL output row -- the same row space
     * {@code outputProjection} produces (or the natural left-then-right concatenation, when there's
     * no {@code outputProjection}) -- since a {@code Sort} directly above a {@code Project} (or
     * directly above the {@code Join} itself, when there's no {@code Project}) reports its own
     * collation ordinals relative to exactly that row shape already; no further translation needed.
     * Deliberately ignores {@code NULLS FIRST}/{@code NULLS LAST} nuance -- nulls sort last
     * regardless of direction, a real, disclosed simplification consistent with this engine's other
     * narrow scoping choices, not a general SQL null-ordering implementation. */
    record SortKey(int ordinal, boolean descending) {
    }

    /** One join side, fully resolved: its own leaf backend scan, and -- when Calcite left a plain
     * column-selection/reordering {@code Project} and/or a {@code Filter} directly above that scan
     * un-pushed-down -- the ordinal remap and/or compiled row predicate needed to turn a raw
     * streamed row into this side's logical, filtered shape. Both {@code baseProjection} and {@code
     * residualFilter} (when the shape includes both) are expressed in terms of the LEAF's own raw
     * row -- a {@code Filter} directly on the leaf, with an optional {@code Project} above it, never
     * changes the row shape the filter's own ordinals refer to. */
    private record SideExtraction(RelNode leaf, List<Integer> baseProjection, RexRowEvaluator.RowPredicate residualFilter) {
    }

    /** The shared, structural result of peeling any leading {@code Sort}/{@code Limit}, {@code
     * Aggregate}, and top-level {@code Project} off the optimized plan's root -- extracted so both
     * {@link #tryPlan} (a single 2-way join) and {@link #tryChainPlan} (a left-deep chain of 3+
     * leaves) share exactly one implementation of this peeling, rather than risk two copies drifting
     * apart on a future fix. {@code root}, once returned, is exactly the join structure underneath --
     * either the sole {@link Join} ({@link #tryPlan}'s own case) or the outermost {@link Join} of a
     * left-deep chain ({@link #tryChainPlan}'s case). */
    private record PeeledHead(RelNode root, List<SortKey> sortKeys, Integer fetchLimit,
            AggregateSpec aggregateSpec, List<Integer> outputProjection) {
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
        PeeledHead head = peelSortAggregateProject(optimized);
        if (head == null) {
            return null;
        }
        Join join = findSoleInnerJoin(head.root());
        if (join == null) {
            log.debug("parallel join planner: no single top-level INNER join found in the optimized plan -- skipping");
            return null;
        }
        return buildTwoWayPlan(join, mountDialects, mountToBackend, head.outputProjection(), head.sortKeys(),
                head.fetchLimit(), head.aggregateSpec());
    }

    /** N-way (left-deep chain) counterpart to {@link #tryPlan}: only attempted when there are 3+
     * federated backends at all (see {@link SchemaFederationStage}'s own call site) -- a genuine 2-way
     * join is always {@link #tryPlan}'s job. Reuses the exact same {@link #buildTwoWayPlan} extraction
     * for the chain's FIRST pairwise join (the two left-most leaves), which alone gets Phase 0-2's
     * full partitioned/parallel/remote-capable treatment; {@link ChainedJoinExecutor} joins each
     * subsequent leaf against the running, already-materialized result via a single, in-memory hash
     * join -- a real, disclosed scope narrowing: only the chain's first pairwise join is parallelized.
     * Only a LEFT-DEEP chain shape ({@code Join(Join(...,leaf),leaf)}, confirmed via a live diagnostic
     * to be exactly what Calcite produces for {@code A JOIN B JOIN C}) is supported; a right-deep or
     * bushy join tree falls back to the sequential path, same as every other real narrowing here. */
    static ChainPlan tryChainPlan(RelNode optimized, Map<String, SqlDialect> mountDialects,
            Map<String, LeafScanProfiler.MountedBackend> mountToBackend, boolean hasBindParams) {
        if (hasBindParams) {
            return null;
        }
        PeeledHead head = peelSortAggregateProject(optimized);
        if (head == null) {
            return null;
        }
        if (!(head.root() instanceof Join topJoin)) {
            log.debug("parallel join planner: no top-level join found for a chain -- skipping");
            return null;
        }
        JoinChain chain = findLeftDeepChain(topJoin);
        if (chain == null || chain.leaves().size() < 3) {
            log.debug("parallel join planner: no left-deep chain of 3+ backend leaves found -- skipping");
            return null;
        }
        Join innermostJoin = findInnermostJoin(topJoin);
        Plan firstStepPlan = buildTwoWayPlan(innermostJoin, mountDialects, mountToBackend, null, null, null, null);
        if (firstStepPlan == null) {
            return null;
        }
        List<ChainExtensionStep> extensionSteps = new ArrayList<>();
        // chain.leaves()[0]/[1] (the first pair) are already fully covered by firstStepPlan above;
        // chain.keyOrdinalPairs()[0] is that first pair's own join key (unused here, buildTwoWayPlan
        // recomputed it independently from innermostJoin). Each subsequent pairs[i] (i>=1) is the
        // join key between the RUNNING result-so-far and chain.leaves()[i+1].
        for (int i = 1; i < chain.keyOrdinalPairs().size(); i++) {
            SideExtraction leaf = chain.leaves().get(i + 1);
            int[] pair = chain.keyOrdinalPairs().get(i);
            String mount = mountNameOf(leaf.leaf());
            LeafScanProfiler.MountedBackend backend = mount == null ? null : mountToBackend.get(mount);
            SqlDialect dialect = mount == null ? null : mountDialects.get(mount);
            if (backend == null || dialect == null) {
                log.debug("parallel join planner: couldn't resolve a chain leaf's mount to a real backend -- skipping");
                return null;
            }
            String sql;
            try {
                sql = toSql(leaf.leaf(), dialect);
            } catch (RuntimeException e) {
                log.debug("parallel join planner: failed to convert a chain leaf back to SQL -- skipping ({})", e.toString());
                return null;
            }
            extensionSteps.add(new ChainExtensionStep(sql, leaf.baseProjection(), leaf.residualFilter(), backend,
                    pair[0], pair[1]));
        }
        return new ChainPlan(firstStepPlan, extensionSteps, head.outputProjection(), head.sortKeys(),
                head.fetchLimit(), head.aggregateSpec());
    }

    private static PeeledHead peelSortAggregateProject(RelNode optimized) {
        RelNode root = optimized;
        List<SortKey> sortKeys = null;
        Integer fetchLimit = null;
        // Real, found-live Calcite/EnumerableConvention quirk: "ORDER BY ... LIMIT" does NOT
        // compile to one fused Sort node with both a collation and a fetch -- EnumerableConvention
        // splits it into a SEPARATE EnumerableLimit (carries fetch/offset) wrapping an
        // EnumerableSort (carries the collation, its own fetch always null in this configuration).
        // A bare Sort-with-fetch (the shape a non-Enumerable/logical plan would use) is handled too,
        // for robustness, even though the live optimized plan here is always EnumerableConvention.
        RexNode fetchNode = null;
        RelNode afterFetchNode = null;
        if (root instanceof org.apache.calcite.adapter.enumerable.EnumerableLimit limit) {
            fetchNode = limit.fetch;
            afterFetchNode = limit.getInput();
        } else if (root instanceof Sort sortWithOwnFetch && sortWithOwnFetch.fetch != null) {
            fetchNode = sortWithOwnFetch.fetch;
            afterFetchNode = sortWithOwnFetch;
        } else if (root instanceof Sort) {
            // An unbounded ORDER BY (no LIMIT) stays genuinely out of scope -- it's inherently
            // blocking end-to-end (the whole input must be seen before any output can be produced),
            // and a distributed/bounded-partial-sort optimization for that case is real, separate
            // work this pass doesn't attempt.
            log.debug("parallel join planner: an ORDER BY with no LIMIT is out of scope -- skipping");
            return null;
        }
        if (fetchNode != null) {
            Integer fetch = fetchValue(fetchNode);
            if (fetch == null) {
                log.debug("parallel join planner: couldn't resolve the LIMIT to a plain integer -- skipping");
                return null;
            }
            fetchLimit = fetch;
            if (afterFetchNode instanceof Sort sort) {
                sortKeys = new ArrayList<>();
                for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
                    boolean descending = collation.getDirection() == RelFieldCollation.Direction.DESCENDING
                            || collation.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
                    sortKeys.add(new SortKey(collation.getFieldIndex(), descending));
                }
                root = sort.getInput();
            } else {
                // A bare LIMIT with no ORDER BY above it -- a real, valid shape (row order is
                // otherwise unspecified by SQL semantics anyway). No sort keys to apply, just the
                // final truncation once rows are collected.
                sortKeys = List.of();
                root = afterFetchNode;
            }
        }
        // A GROUP BY aggregation directly above the join's own (pre-aggregation) projection --
        // see AggregateSpec's own javadoc for the exact supported shape and why it's this narrow.
        // Two real shapes: a bare Aggregate (every call compiles to EnumerableAggregate directly),
        // or a Project(Aggregate(...)) -- the shape Calcite ALWAYS uses once SUM or AVG is involved
        // (AggregateReduceFunctionsRule's own null-guard/division rewrite lands in that outer
        // Project), and also whenever the user's own SELECT list doesn't happen to list group keys
        // before aggregates in GROUP BY's own order.
        AggregateSpec aggregateSpec = null;
        if (root instanceof Project outerAggProject && outerAggProject.getInput() instanceof Aggregate aggregate) {
            aggregateSpec = tryExtractReducedAggregateSpec(outerAggProject, aggregate);
            if (aggregateSpec == null) {
                log.debug("parallel join planner: the aggregation above the join isn't a supported "
                        + "shape (non-SUM/COUNT/MIN/MAX/AVG function, DISTINCT, a FILTER clause, "
                        + "GROUPING SETS/ROLLUP/CUBE, a multi-argument call, or an unrecognized "
                        + "computed expression above the aggregate) -- skipping");
                return null;
            }
            root = aggregate.getInput();
        } else if (root instanceof Aggregate aggregate) {
            aggregateSpec = extractAggregateSpec(aggregate);
            if (aggregateSpec == null) {
                log.debug("parallel join planner: the aggregation above the join isn't a supported "
                        + "shape (non-SUM/COUNT/MIN/MAX/AVG function, DISTINCT, a FILTER clause, "
                        + "GROUPING SETS/ROLLUP/CUBE, or a multi-argument call) -- skipping");
                return null;
            }
            root = aggregate.getInput();
        }
        // When aggregateSpec is present, this projection selects/reorders the join's own columns
        // into what the aggregation above needs (group keys + aggregate arguments) -- otherwise
        // it's the query's own final SELECT-list projection. Mechanically identical (a plain
        // ordinal remap of the join's natural row); ParallelJoinExecutor applies it at the right
        // point in the pipeline depending on which case it is.
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
        return new PeeledHead(root, sortKeys, fetchLimit, aggregateSpec, outputProjection);
    }

    /** Resolves a single equi-join into a full {@link Plan} -- the extraction logic shared by {@link
     * #tryPlan} (where {@code join} is the plan's sole join) and {@link #tryChainPlan} (where {@code
     * join} is the left-deep chain's INNERMOST join, between its first two leaves; the outer-layer
     * parameters are then {@code null}/{@code null}/{@code null}/{@code null} since those apply to
     * the WHOLE chain's final result, not this intermediate pairwise step). */
    private static Plan buildTwoWayPlan(Join join, Map<String, SqlDialect> mountDialects,
            Map<String, LeafScanProfiler.MountedBackend> mountToBackend, List<Integer> outputProjection,
            List<SortKey> sortKeys, Integer fetchLimit, AggregateSpec aggregateSpec) {
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.size() != 1) {
            log.debug("parallel join planner: join condition isn't a single equi-join key pair -- skipping");
            return null;
        }
        SideExtraction leftSide = extractSide(join.getLeft());
        SideExtraction rightSide = extractSide(join.getRight());
        if (leftSide == null || rightSide == null) {
            log.debug("parallel join planner: one side of the join isn't exactly one backend (with, at most, a "
                    + "plain un-pushed-down column selection above it) -- skipping");
            return null;
        }
        String leftMount = mountNameOf(leftSide.leaf());
        String rightMount = mountNameOf(rightSide.leaf());
        LeafScanProfiler.MountedBackend leftBackend = leftMount == null ? null : mountToBackend.get(leftMount);
        LeafScanProfiler.MountedBackend rightBackend = rightMount == null ? null : mountToBackend.get(rightMount);
        SqlDialect leftDialect = leftMount == null ? null : mountDialects.get(leftMount);
        SqlDialect rightDialect = rightMount == null ? null : mountDialects.get(rightMount);
        if (leftBackend == null || rightBackend == null || leftDialect == null || rightDialect == null) {
            log.debug("parallel join planner: couldn't resolve both sides' mount names back to a real backend -- skipping");
            return null;
        }
        // These ordinals are relative to join.getLeft()/getRight()'s own row type -- i.e. each
        // side's LOGICAL (post-side-projection) shape, exactly what buildProjection/probeProjection
        // (below) produce from the raw streamed row. No name-based lookup needed.
        int leftKeyOrdinal = info.leftKeys.getInt(0);
        int rightKeyOrdinal = info.rightKeys.getInt(0);
        String leftSql;
        String rightSql;
        try {
            leftSql = toSql(leftSide.leaf(), leftDialect);
            rightSql = toSql(rightSide.leaf(), rightDialect);
        } catch (RuntimeException e) {
            log.debug("parallel join planner: failed to convert one side back to SQL -- skipping ({})", e.toString());
            return null;
        }
        Long leftCount = countRows(leftBackend, leftSql);
        Long rightCount = countRows(rightBackend, rightSql);
        if (leftCount != null && rightCount != null && Math.min(leftCount, rightCount) < minRowsFromEnvOrDefault()) {
            log.debug("parallel join planner: smaller side has only ~{} estimated row(s), below the "
                    + "WARP_PARALLEL_JOIN_MIN_ROWS threshold -- partitioning overhead isn't worth it, skipping",
                    Math.min(leftCount, rightCount));
            return null;
        }
        boolean leftIsBuild = leftCount == null || rightCount == null || leftCount <= rightCount;
        // The PROBE side (the larger one) is what actually drives how much per-partition work
        // there is -- a real signal for sizing partition count, not just picking a build side.
        // -1 (unknown) when either probe failed, same "don't guess" stance as everywhere else here.
        long probeRowCountEstimate = (leftCount == null || rightCount == null) ? -1L : Math.max(leftCount, rightCount);
        if (leftIsBuild) {
            return new Plan(leftSql, leftSide.baseProjection(), leftSide.residualFilter(), leftKeyOrdinal, leftBackend,
                    rightSql, rightSide.baseProjection(), rightSide.residualFilter(), rightKeyOrdinal, rightBackend,
                    true, outputProjection, probeRowCountEstimate, sortKeys, fetchLimit, aggregateSpec,
                    rawColumnNameFor(rightSide, rightKeyOrdinal));
        }
        return new Plan(rightSql, rightSide.baseProjection(), rightSide.residualFilter(), rightKeyOrdinal, rightBackend,
                leftSql, leftSide.baseProjection(), leftSide.residualFilter(), leftKeyOrdinal, leftBackend,
                false, outputProjection, probeRowCountEstimate, sortKeys, fetchLimit, aggregateSpec,
                rawColumnNameFor(leftSide, leftKeyOrdinal));
    }

    /** Dynamic filtering (this session's own follow-up): resolves the probe side's join-key ordinal
     * (relative to its LOGICAL, post-{@code baseProjection} row -- see {@link Plan}'s own javadoc)
     * back to the REAL underlying column name Calcite knows for the raw leaf scan -- exactly the name
     * that appears in {@code toSql(leaf, dialect)}'s own generated {@code SELECT} list, so {@link
     * ParallelJoinExecutor} can safely wrap that extracted SQL in a {@code WHERE <name> IN (...)}
     * filter built from the build side's own real, already-collected keys once the build phase
     * completes -- the same exact-semi-join idea {@link SemiJoinPushdown} already uses for the
     * sequential path, now reaching the parallel path too. {@code null} when the ordinal can't be
     * resolved (should not normally happen once {@link #extractSide} has already succeeded) -- the
     * caller simply skips this optimization in that case, same as every other real, disclosed
     * narrowing here. */
    private static String rawColumnNameFor(SideExtraction side, int logicalKeyOrdinal) {
        int rawOrdinal = side.baseProjection() == null ? logicalKeyOrdinal : side.baseProjection().get(logicalKeyOrdinal);
        List<org.apache.calcite.rel.type.RelDataTypeField> fields = side.leaf().getRowType().getFieldList();
        return rawOrdinal >= 0 && rawOrdinal < fields.size() ? fields.get(rawOrdinal).getName() : null;
    }

    /** Extracts a supported {@link AggregateSpec} from a BARE {@code aggregate} with no wrapping
     * {@code Project} above it -- every call must already compile to {@code EnumerableAggregate}
     * directly (true for {@code COUNT}/{@code MIN}/{@code MAX}, never true for plain {@code SUM} or
     * {@code AVG} -- see {@link #tryExtractReducedAggregateSpec} for that shape instead). Returns
     * {@code null} when its shape isn't one this executor can confidently re-apply outside of
     * Calcite -- see {@link AggregateSpec}'s own javadoc for exactly what's supported and why. The
     * output layout here is always the identity (group keys, in order, then aggregate results, in
     * order) -- Calcite's own row-type convention for a bare {@code Aggregate}, unchanged by any
     * further reordering since there's no outer {@code Project} in this shape. */
    private static AggregateSpec extractAggregateSpec(Aggregate aggregate) {
        if (aggregate.getGroupSets().size() != 1) {
            return null;
        }
        List<Integer> groupKeyOrdinals = aggregate.getGroupSet().asList();
        List<AggCall> calls = new ArrayList<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            AggCall direct = asDirectAggCall(call);
            if (direct == null) {
                return null;
            }
            calls.add(direct);
        }
        List<String> outputColumnNames = new ArrayList<>();
        for (org.apache.calcite.rel.type.RelDataTypeField field : aggregate.getRowType().getFieldList()) {
            outputColumnNames.add(field.getName());
        }
        List<OutputColumn> outputLayout = new ArrayList<>();
        for (int i = 0; i < groupKeyOrdinals.size(); i++) {
            outputLayout.add(new OutputColumn(true, i));
        }
        for (int i = 0; i < calls.size(); i++) {
            outputLayout.add(new OutputColumn(false, i));
        }
        return new AggregateSpec(groupKeyOrdinals, calls, outputColumnNames, outputLayout);
    }

    /** Extracts a supported {@link AggregateSpec} when {@code outerProject} sits directly above
     * {@code aggregate} -- the shape Calcite always uses once {@code SUM}/{@code AVG} is involved
     * (see {@link AggregateSpec}'s own javadoc), and also whenever the user's {@code SELECT} list
     * doesn't list columns in the aggregate's own natural group-keys-then-aggregates order. Walks
     * {@code outerProject}'s own expressions in order, classifying each as: a plain passthrough of
     * a group key or an already-direct aggregate result; or (only when the expression is computed)
     * one of the two known {@code AggregateReduceFunctionsRule} decomposition shapes -- a {@code
     * CASE(count = 0, NULL, $SUM0result)} null-guard (reconstructs a plain {@code SUM}) or a {@code
     * sum / count} division, optionally {@code CAST} (reconstructs {@code AVG}). Any expression
     * matching neither refuses the whole plan outright ({@code null}) rather than risk silently
     * misevaluating an aggregate this class doesn't actually understand. */
    private static AggregateSpec tryExtractReducedAggregateSpec(Project outerProject, Aggregate aggregate) {
        if (aggregate.getGroupSets().size() != 1) {
            return null;
        }
        List<Integer> groupKeyOrdinals = aggregate.getGroupSet().asList();
        int groupKeyCount = groupKeyOrdinals.size();
        List<AggregateCall> underlyingCalls = aggregate.getAggCallList();
        List<AggCall> aggCalls = new ArrayList<>();
        List<OutputColumn> outputLayout = new ArrayList<>();
        List<String> outputColumnNames = new ArrayList<>();
        List<org.apache.calcite.rel.type.RelDataTypeField> outerFields = outerProject.getRowType().getFieldList();
        List<RexNode> exprs = outerProject.getProjects();
        for (int i = 0; i < exprs.size(); i++) {
            RexNode expr = exprs.get(i);
            outputColumnNames.add(outerFields.get(i).getName());
            if (expr instanceof RexInputRef ref) {
                int aggregateOutputOrdinal = ref.getIndex();
                if (aggregateOutputOrdinal < groupKeyCount) {
                    outputLayout.add(new OutputColumn(true, aggregateOutputOrdinal));
                    continue;
                }
                AggCall direct = asDirectAggCall(underlyingCalls.get(aggregateOutputOrdinal - groupKeyCount));
                if (direct == null) {
                    return null;
                }
                aggCalls.add(direct);
                outputLayout.add(new OutputColumn(false, aggCalls.size() - 1));
                continue;
            }
            AggCall reconstructed = matchReducedAggExpression(expr, underlyingCalls, groupKeyCount);
            if (reconstructed == null) {
                return null;
            }
            aggCalls.add(reconstructed);
            outputLayout.add(new OutputColumn(false, aggCalls.size() - 1));
        }
        return new AggregateSpec(groupKeyOrdinals, aggCalls, outputColumnNames, outputLayout);
    }

    /** Recognizes exactly the two shapes {@code AggregateReduceFunctionsRule} produces -- see
     * {@link AggregateSpec}'s own javadoc -- against {@code underlyingCalls} (the {@code
     * Aggregate}'s OWN, already-decomposed call list); {@code null} for anything else. */
    private static AggCall matchReducedAggExpression(RexNode expr, List<AggregateCall> underlyingCalls, int groupKeyCount) {
        if (!(expr instanceof org.apache.calcite.rex.RexCall call)) {
            return null;
        }
        if (call.getKind() == SqlKind.CASE && call.getOperands().size() == 3
                && call.getOperands().get(2) instanceof RexInputRef sumRef) {
            int idx = sumRef.getIndex() - groupKeyCount;
            if (idx >= 0 && idx < underlyingCalls.size()) {
                AggregateCall underlying = underlyingCalls.get(idx);
                if (underlying.getAggregation().getKind() == SqlKind.SUM0 && underlying.getArgList().size() == 1) {
                    return new AggCall(SqlKind.SUM, underlying.getArgList().get(0));
                }
            }
            return null;
        }
        RexNode divideExpr = call.getKind() == SqlKind.CAST && call.getOperands().size() == 1
                ? call.getOperands().get(0) : expr;
        if (divideExpr instanceof org.apache.calcite.rex.RexCall divCall && divCall.getKind() == SqlKind.DIVIDE
                && divCall.getOperands().size() == 2
                && divCall.getOperands().get(0) instanceof RexInputRef sumRef
                && divCall.getOperands().get(1) instanceof RexInputRef countRef) {
            int sumIdx = sumRef.getIndex() - groupKeyCount;
            int countIdx = countRef.getIndex() - groupKeyCount;
            if (sumIdx >= 0 && sumIdx < underlyingCalls.size() && countIdx >= 0 && countIdx < underlyingCalls.size()) {
                AggregateCall sumCall = underlyingCalls.get(sumIdx);
                AggregateCall countCall = underlyingCalls.get(countIdx);
                boolean sumIsSummy = sumCall.getAggregation().getKind() == SqlKind.SUM
                        || sumCall.getAggregation().getKind() == SqlKind.SUM0;
                if (sumIsSummy && countCall.getAggregation().getKind() == SqlKind.COUNT
                        && sumCall.getArgList().size() == 1 && sumCall.getArgList().equals(countCall.getArgList())) {
                    return new AggCall(SqlKind.AVG, sumCall.getArgList().get(0));
                }
            }
        }
        return null;
    }

    /** A directly-supported aggregate call (one of {@code SUM}/{@code COUNT}/{@code MIN}/{@code
     * MAX}/{@code AVG}, non-{@code DISTINCT}, no {@code FILTER} clause, at most one argument) --
     * {@code null} for anything else. Shared by both {@link #extractAggregateSpec} (a bare {@code
     * Aggregate}, where every call must already be one of these) and {@link
     * #tryExtractReducedAggregateSpec} (a plain passthrough of an already-direct call, as opposed
     * to a decomposed one it reconstructs separately). */
    private static AggCall asDirectAggCall(AggregateCall call) {
        if (call.isDistinct() || call.hasFilter() || call.getArgList().size() > 1) {
            return null;
        }
        SqlKind kind = call.getAggregation().getKind();
        if (kind != SqlKind.SUM && kind != SqlKind.COUNT && kind != SqlKind.MIN
                && kind != SqlKind.MAX && kind != SqlKind.AVG) {
            return null;
        }
        Integer argOrdinal = call.getArgList().isEmpty() ? null : call.getArgList().get(0);
        if (argOrdinal == null && kind != SqlKind.COUNT) {
            // Only COUNT(*) is a real, valid zero-argument aggregate call -- SUM/MIN/MAX/AVG
            // always need an argument.
            return null;
        }
        return new AggCall(kind, argOrdinal);
    }

    /** Resolves a {@code Sort}'s own {@code fetch} (the {@code LIMIT} count) to a plain {@code
     * int} -- {@code null} when it isn't the simple literal shape expected (a bind parameter would
     * already have been refused by {@code hasBindParams} above; anything else is unexpected enough
     * to just decline rather than guess). */
    private static Integer fetchValue(RexNode fetch) {
        if (!(fetch instanceof RexLiteral literal)) {
            return null;
        }
        try {
            Number value = literal.getValueAs(Number.class);
            return value == null ? null : value.intValue();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** {@code WARP_PARALLEL_JOIN_MIN_ROWS} -- the smaller (build) side's estimated row count must
     * clear this before the parallel path is attempted at all; below it, partitioning/thread-handoff
     * overhead isn't worth it. When either side's row-count probe itself failed, this check is
     * skipped entirely (proceeds anyway) rather than blocking the feature on an unrelated probe
     * failure -- the same "don't let a missing signal disable a real optimization" stance {@link
     * #chooseBuildSide} historically took before this method absorbed its row-count reuse. */
    private static long minRowsFromEnvOrDefault() {
        String raw = System.getenv("WARP_PARALLEL_JOIN_MIN_ROWS");
        if (raw != null && !raw.isBlank()) {
            try {
                long parsed = Long.parseLong(raw.trim());
                if (parsed >= 0) {
                    return parsed;
                }
            } catch (NumberFormatException ignoredNotANumber) {
                // falls through to the default below
            }
        }
        return 10_000L;
    }

    /** Resolves one join input to a leaf backend scan, optionally wrapped in a plain
     * column-selection/reordering {@code Project} and/or a residual {@code Filter} Calcite left
     * un-pushed-down directly above it -- {@code null} for anything else (more than one leaf, a
     * nested combination deeper than one {@code Project}/{@code Filter} layer each, a projection
     * with a computed expression, or a filter condition {@link RexRowEvaluator#compile} can't
     * confidently evaluate, e.g. a function call or subquery). Handles, specifically: the bare leaf;
     * {@code Project(leaf)}; {@code Filter(leaf)}; and {@code Project(Filter(leaf))} -- in every
     * shape that includes a {@code Filter} directly on the leaf, the filter's own ordinals (and, in
     * the {@code Project(Filter(leaf))} case, the projection's too) are relative to the LEAF's raw
     * row, since a {@code Filter} never changes row shape. */
    private static SideExtraction extractSide(RelNode sideInput) {
        List<RelNode> leaves = new ArrayList<>();
        collectJdbcLeaves(sideInput, leaves);
        if (leaves.size() != 1) {
            return null;
        }
        RelNode leaf = leaves.get(0);
        if (sideInput == leaf) {
            return new SideExtraction(leaf, null, null);
        }
        if (sideInput instanceof Project project && project.getInput() == leaf) {
            List<Integer> ordinals = asPlainColumnSelection(project.getProjects());
            return ordinals == null ? null : new SideExtraction(leaf, ordinals, null);
        }
        if (sideInput instanceof Filter filter && filter.getInput() == leaf) {
            RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(filter.getCondition());
            return predicate == null ? null : new SideExtraction(leaf, null, predicate);
        }
        if (sideInput instanceof Project project && project.getInput() instanceof Filter filter && filter.getInput() == leaf) {
            List<Integer> ordinals = asPlainColumnSelection(project.getProjects());
            if (ordinals == null) {
                return null;
            }
            RexRowEvaluator.RowPredicate predicate = RexRowEvaluator.compile(filter.getCondition());
            return predicate == null ? null : new SideExtraction(leaf, ordinals, predicate);
        }
        return null;
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

    /** Real, measured row-count probe -- a lightweight {@code COUNT(*)} against a side's own
     * extracted SQL, run directly through that side's own backend connection (not through Calcite).
     * Feeds both build-side selection (the smaller side builds) and the {@code
     * WARP_PARALLEL_JOIN_MIN_ROWS} threshold check above, from the SAME two round trips. Deliberately
     * doesn't depend on {@link StatisticsStore} (unlike {@link SemiJoinPushdown}'s own build-side
     * choice) so this path works whether or not statistics are configured. Returns {@code null} on
     * any failure -- callers treat a missing count as "proceed anyway, just skip the size-based
     * decision it would have fed," never as a reason to fail the query. */
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

    /** By the time this is called, {@code node} is the optimized plan's root with any leading
     * {@code Sort} (extracted into {@code sortKeys}/{@code fetchLimit} above) and {@code Project}
     * (extracted into {@code outputProjection}) already peeled off -- {@code node} must be the
     * {@link Join} itself at that point, deliberately NOT a recursive search through further
     * wrapping nodes. Anything else there (a computed top expression, a second join, an unbounded
     * sort already refused above) encodes real query semantics this executor has no general way to
     * re-apply, so it safely falls back to today's unchanged sequential path instead of ever risking
     * a wrong-but-plausible-looking result. Also requires {@link JoinRelType#INNER} and that neither
     * input itself contains a further join (Phase 0 handles one two-way join, not an arbitrary join
     * graph). */
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

    /** One extension step of a left-deep join chain beyond its first pairwise join: joins the new
     * leaf ({@code leafSql}/{@code leafProjection}/{@code leafFilter}/{@code leafBackend}, exactly
     * like a {@link Plan}'s own build/probe side) against the RUNNING (already fully materialized)
     * result of every prior step. {@code runningResultKeyOrdinal} is relative to the running result's
     * own natural row (the concatenation of every leaf so far, in order -- exactly what {@link
     * ChainedJoinExecutor} produces at each step); {@code leafKeyOrdinal} is relative to this leaf's
     * own logical (post-{@code leafProjection}) row, same convention as {@link Plan#probeKeyOrdinal()}. */
    record ChainExtensionStep(String leafSql, List<Integer> leafProjection, RexRowEvaluator.RowPredicate leafFilter,
            LeafScanProfiler.MountedBackend leafBackend, int runningResultKeyOrdinal, int leafKeyOrdinal) {
    }

    /** A left-deep chain of 3+ backend leaves (see {@link #tryChainPlan}): {@code firstStepPlan} is a
     * normal 2-way {@link Plan} for the two left-most leaves, with no outer projection/sort/aggregate
     * of its own (those apply to the chain's FINAL result, carried here instead); {@code
     * extensionSteps} then join each subsequent leaf against the running result, left to right. */
    record ChainPlan(Plan firstStepPlan, List<ChainExtensionStep> extensionSteps,
            List<Integer> outputProjection, List<SortKey> sortKeys, Integer fetchLimit, AggregateSpec aggregateSpec) {
    }

    /** A left-deep chain's own structural shape, before mount/backend/SQL resolution: {@code
     * leaves()} are every backend leaf in left-to-right order; {@code keyOrdinalPairs()[i]} is
     * {@code {leftKeyOrdinal, rightKeyOrdinal}} for the join that attaches {@code leaves[i+1]} --
     * {@code leftKeyOrdinal} relative to the running combined row of {@code leaves[0..i]} (exactly
     * what {@link JoinInfo#leftKeys} already reports, since Calcite's own left-deep row types are
     * always the natural concatenation of every leaf so far), {@code rightKeyOrdinal} relative to
     * {@code leaves[i+1]}'s own row. */
    private record JoinChain(List<SideExtraction> leaves, List<int[]> keyOrdinalPairs) {
    }

    /** Walks a left-deep join tree bottom-up: {@code node} must be either another INNER {@link Join}
     * (continuing the chain, with its own RIGHT input required to be a single {@link #extractSide}-
     * compatible leaf -- this is exactly what excludes a bushy tree, since a leaf-only shape is the
     * only one {@link #extractSide} ever accepts) or itself directly reducible to a single leaf (the
     * chain's base case, an ordinary bare/{@code Project}/{@code Filter}-wrapped backend scan).
     * {@code null} for anything else -- a right-deep or bushy join tree, more than one equi-join key
     * pair per step, or a join input {@link #extractSide} can't confidently resolve. */
    private static JoinChain findLeftDeepChain(RelNode node) {
        if (node instanceof Join join) {
            if (join.getJoinType() != JoinRelType.INNER) {
                return null;
            }
            JoinInfo info = join.analyzeCondition();
            if (info.leftKeys.size() != 1) {
                return null;
            }
            SideExtraction rightLeaf = extractSide(join.getRight());
            if (rightLeaf == null) {
                return null;
            }
            JoinChain left = findLeftDeepChain(join.getLeft());
            if (left == null) {
                return null;
            }
            List<SideExtraction> leaves = new ArrayList<>(left.leaves());
            leaves.add(rightLeaf);
            List<int[]> pairs = new ArrayList<>(left.keyOrdinalPairs());
            pairs.add(new int[] {info.leftKeys.getInt(0), info.rightKeys.getInt(0)});
            return new JoinChain(leaves, pairs);
        }
        SideExtraction leaf = extractSide(node);
        if (leaf == null) {
            return null;
        }
        return new JoinChain(new ArrayList<>(List.of(leaf)), new ArrayList<>());
    }

    /** The chain's own innermost {@link Join} (between its first two leaves) -- {@code join}'s own
     * left input keeps being another {@link Join} until it isn't. */
    private static Join findInnermostJoin(Join join) {
        return join.getLeft() instanceof Join leftJoin ? findInnermostJoin(leftJoin) : join;
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
