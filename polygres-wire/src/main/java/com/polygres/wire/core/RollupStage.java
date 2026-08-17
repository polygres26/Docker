package com.polygres.wire.core;

import com.polygres.wire.rollup.RollupDefinition;
import com.polygres.wire.rollup.RollupStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-aggregation query acceleration — see {@code ARCHITECTURE.md}'s rollup entry (built after
 * comparing PolyWire to Cube/AtScale/dbt Semantic Layer and finding this the one genuine gap: those
 * three all accelerate aggregate queries against huge fact tables via pre-aggregation; {@link
 * com.polygres.wire.cluster.CacheStage} is a plain cache-aside result cache, useless for "any slice of
 * this fact table, fast").
 *
 * <p><b>Reuses Calcite's own materialized-view substitution machinery</b> ({@code
 * org.apache.calcite.plan.RelOptMaterialization}/{@code RelOptMaterializations}/{@code
 * SubstitutionVisitor}) rather than hand-rolling SQL rewriting — real, tested query-rewrite logic
 * already shipping in {@code calcite-core} (confirmed unused anywhere else in this codebase before
 * this class). For each candidate query, builds a fresh single-backend embedded {@link Planner}
 * (same recipe {@link FederationStage#executeViaEmbeddedPlanner} already uses for its own embedded-
 * planner path, simplified: no federation, no column-statistics metadata provider — substitution
 * here is structural, not cost-based). All three conversions this needs (the rollup's defining
 * query, a plain scan of the rollup table, and the incoming query) are done on the <em>same</em>
 * {@link Planner} instance, cycled between statements via {@code close()} then {@link
 * Planner#reset()} together (found live: {@code reset()} alone throws — Calcite 1.42's {@code
 * PlannerImpl} requires the planner to already be closed before it can be reset — and skipping
 * either step leaves each conversion on its own internal {@code RelOptCluster}, which breaks
 * correctness two different ways, see below). {@code RelOptMaterializations.useMaterializedViews}
 * then decides whether the incoming query's {@link RelNode} can be answered from the rollup table
 * instead.
 *
 * <p><b>Execution doesn't reuse Calcite's own embedded-execution path</b> ({@code
 * Planner#transform}/{@code RelRunner}, what {@code FederationStage} uses) — found live that a
 * successful substitution's rewritten {@link RelNode} is spliced from pieces built across the
 * close()+reset() cycles above, each of which gets its own fresh {@code RelOptCluster} despite being
 * the same {@link Planner} object; {@code VolcanoPlanner}'s optimize step (unlike {@code
 * useMaterializedViews}' pure structural match) asserts every node it registers belongs to its own
 * cluster and rejects the spliced tree outright. Instead, a successful rewrite is converted back
 * into real SQL text ({@code org.apache.calcite.rel.rel2sql.RelToSqlConverter} — Calcite is used
 * here purely for the structural rewrite <em>decision</em>, the same correctness guarantee either
 * way) and executed through an ordinary JDBC connection, exactly how every other query in this
 * codebase already runs. Verified live against a real 150k-row Postgres table (see
 * {@code ARCHITECTURE.md}'s rollup entry) across three shapes — an exact-grouping match, a
 * coarser-grouping-plus-{@code WHERE} match, and a same-rollup-different-coarser-grouping match, the
 * last two exercising real further-aggregation, not just a literal restatement of the rollup's own
 * defining query — each cross-checked row-for-row against the same query run directly on the source
 * table.
 *
 * <p><b>Fails open at every step</b> — a regex pre-filter miss, a stale rollup, a parse/validate
 * failure, or "no substitution found" (the overwhelmingly common case: most aggregate queries won't
 * match a given rollup's exact grouping) all fall through to {@code next.proceed(statement)}
 * unchanged. This stage can never turn a working query into a failing one, only sometimes into a
 * faster one — same philosophy as {@code SqlSchemaFidelityGuard} and {@code CacheStage}.
 *
 * <p><b>Access control</b>: positioned in {@code GatewayComponents}' pipeline after {@code
 * AccessControlStage}/{@code FirewallStage}, so any row-filter predicate a policy injected into
 * {@code sqlText} is already there by the time this stage sees the query. That's what makes the
 * app-level row-filter-rewrite enforcement path safe here without any extra guard this class has to
 * hand-write: Calcite's substitution only succeeds when the rollup's own {@code GROUP BY} columns
 * cover every column the query's predicate touches — a filter on a column absent from the rollup's
 * grouping simply can't be expressed against it, so {@code useMaterializedViews} correctly returns
 * no rewrite, and the query falls through to the real (filtered) table. <b>This does not extend to
 * native RLS/VPD pass-through</b> (backend-session-context row security, which never touches {@code
 * sqlText} at all) — a rollup's defining query runs with the refresh job's own backend credentials,
 * not any caller's, so the materialized rollup table is a full, unfiltered aggregate. Rollups on a
 * table that also relies on native RLS/VPD as its <em>only</em> enforcement are not safe to define in
 * this phase — documented plainly here and in {@code ARCHITECTURE.md} rather than silently allowed.
 */
public final class RollupStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RollupStage.class);

    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOOKS_AGGREGATE = Pattern.compile(
            "\\bgroup\\s+by\\b|\\b(sum|count|avg|min|max)\\s*\\(", Pattern.CASE_INSENSITIVE);

    private final RollupStore store;
    private final BackendRegistry backendRegistry;

    public RollupStage(RollupStore store, BackendRegistry backendRegistry) {
        this.store = store;
        this.backendRegistry = backendRegistry;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (!SELECT_PREFIX.matcher(sql).find() || !LOOKS_AGGREGATE.matcher(sql).find()) {
            return next.proceed(statement);
        }
        RollupDefinition candidate = store.matchingDefinition(sql);
        if (candidate == null || !store.isFresh(candidate)) {
            return next.proceed(statement);
        }
        ExecutionResult accelerated;
        try {
            accelerated = tryAccelerate(candidate, statement);
        } catch (RuntimeException | SQLException e) {
            log.debug("rollup: substitution attempt failed for \"{}\", falling through to the real table ({})",
                    candidate.name(), e.toString());
            accelerated = null;
        }
        return accelerated != null ? accelerated : next.proceed(statement);
    }

    /** @return the accelerated result, or {@code null} if no substitution was found (the caller falls through to {@code next.proceed} either way — {@code null} here is not an error, it's the ordinary "this rollup doesn't cover this query" outcome). */
    private ExecutionResult tryAccelerate(RollupDefinition def, Statement statement) throws SQLException {
        BackendTarget target = backendRegistry.get(def.backendName());
        if (target == null) {
            return null; // config drift (rollup references a backend no longer registered) -- fall through, don't fail the query
        }
        List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
        Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:lex=JAVA;caseSensitive=false");
        try {
            CalciteConnection cc = calciteConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();
            // PolyWire is Postgres-only (unlike Polywire, which resolved this per-backend via
            // FederationStage.driverClassNameFor across many dialects) -- always the pgJDBC driver.
            DataSource dataSource = JdbcSchema.dataSource(
                    target.jdbcUrl(), "org.postgresql.Driver", target.user(), target.password());
            SqlDialect dialect = JdbcSchema.createDialect(dataSource);
            org.apache.calcite.linq4j.tree.Expression expression =
                    org.apache.calcite.schema.Schemas.subSchemaExpression(rootSchema, def.backendName(), JdbcSchema.class);
            JdbcConvention convention = JdbcConvention.of(dialect, expression, def.backendName());
            // schema=null (unlike FederationStage's federated mount, which deliberately names it
            // after the backend registry entry for cross-backend addressing -- see that class's
            // "naming convention this depends on" javadoc): this is always a single backend, so
            // there's no need to force a fictitious schema label. null lets JdbcSchema resolve
            // tables through the JDBC connection's own default schema/search_path -- exactly how
            // this same backend's ordinary (non-rollup) queries already resolve unqualified table
            // names elsewhere in this codebase, and how definingSql()/the incoming query's SQL text
            // itself is written (bare or admin-qualified, whichever the real database expects).
            JdbcSchema jdbcSchema = new JdbcSchema(dataSource, dialect, convention, null, null);
            rootSchema.add(def.backendName(), jdbcSchema);
            rules.addAll(JdbcRules.rules(convention));

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema.getSubSchema(def.backendName()))
                    .parserConfig(org.apache.calcite.sql.parser.SqlParser.config()
                            .withCaseSensitive(false)
                            .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED))
                    .programs(Programs.ofRules(rules))
                    .build();
            // All three conversions (the rollup's defining query, a plain scan of the rollup table,
            // and the incoming query) MUST share one Planner/RelOptCluster -- found live: a
            // materialization built from one Planner's RelNodes, substituted into another Planner's
            // query via RelOptMaterializations.useMaterializedViews (a pure structural match, so
            // that part succeeds regardless of cluster), then fails at planner.transform() with
            // "Relational expression ... belongs to a different planner than is currently being
            // used" -- VolcanoPlanner's optimize step, unlike substitution matching, does require
            // every node in the tree it's registering to belong to its own cluster. Reusing one
            // Planner via reset() between statements (Planner#reset's own documented purpose) is
            // therefore not an optional simplification, it's required for this to work at all.
            //
            // reset()'s real Calcite 1.42 behavior (found live, the hard way -- contrary to what its
            // javadoc alone suggests): PlannerImpl.reset() itself requires the planner to already be
            // *closed* first (IllegalArgumentException: "cannot move to STATE_0_CLOSED from
            // STATE_5_CONVERTED" when reset() is called directly on a live, just-converted planner).
            // The real reuse lifecycle for one Planner across multiple statements is close() *then*
            // reset() together, not reset() alone -- verified live against a real 150k-row table
            // (see ARCHITECTURE.md's rollup entry).
            Planner planner = Frameworks.getPlanner(config);
            RelNode materializationQueryRel = convert(planner, def.definingSql());
            planner.close();
            planner.reset();
            RelNode materializationTableRel = convert(planner, "SELECT * FROM " + def.rollupTableName());
            planner.close();
            planner.reset();
            RelNode incomingQueryRel = convert(planner, statement.sqlText());

            RelOptMaterialization materialization = new RelOptMaterialization(materializationTableRel,
                    materializationQueryRel, null, List.of(def.backendName(), def.rollupTableName()));
            List<Pair<RelNode, List<RelOptMaterialization>>> rewrites =
                    RelOptMaterializations.useMaterializedViews(incomingQueryRel, List.of(materialization));
            if (rewrites.isEmpty()) {
                return null; // no substitution found -- ordinary, expected outcome for most queries
            }
            RelNode rewritten = rewrites.get(0).left;
            // Not executed via planner.transform()/RelRunner (Calcite's own embedded-execution
            // path, and what FederationStage uses for its single-cluster case): the rewritten
            // RelNode is spliced together from RelNodes built across the earlier close()+reset()
            // cycles above, each of which -- found live -- gets its own fresh internal RelOptCluster
            // despite being the same Planner object. VolcanoPlanner's transform() step (unlike
            // useMaterializedViews' pure structural match) asserts every node it registers belongs
            // to its own cluster, and rejects this tree outright ("belongs to a different planner
            // than is currently being used"). Instead: convert the rewritten RelNode back into real
            // SQL text (RelToSqlConverter -- Calcite is used here purely for its structural rewrite
            // *decision*, the same correctness guarantee either way) and execute that text through
            // an ordinary JDBC connection, exactly how every other query in this codebase already
            // runs -- sidesteps the whole planner-lifecycle/cluster-consistency problem entirely.
            // Verified live against a real 150k-row table (see ARCHITECTURE.md's rollup entry).
            org.apache.calcite.rel.rel2sql.RelToSqlConverter toSql =
                    new org.apache.calcite.rel.rel2sql.RelToSqlConverter(dialect);
            SqlNode rewrittenSql = toSql.visitRoot(rewritten).asStatement();
            String rewrittenSqlText = rewrittenSql.toSqlString(dialect).getSql();
            try (Connection backendConnection = target.open();
                    PreparedStatement ps = backendConnection.prepareStatement(rewrittenSqlText)) {
                ExecutionResult result = JdbcBackendExecutor.executeOnPreparedStatement(ps, statement.bindParams());
                store.recordHit(def.name());
                log.info("rollup: \"{}\" accelerated a query via {} ({})", def.name(), def.rollupTableName(), rewrittenSqlText);
                return result;
            }
        } finally {
            calciteConnection.close();
        }
    }

    private static RelNode convert(Planner planner, String sql) throws SQLException {
        try {
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);
            RelRoot root = planner.rel(validated);
            return root.rel;
        } catch (Exception e) {
            throw new SQLException("rollup: failed to plan \"" + sql + "\": " + e.getMessage(), e);
        }
    }
}
