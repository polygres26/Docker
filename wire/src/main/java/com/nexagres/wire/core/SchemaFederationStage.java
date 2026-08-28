package com.nexagres.wire.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real cross-backend {@code JOIN} execution for PolyWire's OWN vertical/functional sharding --
 * {@link RouterStage.SchemaRule}/{@link RouterStage.PredicateRule} already route a whole table's
 * worth of traffic to a single named backend by schema prefix (e.g. every {@code orders_db.orders}
 * query goes to backend {@code "orders"}, every {@code customers_db.customers} query goes to
 * backend {@code "customers"}). Nothing before this stage ever handled a query that references TWO
 * different such backends in one statement -- {@code RouterStage.resolveBackend} only ever returns
 * ONE target name, so a real {@code orders_db.orders JOIN customers_db.customers} query would
 * previously have picked whichever rule matched first and sent the WHOLE query (both table
 * references) to that one backend alone, failing outright once it tried to resolve a table that
 * backend doesn't have.
 *
 * <p>This is PolyWire's own analog of the sibling Omnigate project's {@code FederationStage} --
 * genuinely the SAME problem shape (heterogeneous placement: each backend holds a different,
 * complete table, not a row-partitioned slice of the same one), unlike {@link ShardJoinExecutor}'s
 * homogeneous-shard UNION-per-table problem, which needs no such reconciliation: each backend named
 * in a matching {@link RouterStage.SchemaRule} is mounted directly as its own Calcite schema, and
 * Calcite's real planner does the join, pushing predicates/columns down into each backend's own
 * {@code JdbcConvention} -- not a row-pull-and-join-in-Java.
 *
 * <p>Runs BEFORE {@link RouterStage} in the pipeline (see {@code Main}'s stage assembly) --
 * federating across backends has to happen before routing ever narrows a statement down to one
 * target. A statement referencing 0 or 1 configured schema-rule backends is untouched here and
 * falls straight through to {@link RouterStage}'s own single-backend routing, unchanged.
 *
 * <p><b>Real row-count statistics, real plan history, both optional</b> -- see {@link
 * ShardJoinExecutor}'s own matching javadoc section for the full reasoning (shared verbatim by both
 * classes): when {@code statisticsStore} is non-null, every mounted backend's tables are wrapped in
 * {@link StatisticsAwareSchema}; when {@code planStore} is non-null, a real {@code EXPLAIN PLAN FOR}
 * plus timing/row-count/success is captured into {@link SqlPlanStore}, ported from the sibling
 * Omnigate project's own {@code FederationStage}.
 *
 * <p><b>Real semi-join pushdown</b> for exactly 2 federated backends -- see {@link
 * ShardJoinExecutor}'s own matching javadoc section and {@link SemiJoinPushdown}'s own javadoc for
 * the full mechanism, shared by both classes.
 *
 * <p><b>Deliberately narrow scope, still</b>: a fresh Calcite connection built per statement (no
 * connection cache), no native RLS/VPD session pass-through for the federated connection (this
 * statement's own {@code AccessControlStage} row-filter/column-mask SQL rewriting, already run
 * earlier in the pipeline, is the only enforcement for now), and no schema auto-discovery -- every
 * backend mounts exactly the one schema its own {@link RouterStage.SchemaRule} names. A federated
 * backend can now be Postgres OR Oracle (real cross-engine JOIN federation, e.g. a Postgres
 * {@code customers} table joined against an Oracle {@code orders} table, the same shape Omnigate's
 * own cross-dialect Oracle+Postgres federation proved out) -- see {@link BackendDriverRegistry}
 * for the currently-supported engine list; an unrecognized URL prefix is a real, clear error, not
 * a silent misconnection.
 */
public final class SchemaFederationStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(SchemaFederationStage.class);

    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);

    private final List<RouterStage.SchemaRule> schemaRules;
    private final BackendRegistry backendRegistry;
    private final StatisticsStore statisticsStore;
    private final SqlPlanStore planStore;

    public SchemaFederationStage(List<RouterStage.SchemaRule> schemaRules, BackendRegistry backendRegistry) {
        this(schemaRules, backendRegistry, null, null);
    }

    public SchemaFederationStage(List<RouterStage.SchemaRule> schemaRules, BackendRegistry backendRegistry,
            StatisticsStore statisticsStore, SqlPlanStore planStore) {
        this.schemaRules = List.copyOf(schemaRules);
        this.backendRegistry = backendRegistry;
        this.statisticsStore = statisticsStore;
        this.planStore = planStore;
    }

    /** {@code null} when fewer than 2 schema rules exist -- federation across a single named
     * backend is meaningless, so there's nothing for this stage to ever do (same "absent means the
     * feature doesn't exist" shape as {@code CacheStage.fromConfigOrNull}); {@code Main} skips
     * adding it entirely in that case. {@code statisticsStore}/{@code planStore} may themselves be
     * {@code null} (neither configured). */
    public static SchemaFederationStage fromConfigOrNull(RouterStage routerStage, BackendRegistry backendRegistry,
            StatisticsStore statisticsStore, SqlPlanStore planStore) {
        if (routerStage.schemaRules().size() < 2) {
            return null;
        }
        return new SchemaFederationStage(routerStage.schemaRules(), backendRegistry, statisticsStore, planStore);
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (!SELECT_PREFIX.matcher(sql).find()) {
            return next.proceed(statement);
        }
        // Keyed by schema NAME, not backend name -- a query's own SQL text references the schema
        // it was written against (e.g. "orders_db.orders"), which is what has to be mounted at
        // that exact name in the federated Calcite connection below; the backend name is only an
        // internal routing detail. Real bug, found live: mounting by backend name instead threw a
        // real Calcite "Object 'orders_db' not found" error the moment two schema rules routed
        // through DIFFERENT-looking names (schema "orders_db" -> backend "orders").
        java.util.Map<String, String> referencedSchemas = new java.util.TreeMap<>();
        for (RouterStage.SchemaRule rule : schemaRules) {
            if (rule.schemaPattern().matcher(sql).find()) {
                referencedSchemas.put(rule.schemaName(), rule.backendName());
            }
        }
        if (referencedSchemas.size() < 2) {
            return next.proceed(statement);
        }
        log.info("schema federation: statement references {} schema(s) {} -- executing via a federated "
                + "Calcite connection instead of routing to one", referencedSchemas.size(), referencedSchemas.keySet());
        return execute(referencedSchemas, statement);
    }

    private ExecutionResult execute(Map<String, String> schemaNameToBackendName, Statement statement) throws SQLException {
        // Real bug, found live building ShardJoinExecutor (this class's sibling): Calcite's own SQL
        // parser rejects a trailing ';' outright. Same fix, hit independently here since this class
        // shares no code with ShardJoinExecutor -- see that class's own javadoc for the original
        // finding (itself matching a gap Omnigate's FederationStage already had to work around).
        String sql = stripTrailingSemicolon(statement.sqlText());
        // Kept around ONLY for plan-history display -- semi-join pushdown below may reassign `sql`
        // itself to an internally-rewritten form; see ShardJoinExecutor's own matching comment.
        String originalSql = sql;
        // See ShardJoinExecutor's own matching comment: dropping lex=JAVA keeps this connection's
        // default identifier quoting (double-quote) consistent with the Frameworks Planner's own
        // default, so EXPLAIN PLAN FOR (run through this raw connection, not the Planner) parses
        // correctly.
        Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false");
        List<Connection> statsConnections = new ArrayList<>();
        Map<String, Connection> schemaToStatsConnection = new java.util.LinkedHashMap<>();
        try {
            CalciteConnection cc = calciteConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();
            List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
            Map<String, LeafScanProfiler.MountedBackend> mountToBackend = new java.util.LinkedHashMap<>();
            SqlDialect dialect = null;
            for (Map.Entry<String, String> entry : schemaNameToBackendName.entrySet()) {
                String schemaName = entry.getKey();
                String backendName = entry.getValue();
                BackendTarget target = backendRegistry.resolveForRouting(backendName);
                if (target == null) {
                    throw ErrorCatalog.sqlException("ERR_ROUTER_UNKNOWN_BACKEND", backendName);
                }
                mountToBackend.put(schemaName, new LeafScanProfiler.MountedBackend(target, backendName));
                // Real driver-class lookup -- a federated backend can now genuinely be a
                // non-Postgres engine (Oracle today; see BackendDriverRegistry's own javadoc),
                // unlike this class's original Postgres-only assumption.
                String driverClassName = BackendDriverRegistry.driverClassNameFor(target.jdbcUrl());
                if (driverClassName == null) {
                    throw ErrorCatalog.sqlException("ERR_UNSUPPORTED_BACKEND_ENGINE", backendName, target.jdbcUrl());
                }
                DataSource dataSource = JdbcSchema.dataSource(
                        target.jdbcUrl(), driverClassName, target.user(), target.password());
                dialect = JdbcSchema.createDialect(dataSource);
                org.apache.calcite.linq4j.tree.Expression expression =
                        org.apache.calcite.schema.Schemas.subSchemaExpression(rootSchema, schemaName, JdbcSchema.class);
                JdbcConvention convention = JdbcConvention.of(dialect, expression, schemaName);
                // Mounted (both the rootSchema.add key AND JdbcSchema's own 4th ctor arg) under
                // schemaName -- the name the client's query actually references -- not backendName,
                // which is only ever an internal routing detail (see this method's own caller).
                // schemaName as the 4th arg (not null): a JDBC connection's own default visible
                // schema depends on the connecting user/search_path, which isn't necessarily the
                // schema this backend's tables actually live in -- the same real bug
                // ShardJoinExecutor's own javadoc notes Omnigate found live against Postgres
                // specifically (default search_path is "$user,public", not this backend's own
                // schema).
                String realSchemaName = BackendDriverRegistry.realCatalogSchemaName(target.jdbcUrl(), schemaName);
                JdbcSchema jdbcSchema = new JdbcSchema(dataSource, dialect, convention, null, realSchemaName);
                if (statisticsStore != null) {
                    Connection statsConnection = target.open();
                    statsConnections.add(statsConnection);
                    schemaToStatsConnection.put(schemaName, statsConnection);
                    rootSchema.add(schemaName, new StatisticsAwareSchema(
                            jdbcSchema, statsConnection, schemaName, backendName + "." + schemaName, statisticsStore));
                } else {
                    rootSchema.add(schemaName, jdbcSchema);
                }
                rules.addAll(JdbcRules.rules(convention));
            }

            // Real semi-join pushdown -- see SemiJoinPushdown's own javadoc and ShardJoinExecutor's
            // matching integration for the full reasoning. Scoped to exactly 2 federated backends
            // here (today's common, demoed shape); 3+ backends in one statement falls through
            // unfiltered, same as before this existed.
            if (statisticsStore != null && schemaNameToBackendName.size() == 2) {
                sql = applySemiJoinPushdown(sql, schemaNameToBackendName, schemaToStatsConnection, statisticsStore, calciteConnection);
            }

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema)
                    .parserConfig(org.apache.calcite.sql.parser.SqlParser.config()
                            .withCaseSensitive(false)
                            .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED))
                    .programs(Programs.ofRules(rules))
                    .build();
            Planner planner = Frameworks.getPlanner(config);
            RelNode optimized;
            try {
                SqlNode parsed = planner.parse(sql);
                SqlNode validated = planner.validate(parsed);
                RelRoot relRoot = planner.rel(validated);
                optimized = planner.transform(0,
                        relRoot.rel.getTraitSet().replace(EnumerableConvention.INSTANCE), relRoot.rel);
            } catch (Exception e) {
                throw ErrorCatalog.sqlExceptionWithCause("ERR_SHARD_JOIN_PLAN_FAILED", e, originalSql, e.getMessage());
            }
            String backendsLabel = String.join(",", schemaNameToBackendName.values());
            // sql here (not originalSql) deliberately -- EXPLAIN PLAN FOR has to reflect the query
            // that's actually about to run, semi-join filter included when pushdown applied.
            String planText = planStore == null ? null : capturePlanTextOrNull(calciteConnection, sql, backendsLabel);
            // See ShardJoinExecutor's own matching comment: a real, separate re-execution of each
            // backend's own leaf scan, skipped entirely (empty list) for a parameterized statement
            // or when planStore itself isn't configured.
            List<SqlPlanStore.LeafScanMetric> leafScans = planStore == null ? List.of()
                    : LeafScanProfiler.measure(optimized, dialect, mountToBackend, !statement.bindParams().isEmpty());
            long startNanos = System.nanoTime();
            try (PreparedStatement ps = calciteConnection.unwrap(org.apache.calcite.tools.RelRunner.class)
                    .prepareStatement(optimized)) {
                ExecutionResult result = JdbcBackendExecutor.executeOnPreparedStatement(ps, statement.bindParams());
                if (planStore != null) {
                    long rowCount = result.isQuery() ? result.rows().size() : result.updateCount();
                    planStore.record(backendsLabel, originalSql, planText, elapsedMillisSince(startNanos), rowCount, true, null, leafScans);
                }
                return result;
            } catch (SQLException e) {
                if (planStore != null) {
                    planStore.record(backendsLabel, originalSql, planText, elapsedMillisSince(startNanos), 0, false, e.getMessage(), leafScans);
                }
                throw e;
            }
        } finally {
            for (Connection statsConnection : statsConnections) {
                try {
                    statsConnection.close();
                } catch (SQLException ignoredOnCleanup) {
                    // best-effort -- doesn't affect the real query, which already ran (or failed) above
                }
            }
            calciteConnection.close();
        }
    }

    private static long elapsedMillisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /** As {@link ShardJoinExecutor}'s own matching method -- see its javadoc for the full reasoning.
     * This class's own version has no per-table UNION source to build (each mounted backend already
     * IS the real source), so every candidate reference's own SQL source is just itself -- the
     * identity mapping {@link SemiJoinPushdown#detectEqui} still needs to resolve the {@code ON}
     * clause. Collects EVERY distinct {@code schema.table} reference across the two federated
     * schemas (not just one per schema) -- {@link SemiJoinPushdown#detectEqui} itself requires
     * exactly 2 total candidates, so a query touching more than one table per schema safely bails
     * out to no pushdown, same as any other ambiguous case. */
    private static String applySemiJoinPushdown(String sql, Map<String, String> schemaNameToBackendName,
            Map<String, Connection> schemaToStatsConnection, StatisticsStore statisticsStore, Connection calciteConnection) {
        Map<String, String> refToSource = new java.util.LinkedHashMap<>();
        Map<String, Long> refToRowCount = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, String> entry : schemaNameToBackendName.entrySet()) {
            String schemaName = entry.getKey();
            String backendName = entry.getValue();
            Connection statsConnection = schemaToStatsConnection.get(schemaName);
            Pattern tableRef = Pattern.compile("\\b" + Pattern.quote(schemaName) + "\\.(\\w+)", Pattern.CASE_INSENSITIVE);
            Matcher m = tableRef.matcher(sql);
            while (m.find()) {
                String table = m.group(1);
                String ref = schemaName + "." + table;
                // "SELECT * FROM ref", not the bare ref -- SemiJoinPushdown always wraps a
                // candidate's own source as "FROM (<source>)", which needs a real query expression
                // on the inside, not a bare table name (real bug, found live: a plain "(schema.table)"
                // isn't a valid derived table -- Calcite's own parser rejects it outright).
                refToSource.put(ref, "SELECT * FROM " + ref);
                Long rowCount = statsConnection == null ? null
                        : statisticsStore.rowCount(statsConnection, backendName + "." + schemaName + "." + table, schemaName, table);
                refToRowCount.put(ref, rowCount);
            }
        }
        SemiJoinPushdown.Equi equi = SemiJoinPushdown.detectEqui(sql, refToSource);
        if (equi == null) {
            return sql;
        }
        Long leftCount = refToRowCount.get(equi.leftRef());
        Long rightCount = refToRowCount.get(equi.rightRef());
        if (leftCount == null || rightCount == null) {
            log.debug("schema federation: semi-join pushdown skipped -- no real row-count estimate for both "
                    + "\"{}\" and \"{}\"", equi.leftRef(), equi.rightRef());
            return sql;
        }
        boolean leftIsBuild = leftCount <= rightCount;
        String buildRef = leftIsBuild ? equi.leftRef() : equi.rightRef();
        String buildCol = leftIsBuild ? equi.leftColumn() : equi.rightColumn();
        String probeRef = leftIsBuild ? equi.rightRef() : equi.leftRef();
        String probeCol = leftIsBuild ? equi.rightColumn() : equi.leftColumn();
        try {
            List<Object> keys = SemiJoinPushdown.collectDistinctKeys(calciteConnection, refToSource.get(buildRef),
                    buildCol, SemiJoinPushdown.maxKeysFromEnvOrDefault());
            if (keys == null) {
                return sql;
            }
            String filtered = SemiJoinPushdown.buildFilteredSource(refToSource.get(probeRef), probeCol, keys);
            log.info("schema federation: semi-join pushdown -- build side \"{}\" ({} distinct key(s) from ~{} "
                    + "estimated row(s)) filters probe side \"{}\" (~{} estimated row(s)) before its own join",
                    buildRef, keys.size(), leftIsBuild ? leftCount : rightCount, probeRef,
                    leftIsBuild ? rightCount : leftCount);
            return SemiJoinPushdown.substituteRef(sql, probeRef, filtered);
        } catch (SQLException e) {
            log.warn("schema federation: semi-join pushdown's own build-side key collection failed -- skipping, "
                    + "real query is unaffected ({})", e.toString());
            return sql;
        }
    }

    /** As {@link ShardJoinExecutor}'s own matching method -- see its javadoc. */
    private static String capturePlanTextOrNull(Connection calciteConnection, String sql, String backendsLabel) {
        try (java.sql.Statement explainStatement = calciteConnection.createStatement();
                java.sql.ResultSet rs = explainStatement.executeQuery("EXPLAIN PLAN FOR " + sql)) {
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                if (plan.length() > 0) {
                    plan.append('\n');
                }
                plan.append(rs.getString(1));
            }
            return plan.toString();
        } catch (SQLException e) {
            log.warn("schema federation: EXPLAIN PLAN FOR failed for backends {} -- plan history will show no "
                    + "plan text for this entry, real query is unaffected ({})", backendsLabel, e.toString());
            return null;
        }
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.stripTrailing();
        return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : sql;
    }
}
