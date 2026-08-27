package com.polygres.wire.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Real cross-shard {@code JOIN} execution, via a genuine Calcite-federated connection -- the
 * correctness gap {@link RoutingBackendExecutor#executeScatterGather} otherwise has: PolyWire's
 * shards are homogeneous horizontal partitions (the SAME logical table, e.g. {@code shard.orders},
 * split by row across every shard in {@code registry.shardGroup()}), not the heterogeneous
 * per-backend placement a general federation engine (Calcite's own {@code JdbcSchema}, or the
 * sibling Omnigate project's {@code FederationStage}) is built for. Naively mounting each shard as
 * its own Calcite schema would see {@code shard1.orders}/{@code shard2.orders}/{@code shard3.orders}
 * as three unrelated tables, not one partitioned {@code orders} -- so this class mounts one table
 * per DISTINCT {@code schema.table} reference in the query as a real {@code UNION ALL} across every
 * shard's own copy, then hands the resulting query (with those references now pointing at the
 * unioned view instead of a single shard) to Calcite's real planner, which pushes predicates/columns
 * down into each shard's own {@code JdbcConvention} and computes the join itself -- not a row-pull-
 * and-join-in-Java, and not the scatter-gather path's "broadcast the identical SQL to every shard,
 * concatenate/merge" shape, which is silently wrong the moment a matching row pair spans two shards
 * (never found on either shard alone, and no error raised -- see this class's own introduction).
 *
 * <p><b>Real row-count statistics, real plan history, both optional</b>: when {@code
 * statisticsStore} is non-null, every mounted shard table is wrapped in {@link
 * StatisticsAwareSchema} so Calcite's own join-order cost model sees a real {@code
 * pg_class.reltuples}-based row count instead of {@code Statistics.UNKNOWN} -- {@link
 * StatisticsScheduler} keeps it warm in the background; a cold cache still gets a real number via
 * an on-demand probe. When {@code planStore} is non-null, this method captures a real
 * {@code EXPLAIN PLAN FOR} of the federated query plus its own timing/row-count/success into
 * {@link SqlPlanStore} -- ported from the sibling Omnigate project's own {@code FederationStage}.
 * Both degrade to "not done" (not an error) when their store is {@code null} -- not configured.
 *
 * <p><b>Deliberately narrow scope, still</b>: single Calcite connection built fresh per statement
 * (no connection cache), no bloom-filter/semi-join pushdown to cut what's shipped between shards
 * before the join (a real, valuable follow-up, not implemented here), and Calcite's own default
 * planner/rule set (the same one {@link RollupStage} already relies on for its own real, tested
 * Calcite integration) -- no column NDV/selectivity statistics or Omnigate's own opt-in embedded-
 * planner cost path, row counts alone are already a real improvement over {@code Statistics.UNKNOWN}
 * on their own. A
 * shard-qualified table referenced without an explicit alias where SQL requires one for a derived
 * table (this class turns {@code schema.table} into a parenthesized {@code UNION ALL} subquery,
 * which -- like any derived table -- needs an alias) surfaces as a real, clear Calcite parse error
 * via {@code ERR_SHARD_JOIN_PLAN_FAILED}, not a silent wrong answer.
 */
final class ShardJoinExecutor {

    private static final Logger log = LoggerFactory.getLogger(ShardJoinExecutor.class);

    private static final Pattern JOIN_KEYWORD = Pattern.compile("\\bjoin\\b", Pattern.CASE_INSENSITIVE);

    private ShardJoinExecutor() {
    }

    /** @return the FIRST configured shard schema name (see {@link RouterStage.ShardRule}) that
     *     both appears in {@code sql} AND is followed, somewhere in the statement, by a JOIN
     *     keyword -- {@code null} if there's no JOIN at all, or the query only references a shard
     *     schema without joining anything (that's the existing scatter-gather path's own job,
     *     completely unaffected). Only ever returns one schema: today's {@code ShardRule} config
     *     shape is "one literal schema name per shard-partitioned table family" -- a query joining
     *     two DIFFERENT configured shard schemas together is a real, further case this v1 doesn't
     *     attempt (see this class's own javadoc on scope) and falls through to the existing
     *     scatter-gather path unchanged, same as it does today. */
    static String matchedShardSchema(List<RouterStage.ShardRule> shardRules, String sql) {
        if (shardRules.isEmpty() || !JOIN_KEYWORD.matcher(sql).find()) {
            return null;
        }
        for (RouterStage.ShardRule rule : shardRules) {
            if (rule.schemaPattern().matcher(sql).find()) {
                return rule.schemaName();
            }
        }
        return null;
    }

    static ExecutionResult execute(BackendRegistry registry, List<String> shardNames, String schemaName,
            Statement statement, StatisticsStore statisticsStore, SqlPlanStore planStore) throws SQLException {
        // Real bug, found live: Calcite's own SQL parser rejects a trailing ';' outright ("parse
        // failed: Encountered \";\" ...") -- the exact same gap Omnigate's FederationStage already
        // documented and worked around (its own stripTrailingSemicolon), hit here independently
        // since this class doesn't share code with it.
        String sql = stripTrailingSemicolon(statement.sqlText());
        Set<String> tables = distinctShardTables(schemaName, sql);
        // Real bug, found live via the plan-history feature itself: "lex=JAVA" quotes identifiers
        // with backticks, not the double-quotes this class's own UNION-rewrite generates
        // (`"__polywire_shardN"`) -- the Frameworks Planner above still parsed fine (its own
        // parserConfig defaults to double-quote regardless of this connection's lex setting), but
        // EXPLAIN PLAN FOR runs through THIS raw connection's own default parser, which choked on
        // the stray `"` under lex=JAVA. Dropping lex=JAVA (default lex already double-quotes,
        // matching both this class's own rewrite and the Planner's default) fixes it without
        // touching case-sensitivity behavior, which caseSensitive=false already controls directly.
        Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false");
        // One real connection per shard, held open only long enough to mount that shard's schema
        // (and, when statisticsStore != null, to probe its own real row counts) -- closed in the
        // same finally block as calciteConnection, never reused across statements.
        List<Connection> statsConnections = new ArrayList<>();
        try {
            CalciteConnection cc = calciteConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();
            List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
            List<String> shardMountNames = new ArrayList<>();
            Map<String, LeafScanProfiler.MountedBackend> mountToBackend = new LinkedHashMap<>();
            SqlDialect dialect = null;
            for (int i = 0; i < shardNames.size(); i++) {
                String shardName = shardNames.get(i);
                BackendTarget target = registry.resolveForRouting(shardName);
                if (target == null) {
                    throw ErrorCatalog.sqlException("ERR_SHARD_UNKNOWN_BACKEND", shardName);
                }
                String mountName = "__polywire_shard" + i;
                shardMountNames.add(mountName);
                mountToBackend.put(mountName, new LeafScanProfiler.MountedBackend(target, shardName));
                // Every PolyWire shard is real Postgres (unlike Omnigate's cross-dialect
                // federation, which needs a real dialect->driver-class lookup) -- always
                // "org.postgresql.Driver", same as RollupStage's own single-backend mount.
                DataSource dataSource = JdbcSchema.dataSource(
                        target.jdbcUrl(), "org.postgresql.Driver", target.user(), target.password());
                dialect = JdbcSchema.createDialect(dataSource);
                org.apache.calcite.linq4j.tree.Expression expression =
                        org.apache.calcite.schema.Schemas.subSchemaExpression(rootSchema, mountName, JdbcSchema.class);
                JdbcConvention convention = JdbcConvention.of(dialect, expression, mountName);
                JdbcSchema jdbcSchema = new JdbcSchema(dataSource, dialect, convention, null, schemaName);
                if (statisticsStore != null) {
                    Connection statsConnection = target.open();
                    statsConnections.add(statsConnection);
                    rootSchema.add(mountName, new StatisticsAwareSchema(
                            jdbcSchema, statsConnection, schemaName, shardName + "." + schemaName, statisticsStore));
                } else {
                    rootSchema.add(mountName, jdbcSchema);
                }
                rules.addAll(JdbcRules.rules(convention));
            }

            String rewrittenSql = sql;
            for (String table : tables) {
                StringBuilder union = new StringBuilder("(");
                for (int i = 0; i < shardMountNames.size(); i++) {
                    if (i > 0) {
                        union.append(" UNION ALL ");
                    }
                    union.append("SELECT * FROM \"").append(shardMountNames.get(i)).append("\".").append(table);
                }
                union.append(')');
                // Replaces every occurrence of "schemaName.table" (a real word-boundary match, not
                // a naive substring replace -- won't touch "shard.orders_archive" while replacing
                // "shard.orders"), leaving anything the client wrote immediately after it (an
                // explicit alias, or nothing) completely untouched -- see this class's own javadoc
                // on why a missing alias surfaces as a real Calcite parse error, not silently.
                rewrittenSql = Pattern.compile("(?i)\\b" + Pattern.quote(schemaName) + "\\." + Pattern.quote(table) + "\\b")
                        .matcher(rewrittenSql)
                        .replaceAll(Matcher.quoteReplacement(union.toString()));
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
                SqlNode parsed = planner.parse(rewrittenSql);
                SqlNode validated = planner.validate(parsed);
                RelRoot relRoot = planner.rel(validated);
                optimized = planner.transform(0,
                        relRoot.rel.getTraitSet().replace(EnumerableConvention.INSTANCE), relRoot.rel);
            } catch (Exception e) {
                throw ErrorCatalog.sqlExceptionWithCause("ERR_SHARD_JOIN_PLAN_FAILED", e, sql, e.getMessage());
            }
            log.info("cross-shard join: \"{}\" -> federated across {} shard(s) for schema \"{}\"",
                    sql, shardNames.size(), schemaName);
            String backendsLabel = String.join(",", shardNames);
            String planText = planStore == null ? null : capturePlanTextOrNull(calciteConnection, rewrittenSql, backendsLabel);
            // Real, measured per-shard row count/timing -- see LeafScanProfiler's own javadoc for
            // why this is a genuinely separate re-execution of each shard's own leaf scan, not
            // something extracted from the real join execution below. Skipped (empty list) when
            // planStore itself isn't configured -- no point paying for extra per-shard round trips
            // nothing will ever read.
            List<SqlPlanStore.LeafScanMetric> leafScans = planStore == null ? List.of()
                    : LeafScanProfiler.measure(optimized, dialect, mountToBackend, !statement.bindParams().isEmpty());
            long startNanos = System.nanoTime();
            try (PreparedStatement ps = calciteConnection.unwrap(org.apache.calcite.tools.RelRunner.class)
                    .prepareStatement(optimized)) {
                ExecutionResult result = JdbcBackendExecutor.executeOnPreparedStatement(ps, statement.bindParams());
                if (planStore != null) {
                    long rowCount = result.isQuery() ? result.rows().size() : result.updateCount();
                    planStore.record(backendsLabel, sql, planText, elapsedMillisSince(startNanos), rowCount, true, null, leafScans);
                }
                return result;
            } catch (SQLException e) {
                if (planStore != null) {
                    planStore.record(backendsLabel, sql, planText, elapsedMillisSince(startNanos), 0, false, e.getMessage(), leafScans);
                }
                throw e;
            }
        } finally {
            for (Connection statsConnection : statsConnections) {
                try {
                    statsConnection.close();
                } catch (SQLException ignoredOnCleanup) {
                    // best-effort -- a stats probe connection failing to close cleanly doesn't
                    // affect the real query, which already ran (or failed) above
                }
            }
            calciteConnection.close();
        }
    }

    private static long elapsedMillisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /** {@code EXPLAIN PLAN FOR <sql>} on the same federated Calcite connection the real query is
     * about to run on -- Calcite supports this natively (own JDBC extension, not standard SQL), and
     * it's cheap: pure planning, no backend round trip. Ported from Omnigate's own
     * {@code FederationStage#captureExplainPlanOrNull}. */
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
            log.warn("cross-shard join: EXPLAIN PLAN FOR failed for shards {} -- plan history will show no "
                    + "plan text for this entry, real query is unaffected ({})", backendsLabel, e.toString());
            return null;
        }
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.stripTrailing();
        return trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : sql;
    }

    private static Set<String> distinctShardTables(String schemaName, String sql) {
        Pattern tableRef = Pattern.compile("\\b" + Pattern.quote(schemaName) + "\\.(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher m = tableRef.matcher(sql);
        Set<String> tables = new LinkedHashSet<>();
        while (m.find()) {
            tables.add(m.group(1));
        }
        return tables;
    }
}
