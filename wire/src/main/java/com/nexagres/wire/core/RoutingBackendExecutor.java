package com.nexagres.wire.core;

import com.nexagres.wire.xa.XaBackendFactory;
import com.nexagres.wire.xa.XaRecoveryLog;
import com.nexagres.wire.xa.XaTransaction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RoutingBackendExecutor implements BackendExecutor {

    private static final Logger log = LoggerFactory.getLogger(RoutingBackendExecutor.class);

    public static final String SCATTER_ALL = "*scatter-all*";

    private final BackendRegistry registry;
    private final BackendExecutor defaultExecutor;
    private final XaRecoveryLog recoveryLog;
    private final List<RouterStage.ShardRule> shardRules;
    private final List<RouterStage.TableShardRule> tableShardRules;
    // Set via withFederationSupport, not a constructor param -- same "orthogonal, set once after
    // construction" reasoning as RouterStage's own matching pair; both nullable, meaning "not
    // configured" (ShardJoinExecutor degrades to Calcite's default Statistics.UNKNOWN / no plan
    // history recorded), never an error.
    private StatisticsStore statisticsStore;
    private SqlPlanStore planStore;

    private Map<String, Connection> transactionConnections;

    private Map<String, String> cursorTargets;

    private XaTransaction xaTransaction;

    private boolean transactionFailed;

    private static final Pattern DECLARE_CURSOR = Pattern.compile("(?i)^\\s*DECLARE\\s+(\\w+)\\s+CURSOR\\b");
    private static final Pattern FETCH_OR_CLOSE_CURSOR =
            Pattern.compile("(?i)^\\s*(?:FETCH\\b.*\\b(?:FROM|IN)\\s+(\\w+)|CLOSE\\s+(\\w+))\\s*;?\\s*$");

    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor) {
        this(registry, defaultExecutor, null);
    }

    /** {@code recoveryLog} is nullable -- see {@link XaTransaction}'s matching constructor; every
     * production caller (Main) supplies one so a coordinator crash mid-commit is recoverable, but
     * tests that don't need real crash recovery can omit it. */
    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor, XaRecoveryLog recoveryLog) {
        this(registry, defaultExecutor, recoveryLog, List.of());
    }

    /** {@code shardRules} is the configured shard schema list (see {@link RouterStage#shardRulesIn})
     * -- empty means {@link #executeScatterGather} never attempts {@link ShardJoinExecutor}'s
     * cross-shard JOIN path, same "absent means the feature doesn't exist" shape used elsewhere in
     * this codebase, not an error. */
    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor, XaRecoveryLog recoveryLog,
            List<RouterStage.ShardRule> shardRules) {
        this(registry, defaultExecutor, recoveryLog, shardRules, List.of());
    }

    /** As the other constructor, plus {@code tableShardRules} (see {@link RouterStage#tableShardRulesIn})
     * -- lets {@link #executeScatterGather} resolve THIS statement's own real shard set from its
     * matched table's own {@link RouterStage.TableShardRule} (via {@link
     * ShardingStrategy#allBackends}) instead of unconditionally using {@code registry.shardGroup()},
     * which is the one, project-wide shard list {@code WARP_SHARD_BACKENDS} configures -- a
     * real, declaratively-sharded table's own shard set can be a different subset (or even a
     * disjoint list) of backends. Empty means "no declaratively-sharded tables configured", the
     * same "absent means the feature doesn't exist" shape {@code shardRules} already has. */
    public RoutingBackendExecutor(BackendRegistry registry, BackendExecutor defaultExecutor, XaRecoveryLog recoveryLog,
            List<RouterStage.ShardRule> shardRules, List<RouterStage.TableShardRule> tableShardRules) {
        this.registry = registry;
        this.defaultExecutor = defaultExecutor;
        this.recoveryLog = recoveryLog;
        this.shardRules = List.copyOf(shardRules);
        this.tableShardRules = List.copyOf(tableShardRules);
    }

    /** Fluent, so a call site can chain it right onto the constructor -- see
     * {@link RouterStage#statisticsStoreIn}/{@link RouterStage#planStoreIn} for where the shared
     * instances come from. Either or both may be {@code null} (not configured). */
    public RoutingBackendExecutor withFederationSupport(StatisticsStore statisticsStore, SqlPlanStore planStore) {
        this.statisticsStore = statisticsStore;
        this.planStore = planStore;
        return this;
    }

    public boolean inTransaction() {
        return transactionConnections != null;
    }

    public void markTransactionFailed() {
        if (transactionConnections != null) {
            transactionFailed = true;
        }
    }

    public void beginTransaction() {
        transactionConnections = new LinkedHashMap<>();
        cursorTargets = new LinkedHashMap<>();
        xaTransaction = new XaTransaction(recoveryLog);
        transactionFailed = false;
    }

    public void endTransaction(boolean commit) throws SQLException {
        if (transactionConnections == null) {
            return;
        }
        boolean actuallyCommit = commit && !transactionFailed;
        if (commit && transactionFailed) {
            log.warn("xa: client sent COMMIT but a statement failed earlier in this transaction -- rolling back all branches instead (same as real Postgres implicitly rolling back a COMMIT after an error)");
        }
        SQLException firstFailure = null;
        try {
            if (actuallyCommit) {
                xaTransaction.commit();
            } else {
                xaTransaction.rollback();
            }
        } catch (SQLException e) {
            firstFailure = e;
        }
        for (Connection connection : transactionConnections.values()) {
            try {
                connection.close();
            } catch (SQLException ignoredOnCleanup) {
                
            }
        }
        transactionConnections = null;
        cursorTargets = null;
        xaTransaction = null;
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    @Override
    public ExecutionResult execute(Statement statement) throws SQLException {
        String targetName = statement.targetBackend();
        if (targetName == null && transactionConnections != null) {
            targetName = cursorTargets.get(cursorNameReferenced(statement.sqlText()));
        }
        
        if (targetName == null || registry.isEmpty() || BackendRegistry.DEFAULT_BACKEND_NAME.equals(targetName)) {
            // The common case -- no WARP_ROUTER_* rule matched -- normally always uses
            // defaultExecutor, a connection borrowed once for the whole client session. That's
            // correct and cheapest for the vast majority of statements, but it structurally can't
            // read-route: the same connection serves every statement in the session regardless of
            // read/write. When read routing is enabled, route an eligible read through the fresh-
            // connection-per-statement path instead (same eligibility rule as the explicit-target
            // path below: autocommit, READ-classified, and only when a default target actually
            // exists to route against).
            if (READ_ROUTING_ENABLED && transactionConnections == null && !registry.isEmpty()
                    && SqlMetricsCollector.classify(statement.sqlText()) == SqlMetricsCollector.StatementKind.READ) {
                BackendTarget defaultTarget = registry.resolveForRouting(BackendRegistry.DEFAULT_BACKEND_NAME);
                if (defaultTarget != null) {
                    return executeOnFreshConnection(defaultTarget, statement);
                }
            }
            return defaultExecutor.execute(statement);
        }
        if (SCATTER_ALL.equals(targetName)) {
            return executeScatterGather(statement);
        }
        BackendTarget target = registry.resolveForRouting(targetName);
        if (target == null) {
            throw ErrorCatalog.sqlException("ERR_ROUTER_UNKNOWN_BACKEND", targetName);
        }
        // Real bug, found live: a MongoDB backend has no JDBC driver at all -- letting this reach
        // BackendConnectionPools/HikariCP threw a raw "No suitable driver" RuntimeException that
        // PgWireSessionHandler treats as fatal to the whole session (not a normal, in-band SQL
        // error), silently disconnecting the client instead of returning a clear error. A Mongo
        // backend today only participates via SchemaFederationStage's own two-schema SELECT JOIN
        // path (see that class's own javadoc) -- a real, clear, caught error here instead.
        if (isMongoConnectionString(target.jdbcUrl())) {
            throw ErrorCatalog.sqlException("ERR_MONGO_ROUTING_UNSUPPORTED", targetName);
        }
        if (transactionConnections == null) {
            return executeOnFreshConnection(target, statement);
        }
        rememberCursorTarget(statement.sqlText(), targetName);
        return executeOnTransactionConnection(target, statement);
    }

    private static String cursorNameReferenced(String sql) {
        Matcher matcher = FETCH_OR_CLOSE_CURSOR.matcher(sql);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }

    private void rememberCursorTarget(String sql, String targetName) {
        Matcher matcher = DECLARE_CURSOR.matcher(sql);
        if (matcher.find()) {
            cursorTargets.put(matcher.group(1), targetName);
        }
    }

    private ExecutionResult executeScatterGather(Statement statement) throws SQLException {
        if (!statement.sqlText().strip().regionMatches(true, 0, "SELECT", 0, 6)) {
            throw ErrorCatalog.sqlException("ERR_SCATTER_ONLY_SELECT", statement.sqlText());
        }

        // Real, declarative per-table sharding (WARP_TABLE_SHARDS) takes priority when it
        // matches: this table's own declared shard set (from its own ShardingStrategy) is what
        // gets scattered/joined across, NOT necessarily the same registry.shardGroup() every OTHER
        // table shares -- a table declared with its own backend list can be a disjoint subset.
        // Falls straight through, unchanged, to the schema-qualified ShardRule/registry.shardGroup()
        // path below when no table-shard rule matches this statement at all.
        List<RouterStage.TableShardRule> matchedTableRules = matchedTableShardRules(statement.sqlText());
        if (!matchedTableRules.isEmpty()) {
            List<String> tableShardNames = unionOfAllBackends(matchedTableRules);
            List<RouterStage.TableShardRule> joinRules =
                    ShardJoinExecutor.matchedTableShardRules(matchedTableRules, statement.sqlText());
            if (!joinRules.isEmpty()) {
                java.util.Set<String> tableNames = new java.util.LinkedHashSet<>();
                for (RouterStage.TableShardRule rule : joinRules) {
                    tableNames.add(rule.tableName());
                }
                return ShardJoinExecutor.executeByTableNames(registry, tableShardNames, tableNames, "public",
                        statement, statisticsStore, planStore);
            }
            return scatterAcross(tableShardNames, statement);
        }

        List<String> shardNames = registry.shardGroup();
        if (shardNames.isEmpty()) {
            throw ErrorCatalog.sqlException("ERR_SCATTER_NOT_CONFIGURED");
        }

        // A genuine cross-shard JOIN (two shard-qualified tables, each independently horizontally
        // partitioned across shardNames -- a row's match may live on a DIFFERENT physical shard,
        // not just a different row on the same one) needs real federation, not scatter-gather: the
        // plain-append/aggregate-merge paths below both send the query text unmodified to every
        // shard and combine each shard's own LOCAL join result -- silently wrong the moment a
        // matching row pair spans two shards, since it's never found on either shard alone. Checked
        // before the aggregate-merge/plain-append dispatch below, not instead of it -- a query with
        // no JOIN (the overwhelming common case) is completely unaffected.
        String matchedSchema = ShardJoinExecutor.matchedShardSchema(shardRules, statement.sqlText());
        if (matchedSchema != null) {
            return ShardJoinExecutor.execute(registry, shardNames, matchedSchema, statement, statisticsStore, planStore);
        }
        return scatterAcross(shardNames, statement);
    }

    /** @return every configured {@link RouterStage.TableShardRule} whose bare table name is
     *     actually referenced in {@code sql} -- regardless of whether it's a JOIN or a plain
     *     scatter, unlike {@link ShardJoinExecutor#matchedTableShardRules}, which additionally
     *     requires a JOIN keyword. */
    private List<RouterStage.TableShardRule> matchedTableShardRules(String sql) {
        List<RouterStage.TableShardRule> matched = new ArrayList<>();
        for (RouterStage.TableShardRule rule : tableShardRules) {
            if (rule.tablePattern().matcher(sql).find()) {
                matched.add(rule);
            }
        }
        return matched;
    }

    private static List<String> unionOfAllBackends(List<RouterStage.TableShardRule> rules) {
        java.util.LinkedHashSet<String> all = new java.util.LinkedHashSet<>();
        for (RouterStage.TableShardRule rule : rules) {
            all.addAll(ShardingStrategy.allBackends(rule.strategy()));
        }
        return List.copyOf(all);
    }

    /** The real, existing aggregate-merge/plain-append scatter-gather logic -- extracted so both
     * the schema-qualified {@link RouterStage.ShardRule} path (its own {@code registry.shardGroup()})
     * and the declarative {@link RouterStage.TableShardRule} path (its own matched table's shard
     * set) can share it unchanged; neither the ORDER BY/LIMIT rewriting nor the aggregate-vs-plain
     * dispatch below cares which path produced {@code shardNames}. */
    private ExecutionResult scatterAcross(List<String> shardNames, Statement statement) throws SQLException {

        // Real bug fixed here, flagged by a competitive comparison against ShardingSphere: this
        // used to always append raw per-shard rows unchanged, which is correct for a plain SELECT
        // but silently wrong for an aggregate -- COUNT(*)/SUM/AVG/MIN/MAX each need real
        // cross-shard combination, not concatenation. ScatterGatherAggregateMerge.plan() returns
        // null for anything that doesn't need merging (no aggregate present), so the plain-append
        // path below is unchanged for every query shape it was already correct for.
        //
        // A second, related bug fixed here (same audit, follow-up finding): whichever path below
        // runs, the client's own ORDER BY/LIMIT/OFFSET used to be sent to EVERY shard unmodified
        // and the per-shard results just concatenated -- each shard locally sorted/truncated its
        // own rows, so e.g. a 3-shard "... ORDER BY x LIMIT 10" could return up to 30 rows, in
        // shard-arrival order, not the correct globally-ordered top 10. ScatterGatherOrderLimit
        // strips those clauses from what's sent to each shard (so every shard returns its full,
        // unsorted/untruncated matching set) and applies them once, centrally, after gathering --
        // see its class doc for why no partial per-shard LIMIT pushdown is attempted here.
        ScatterGatherOrderLimit.Parsed orderLimit = ScatterGatherOrderLimit.parse(statement.sqlText());
        String coreSql = orderLimit.withoutOrderLimitOffset();

        ScatterGatherAggregateMerge.Plan plan = ScatterGatherAggregateMerge.plan(coreSql);
        ExecutionResult merged;
        if (plan != null) {
            Map<List<Object>, Object[]> accumulators = new LinkedHashMap<>();
            for (String shardName : shardNames) {
                BackendTarget target = registry.resolveForRouting(shardName);
                if (target == null) {
                    throw ErrorCatalog.sqlException("ERR_SHARD_UNKNOWN_BACKEND", shardName);
                }
                Statement rewritten = statement.withSqlText(plan.rewrittenSql());
                ExecutionResult shardResult = executeOnFreshConnection(target, rewritten);
                ScatterGatherAggregateMerge.mergeShardResult(plan, shardResult, accumulators);
            }
            merged = ScatterGatherAggregateMerge.buildResult(plan, accumulators);
        } else {
            List<ColumnInfo> columns = null;
            List<List<Object>> mergedRows = new ArrayList<>();
            Statement coreStatement = statement.withSqlText(coreSql);
            for (String shardName : shardNames) {
                BackendTarget target = registry.resolveForRouting(shardName);
                if (target == null) {
                    throw ErrorCatalog.sqlException("ERR_SHARD_UNKNOWN_BACKEND", shardName);
                }
                ExecutionResult result = executeOnFreshConnection(target, coreStatement);
                if (columns == null) {
                    columns = result.columns();
                }
                mergedRows.addAll(result.rows());
            }
            merged = ExecutionResult.ofQuery(columns, mergedRows);
        }
        return ScatterGatherOrderLimit.applyOrderAndLimit(merged, orderLimit.spec());
    }

    // Opt-in (default off): reading from a standby means reading data that may be behind the
    // primary by however long replication lag currently is -- a real correctness tradeoff, not
    // a free win, so this must never be silently on. Off by default matches every other
    // behavior-changing toggle in this codebase (e.g. WARP_DYNAMOWIRE_CACHE_ENABLED's
    // sibling pattern), except inverted: here the safer default (always read the primary) is
    // the one that ships without an explicit opt-in.
    private static final boolean READ_ROUTING_ENABLED =
            "true".equalsIgnoreCase(System.getenv("WARP_READ_ROUTING_ENABLED"));

    private ExecutionResult executeOnFreshConnection(BackendTarget target, Statement statement) throws SQLException {
        // Only single, autocommit, read-classified statements are eligible -- this method is
        // only ever called when transactionConnections == null (see execute()), so "not inside a
        // transaction" is already guaranteed by the caller; the remaining condition is purely
        // "would sending this to a standby be safe", which for a WRITE or an unclassifiable
        // statement it is not.
        boolean preferStandby = READ_ROUTING_ENABLED
                && SqlMetricsCollector.classify(statement.sqlText()) == SqlMetricsCollector.StatementKind.READ;
        try (Connection connection = preferStandby ? target.openPreferringStandby() : target.open()) {
            return new JdbcBackendExecutor(connection).execute(statement);
        }
    }

    private ExecutionResult executeOnTransactionConnection(BackendTarget target, Statement statement) throws SQLException {
        Connection connection = transactionConnections.get(target.name());
        if (connection == null) {
            
            XaBackendFactory.XaBranch branch = XaBackendFactory.open(target);
            xaTransaction.addBranch(target, branch.resource());
            connection = branch.connection();
            transactionConnections.put(target.name(), connection);
        }
        try {
            return new JdbcBackendExecutor(connection).execute(statement);
        } catch (SQLException e) {

            transactionFailed = true;
            throw e;
        }
    }

    /** As {@link SchemaFederationStage}'s own matching method -- see its javadoc. Duplicated
     * rather than shared: a tiny, self-contained check, not worth a cross-class dependency for. */
    private static boolean isMongoConnectionString(String jdbcUrl) {
        return jdbcUrl != null && (jdbcUrl.startsWith("mongodb://") || jdbcUrl.startsWith("mongodb+srv://"));
    }
}
