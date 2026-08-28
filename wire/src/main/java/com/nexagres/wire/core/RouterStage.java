package com.nexagres.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RouterStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RouterStage.class);

    /** @param schemaName the raw configured schema name (e.g. {@code "orders_db"}), kept alongside
     *     the already-compiled {@link #schemaPattern()} for the same reason {@link ShardRule} now
     *     does -- {@link SchemaFederationStage} needs the literal string to mount a Calcite schema
     *     under the exact name a client's query itself references, not the (potentially
     *     differently-named) backend the rule routes to. */
    public record SchemaRule(String schemaName, Pattern schemaPattern, String backendName) {
    }

    public record PredicateRule(int bindIndex, String expectedValue, String backendName) {
    }

    public record ValueShardRule(int bindIndex, ShardingStrategy strategy) {
    }

    /** Same sharding-value routing as {@link ValueShardRule}, but for a client that sent the
     * sharding value as a plain SQL literal (e.g. {@code WHERE tenant_id = 42} from psql,
     * simple-query mode, or any client/ORM not using bind parameters) instead of a bind
     * parameter. {@link ValueShardRule} indexes {@code statement.bindParams()}, which is empty
     * for a literal -- that statement fell through to the default backend silently, a real
     * correctness gap (wrong shard, no error) flagged by a fresh competitive-comparison audit
     * against ShardingSphere, which extracts sharding values from literals via real SQL parsing.
     * This is the regex-based equivalent, matching {@link SchemaRule}/{@link ShardRule}'s existing
     * (also regex-on-raw-SQL, not a real parser) scope in this class -- see
     * {@link ValueShardLiteralMatcher} for exactly what it can and can't find. */
    public record ValueShardColumnRule(String columnName, ShardingStrategy strategy) {
    }

    /** @param schemaName the raw configured schema name (e.g. {@code "shard"}), kept alongside the
     *     already-compiled {@link #schemaPattern()} -- {@link ShardJoinExecutor} needs the literal
     *     string itself (to mount a Calcite view under that exact name and to find every distinct
     *     {@code schemaName.table} reference in the query), which a compiled {@link Pattern} can't
     *     hand back out. */
    public record ShardRule(String schemaName, Pattern schemaPattern) {
    }

    /** Real, declarative per-table horizontal partitioning ({@code POLYWIRE_TABLE_SHARDS}) --
     * unlike {@link ShardRule} (which needs a client to type a schema-qualifier prefix like {@code
     * "public."} in every query just to opt a statement into scatter-gather), this matches the
     * table's own bare name directly (real word-boundary regex, no qualifier needed at all) and
     * knows its own real partition strategy, so a query that supplies the partition column's real
     * value routes straight to the ONE shard that owns it -- no scatter, no merge -- while a query
     * that doesn't (a full-table aggregate, or a JOIN) transparently falls back to scatter-gather
     * (or a real federated JOIN, see {@link ShardJoinExecutor}) across exactly this table's own
     * declared shard set, not necessarily the same as any OTHER table's.
     *
     * @param tableName the bare table name as configured (e.g. {@code "orders"}), kept alongside
     *     {@link #tablePattern()} for the same reason {@link ShardRule#schemaName()} is -- callers
     *     that need the literal string back (log messages, {@link RoutingBackendExecutor}'s own
     *     per-table shard-set lookup) can't get it back out of a compiled {@link Pattern}.
     * @param column the partition column name, matched against a real SQL literal via {@link
     *     ValueShardLiteralMatcher} -- a client that bound the value as a parameter instead of a
     *     literal isn't detected here (same real, disclosed limitation {@link
     *     ValueShardColumnRule} already has for the identical reason: no real SQL parser binding
     *     bind-parameter positions back to column names), and just falls through to the scatter/
     *     join fallback instead of the single-shard fast path -- correct, just not the fastest
     *     path available for that one case.
     * @param strategy the real partition strategy (hash/consistent/list/range/date) -- {@link
     *     ShardingStrategy#resolve} for the single-shard fast path, {@link
     *     ShardingStrategy#allBackends} for the scatter/join fallback's own shard set. */
    public record TableShardRule(String tableName, Pattern tablePattern, String column, ShardingStrategy strategy) {
    }

    private volatile List<SchemaRule> schemaRules;
    private volatile List<PredicateRule> predicateRules;
    private volatile List<ValueShardRule> valueShardRules;
    private volatile List<ValueShardColumnRule> valueShardColumnRules;
    private volatile List<ShardRule> shardRules;
    private volatile List<TableShardRule> tableShardRules;

    // Set once, after construction (see setFederationSupport) -- not threaded through RouterStage's
    // own many constructor overloads (every existing caller/test would need updating for something
    // orthogonal to routing itself). Shared by reference with ShardJoinExecutor/SchemaFederationStage
    // so every federated query -- regardless of which of the two problem shapes it hits -- reads and
    // writes the SAME StatisticsStore/SqlPlanStore, not one per stage.
    private volatile StatisticsStore statisticsStore;
    private volatile SqlPlanStore planStore;

    private final BackendRegistry backendRegistry;

    /** Called once from {@code Main} right after both are constructed -- see this field pair's own
     * comment for why they're not threaded through a constructor instead. */
    public void setFederationSupport(StatisticsStore statisticsStore, SqlPlanStore planStore) {
        this.statisticsStore = statisticsStore;
        this.planStore = planStore;
    }

    /** Same "find the one instance of this stage in the shared pipeline stage list" convention as
     * {@link #shardRulesIn}/{@link StatsCollectorStage#findIn} -- {@code null} if absent, or if
     * present but {@link #setFederationSupport} was never called (statistics collection not
     * configured). */
    public static StatisticsStore statisticsStoreIn(List<PipelineStage> stages) {
        for (PipelineStage stage : stages) {
            if (stage instanceof RouterStage router) {
                return router.statisticsStore;
            }
        }
        return null;
    }

    /** As {@link #statisticsStoreIn}, for the plan-history store. */
    public static SqlPlanStore planStoreIn(List<PipelineStage> stages) {
        for (PipelineStage stage : stages) {
            if (stage instanceof RouterStage router) {
                return router.planStore;
            }
        }
        return null;
    }

    public RouterStage() {
        this(List.of(), List.of(), List.of(), List.of(), List.of(), null);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules, List<ShardRule> shardRules) {
        this(schemaRules, predicateRules, List.of(), List.of(), shardRules, null);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules,
            List<ValueShardRule> valueShardRules, List<ShardRule> shardRules) {
        this(schemaRules, predicateRules, valueShardRules, List.of(), shardRules, null);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules,
            List<ValueShardRule> valueShardRules, List<ValueShardColumnRule> valueShardColumnRules,
            List<ShardRule> shardRules, BackendRegistry backendRegistry) {
        this(schemaRules, predicateRules, valueShardRules, valueShardColumnRules, shardRules, List.of(), backendRegistry);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules,
            List<ValueShardRule> valueShardRules, List<ValueShardColumnRule> valueShardColumnRules,
            List<ShardRule> shardRules, List<TableShardRule> tableShardRules, BackendRegistry backendRegistry) {
        this.schemaRules = List.copyOf(schemaRules);
        this.predicateRules = List.copyOf(predicateRules);
        this.valueShardRules = List.copyOf(valueShardRules);
        this.valueShardColumnRules = List.copyOf(valueShardColumnRules);
        this.shardRules = List.copyOf(shardRules);
        this.tableShardRules = List.copyOf(tableShardRules);
        this.backendRegistry = backendRegistry;
    }

    public List<SchemaRule> schemaRules() {
        return schemaRules;
    }

    public List<PredicateRule> predicateRules() {
        return predicateRules;
    }

    public List<ValueShardRule> valueShardRules() {
        return valueShardRules;
    }

    public List<ValueShardColumnRule> valueShardColumnRules() {
        return valueShardColumnRules;
    }

    public List<ShardRule> shardRules() {
        return shardRules;
    }

    public List<TableShardRule> tableShardRules() {
        return tableShardRules;
    }

    /** Same "find the one instance of this stage in the shared pipeline stage list" convention as
     * {@link StatsCollectorStage#findIn} -- lets a session handler that only has {@code sharedStages}
     * (not a direct {@link RouterStage} reference) build a {@link RoutingBackendExecutor} that knows
     * the configured shard schema names, without threading a new constructor parameter through every
     * caller. Returns {@code List.of()} (not {@code null}) when no {@link RouterStage} is present, so
     * every caller can pass the result straight through without a null check. */
    public static List<ShardRule> shardRulesIn(List<PipelineStage> stages) {
        for (PipelineStage stage : stages) {
            if (stage instanceof RouterStage router) {
                return router.shardRules();
            }
        }
        return List.of();
    }

    /** As {@link #shardRulesIn}, for {@link TableShardRule}s. */
    public static List<TableShardRule> tableShardRulesIn(List<PipelineStage> stages) {
        for (PipelineStage stage : stages) {
            if (stage instanceof RouterStage router) {
                return router.tableShardRules();
            }
        }
        return List.of();
    }

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String shardTablesSpec) {
        return fromConfig(schemaSpec, predicateSpec, null, shardTablesSpec, null);
    }

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        return fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, null);
    }

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec,
            String shardTablesSpec, BackendRegistry backendRegistry) {
        return fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, null, backendRegistry);
    }

    /** As the other {@code fromConfig} overload, plus {@code tableShardSpec}
     * ({@code POLYWIRE_TABLE_SHARDS}) -- real, declarative per-table horizontal partitioning, see
     * {@link TableShardRule}'s own javadoc. Format: {@code table:strategy:column:params}, entries
     * {@code |}-delimited (matching {@code valueShardSpec}'s own convention -- {@code strategy}'s
     * own {@code params} may itself use {@code ;}, e.g. a range/date strategy's own boundary list,
     * so a DIFFERENT delimiter has to separate whole table entries). {@code strategy} is anything
     * {@link ShardingStrategy#fromConfig} accepts (hash/consistent/list/range/date). Example:
     * {@code orders:hash:customer_id:shard1,shard2,shard3}. */
    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec,
            String shardTablesSpec, String tableShardSpec, BackendRegistry backendRegistry) {
        List<SchemaRule> schemaRules = new ArrayList<>();
        if (schemaSpec != null && !schemaSpec.isBlank()) {
            for (String entry : schemaSpec.split(",")) {
                String[] parts = entry.split(":", 2);
                if (parts.length == 2) {
                    schemaRules.add(new SchemaRule(parts[0].trim(),
                            Pattern.compile("\\b" + Pattern.quote(parts[0].trim()) + "\\.", Pattern.CASE_INSENSITIVE),
                            parts[1].trim()));
                }
            }
        }
        List<PredicateRule> predicateRules = new ArrayList<>();
        if (predicateSpec != null && !predicateSpec.isBlank()) {
            for (String entry : predicateSpec.split(",")) {
                String[] parts = entry.split(":", 3);
                if (parts.length == 3) {
                    predicateRules.add(new PredicateRule(Integer.parseInt(parts[0].trim()), parts[1].trim(), parts[2].trim()));
                }
            }
        }
        List<ValueShardRule> valueShardRules = new ArrayList<>();
        List<ValueShardColumnRule> valueShardColumnRules = new ArrayList<>();
        if (valueShardSpec != null && !valueShardSpec.isBlank()) {
            for (String rule : valueShardSpec.split("\\|")) {
                String[] parts = rule.split(":", 3);
                if (parts.length != 3) {
                    continue;
                }
                String key = parts[0].trim();
                ShardingStrategy strategy = ShardingStrategy.fromConfig(parts[1].trim(), parts[2].trim());
                // A purely-numeric first field is (unchanged, back-compat) a bind-parameter index.
                // Anything else is a column name -- routes a client that sent the sharding value
                // as a plain SQL literal instead of a bind parameter (see ValueShardColumnRule's
                // javadoc for why that case previously fell through to the wrong backend silently).
                if (key.matches("\\d+")) {
                    valueShardRules.add(new ValueShardRule(Integer.parseInt(key), strategy));
                } else {
                    valueShardColumnRules.add(new ValueShardColumnRule(key, strategy));
                }
            }
        }
        List<ShardRule> shardRules = new ArrayList<>();
        if (shardTablesSpec != null && !shardTablesSpec.isBlank()) {
            for (String entry : shardTablesSpec.split(",")) {
                String schema = entry.trim();
                if (!schema.isEmpty()) {
                    shardRules.add(new ShardRule(schema,
                            Pattern.compile("\\b" + Pattern.quote(schema) + "\\.", Pattern.CASE_INSENSITIVE)));
                }
            }
        }
        List<TableShardRule> tableShardRules = new ArrayList<>();
        if (tableShardSpec != null && !tableShardSpec.isBlank()) {
            for (String entry : tableShardSpec.split("\\|")) {
                String[] parts = entry.split(":", 4);
                if (parts.length != 4) {
                    continue;
                }
                String table = parts[0].trim();
                if (table.isEmpty()) {
                    continue;
                }
                ShardingStrategy strategy = ShardingStrategy.fromConfig(parts[1].trim(), parts[3].trim());
                tableShardRules.add(new TableShardRule(table,
                        Pattern.compile("\\b" + Pattern.quote(table) + "\\b", Pattern.CASE_INSENSITIVE),
                        parts[2].trim(), strategy));
            }
        }
        return new RouterStage(schemaRules, predicateRules, valueShardRules, valueShardColumnRules, shardRules,
                tableShardRules, backendRegistry);
    }

    public void reconfigure(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        reconfigure(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, null);
    }

    public void reconfigure(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec,
            String tableShardSpec) {
        RouterStage fresh = fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, tableShardSpec, null);
        this.schemaRules = fresh.schemaRules;
        this.predicateRules = fresh.predicateRules;
        this.valueShardRules = fresh.valueShardRules;
        this.valueShardColumnRules = fresh.valueShardColumnRules;
        this.shardRules = fresh.shardRules;
        this.tableShardRules = fresh.tableShardRules;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String workloadClass = "default".equals(statement.workloadClass())
                ? classifyWorkload(statement.sqlText())
                : statement.workloadClass();
        String targetBackend = statement.targetBackend() != null ? statement.targetBackend() : resolveBackend(statement);
        return next.proceed(statement.withRouting(workloadClass, targetBackend));
    }

    private String resolveBackend(Statement statement) {
        for (SchemaRule rule : schemaRules) {
            if (rule.schemaPattern().matcher(statement.sqlText()).find()) {
                log.debug("router: schema rule matched -> backend={}", rule.backendName());
                return rule.backendName();
            }
        }
        for (PredicateRule rule : predicateRules) {
            List<Object> binds = statement.bindParams();
            if (rule.bindIndex() < binds.size()
                    && rule.expectedValue().equals(String.valueOf(binds.get(rule.bindIndex())))) {
                log.debug("router: predicate rule matched (bind[{}]={}) -> backend={}",
                        rule.bindIndex(), rule.expectedValue(), rule.backendName());
                return rule.backendName();
            }
        }
        for (ValueShardRule rule : valueShardRules) {
            List<Object> binds = statement.bindParams();
            if (rule.bindIndex() < binds.size()) {
                Object bindValue = binds.get(rule.bindIndex());
                if (bindValue != null) {
                    String backend = rule.strategy().resolve(String.valueOf(bindValue));
                    if (backend != null) {
                        log.debug("router: value-shard rule matched (bind[{}]={}) -> backend={}",
                                rule.bindIndex(), bindValue, backend);
                        return backend;
                    }
                }
            }
        }
        // Covers the case ValueShardRule can't: a client that sent the sharding value as a plain
        // SQL literal (WHERE tenant_id = 42) rather than a bind parameter -- bindParams() is empty
        // for that statement, so the loop above never even runs. Always tried, not just when
        // bindParams() is empty, since a client can mix bind params for some columns with literals
        // for others in the same statement.
        for (ValueShardColumnRule rule : valueShardColumnRules) {
            String literal = ValueShardLiteralMatcher.findLiteralValue(statement.sqlText(), rule.columnName());
            if (literal != null) {
                String backend = rule.strategy().resolve(literal);
                if (backend != null) {
                    log.debug("router: value-shard literal rule matched ({}={}) -> backend={}",
                            rule.columnName(), literal, backend);
                    return backend;
                }
            }
        }
        // Real, declarative per-table sharding (POLYWIRE_TABLE_SHARDS) -- unlike ShardRule below,
        // needs no schema-qualifier prefix in the query at all: the table's own bare name is
        // matched directly. A literal value for the table's own declared partition column routes
        // straight to the ONE shard that owns it; anything else (no such predicate -- a full-table
        // aggregate or a JOIN) falls through to the scatter-gather/federated-join path exactly like
        // ShardRule's own scatter trigger, except RoutingBackendExecutor resolves the shard SET for
        // this specific table from its own TableShardRule (ShardingStrategy#allBackends), not
        // necessarily the same set every other table shares.
        for (TableShardRule rule : tableShardRules) {
            if (!rule.tablePattern().matcher(statement.sqlText()).find()) {
                continue;
            }
            String literal = ValueShardLiteralMatcher.findLiteralValue(statement.sqlText(), rule.column());
            if (literal != null) {
                String backend = rule.strategy().resolve(literal);
                if (backend != null) {
                    log.debug("router: table-shard rule matched (table={}, {}={}) -> single-shard backend={}",
                            rule.tableName(), rule.column(), literal, backend);
                    return backend;
                }
            }
            if (statement.sqlText().strip().regionMatches(true, 0, "SELECT", 0, 6)) {
                log.debug("router: table-shard rule matched (table={}) with no routable {} value -> scatter-gather "
                        + "across this table's own shard set", rule.tableName(), rule.column());
                return RoutingBackendExecutor.SCATTER_ALL;
            }
        }
        if (statement.sqlText().strip().regionMatches(true, 0, "SELECT", 0, 6)) {
            for (ShardRule rule : shardRules) {
                if (rule.schemaPattern().matcher(statement.sqlText()).find()) {
                    log.debug("router: shard rule matched -> scatter-gather");
                    return RoutingBackendExecutor.SCATTER_ALL;
                }
            }
        }
        return resolveUnambiguousDefault();
    }

    private String resolveUnambiguousDefault() {
        if (backendRegistry == null) {
            return null;
        }
        var all = backendRegistry.all();
        if (all.size() != 1) {
            return null;
        }
        BackendTarget only = all.iterator().next();
        if (!BackendRegistry.DEFAULT_BACKEND_NAME.equals(only.name())) {
            return null;
        }
        log.debug("router: no rule matched, falling back to implicit single backend '{}'", only.name());
        return only.name();
    }

    private static String classifyWorkload(String sql) {
        return switch (firstWord(sql)) {
            case "SELECT" -> "query";
            case "INSERT", "UPDATE", "DELETE", "MERGE" -> "write";
            case "COMMIT", "ROLLBACK" -> "txn";
            case "CREATE", "ALTER", "DROP", "TRUNCATE" -> "ddl";
            default -> "other";
        };
    }

    private static String firstWord(String sql) {
        String trimmed = sql.strip();
        int end = 0;
        while (end < trimmed.length() && !Character.isWhitespace(trimmed.charAt(end))) {
            end++;
        }
        return trimmed.substring(0, end).toUpperCase(Locale.ROOT);
    }
}
