package com.polygres.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RouterStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RouterStage.class);

    public record SchemaRule(Pattern schemaPattern, String backendName) {
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

    public record ShardRule(Pattern schemaPattern) {
    }

    private volatile List<SchemaRule> schemaRules;
    private volatile List<PredicateRule> predicateRules;
    private volatile List<ValueShardRule> valueShardRules;
    private volatile List<ValueShardColumnRule> valueShardColumnRules;
    private volatile List<ShardRule> shardRules;

    private final BackendRegistry backendRegistry;

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
        this.schemaRules = List.copyOf(schemaRules);
        this.predicateRules = List.copyOf(predicateRules);
        this.valueShardRules = List.copyOf(valueShardRules);
        this.valueShardColumnRules = List.copyOf(valueShardColumnRules);
        this.shardRules = List.copyOf(shardRules);
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

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String shardTablesSpec) {
        return fromConfig(schemaSpec, predicateSpec, null, shardTablesSpec, null);
    }

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        return fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, null);
    }

    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec,
            String shardTablesSpec, BackendRegistry backendRegistry) {
        List<SchemaRule> schemaRules = new ArrayList<>();
        if (schemaSpec != null && !schemaSpec.isBlank()) {
            for (String entry : schemaSpec.split(",")) {
                String[] parts = entry.split(":", 2);
                if (parts.length == 2) {
                    schemaRules.add(new SchemaRule(
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
                    shardRules.add(new ShardRule(
                            Pattern.compile("\\b" + Pattern.quote(schema) + "\\.", Pattern.CASE_INSENSITIVE)));
                }
            }
        }
        return new RouterStage(schemaRules, predicateRules, valueShardRules, valueShardColumnRules, shardRules, backendRegistry);
    }

    public void reconfigure(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        RouterStage fresh = fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec);
        this.schemaRules = fresh.schemaRules;
        this.predicateRules = fresh.predicateRules;
        this.valueShardRules = fresh.valueShardRules;
        this.valueShardColumnRules = fresh.valueShardColumnRules;
        this.shardRules = fresh.shardRules;
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
