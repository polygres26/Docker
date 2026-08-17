package com.polygres.wire.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ARCHITECTURE.md §5.3: covers all three of "routing based on text, schema
 * and predicate values" from the original requirements, plus §5.5 sharding.
 * <ul>
 *   <li><b>Text</b>: classifies every statement into a
 *   {@link Statement#workloadClass()} by its SQL verb — feeds
 *   {@link QosControlStage}.</li>
 *   <li><b>Schema</b>: if the SQL text references a configured schema name
 *   (e.g. {@code tenant2.orders}), assigns {@link Statement#targetBackend()}
 *   to that schema's configured backend.</li>
 *   <li><b>Predicate value</b>: if a configured positional bind parameter
 *   equals a configured value, assigns {@code targetBackend} accordingly.
 *   This is deliberately positional, not column-name-aware — the pipeline
 *   never sees column names (bind rewriting to "?" already stripped them
 *   upstream in each frontend), so a rule like "bind[0] == 'east'" requires
 *   the operator to know their own query's parameter order. True
 *   column-aware predicate routing needs a real SQL parser — future work,
 *   not implemented here.</li>
 *   <li><b>Sharding</b>: if the SQL text references a configured sharded
 *   schema/table and no schema or predicate rule already matched (i.e. the
 *   query has no known shard key), assigns
 *   {@link RoutingBackendExecutor#SCATTER_ALL} — fan the read out to every
 *   backend in {@link BackendRegistry#shardGroup()} and merge. SELECT-only;
 *   see {@link RoutingBackendExecutor}'s javadoc for why.</li>
 * </ul>
 * Schema and predicate rules (single-shard point routing) are checked
 * before shard rules (multi-shard scatter) — a query that already resolves
 * to one specific backend never needs to fan out. {@code targetBackend} is
 * left {@code null} (today's single-backend behavior, unchanged) when
 * nothing matches or no rules are configured.
 */
public final class RouterStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RouterStage.class);

    public record SchemaRule(Pattern schemaPattern, String backendName) {
    }

    public record PredicateRule(int bindIndex, String expectedValue, String backendName) {
    }

    public record ShardRule(Pattern schemaPattern) {
    }

    private final List<SchemaRule> schemaRules;
    private final List<PredicateRule> predicateRules;
    private final List<ShardRule> shardRules;

    public RouterStage() {
        this(List.of(), List.of(), List.of());
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules, List<ShardRule> shardRules) {
        this.schemaRules = List.copyOf(schemaRules);
        this.predicateRules = List.copyOf(predicateRules);
        this.shardRules = List.copyOf(shardRules);
    }

    /** For read-only config introspection (the HTTP admin API). */
    public List<SchemaRule> schemaRules() {
        return schemaRules;
    }

    public List<PredicateRule> predicateRules() {
        return predicateRules;
    }

    public List<ShardRule> shardRules() {
        return shardRules;
    }

    /**
     * {@code schemaSpec}: "schema1:backend1,schema2:backend2".
     * {@code predicateSpec}: "bindIndex:value:backend,...".
     * {@code shardTablesSpec}: "schema1,schema2" — schemas whose queries scatter-gather
     * across {@code POLYWIRE_SHARD_BACKENDS} when no schema/predicate rule already routed them.
     */
    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String shardTablesSpec) {
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
        return new RouterStage(schemaRules, predicateRules, shardRules);
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String workloadClass = "default".equals(statement.workloadClass())
                ? classifyWorkload(statement.sqlText())
                : statement.workloadClass(); // caller already assigned a class — don't override it
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
        if (statement.sqlText().strip().regionMatches(true, 0, "SELECT", 0, 6)) {
            for (ShardRule rule : shardRules) {
                if (rule.schemaPattern().matcher(statement.sqlText()).find()) {
                    log.debug("router: shard rule matched -> scatter-gather");
                    return RoutingBackendExecutor.SCATTER_ALL;
                }
            }
        }
        return null;
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
