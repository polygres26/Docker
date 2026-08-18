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

    /** Sharding-key routing via a {@link ShardingStrategy} (hash/consistent-hash/list/range) rather than {@link PredicateRule}'s fixed one-value-to-one-backend map — see {@link ShardingStrategy}'s javadoc and {@link #fromConfig}'s grammar. */
    public record ValueShardRule(int bindIndex, ShardingStrategy strategy) {
    }

    public record ShardRule(Pattern schemaPattern) {
    }

    // volatile, not final: see #reconfigure -- same "read fresh on every handle() call" hot-reload
    // shape as QosControlStage's fields, driven by com.polygres.wire.config.ConfigStore's
    // LISTEN/NOTIFY callback in Main.
    private volatile List<SchemaRule> schemaRules;
    private volatile List<PredicateRule> predicateRules;
    private volatile List<ValueShardRule> valueShardRules;
    private volatile List<ShardRule> shardRules;
    // Nullable: only set when a caller wants the "genuinely one unambiguous backend" fallback
    // described in #resolveBackend's javadoc. Not swapped by #reconfigure -- it's the same
    // BackendRegistry instance for the process lifetime (Main wires one in at startup), and that
    // instance's own #reload already keeps its *contents* current in place.
    private final BackendRegistry backendRegistry;

    public RouterStage() {
        this(List.of(), List.of(), List.of(), List.of(), null);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules, List<ShardRule> shardRules) {
        this(schemaRules, predicateRules, List.of(), shardRules, null);
    }

    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules,
            List<ValueShardRule> valueShardRules, List<ShardRule> shardRules) {
        this(schemaRules, predicateRules, valueShardRules, shardRules, null);
    }

    /**
     * Same as the four-arg constructor, plus {@code backendRegistry} — see
     * {@link #resolveBackend}'s javadoc for exactly when and why it's consulted as a last-resort
     * fallback after every rule list has been checked and none matched.
     */
    public RouterStage(List<SchemaRule> schemaRules, List<PredicateRule> predicateRules,
            List<ValueShardRule> valueShardRules, List<ShardRule> shardRules, BackendRegistry backendRegistry) {
        this.schemaRules = List.copyOf(schemaRules);
        this.predicateRules = List.copyOf(predicateRules);
        this.valueShardRules = List.copyOf(valueShardRules);
        this.shardRules = List.copyOf(shardRules);
        this.backendRegistry = backendRegistry;
    }

    /** For read-only config introspection (the HTTP admin API). */
    public List<SchemaRule> schemaRules() {
        return schemaRules;
    }

    public List<PredicateRule> predicateRules() {
        return predicateRules;
    }

    public List<ValueShardRule> valueShardRules() {
        return valueShardRules;
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
        return fromConfig(schemaSpec, predicateSpec, null, shardTablesSpec, null);
    }

    /**
     * Same as the three-arg {@link #fromConfig}, plus {@code valueShardSpec}
     * ({@code POLYWIRE_ROUTER_VALUE_SHARD_RULES}) — sharding-key routing via a
     * {@link ShardingStrategy} instead of {@link PredicateRule}'s fixed one-value-to-one-backend
     * map. Grammar: rules separated by {@code |}, each rule {@code bindIndex:type:params}:
     * <ul>
     *   <li>{@code hash}: {@code params} = comma-separated backend names, e.g.
     *   {@code "0:hash:shard1,shard2,shard3"} — {@code hash(bindValue) % backends.size()}.</li>
     *   <li>{@code consistent}: same {@code params} shape as {@code hash}, e.g.
     *   {@code "0:consistent:shard1,shard2,shard3"} — a hash ring (150 virtual nodes/backend)
     *   instead of plain modulo, so adding/removing a backend only remaps the keys that fell in
     *   the affected ring arc(s), not everything.</li>
     *   <li>{@code list}: {@code params} = {@code "backend1=v1,v2;backend2=v3"} (semicolon between
     *   backends, comma between that backend's values), e.g.
     *   {@code "0:list:shard1=east,north;shard2=west,south"}.</li>
     *   <li>{@code range}: {@code params} = {@code "backend1<1000;backend2<3000;backend3"}
     *   (semicolon-separated, ascending, half-open {@code [low, high)} bounds — each entry's
     *   {@code low} is the previous entry's {@code high}, starting at {@code -infinity}; the last
     *   entry, with no {@code <bound}, catches everything {@code >=} the previous bound), e.g.
     *   {@code "0:range:shard1<1000;shard2<3000;shard3"} routes {@code bindValue < 1000} to
     *   shard1, {@code 1000 <= bindValue < 3000} to shard2, {@code bindValue >= 3000} to shard3.</li>
     * </ul>
     * Checked in {@link #resolveBackend} right after {@link PredicateRule} and before
     * {@link ShardRule} (scatter-gather) — same "most specific first" ordering rationale as the
     * rest of this class's javadoc.
     */
    public static RouterStage fromConfig(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        return fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec, null);
    }

    /**
     * Same as the four-arg {@link #fromConfig}, plus {@code backendRegistry} — threaded through to
     * the constructed {@link RouterStage} so {@link #resolveBackend} can fall back to a genuinely
     * unambiguous single registered backend when no rule matched. Pass the same
     * {@link BackendRegistry} instance the caller also hands to {@link DialectTranslationStage} —
     * see that field's javadoc.
     */
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
        if (valueShardSpec != null && !valueShardSpec.isBlank()) {
            for (String rule : valueShardSpec.split("\\|")) {
                String[] parts = rule.split(":", 3);
                if (parts.length != 3) {
                    continue;
                }
                int bindIndex = Integer.parseInt(parts[0].trim());
                ShardingStrategy strategy = ShardingStrategy.fromConfig(parts[1].trim(), parts[2].trim());
                valueShardRules.add(new ValueShardRule(bindIndex, strategy));
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
        return new RouterStage(schemaRules, predicateRules, valueShardRules, shardRules, backendRegistry);
    }

    /**
     * Replaces all four rule lists in place, reusing {@link #fromConfig}'s own parsing so a
     * reload can never diverge from what a fresh startup would have produced for the same spec
     * strings — same pattern as {@code BackendRegistry#reload}. Each field is swapped by a single
     * volatile assignment (never partially updated), so a concurrent {@link #handle} call always
     * sees either the fully-old or fully-new rule set for each list, never a mix within one list;
     * across the four lists a caller could in principle observe old schemaRules alongside new
     * predicateRules for one statement, but since every list already independently represents a
     * complete, valid rule set on its own (there's no cross-list invariant to preserve), that's
     * harmless — at worst one statement is routed by a config version that's one write "behind" on
     * a single rule category, never routed by a torn/partial rule.
     */
    public void reconfigure(String schemaSpec, String predicateSpec, String valueShardSpec, String shardTablesSpec) {
        RouterStage fresh = fromConfig(schemaSpec, predicateSpec, valueShardSpec, shardTablesSpec);
        this.schemaRules = fresh.schemaRules;
        this.predicateRules = fresh.predicateRules;
        this.valueShardRules = fresh.valueShardRules;
        this.shardRules = fresh.shardRules;
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

    /**
     * Last resort, checked only after every schema/predicate/value-shard/shard rule above already
     * failed to match: falls back to {@link BackendRegistry#DEFAULT_BACKEND_NAME} when that's the
     * <em>only</em> entry present in {@link #backendRegistry} — which happens precisely when
     * {@code POLYWIRE_BACKENDS} was never configured (single-backend deployment, the default path
     * this fixes — see {@code BackendRegistry#fromConfig}'s three-arg overload, which only ever
     * registers that synthetic name when {@code spec} is unset). In that case there is genuinely
     * only one place any statement could go — the same Postgres connection every frontend already
     * opens for itself via {@code POLYWIRE_PG_*} — so route there instead of leaving
     * {@code targetBackend} {@code null} (which would make {@link DialectTranslationStage} silently
     * skip translation for that single-backend path).
     *
     * <p><b>Deliberately keyed on the name, not just the count.</b> An earlier version of this
     * fallback fired whenever exactly one backend was registered for any reason, including an
     * operator explicitly setting {@code POLYWIRE_BACKENDS} to a single named entry (e.g.
     * {@code "shardb=..."}, no {@code "default"} entry involved at all). That is a real, meaningful
     * "no rule matched" case — a query against an unrelated schema has no business being silently
     * redirected to a shard that was only ever meant for schemas an explicit {@link SchemaRule}
     * names — and live-testing this exact scenario (one {@code POLYWIRE_BACKENDS} entry plus one
     * schema rule) reproduced real misrouting: an unrelated table lookup went to the shard instead
     * of staying on the frontend's own default connection, and failed there with a wrong "relation
     * does not exist" instead of resolving correctly against the actual default backend. Checking
     * the name closes that gap: this fallback only ever fires for the implicit single-backend case
     * {@code BackendRegistry} synthesizes on its own, never for an operator-configured
     * {@code POLYWIRE_BACKENDS}, no matter how many entries it has.
     */
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
