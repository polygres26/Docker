package com.sayonora.wire.rollup;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.Yaml;

public final class RollupConfig {

    public static final String CONFIG_STORE_KEY = "ROLLUP_DEFINITIONS_YAML";

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern AGGREGATION = Pattern.compile(
            "^(SUM|COUNT|AVG|MIN|MAX)\\s*\\([^()]*\\)\\s+AS\\s+[A-Za-z_][A-Za-z0-9_]*$", Pattern.CASE_INSENSITIVE);

    private RollupConfig() {
    }

    public static List<RollupDefinition> parse(String yamlText) {
        if (yamlText == null || yamlText.isBlank()) {
            return List.of();
        }
        return parse(new java.io.ByteArrayInputStream(yamlText.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @SuppressWarnings("unchecked")
    static List<RollupDefinition> parse(java.io.InputStream yamlContent) {
        Yaml yaml = new Yaml();
        Object root = yaml.load(yamlContent);
        if (root == null) {
            return List.of();
        }
        if (!(root instanceof Map)) {
            throw new IllegalArgumentException("rollup config: expected a YAML mapping at the top level");
        }
        Map<String, Object> rootMap = (Map<String, Object>) root;
        Object rollupsNode = rootMap.get("rollups");
        if (rollupsNode == null) {
            return List.of();
        }
        if (!(rollupsNode instanceof List)) {
            throw new IllegalArgumentException("rollup config: \"rollups\" must be a YAML list");
        }
        List<RollupDefinition> definitions = new ArrayList<>();
        Set<String> seenNames = new LinkedHashSet<>();
        for (Object entryObj : (List<Object>) rollupsNode) {
            if (!(entryObj instanceof Map)) {
                throw new IllegalArgumentException("rollup config: each entry under \"rollups\" must be a YAML mapping");
            }
            RollupDefinition def = parseOne((Map<String, Object>) entryObj);
            if (!seenNames.add(def.name())) {
                throw new IllegalArgumentException("rollup config: duplicate rollup name \"" + def.name() + "\"");
            }
            definitions.add(def);
        }
        return definitions;
    }

    private static RollupDefinition parseOne(Map<String, Object> entry) {
        String name = requireString(entry, "name");
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "rollup config: \"name\" must be a plain identifier (letters/digits/underscore, not starting "
                            + "with a digit) — got \"" + name + "\"");
        }
        String backend = requireString(entry, "backend");
        String sourceTable = requireString(entry, "source_table");
        List<String> groupBy = stringList(entry.get("group_by"));
        if (groupBy.isEmpty()) {
            throw new IllegalArgumentException("rollup config: rollup \"" + name + "\" has no \"group_by\" columns");
        }
        List<String> aggregations = stringList(entry.get("aggregations"));
        for (String agg : aggregations) {
            if (!AGGREGATION.matcher(agg.trim()).matches()) {
                throw new IllegalArgumentException("rollup config: rollup \"" + name + "\" has an invalid aggregation "
                        + "expression \"" + agg + "\" — expected \"SUM|COUNT|AVG|MIN|MAX(expr) AS alias\"");
            }
        }
        int refreshIntervalMinutes = requirePositiveInt(entry, "refresh_interval_minutes", name);
        int maxStalenessMinutes = requirePositiveInt(entry, "max_staleness_minutes", name);
        return new RollupDefinition(name, backend, sourceTable, groupBy, aggregations,
                refreshIntervalMinutes, maxStalenessMinutes);
    }

    private static String requireString(Map<String, Object> entry, String field) {
        Object value = entry.get(field);
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException("rollup config: entry missing required \"" + field + "\"");
        }
        return String.valueOf(value);
    }

    private static int requirePositiveInt(Map<String, Object> entry, String field, String rollupName) {
        Object value = entry.get(field);
        if (!(value instanceof Number number) || number.intValue() <= 0) {
            throw new IllegalArgumentException(
                    "rollup config: rollup \"" + rollupName + "\" needs a positive integer \"" + field + "\"");
        }
        return number.intValue();
    }

    /** The inverse of {@link #parse} -- serializes a full definition list back into the same
     * {@code rollups:} YAML document shape, in list order. Exposed (not just used internally) so
     * other admin-surface code that needs to rebuild this exact document -- e.g. {@code
     * MetricsServer}'s rollup-suggestion draft endpoint, which merges one LLM-proposed definition
     * into the REST of the current definitions unchanged -- shares this one serializer instead of
     * a second, possibly-drifting copy of the YAML shape. Round-trips through {@link #parse}: the
     * shape produced here is exactly what that method accepts. */
    public static String toYaml(List<RollupDefinition> definitions) {
        Map<String, Object> root = new java.util.LinkedHashMap<>();
        List<Object> rollups = new ArrayList<>();
        for (RollupDefinition def : definitions) {
            Map<String, Object> entry = new java.util.LinkedHashMap<>();
            entry.put("name", def.name());
            entry.put("backend", def.backendName());
            entry.put("source_table", def.sourceTable());
            entry.put("group_by", def.groupByColumns());
            entry.put("aggregations", def.aggregations());
            entry.put("refresh_interval_minutes", def.refreshIntervalMinutes());
            entry.put("max_staleness_minutes", def.maxStalenessMinutes());
            rollups.add(entry);
        }
        root.put("rollups", rollups);
        var options = new org.yaml.snakeyaml.DumperOptions();
        options.setDefaultFlowStyle(org.yaml.snakeyaml.DumperOptions.FlowStyle.BLOCK);
        return new Yaml(options).dump(root);
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(Object node) {
        if (node == null) {
            return List.of();
        }
        if (!(node instanceof List)) {
            throw new IllegalArgumentException("rollup config: expected a YAML list of strings");
        }
        List<String> result = new ArrayList<>();
        for (Object item : (List<Object>) node) {
            result.add(String.valueOf(item));
        }
        return result;
    }
}
