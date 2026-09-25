package com.sayonora.wire.core.access;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.Yaml;

public final class AccessPolicyYamlConfig {

    public static final String CONFIG_STORE_KEY = "ACCESS_POLICY_YAML";

    private AccessPolicyYamlConfig() {
    }

    public static AccessPolicy load(Path yamlFile) throws IOException {
        try (InputStream in = Files.newInputStream(yamlFile)) {
            return parse(in);
        }
    }

    public static AccessPolicy parse(String yamlText) {
        return parse(new java.io.ByteArrayInputStream(yamlText.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @SuppressWarnings("unchecked")
    static AccessPolicy parse(InputStream yamlContent) {
        Yaml yaml = new Yaml();
        Object root = yaml.load(yamlContent);
        if (root == null) {
            return AccessPolicy.EMPTY;
        }
        if (!(root instanceof Map)) {
            throw new IllegalArgumentException("access policy config: expected a YAML mapping at the top level");
        }
        Map<String, Object> rootMap = (Map<String, Object>) root;

        List<AccessPolicy.ColumnGrant> columnGrants = new ArrayList<>();
        for (Map<String, Object> entry : entries(rootMap.get("column_grants"), "column_grants")) {
            columnGrants.add(parseColumnGrant(entry));
        }

        List<AccessPolicy.RowFilter> rowFilters = new ArrayList<>();
        for (Map<String, Object> entry : entries(rootMap.get("row_filters"), "row_filters")) {
            rowFilters.add(parseRowFilter(entry));
        }

        return new AccessPolicy(columnGrants, rowFilters);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> entries(Object node, String sectionName) {
        if (node == null) {
            return List.of();
        }
        if (!(node instanceof List)) {
            throw new IllegalArgumentException("access policy config: \"" + sectionName + "\" must be a YAML list");
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object entry : (List<Object>) node) {
            if (!(entry instanceof Map)) {
                throw new IllegalArgumentException(
                        "access policy config: each entry under \"" + sectionName + "\" must be a YAML mapping");
            }
            result.add((Map<String, Object>) entry);
        }
        return result;
    }

    private static AccessPolicy.ColumnGrant parseColumnGrant(Map<String, Object> entry) {
        Pattern tablePattern = compile(requireString(entry, "table_pattern", "column_grants"));
        List<String> columns = stringList(entry.get("columns"));
        String requiredAttribute = requireString(entry, "required_attribute", "column_grants");
        List<String> allowedValues = stringList(entry.get("allowed_values"));
        AccessPolicy.OnViolation onViolation = "mask".equalsIgnoreCase(stringValue(entry.get("on_violation")))
                ? AccessPolicy.OnViolation.MASK
                : AccessPolicy.OnViolation.DENY;
        return new AccessPolicy.ColumnGrant(tablePattern, columns, requiredAttribute, allowedValues, onViolation);
    }

    private static AccessPolicy.RowFilter parseRowFilter(Map<String, Object> entry) {
        Pattern tablePattern = compile(requireString(entry, "table_pattern", "row_filters"));
        String filterColumn = requireString(entry, "filter_column", "row_filters");
        String requiredAttribute = requireString(entry, "required_attribute", "row_filters");
        List<String> bypassRoles = stringList(entry.get("bypass_roles"));
        List<String> valuesForUnfiltered = stringList(entry.get("values_for_unfiltered"));
        return new AccessPolicy.RowFilter(tablePattern, filterColumn, requiredAttribute, bypassRoles, valuesForUnfiltered);
    }

    private static String requireString(Map<String, Object> entry, String field, String sectionName) {
        String value = stringValue(entry.get(field));
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "access policy config: \"" + sectionName + "\" entry missing required \"" + field + "\"");
        }
        return value;
    }

    private static Pattern compile(String regex) {
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(Object node) {
        if (node == null) {
            return List.of();
        }
        if (!(node instanceof List)) {
            throw new IllegalArgumentException("access policy config: expected a YAML list of strings");
        }
        List<String> result = new ArrayList<>();
        for (Object item : (List<Object>) node) {
            result.add(String.valueOf(item));
        }
        return result;
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
