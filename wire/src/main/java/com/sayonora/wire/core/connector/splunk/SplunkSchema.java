package com.sayonora.wire.core.connector.splunk;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * One Splunk management endpoint ({@code endpoint} + {@code authHeader} + optional
 * {@code pollTimeoutMs}) plus a fixed set of {@code table name -> {search, pushdownColumns}}
 * entries, each becoming a {@link SplunkTable}. No persistent client of its own ({@link
 * SplunkTable} uses a fresh {@code java.net.http.HttpClient} per request) -- {@code close()} is a
 * no-op, still {@link AutoCloseable} so {@code SchemaFederationStage}'s uniform connector-schema
 * cleanup applies to it like every other connector schema.
 */
final class SplunkSchema extends AbstractSchema implements AutoCloseable {

    private final Map<String, Table> tables;

    @SuppressWarnings("unchecked")
    SplunkSchema(String endpoint, Map<String, Object> tableOperands, String authHeader, Duration pollTimeout) {
        Map<String, Table> built = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : tableOperands.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                throw new IllegalArgumentException(
                        "Splunk table '" + entry.getKey() + "' must be an object with at least a 'search' field");
            }
            Map<String, Object> tableOperand = (Map<String, Object>) entry.getValue();
            String search = stringOperandOrNull(tableOperand, "search");
            if (search == null) {
                throw new IllegalArgumentException("Splunk table '" + entry.getKey() + "' requires string field 'search'");
            }
            var pushdownColumns = stringListOperand(tableOperand, "pushdownColumns");
            built.put(entry.getKey(), new SplunkTable(endpoint, search, authHeader, pushdownColumns, pollTimeout));
        }
        this.tables = built;
    }

    private static String stringOperandOrNull(Map<String, Object> operand, String key) {
        Object value = operand.get(key);
        return value instanceof String ? (String) value : null;
    }

    @SuppressWarnings("unchecked")
    private static java.util.Set<String> stringListOperand(Map<String, Object> operand, String key) {
        Object value = operand.get(key);
        if (!(value instanceof List)) {
            return java.util.Set.of();
        }
        return java.util.Set.copyOf((List<String>) value);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }

    @Override
    public void close() {
        // No persistent client -- see class javadoc.
    }
}
