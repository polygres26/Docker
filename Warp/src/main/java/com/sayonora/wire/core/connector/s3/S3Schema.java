package com.sayonora.wire.core.connector.s3;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * One S3 (or S3-compatible) bucket/connection plus a fixed set of configured tables, each becoming
 * an {@link S3Table}. {@code pushdownColumns} is per-table, a comma-separated string (e.g.
 * {@code "id,tier"}) -- see {@link S3SchemaFactory}'s javadoc for the full config shape. Owns the
 * {@link ObjectFetcher}'s own client -- {@code SchemaFederationStage} closes it in its own
 * {@code finally}, so no client outlives the query it was built for.
 */
final class S3Schema extends AbstractSchema implements AutoCloseable {

    private final ObjectFetcher fetcher;
    private final Map<String, Table> tables;

    S3Schema(ObjectFetcher fetcher, Map<String, Map<String, String>> tableConfigs) {
        this.fetcher = fetcher;
        Map<String, Table> built = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : tableConfigs.entrySet()) {
            Map<String, String> config = entry.getValue();
            String key = config.get("key");
            String format = config.get("format");
            String recordElement = config.get("recordElement");
            Set<String> pushdownColumns = parsePushdownColumns(config.get("pushdownColumns"));
            if (key == null || format == null) {
                throw new IllegalArgumentException(
                        "S3 table '" + entry.getKey() + "' requires 'key' and 'format' operands");
            }
            built.put(entry.getKey(), new S3Table(fetcher, key, format, recordElement, pushdownColumns));
        }
        this.tables = built;
    }

    private static Set<String> parsePushdownColumns(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }

    @Override
    public void close() {
        if (fetcher instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignoredOnCleanup) {
                // best-effort, same posture as every other connector schema's own close()
            }
        }
    }
}
