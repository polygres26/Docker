package com.sayonora.warp.core.connector.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/** Maps the operand's {@code sqlName -> {topic, format, fields}} map to real Kafka tables. No
 * persistent client of its own -- {@link KafkaTable} opens a fresh consumer per scan (see its own
 * javadoc) -- so {@code close()} is a no-op; still {@link AutoCloseable} so {@code
 * SchemaFederationStage}'s uniform connector-schema cleanup applies to it like every other
 * connector schema. */
final class KafkaSchema extends AbstractSchema implements AutoCloseable {

    private final String bootstrapServers;
    private final Map<String, Map<String, Object>> tableDefs;

    KafkaSchema(String bootstrapServers, Map<String, Map<String, Object>> tableDefs) {
        this.bootstrapServers = bootstrapServers;
        this.tableDefs = Map.copyOf(tableDefs);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new LinkedHashMap<>();
        tableDefs.forEach((sqlName, def) -> {
            String topic = (String) def.get("topic");
            if (topic == null) {
                throw new IllegalArgumentException("KafkaSchemaFactory: table \"" + sqlName + "\" requires 'topic'");
            }
            String format = def.get("format") != null ? (String) def.get("format") : "json";
            List<String> fields = (List<String>) def.get("fields");
            if ("json".equals(format) && (fields == null || fields.isEmpty())) {
                throw new IllegalArgumentException("KafkaSchemaFactory: table \"" + sqlName
                        + "\" with format \"json\" requires a non-empty 'fields' list");
            }
            tables.put(sqlName, new KafkaTable(bootstrapServers, topic, format, fields == null ? List.of() : fields));
        });
        return tables;
    }

    @Override
    public void close() {
        // No persistent client -- see class javadoc.
    }
}
