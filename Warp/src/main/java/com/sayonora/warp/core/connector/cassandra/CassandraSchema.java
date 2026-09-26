package com.sayonora.warp.core.connector.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Maps the operand's {@code sqlName -> {keyspace, table, partitionKeyEquals, allowFullScan}} map to
 * real Cassandra tables, resolving each one's real column definitions via the driver's own schema
 * metadata -- see {@link CassandraSchemaFactory}'s javadoc for why this connector doesn't need
 * {@code MongoSchema}'s declared-field-list workaround. Owns the {@link CqlSession} -- {@code
 * SchemaFederationStage} closes it in its own {@code finally}.
 */
final class CassandraSchema extends AbstractSchema implements AutoCloseable {

    private final CqlSession session;
    private final Map<String, Map<String, Object>> tableDefs;

    CassandraSchema(CqlSession session, Map<String, Map<String, Object>> tableDefs) {
        this.session = session;
        this.tableDefs = Map.copyOf(tableDefs);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new LinkedHashMap<>();
        tableDefs.forEach((sqlName, def) -> {
            String keyspace = (String) def.get("keyspace");
            String table = (String) def.get("table");
            if (keyspace == null || table == null) {
                throw new IllegalArgumentException(
                        "CassandraSchemaFactory: table \"" + sqlName + "\" requires 'keyspace' and 'table'");
            }
            TableMetadata metadata = session.getMetadata().getKeyspace(keyspace)
                    .flatMap(ks -> ks.getTable(table))
                    .orElseThrow(() -> new IllegalArgumentException(
                            "CassandraSchemaFactory: no such table \"" + keyspace + "." + table + "\""));
            Set<String> partitionKeyEquals = parseCsv((String) def.get("partitionKeyEquals"));
            boolean allowFullScan = Boolean.TRUE.equals(def.get("allowFullScan"));
            tables.put(sqlName, new CassandraTable(session, keyspace, table, metadata, partitionKeyEquals, allowFullScan));
        });
        return tables;
    }

    private static Set<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    @Override
    public void close() {
        session.close();
    }
}
