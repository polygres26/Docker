package com.sayonora.wire.core.connector.dynamodb;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Maps the operand's {@code sqlName -> {table, fields}} map to real DynamoDB tables. Owns its
 * {@link DynamoDbClient} -- {@code SchemaFederationStage} builds one per federated statement and
 * closes it in its own {@code finally}, so no client (and its HTTP connection pool) outlives the
 * query it was built for. */
public final class DynamoSchema extends AbstractSchema implements AutoCloseable {

    private final DynamoDbClient client;
    private final Map<String, Map<String, Object>> tableDefs;

    DynamoSchema(DynamoDbClient client, Map<String, Map<String, Object>> tableDefs) {
        this.client = client;
        this.tableDefs = Map.copyOf(tableDefs);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new LinkedHashMap<>();
        tableDefs.forEach((sqlName, def) -> {
            String tableName = (String) def.get("table");
            List<String> fields = (List<String>) def.get("fields");
            if (tableName == null || fields == null || fields.isEmpty()) {
                throw new IllegalArgumentException("DynamoSchemaFactory: table \"" + sqlName
                        + "\" requires 'table' and a non-empty 'fields' list");
            }
            tables.put(sqlName, new DynamoTable(client, tableName, fields));
        });
        return tables;
    }

    @Override
    public void close() {
        client.close();
    }
}
