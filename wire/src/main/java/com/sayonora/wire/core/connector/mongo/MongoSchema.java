package com.sayonora.wire.core.connector.mongo;

import com.mongodb.client.MongoClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/** Maps the operand's {@code sqlName -> {database, collection, fields}} map to real, per-collection
 * Mongo tables. Owns its {@link MongoClient}: {@code SchemaFederationStage} builds one per federated
 * statement and closes it in its own {@code finally}, so no client/connection pool outlives the
 * query it was built for. */
public final class MongoSchema extends AbstractSchema implements AutoCloseable {

    private final MongoClient client;
    private final Map<String, Map<String, Object>> tableDefs;

    MongoSchema(MongoClient client, Map<String, Map<String, Object>> tableDefs) {
        this.client = client;
        this.tableDefs = Map.copyOf(tableDefs);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new LinkedHashMap<>();
        tableDefs.forEach((sqlName, def) -> {
            String database = (String) def.get("database");
            String collection = (String) def.get("collection");
            List<String> fields = (List<String>) def.get("fields");
            if (database == null || collection == null || fields == null || fields.isEmpty()) {
                throw new IllegalArgumentException("MongoSchemaFactory: table \"" + sqlName
                        + "\" requires 'database', 'collection', and a non-empty 'fields' list");
            }
            tables.put(sqlName, new MongoTable(client.getDatabase(database).getCollection(collection), fields));
        });
        return tables;
    }

    @Override
    public void close() {
        client.close();
    }
}
