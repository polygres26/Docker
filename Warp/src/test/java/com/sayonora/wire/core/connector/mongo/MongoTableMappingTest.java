package com.sayonora.wire.core.connector.mongo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

/** Pure row/type-mapping coverage for {@link MongoTable} plus credential splicing -- no container. */
class MongoTableMappingTest {

    @Test
    void stringifiesHeterogeneousBsonValuesViaStringValueOf() {
        ObjectId oid = new ObjectId("65f000000000000000000001");
        Document doc = new Document("_id", oid).append("n", 42).append("d", 1.5).append("b", false)
                .append("s", "text").append("nested", new Document("k", "v"));
        Object[] row = MongoTable.toRow(doc, List.of("_id", "n", "d", "b", "s", "missing"));
        assertArrayEquals(new Object[] {"65f000000000000000000001", "42", "1.5", "false", "text", null}, row);
        String nested = (String) MongoTable.toRow(doc, List.of("nested"))[0];
        assertTrue(nested.contains("k") && nested.contains("v"), nested);
    }

    @Test
    void splicesUrlEncodedCredentialsIntoConnectionString() {
        assertEquals("mongodb://u%40x:p%3Aw@h:1/db?authSource=admin",
                MongoSchemaFactory.withCredentials("mongodb://h:1/db?authSource=admin", "u@x", "p:w"));
        assertEquals("mongodb://h:1/db", MongoSchemaFactory.withCredentials("mongodb://h:1/db", null, "ignored"));
        assertEquals("mongodb://a:b@h:1/db", MongoSchemaFactory.withCredentials("mongodb://a:b@h:1/db", "u", "p"),
                "a connection string that already carries credentials is left alone");
    }

    @Test
    void factoryRejectsMissingTablesAndMissingDatabase() {
        assertThrows(IllegalArgumentException.class, () -> MongoSchemaFactory.createSchema(
                Map.of("connectionString", "mongodb://h:1/db", "tables", Map.of())));
        java.util.Map<String, Object> noDb = new java.util.HashMap<>();
        noDb.put("database", null);
        noDb.put("collection", "c");
        noDb.put("fields", List.of("a"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> MongoSchemaFactory.createSchema(
                Map.of("connectionString", "mongodb://h:1", "tables", Map.of("c", noDb))));
        assertTrue(e.getMessage().contains("DATABASE_NAME"), e.getMessage());
    }
}
