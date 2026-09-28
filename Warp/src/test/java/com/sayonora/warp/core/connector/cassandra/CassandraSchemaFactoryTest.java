package com.sayonora.warp.core.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure config-validation coverage for {@link CassandraSchemaFactory} -- no real cluster needed for
 * these rejection paths (they all fail before a {@code CqlSession} is ever built). Real column
 * introspection and the full-scan guard's real {@code TableMetadata} check are exercised in
 * {@code CassandraSchemaTest} against a real Cassandra container instead. */
class CassandraSchemaFactoryTest {

    @Test
    void rejectsMissingTables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                CassandraSchemaFactory.createSchema(Map.of(
                        "contactPoints", List.of("localhost:9042"), "localDatacenter", "dc1", "tables", Map.of())));
        assertTrue(e.getMessage().contains("at least one table"), e.getMessage());
    }

    @Test
    void rejectsMissingContactPoints() {
        Map<String, Object> tableDefs = Map.of("orders", Map.of("keyspace", "ks", "table", "orders"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                CassandraSchemaFactory.createSchema(Map.of("localDatacenter", "dc1", "tables", tableDefs)));
        assertTrue(e.getMessage().contains("contactPoints"), e.getMessage());
    }

    @Test
    void rejectsMissingLocalDatacenter() {
        Map<String, Object> tableDefs = Map.of("orders", Map.of("keyspace", "ks", "table", "orders"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                CassandraSchemaFactory.createSchema(Map.of(
                        "contactPoints", List.of("localhost:9042"), "tables", tableDefs)));
        assertTrue(e.getMessage().contains("localDatacenter"), e.getMessage());
    }

    @Test
    void rejectsMalformedContactPointMissingPort() {
        Map<String, Object> tableDefs = Map.of("orders", Map.of("keyspace", "ks", "table", "orders"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                CassandraSchemaFactory.createSchema(Map.of(
                        "contactPoints", List.of("localhost-no-port"), "localDatacenter", "dc1", "tables", tableDefs)));
        assertTrue(e.getMessage().contains("host:port"), e.getMessage());
    }
}
