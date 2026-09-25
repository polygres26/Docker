package com.sayonora.wire.core.connector.splunk;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pure config-validation coverage for {@link SplunkSchemaFactory} -- no real Splunk endpoint
 * needed for these rejection paths. Real search submit/poll/fetch and SPL pushdown are exercised in
 * {@code SplunkSchemaTest} against a real Splunk container instead. */
class SplunkSchemaFactoryTest {

    @Test
    void rejectsMissingEndpoint() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SplunkSchemaFactory.createSchema(Map.of("tables", Map.of("t", Map.of("search", "index=main")))));
        assertTrue(e.getMessage().contains("endpoint"), e.getMessage());
    }

    @Test
    void rejectsMissingTables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SplunkSchemaFactory.createSchema(Map.of("endpoint", "https://host:8089", "tables", Map.of())));
        assertTrue(e.getMessage().contains("table"), e.getMessage());
    }

    @Test
    void rejectsTableWithoutSearchField() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SplunkSchemaFactory.createSchema(
                Map.of("endpoint", "https://host:8089", "tables", Map.of("t", Map.of("pushdownColumns", List.of("host"))))));
        assertTrue(e.getMessage().contains("search"), e.getMessage());
    }
}
