package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sayonora.warp.core.BackendRegistry;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InterfaceRegistryTest {

    @AfterEach
    void reset() {
        InterfaceRegistry.clear();
    }

    @Test
    void listsOnlyRegisteredFrontendsWithRequestCountsWhenKnown() {
        InterfaceRegistry.register("pgwire", "PostgreSQL", "sql", "PostgreSQL wire", 15432, "Relay", null, "pgwire");
        InterfaceRegistry.register("a2a", "A2A agent", "mcp", "Agent2Agent JSON-RPC", 18030, null, null, null);
        JsonObject out = InterfaceRegistry.toJson(null, Map.of("pgwire", 42L), 3);
        assertEquals(3, out.get("activeSessions").getAsInt());
        var arr = out.getAsJsonArray("interfaces");
        assertEquals(2, arr.size());
        JsonObject first = arr.get(0).getAsJsonObject();
        assertEquals("pgwire", first.get("id").getAsString());
        assertEquals(42L, first.get("requests").getAsLong());
        assertEquals("Relay", first.get("mode").getAsString());
        JsonObject second = arr.get(1).getAsJsonObject();
        assertTrue(second.get("requests").isJsonNull(), "no metrics key means the count is unknown, not zero");
        InterfaceRegistry.register("sqswire", "SQS", "api", "SQS", 9324, "Emulate", null, "sqswire");
        JsonObject sqs = InterfaceRegistry.toJson(null, Map.of("pgwire", 42L), 0).getAsJsonArray("interfaces").get(1).getAsJsonObject();
        assertEquals("sqswire", sqs.get("id").getAsString(), "sql first, then api, then mcp");
        assertEquals(0L, sqs.get("requests").getAsLong(), "a keyed protocol with no entry has served nothing yet");
    }

    @Test
    void statusIsPlainListeningWhenNotStoreBacked() {
        InterfaceRegistry.register("pgwire", "PostgreSQL", "sql", "PostgreSQL wire", 15432, "Relay", null, "pgwire");
        JsonObject out = InterfaceRegistry.toJson(null, null, 0).getAsJsonArray("interfaces").get(0).getAsJsonObject();
        assertEquals("listening", out.get("status").getAsString());
    }

    @Test
    void statusFlagsAStoreConfiguredButNotHostedAnywhere() {
        InterfaceRegistry.register("s3wire", "S3", "api", "S3", 18000, "Emulate", "s3", "s3wire");
        BackendRegistry registry = BackendRegistry.fromConfig("pg=jdbc:postgresql://h:5432/db|u|p", null);
        JsonObject out = InterfaceRegistry.toJson(registry, null, 0).getAsJsonArray("interfaces").get(0).getAsJsonObject();
        assertEquals("listening_no_store", out.get("status").getAsString(),
                "s3 is not enabled on any backend, so the frontend is quietly on the legacy default-backend fallback");
    }

    @Test
    void statusIsPlainListeningWhenTheStoreIsActuallyHosted() {
        InterfaceRegistry.register("s3wire", "S3", "api", "S3", 18000, "Emulate", "s3", "s3wire");
        BackendRegistry registry = BackendRegistry.fromConfig("pg=jdbc:postgresql://h:5432/db|u|p", null);
        registry.applyStoreConfig("pg=s3", null);
        JsonObject out = InterfaceRegistry.toJson(registry, null, 0).getAsJsonArray("interfaces").get(0).getAsJsonObject();
        assertEquals("listening", out.get("status").getAsString());
    }

    @Test
    void joinsAccessSummaryAuthOntoAKnownFrontend() {
        InterfaceRegistry.register("s3wire", "S3", "api", "S3", 18000, "Emulate", "s3", "s3wire");
        JsonObject out = InterfaceRegistry.toJson(null, null, 0).getAsJsonArray("interfaces").get(0).getAsJsonObject();
        assertEquals("aws-sigv4", out.get("authMethod").getAsString());
        assertTrue(out.get("authEnforced").getAsBoolean());
        assertTrue(out.has("authDetail"));
    }

    @Test
    void omitsAuthFieldsForAFrontendAccessSummaryDoesNotCover() {
        InterfaceRegistry.register("a2a", "A2A agent", "mcp", "Agent2Agent JSON-RPC", 18030, null, null, null);
        JsonObject out = InterfaceRegistry.toJson(null, null, 0).getAsJsonArray("interfaces").get(0).getAsJsonObject();
        assertTrue(!out.has("authMethod"), "a2a has no AccessSummary entry -- must be omitted, never guessed");
    }
}
