package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
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
}
