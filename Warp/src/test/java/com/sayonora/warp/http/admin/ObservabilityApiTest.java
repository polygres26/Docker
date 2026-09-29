package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link ObservabilityApi#boolField}'s tri-state PATCH semantics -- the part
 * that distinguishes "omit this field" (unchanged) from "send it as null" (explicit clear), which
 * is easy to get backwards. Exercised directly against a plain {@link JsonObject} so this doesn't
 * need a real {@code ConfigStore}/Postgres. */
class ObservabilityApiTest {

    @Test
    void anAbsentKeyLeavesTheExistingValueUnchanged() {
        JsonObject body = new JsonObject();
        assertEquals("true", ObservabilityApi.boolField(body, "otlpEnabled", "true"));
        assertNull(ObservabilityApi.boolField(body, "otlpEnabled", null));
    }

    @Test
    void anExplicitNullClearsTheOverrideRegardlessOfTheExistingValue() {
        JsonObject body = new JsonObject();
        body.add("otlpEnabled", com.google.gson.JsonNull.INSTANCE);
        assertNull(ObservabilityApi.boolField(body, "otlpEnabled", "true"));
        assertNull(ObservabilityApi.boolField(body, "otlpEnabled", "false"));
    }

    @Test
    void aRealBooleanSetsTheOverride() {
        JsonObject body = new JsonObject();
        body.addProperty("otlpEnabled", false);
        assertEquals("false", ObservabilityApi.boolField(body, "otlpEnabled", "true"));

        body.addProperty("otlpEnabled", true);
        assertEquals("true", ObservabilityApi.boolField(body, "otlpEnabled", "false"));
    }

    @Test
    void aMalformedValueFallsBackRatherThanThrowing() {
        JsonObject body = new JsonObject();
        body.addProperty("otlpEnabled", "not-a-boolean");
        assertEquals("keep-me", ObservabilityApi.boolField(body, "otlpEnabled", "keep-me"));
    }

    @Test
    void twoIndependentKeysDoNotInterfere() {
        JsonObject body = new JsonObject();
        body.addProperty("otlpEnabled", false);
        // prometheusEnabled is absent entirely -- must be unaffected by otlpEnabled being present.
        assertEquals("false", ObservabilityApi.boolField(body, "otlpEnabled", "true"));
        assertEquals("true", ObservabilityApi.boolField(body, "prometheusEnabled", "true"));
    }
}
