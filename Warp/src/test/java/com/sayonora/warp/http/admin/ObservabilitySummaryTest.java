package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link ObservabilitySummary} -- confirms it never constructs a real OTLP SDK
 * exporter (unlike {@code WarpTelemetry.fromEnv()}) while still reporting the same enabled/disabled
 * and endpoint-defaulting logic. */
class ObservabilitySummaryTest {

    @Test
    void disabledWhenEndpointIsLiterallyDisabled() {
        JsonObject out = ObservabilitySummary.toJson(Map.of("WARP_OTEL_ENDPOINT", "disabled"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertFalse(otlp.get("enabled").getAsBoolean());
        assertTrue(otlp.get("protocol").isJsonNull());
        assertTrue(otlp.get("endpoint").isJsonNull());
    }

    @Test
    void defaultsToEnabledWithGrpcOn4317WhenNothingIsConfigured() {
        JsonObject out = ObservabilitySummary.toJson(Map.of());
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertTrue(otlp.get("enabled").getAsBoolean(), "WarpTelemetry.fromEnv() itself defaults to enabled");
        assertEquals("grpc", otlp.get("protocol").getAsString());
        assertEquals("http://localhost:4317", otlp.get("endpoint").getAsString());
        assertEquals(5000, otlp.get("exportIntervalMs").getAsLong());
    }

    @Test
    void httpProtocolDefaultsToPort4318() {
        JsonObject out = ObservabilitySummary.toJson(Map.of("WARP_OTEL_PROTOCOL", "http"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertEquals("http://localhost:4318", otlp.get("endpoint").getAsString());
    }

    @Test
    void countsHeadersWithoutExposingTheirValues() {
        JsonObject out = ObservabilitySummary.toJson(Map.of("WARP_OTEL_HEADERS", "api-key=NRAK-xxx,x-other=value"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertEquals(2, otlp.get("headerCount").getAsInt());
        assertFalse(out.toString().contains("NRAK-xxx"), "header values (secrets) must never be echoed back");
    }

    @Test
    void exportVerifiedIsAlwaysFalse() {
        JsonObject out = ObservabilitySummary.toJson(Map.of());
        assertFalse(out.getAsJsonObject("otlp").get("exportVerified").getAsBoolean(),
                "WarpTelemetry has no delivery-confirmation signal -- must never be reported as verified");
    }

    @Test
    void reportsMetricsOnlyAndAnAlwaysAvailablePrometheusEndpoint() {
        JsonObject out = ObservabilitySummary.toJson(Map.of());
        assertTrue(out.get("metricsOnly").getAsBoolean());
        JsonObject prom = out.getAsJsonObject("prometheus");
        assertTrue(prom.get("available").getAsBoolean());
        assertEquals("/metrics", prom.get("path").getAsString());
    }

    @Test
    void catalogListsRealMetricNamesWithLabelsAndIsNonEmpty() {
        JsonObject out = ObservabilitySummary.toJson(Map.of());
        var catalog = out.getAsJsonArray("catalog");
        assertTrue(catalog.size() > 10);
        boolean foundStatements = false;
        for (var e : catalog) {
            JsonObject entry = e.getAsJsonObject();
            if ("warp_statements_total".equals(entry.get("name").getAsString())) {
                foundStatements = true;
                assertEquals("counter", entry.get("type").getAsString());
                assertEquals(1, entry.getAsJsonArray("labels").size());
            }
        }
        assertTrue(foundStatements, "warp_statements_total must be in the catalog");
    }
}
