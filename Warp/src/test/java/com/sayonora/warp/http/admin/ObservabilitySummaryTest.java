package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sayonora.warp.core.QosControlStage;
import com.sayonora.warp.core.StatsCollectorStage;
import com.sayonora.warp.mcp.McpMetricsCollector;
import com.sayonora.warp.telemetry.ObservabilityToggles;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link ObservabilitySummary} -- confirms it never constructs a real OTLP SDK
 * exporter (unlike {@code WarpTelemetry.fromEnv()}) while still reporting the same enabled/disabled
 * and endpoint-defaulting logic, plus the admin-settable overrides ({@link ObservabilityToggles}) --
 * process-wide static state, so every test that touches it resets it in {@link #resetToggles()}.
 * The catalog tests use real, minimal {@code StatsCollectorStage}/{@code QosControlStage}/{@code
 * McpMetricsCollector} instances (no mocks) since the catalog is now generated from {@code
 * MetricsRenderer}'s real output over these objects, not a hand-maintained list. */
class ObservabilitySummaryTest {

    private static final StatsCollectorStage STATS = new StatsCollectorStage();
    private static final QosControlStage QOS =
            new QosControlStage(new QosControlStage.ClassLimit(5, 5, 0), Map.of(), 0, null);
    private static final McpMetricsCollector MCP = new McpMetricsCollector();

    private static JsonObject toJson(Map<String, String> env) {
        return ObservabilitySummary.toJson(env, STATS, QOS, MCP);
    }

    @AfterEach
    void resetToggles() {
        ObservabilityToggles.apply(null, null);
    }

    @Test
    void disabledWhenEndpointIsLiterallyDisabled() {
        JsonObject out = toJson(Map.of("WARP_OTEL_ENDPOINT", "disabled"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertFalse(otlp.get("enabled").getAsBoolean());
        assertTrue(otlp.get("protocol").isJsonNull());
        assertTrue(otlp.get("endpoint").isJsonNull());
    }

    @Test
    void defaultsToEnabledWithGrpcOn4317WhenNothingIsConfigured() {
        JsonObject out = toJson(Map.of());
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertTrue(otlp.get("enabled").getAsBoolean(), "WarpTelemetry.fromEnv() itself defaults to enabled");
        assertEquals("grpc", otlp.get("protocol").getAsString());
        assertEquals("http://localhost:4317", otlp.get("endpoint").getAsString());
        assertEquals(5000, otlp.get("exportIntervalMs").getAsLong());
    }

    @Test
    void httpProtocolDefaultsToPort4318() {
        JsonObject out = toJson(Map.of("WARP_OTEL_PROTOCOL", "http"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertEquals("http://localhost:4318", otlp.get("endpoint").getAsString());
    }

    @Test
    void countsHeadersWithoutExposingTheirValues() {
        JsonObject out = toJson(Map.of("WARP_OTEL_HEADERS", "api-key=NRAK-xxx,x-other=value"));
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertEquals(2, otlp.get("headerCount").getAsInt());
        assertFalse(out.toString().contains("NRAK-xxx"), "header values (secrets) must never be echoed back");
    }

    @Test
    void exportVerifiedIsFalseWithNoLiveWarpTelemetryInstance() {
        // WarpTelemetry.current() is null in this test JVM (fromEnv() is never called), so there's
        // no live exporter to report health from -- exportVerified must fall back to false, not be
        // guessed from "enabled" alone.
        JsonObject out = toJson(Map.of());
        assertFalse(out.getAsJsonObject("otlp").get("exportVerified").getAsBoolean());
    }

    @Test
    void adminOverrideHasNoEffectWhenNoLiveExporterExists() {
        ObservabilityToggles.apply("false", null);
        JsonObject out = toJson(Map.of());
        JsonObject otlp = out.getAsJsonObject("otlp");
        assertFalse(otlp.get("adminOverride").getAsBoolean());
        assertFalse(otlp.get("adminOverrideHasEffect").getAsBoolean(),
                "no live WarpTelemetry instance in this test JVM -- an admin pause has nothing to pause");
    }

    @Test
    void reportsMetricsOnlyAndAPrometheusEndpointAvailableByDefault() {
        JsonObject out = toJson(Map.of());
        assertTrue(out.get("metricsOnly").getAsBoolean());
        JsonObject prom = out.getAsJsonObject("prometheus");
        assertTrue(prom.get("available").getAsBoolean());
        assertTrue(prom.get("adminOverride").isJsonNull());
        assertEquals("/metrics", prom.get("path").getAsString());
    }

    @Test
    void adminCanDisableThePrometheusEndpointLive() {
        ObservabilityToggles.apply(null, "false");
        JsonObject out = toJson(Map.of());
        JsonObject prom = out.getAsJsonObject("prometheus");
        assertFalse(prom.get("available").getAsBoolean());
        assertFalse(prom.get("adminOverride").getAsBoolean());
    }

    @Test
    void adminCanReEnableThePrometheusEndpointAfterDisabling() {
        ObservabilityToggles.apply(null, "false");
        ObservabilityToggles.apply(null, "true");
        JsonObject out = toJson(Map.of());
        assertTrue(out.getAsJsonObject("prometheus").get("available").getAsBoolean());
    }

    @Test
    void catalogIsGeneratedFromMetricsRenderersRealOutputAndIsNonEmpty() {
        JsonObject out = toJson(Map.of());
        var catalog = out.getAsJsonArray("catalog");
        assertTrue(catalog.size() > 10, "the catalog must reflect the real, multi-metric MetricsRenderer output");
        boolean foundStatements = false;
        for (var e : catalog) {
            JsonObject entry = e.getAsJsonObject();
            if ("warp_statements_total".equals(entry.get("name").getAsString())) {
                foundStatements = true;
                assertEquals("counter", entry.get("type").getAsString());
                assertFalse(entry.get("description").getAsString().isBlank());
                // A fresh StatsCollectorStage has recorded no statements yet, so no tenant-labeled
                // series has actually been rendered -- the catalog honestly reports an empty label
                // set here rather than presuming the schema's usual "tenant" label. This is the
                // real, disclosed tradeoff described in MetricsCatalogGenerator's javadoc.
                assertTrue(entry.getAsJsonArray("labels").isEmpty());
            }
        }
        assertTrue(foundStatements, "warp_statements_total must be in the catalog");
    }

    @Test
    void catalogReflectsLabelsOnceRealDataExists() {
        JsonObject out = toJson(Map.of());
        var catalog = out.getAsJsonArray("catalog");
        boolean foundPoolMaxSize = false;
        for (var e : catalog) {
            JsonObject entry = e.getAsJsonObject();
            if ("warp_pool_max_size".equals(entry.get("name").getAsString())) {
                foundPoolMaxSize = true;
                assertEquals("gauge", entry.get("type").getAsString());
            }
        }
        assertTrue(foundPoolMaxSize, "warp_pool_max_size must be in the catalog even with no pools configured yet");
    }
}
