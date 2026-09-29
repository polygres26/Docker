package com.sayonora.warp.http.admin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.core.QosControlStage;
import com.sayonora.warp.core.StatsCollectorStage;
import com.sayonora.warp.mcp.McpMetricsCollector;
import com.sayonora.warp.telemetry.ObservabilityToggles;
import com.sayonora.warp.telemetry.OtlpConnectivityTest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code PATCH /api/observability}: the admin-settable, {@code warp_config}-persisted enable/
 * disable overrides for the two observability destinations -- the UI/API alternative to hand-
 * editing {@code WARP_OTEL_ENDPOINT} and restarting the process. Mirrors {@link BackendSetsApi
 * #setStoreFrontendSet}'s pattern: read the latest {@code WarpConfig}, write an updated version,
 * then immediately re-apply the live in-process toggle ({@link ObservabilityToggles}) so the
 * effect is visible without waiting for a restart or the cross-node LISTEN/NOTIFY path.
 *
 * <p>Body: {@code {"otlpEnabled": true|false|null, "prometheusEnabled": true|false|null}}. A key
 * that is OMITTED entirely leaves that toggle unchanged; a key explicitly present with value
 * {@code null} CLEARS the override (back to "no admin opinion, defer to the env-var-derived
 * default"). These are deliberately different: {@code {}} is a no-op PATCH, {@code {"otlpEnabled":
 * null}} is a real, intentional "forget my previous choice."
 */
public final class ObservabilityApi {

    private static final Object WRITE_LOCK = new Object();

    private ObservabilityApi() {
    }

    public static void handlePatch(HttpServletRequest request, HttpServletResponse response, ConfigStore configStore,
            StatsCollectorStage statsStage, QosControlStage qosStage, McpMetricsCollector mcpMetrics)
            throws IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            JsonObject body = readBody(request);
            synchronized (WRITE_LOCK) {
                WarpConfig before = configStore.readLatest()
                        .map(ConfigStore.Version::payload)
                        .orElseGet(WarpConfig::fromEnvDefaults);
                String otlpOverride = boolField(body, "otlpEnabled", before.otlpExportOverride());
                String prometheusOverride = boolField(body, "prometheusEnabled", before.prometheusScrapeOverride());
                long version = configStore.write(before.withObservabilityOverrides(otlpOverride, prometheusOverride));
                ObservabilityToggles.apply(otlpOverride, prometheusOverride);

                JsonObject out = new JsonObject();
                out.addProperty("ok", true);
                out.addProperty("version", version);
                out.add("observability", ObservabilitySummary.toJson(System.getenv(), statsStage, qosStage, mcpMetrics));
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(out.toString());
            }
        } catch (JsonParseException | IllegalStateException | ClassCastException e) {
            error(response, HttpServletResponse.SC_BAD_REQUEST, "invalid request body: " + e.getMessage());
        } catch (SQLException e) {
            error(response, HttpServletResponse.SC_BAD_GATEWAY, e.getMessage());
        }
    }

    private static final Duration TEST_CONNECTION_TIMEOUT = Duration.ofSeconds(5);

    /**
     * {@code POST /api/observability/test-connection}: runs {@link OtlpConnectivityTest} against a
     * candidate protocol/endpoint/headers -- a REAL test export (see that class's own javadoc for
     * why this is a genuine network probe, not a bare socket check), never against the live
     * configured exporter's own counters. Body: {@code {"protocol": "grpc"|"http", "endpoint":
     * "...", "headers": "key=value,key2=value2"}}; {@code endpoint} is required, the rest default
     * to {@code grpc} and no headers. Always responds 200 with the real result (success or not) --
     * a failed connectivity test is a normal, expected outcome, not a server error.
     */
    public static void handleTestConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=utf-8");
        try {
            JsonObject body = readBody(request);
            String endpoint = str(body, "endpoint");
            if (endpoint == null || endpoint.isBlank()) {
                error(response, HttpServletResponse.SC_BAD_REQUEST, "\"endpoint\" is required");
                return;
            }
            String protocol = str(body, "protocol");
            Map<String, String> headers = parseHeaderSpec(str(body, "headers"));

            OtlpConnectivityTest.Result result = OtlpConnectivityTest.test(
                    protocol == null ? "grpc" : protocol, endpoint, headers, TEST_CONNECTION_TIMEOUT);

            JsonObject out = new JsonObject();
            out.addProperty("success", result.success());
            out.addProperty("tookMs", result.tookMs());
            out.addProperty("protocol", result.protocol());
            out.addProperty("endpoint", result.endpoint());
            out.addProperty("errorMessage", result.errorMessage());
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(out.toString());
        } catch (JsonParseException | IllegalStateException | ClassCastException e) {
            error(response, HttpServletResponse.SC_BAD_REQUEST, "invalid request body: " + e.getMessage());
        }
    }

    private static String str(JsonObject body, String key) {
        JsonElement e = body.get(key);
        return e == null || e.isJsonNull() ? null : e.getAsString();
    }

    /** Same {@code key=value,key2=value2} grammar {@code WARP_OTEL_HEADERS} uses -- deliberately
     * lenient (skip a malformed entry, never throw) since a candidate value being tested is, by
     * definition, not yet known-good. */
    private static Map<String, String> parseHeaderSpec(String spec) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (spec == null || spec.isBlank()) {
            return headers;
        }
        for (String pair : spec.split(",")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                headers.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
            }
        }
        return headers;
    }

    /** Reads {@code key} as a tri-state boolean override. Absent key: unchanged (returns {@code
     * fallback}). Present and {@code null}: an explicit CLEAR (returns {@code null} -- "no admin
     * opinion, defer to env"). Present and a real boolean: sets the override. Never throws on an
     * unexpected value (falls back to {@code fallback}, matching this codebase's other config-apply
     * methods -- a malformed field is ignored, not a 500). Package-private (not private) so a unit
     * test can exercise the tri-state parsing directly, without a real ConfigStore/Postgres. */
    static String boolField(JsonObject body, String key, String fallback) {
        if (!body.has(key)) {
            return fallback;
        }
        JsonElement e = body.get(key);
        if (e.isJsonNull()) {
            return null;
        }
        // Deliberately NOT e.getAsBoolean() -- Gson's JsonPrimitive#getAsBoolean() silently treats
        // ANY non-"true" string (e.g. "not-a-boolean") as false rather than throwing, which would
        // turn a malformed request body into a real, silent "disable" instead of a no-op. Only the
        // exact JSON booleans (or the exact strings "true"/"false") are accepted.
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isBoolean()) {
            return String.valueOf(e.getAsBoolean());
        }
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isString()) {
            String s = e.getAsString();
            if ("true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s)) {
                return s.toLowerCase(java.util.Locale.ROOT);
            }
        }
        return fallback;
    }

    private static JsonObject readBody(HttpServletRequest request) throws IOException {
        JsonElement e = JsonParser.parseReader(request.getReader());
        if (e == null || !e.isJsonObject()) {
            throw new JsonParseException("a JSON object body is required");
        }
        return e.getAsJsonObject();
    }

    private static void error(HttpServletResponse response, int status, String message) throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("error", message);
        response.setStatus(status);
        response.getWriter().write(o.toString());
    }
}
