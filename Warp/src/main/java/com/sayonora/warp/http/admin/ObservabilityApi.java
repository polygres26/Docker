package com.sayonora.warp.http.admin;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sayonora.warp.config.ConfigStore;
import com.sayonora.warp.config.WarpConfig;
import com.sayonora.warp.telemetry.ObservabilityToggles;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

/**
 * {@code PATCH /api/observability}: the admin-settable, {@code warp_config}-persisted enable/
 * disable overrides for the two observability destinations -- the UI/API alternative to hand-
 * editing {@code WARP_OTEL_ENDPOINT} and restarting the process. Mirrors {@link BackendSetsApi
 * #setStoreFrontendSet}'s pattern: read the latest {@code WarpConfig}, write an updated version,
 * then immediately re-apply the live in-process toggle ({@link ObservabilityToggles}) so the
 * effect is visible without waiting for a restart or the cross-node LISTEN/NOTIFY path.
 *
 * <p>Body: {@code {"otlpEnabled": true|false|null, "prometheusEnabled": true|false|null}}. Either
 * key may be omitted or {@code null} to leave that toggle unchanged (not to clear it -- clearing
 * an override entirely isn't exposed by this route today; a disclosed, narrow scope, not an
 * oversight, since "no admin opinion, defer to env" is indistinguishable in practice from "admin
 * explicitly matched today's env-derived default").
 */
public final class ObservabilityApi {

    private static final Object WRITE_LOCK = new Object();

    private ObservabilityApi() {
    }

    public static void handlePatch(HttpServletRequest request, HttpServletResponse response, ConfigStore configStore)
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
                out.add("observability", ObservabilitySummary.toJson(System.getenv()));
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(out.toString());
            }
        } catch (JsonParseException | IllegalStateException | ClassCastException e) {
            error(response, HttpServletResponse.SC_BAD_REQUEST, "invalid request body: " + e.getMessage());
        } catch (SQLException e) {
            error(response, HttpServletResponse.SC_BAD_GATEWAY, e.getMessage());
        }
    }

    /** Reads {@code key} as a tri-state boolean ({@code "true"}/{@code "false"}/absent-or-null
     * keeps {@code fallback}) -- never throws on an unexpected value, matching this codebase's
     * other config-apply methods (a malformed field is ignored, not a 500). */
    private static String boolField(JsonObject body, String key, String fallback) {
        JsonElement e = body.get(key);
        if (e == null || e.isJsonNull()) {
            return fallback;
        }
        try {
            return String.valueOf(e.getAsBoolean());
        } catch (RuntimeException ignored) {
            return fallback;
        }
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
