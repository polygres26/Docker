package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.core.QosControlStage;
import com.sayonora.warp.core.StatsCollectorStage;
import com.sayonora.warp.mcp.McpMetricsCollector;
import com.sayonora.warp.telemetry.ObservabilityToggles;
import com.sayonora.warp.telemetry.WarpTelemetry;
import java.util.Locale;
import java.util.Map;

/**
 * Real observability/telemetry status for {@code GET /api/observability} -- the OTLP metrics
 * exporter's actual configuration ({@code com.sayonora.warp.telemetry.WarpTelemetry}) and the
 * always-on Prometheus scrape endpoint ({@code GET /metrics}, {@code MetricsRenderer}), plus a
 * catalog GENERATED from {@code MetricsRenderer}'s own real output ({@link
 * MetricsCatalogGenerator}) -- not a hand-maintained list kept in sync by hand, which could
 * silently drift from what {@code MetricsRenderer} actually emits.
 *
 * <p>Config fields (protocol/endpoint/interval/header count) are read directly from env, NOT via
 * {@code WarpTelemetry.fromEnv()} -- that constructs a real OTLP SDK exporter and meter provider as
 * a side effect (registers instruments, opens a gRPC/HTTP client), so calling this method must
 * never build a second exporter alongside whatever {@code Main} already started at boot.
 *
 * <p>{@code exportVerified}/attempt-and-success counters ARE live now: {@code WarpTelemetry}
 * exposes {@link WarpTelemetry#current()} (the one real instance this process constructed, or
 * {@code null} when OTLP is disabled) and {@link WarpTelemetry#exportHealth()}, which tracks each
 * export's real, async {@code CompletableResultCode} outcome (see {@code WarpTelemetry}'s own
 * {@code ExportHealthTrackingExporter}). {@code verified} means a real export has SUCCEEDED, and
 * recently -- not merely that one was attempted.
 *
 * <p>Both destinations are also admin-toggleable, live, from {@code PATCH /api/observability}
 * ({@link ObservabilityApi}) -- {@link ObservabilityToggles} is the process-wide, {@code
 * warp_config}-persisted override this method reads. {@code prometheus.available} is a real,
 * unconditional live toggle (no network client to construct, so an admin turning it off is
 * exactly as real as turning it on). The OTLP override can only pause/resume an exporter that was
 * already constructed from {@code WARP_OTEL_ENDPOINT} at process startup -- {@code
 * otlp.adminOverrideHasEffect} is {@code false} when OTLP was never configured via env, since no
 * admin toggle can conjure a live exporter/network-client into existence without a restart; this
 * is disclosed to the UI rather than silently implying the toggle always works.
 */
public final class ObservabilitySummary {

    private ObservabilitySummary() {
    }

    public static JsonObject toJson(Map<String, String> env, StatsCollectorStage statsStage,
            QosControlStage qosStage, McpMetricsCollector mcpMetrics) {
        JsonObject out = new JsonObject();

        String protocol = env.getOrDefault("WARP_OTEL_PROTOCOL", "grpc").toLowerCase(Locale.ROOT);
        if (!"http".equals(protocol) && !"grpc".equals(protocol)) {
            protocol = "grpc"; // WarpTelemetry itself falls back the same way on an unrecognized value
        }
        String defaultEndpoint = "http".equals(protocol) ? "http://localhost:4318" : "http://localhost:4317";
        String endpoint = env.getOrDefault("WARP_OTEL_ENDPOINT", defaultEndpoint);
        boolean enabled = !"disabled".equalsIgnoreCase(endpoint);
        long intervalMs = parseLongEnv(env, "WARP_OTEL_EXPORT_INTERVAL_MS", 5_000);
        int headerCount = countHeaders(env.get("WARP_OTEL_HEADERS"));

        JsonObject otlp = new JsonObject();
        otlp.addProperty("enabled", enabled);
        otlp.addProperty("protocol", enabled ? protocol : null);
        otlp.addProperty("endpoint", enabled ? endpoint : null);
        otlp.addProperty("exportIntervalMs", enabled ? intervalMs : null);
        otlp.addProperty("headerCount", headerCount);

        // The live instance, if OTLP is actually enabled AND this call is running inside the real
        // process that constructed it (WarpTelemetry.current() is null in a unit test that never
        // calls WarpTelemetry.fromEnv(), and also null whenever OTLP is disabled) -- never guessed,
        // never a second exporter constructed just to answer this.
        WarpTelemetry live = WarpTelemetry.current();
        otlp.addProperty("adminOverride", ObservabilityToggles.otlpOverride());
        // An admin override can only pause/resume an exporter WARP_OTEL_ENDPOINT already built at
        // boot -- it never has an effect when OTLP was never configured, since there's no live
        // exporter to pause. Surfaced explicitly so the UI can grey out the toggle honestly rather
        // than imply it always works.
        otlp.addProperty("adminOverrideHasEffect", enabled && live != null);
        if (enabled && live != null) {
            WarpTelemetry.ExportHealth health = live.exportHealth();
            otlp.addProperty("exportAttempts", health.attempts());
            otlp.addProperty("exportSuccesses", health.successes());
            otlp.addProperty("exportFailures", health.failures());
            otlp.addProperty("lastExportAt", health.lastAttemptAt() != null ? health.lastAttemptAt().toString() : null);
            otlp.addProperty("lastSuccessAt", health.lastSuccessAt() != null ? health.lastSuccessAt().toString() : null);
            otlp.addProperty("lastError", health.lastError());
            otlp.addProperty("exportVerified", health.verified());
            otlp.addProperty("pausedByAdmin", health.pausedByAdmin());
            // Real per-node identity attached to every export (WarpTelemetry#buildResource) --
            // without this a multi-node deployment's metrics were indistinguishable from each
            // other on the receiving end. Surfaced here so an operator can confirm what identity
            // is actually being sent without needing to inspect a collector's own logs.
            io.opentelemetry.sdk.resources.Resource resource = live.resourceAttributes();
            JsonObject resourceJson = new JsonObject();
            resource.getAttributes().forEach((key, value) -> resourceJson.addProperty(key.getKey(), String.valueOf(value)));
            otlp.add("resource", resourceJson);
        } else {
            otlp.addProperty("exportAttempts", 0);
            otlp.addProperty("exportSuccesses", 0);
            otlp.addProperty("exportFailures", 0);
            otlp.addProperty("lastExportAt", (String) null);
            otlp.addProperty("lastSuccessAt", (String) null);
            otlp.addProperty("lastError", (String) null);
            otlp.addProperty("exportVerified", false);
            otlp.addProperty("pausedByAdmin", false);
            otlp.add("resource", null);
        }
        out.add("otlp", otlp);

        // WarpTelemetry only ever registers metric instruments (counters/histograms/gauges) --
        // confirmed by reading the full class: no trace or log exporter exists anywhere in this
        // codebase. Stated explicitly so the UI doesn't imply broader signal coverage than exists.
        out.addProperty("metricsOnly", true);

        JsonObject prometheus = new JsonObject();
        // A real, unconditional live toggle -- no network client to construct, so an admin
        // disabling/re-enabling this takes effect on the very next scrape, no restart needed.
        prometheus.addProperty("available", ObservabilityToggles.prometheusEnabled());
        prometheus.addProperty("adminOverride", ObservabilityToggles.prometheusOverride());
        prometheus.addProperty("path", "/metrics");
        out.add("prometheus", prometheus);

        // Generated from MetricsRenderer's own real output (the exact text GET /metrics returns),
        // not a hand-maintained list -- see MetricsCatalogGenerator's javadoc for the one real
        // tradeoff (a metric with zero recorded data yet reports an empty label set, not a
        // presumed schema, until real traffic exists to observe it from).
        String rendered = MetricsRenderer.render(statsStage, qosStage, mcpMetrics);
        JsonArray catalog = new JsonArray();
        for (MetricsCatalogGenerator.Entry c : MetricsCatalogGenerator.fromPrometheusText(rendered)) {
            JsonObject e = new JsonObject();
            e.addProperty("name", c.name());
            e.addProperty("type", c.type());
            e.addProperty("description", c.description());
            JsonArray labels = new JsonArray();
            c.labels().forEach(labels::add);
            e.add("labels", labels);
            catalog.add(e);
        }
        out.add("catalog", catalog);

        return out;
    }

    private static long parseLongEnv(Map<String, String> env, String name, long defaultValue) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static int countHeaders(String spec) {
        if (spec == null || spec.isBlank()) {
            return 0;
        }
        int count = 0;
        for (String pair : spec.split(",")) {
            if (pair.indexOf('=') > 0) {
                count++;
            }
        }
        return count;
    }
}
