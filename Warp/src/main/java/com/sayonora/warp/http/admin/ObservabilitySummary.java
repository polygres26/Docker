package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Real observability/telemetry status for {@code GET /api/observability} -- the OTLP metrics
 * exporter's actual configuration ({@code com.sayonora.warp.telemetry.WarpTelemetry}) and the
 * always-on Prometheus scrape endpoint ({@code GET /metrics}, {@code MetricsRenderer}), plus a
 * static catalog of the real metric names/types/labels {@code MetricsRenderer} actually emits.
 *
 * <p>Deliberately does NOT call {@code WarpTelemetry.fromEnv()} -- that constructs a real OTLP SDK
 * exporter and meter provider as a side effect (registers instruments, opens a gRPC/HTTP client).
 * This class re-parses the same env vars read-only, mirroring {@code WarpTelemetry.fromEnv()}'s own
 * defaulting logic, so calling {@code GET /api/observability} repeatedly (the admin console polls
 * it) never constructs a second exporter alongside whatever {@code Main} already started.
 *
 * <p>{@code exportVerified} is always {@code false}: {@code WarpTelemetry} is genuinely fire-and-
 * forget today (confirmed by reading the full class) -- {@code PeriodicMetricReader} runs on a
 * timer with no callback, counter or stored timestamp recording whether an export ever reached the
 * collector. This is a real, disclosed gap, not a bug this class works around; the UI must say so
 * rather than imply a health check that doesn't exist.
 */
public final class ObservabilitySummary {

    private ObservabilitySummary() {
    }

    /** Mirrors {@code WarpTelemetry}'s own instrument list (name, OTel type, Prometheus name,
     * description, label names) -- kept here as a separate, disclosed source of truth rather than
     * refactoring {@code MetricsRenderer}'s already-proven {@code render()} method to read from a
     * shared registry; see this class's own javadoc. If a metric is added to {@code WarpTelemetry}/
     * {@code MetricsRenderer}, add it here too. */
    private static final List<CatalogEntry> CATALOG = List.of(
            new CatalogEntry("warp_statements_total", "counter", "Total statements executed.", List.of("tenant")),
            new CatalogEntry("warp_statement_errors_total", "counter", "Total statements that raised a SQLException.", List.of("tenant")),
            new CatalogEntry("warp_statement_latency_seconds_total", "counter", "Cumulative per-statement execution time.", List.of("tenant")),
            new CatalogEntry("warp_pool_connections", "gauge", "Physical backend connections per pool, by state.", List.of("pool", "state")),
            new CatalogEntry("warp_pool_max_size", "gauge", "Configured maximum physical connections for this pool.", List.of("pool")),
            new CatalogEntry("warp_pool_waiting", "gauge", "Frontend sessions currently blocked waiting for a pooled backend connection.", List.of("pool")),
            new CatalogEntry("warp_qos_admitted_total", "counter", "Statements admitted by QosControlStage.", List.of("tenant", "workload_class")),
            new CatalogEntry("warp_qos_rejected_total", "counter", "Statements rejected by QosControlStage (rate limit or pool saturation).", List.of("tenant", "workload_class")),
            new CatalogEntry("warp_protocol_statements_total", "counter", "Statements handled per wire protocol since process start.", List.of("protocol")),
            new CatalogEntry("warp_statements_by_kind_total", "counter", "Statements by read/write/other classification since process start.", List.of("kind")),
            new CatalogEntry("warp_statements_rate", "gauge", "Reads/writes per second, computed since the previous export tick.", List.of("kind")),
            new CatalogEntry("warp_backend_statements_total", "counter", "Statements routed to each backend since process start.", List.of("backend")),
            new CatalogEntry("warp_backend_statement_duration_seconds_total", "counter", "Cumulative execution time of statements routed to each backend.", List.of("backend")),
            new CatalogEntry("warp_rtt_seconds_total", "counter", "Cumulative round-trip time by outcome (cache hit, Postgres read/write).", List.of("protocol", "outcome")),
            new CatalogEntry("warp_rtt_calls_total", "counter", "Call count by outcome, the denominator for the RTT average.", List.of("protocol", "outcome")),
            new CatalogEntry("warp_mcp_tool_calls_total", "counter", "MCP tool invocations, only emitted once an MCP call has been made.", List.of("tool")),
            new CatalogEntry("warp_mcp_tool_errors_total", "counter", "MCP tool invocations that raised an error.", List.of("tool")),
            new CatalogEntry("warp_mcp_tool_latency_seconds_total", "counter", "Cumulative MCP tool execution time.", List.of("tool")));

    private record CatalogEntry(String name, String type, String description, List<String> labels) {
    }

    public static JsonObject toJson(Map<String, String> env) {
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
        // Real, disclosed limitation -- see this class's own javadoc. Never true today.
        otlp.addProperty("exportVerified", false);
        out.add("otlp", otlp);

        // WarpTelemetry only ever registers metric instruments (counters/histograms/gauges) --
        // confirmed by reading the full class: no trace or log exporter exists anywhere in this
        // codebase. Stated explicitly so the UI doesn't imply broader signal coverage than exists.
        out.addProperty("metricsOnly", true);

        JsonObject prometheus = new JsonObject();
        prometheus.addProperty("available", true); // GET /metrics has no on/off switch, no auth gate
        prometheus.addProperty("path", "/metrics");
        out.add("prometheus", prometheus);

        JsonArray catalog = new JsonArray();
        for (CatalogEntry c : CATALOG) {
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
