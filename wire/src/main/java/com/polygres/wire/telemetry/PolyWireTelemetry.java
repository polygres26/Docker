package com.polygres.wire.telemetry;

import com.polygres.wire.core.BackendConnectionPools;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PolyWireTelemetry {

    private static final Logger log = LoggerFactory.getLogger(PolyWireTelemetry.class);
    private static final AttributeKey<String> TENANT = AttributeKey.stringKey("tenant");
    private static final AttributeKey<String> POOL = AttributeKey.stringKey("pool");
    private static final AttributeKey<String> STATE = AttributeKey.stringKey("state");
    private static final AttributeKey<String> WORKLOAD_CLASS = AttributeKey.stringKey("workload_class");

    private final LongCounter statementCounter;
    private final LongCounter errorCounter;
    private final DoubleHistogram latencyHistogram;
    private final LongCounter qosAdmittedCounter;
    private final LongCounter qosRejectedCounter;
    private final Meter meter;

    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("protocol");
    private static final AttributeKey<String> BACKEND = AttributeKey.stringKey("backend");
    private static final AttributeKey<String> KIND = AttributeKey.stringKey("kind");

    private PolyWireTelemetry(String protocol, String otlpEndpoint, long exportIntervalMs,
            java.util.Map<String, String> headers) {
        if (!"http".equalsIgnoreCase(protocol) && !"grpc".equalsIgnoreCase(protocol)) {
            log.warn("POLYWIRE_OTEL_PROTOCOL={} not recognized (expected 'grpc' or 'http'); defaulting to grpc", protocol);
            protocol = "grpc";
        }
        MetricExporter exporter = buildExporter(protocol, otlpEndpoint, headers);
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(exporter)
                        .setInterval(Duration.ofMillis(exportIntervalMs))
                        .build())
                .build();
        Meter meter = meterProvider.meterBuilder("com.polygres.wire").setInstrumentationVersion("0.1.0").build();

        this.statementCounter = meter.counterBuilder("polywire.statements")
                .setDescription("Total statements executed.").build();
        this.errorCounter = meter.counterBuilder("polywire.statement.errors")
                .setDescription("Total statements that raised a SQLException.").build();
        this.latencyHistogram = meter.histogramBuilder("polywire.statement.duration")
                .setDescription("Per-statement execution time.").setUnit("s").build();
        this.qosAdmittedCounter = meter.counterBuilder("polywire.qos.admitted")
                .setDescription("Statements admitted by QosControlStage.").build();
        this.qosRejectedCounter = meter.counterBuilder("polywire.qos.rejected")
                .setDescription("Statements rejected by QosControlStage (rate limit or pool saturation).").build();

        meter.gaugeBuilder("polywire.pool.connections").ofLongs()
                .setDescription("Physical backend connections per pool, by state.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.activeConnections(), Attributes.of(POOL, pool.poolKey(), STATE, "active"));
                        measurement.record(pool.idleConnections(), Attributes.of(POOL, pool.poolKey(), STATE, "idle"));
                    }
                });
        meter.gaugeBuilder("polywire.pool.waiting").ofLongs()
                .setDescription("Frontend sessions currently blocked waiting for a pooled backend connection.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.threadsAwaitingConnection(), Attributes.of(POOL, pool.poolKey()));
                    }
                });
        meter.gaugeBuilder("polywire.pool.max_size").ofLongs()
                .setDescription("Configured maximum physical connections for this pool.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.maxPoolSize(), Attributes.of(POOL, pool.poolKey()));
                    }
                });

        this.meter = meter;
        log.info("OpenTelemetry metrics export enabled: OTLP/{} to {} every {}ms{}", protocol.toUpperCase(),
                otlpEndpoint, exportIntervalMs, headers.isEmpty() ? "" : " (" + headers.size() + " header(s) attached)");
    }

    /**
     * gRPC is the default -- it's what {@code POLYWIRE_OTEL_ENDPOINT}'s historical default
     * ({@code http://localhost:4317}) pointed at, and it's the lower-overhead choice when nothing
     * forces the alternative. HTTP is the fallback for the environments gRPC doesn't reach:
     * corporate proxies and L7 load balancers that only forward plain HTTP/HTTPS, and some
     * serverless/sidecar setups that don't support long-lived HTTP/2 streams. Both exporters are
     * already on the classpath via the single {@code opentelemetry-exporter-otlp} dependency, so
     * this is a same-jar sibling-class swap, not a new integration.
     */
    private static MetricExporter buildExporter(String protocol, String endpoint, java.util.Map<String, String> headers) {
        if ("http".equalsIgnoreCase(protocol)) {
            var builder = OtlpHttpMetricExporter.builder().setEndpoint(endpoint);
            headers.forEach(builder::addHeader);
            return builder.build();
        }
        // Caller has already normalized anything other than "http" down to "grpc".
        var builder = OtlpGrpcMetricExporter.builder().setEndpoint(endpoint);
        // SaaS OTLP endpoints (New Relic's otlp.nr-data.net, Datadog's own OTLP intake if not
        // routing through a local Agent) need an API-key header on every export request -- a
        // local collector/agent (the more common enterprise pattern) needs none, hence this being
        // optional rather than assumed.
        headers.forEach(builder::addHeader);
        return builder.build();
    }

    /**
     * Registers the wire-protocol-traffic / per-backend / read-write metrics from {@link
     * com.polygres.wire.core.SqlMetricsCollector} as async OTel gauges, reporting whatever {@code
     * snapshotSupplier} returns at each export tick -- same pattern as the pool-stats gauges
     * above, and the same reasoning: these are cumulative counters read from a live collector,
     * not values this class accumulates itself, so an observable gauge reporting the current
     * total is simpler and just as correct as threading a LongCounter.add() call through every
     * call site that already has direct access to the collector.
     */
    public void attachSqlMetrics(java.util.function.Supplier<com.polygres.wire.core.SqlMetricsCollector.Snapshot> snapshotSupplier) {
        meter.gaugeBuilder("polywire.protocol.statements").ofLongs()
                .setDescription("Statements handled per wire protocol since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    snap.protocolCounts().forEach((protocol, count) -> measurement.record(count, Attributes.of(PROTOCOL, protocol)));
                });
        meter.gaugeBuilder("polywire.statements.by_kind").ofLongs()
                .setDescription("Statements by read/write/other classification since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    measurement.record(snap.totalReads(), Attributes.of(KIND, "read"));
                    measurement.record(snap.totalWrites(), Attributes.of(KIND, "write"));
                    measurement.record(snap.totalOther(), Attributes.of(KIND, "other"));
                });
        meter.gaugeBuilder("polywire.statements.rate")
                .setDescription("Reads/writes per second, computed since the previous export tick.")
                .setUnit("1/s")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    measurement.record(snap.readsPerSec(), Attributes.of(KIND, "read"));
                    measurement.record(snap.writesPerSec(), Attributes.of(KIND, "write"));
                });
        meter.gaugeBuilder("polywire.backend.statements").ofLongs()
                .setDescription("Statements routed to each backend since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    for (var b : snap.byBackend()) {
                        measurement.record(b.calls(), Attributes.of(BACKEND, b.backend()));
                    }
                });
        meter.gaugeBuilder("polywire.backend.statement_duration_total")
                .setDescription("Cumulative execution time of statements routed to each backend.")
                .setUnit("s")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    for (var b : snap.byBackend()) {
                        measurement.record(b.totalMillis() / 1000.0, Attributes.of(BACKEND, b.backend()));
                    }
                });
    }

    public void recordStatement(String tenant, boolean failed, double durationSeconds) {
        Attributes attrs = Attributes.of(TENANT, tenant);
        statementCounter.add(1, attrs);
        if (failed) {
            errorCounter.add(1, attrs);
        }
        latencyHistogram.record(durationSeconds, attrs);
    }

    public void recordAdmission(String tenant, String workloadClass, boolean admitted) {
        Attributes attrs = Attributes.of(TENANT, tenant, WORKLOAD_CLASS, workloadClass);
        (admitted ? qosAdmittedCounter : qosRejectedCounter).add(1, attrs);
    }

    public static PolyWireTelemetry fromEnv() {
        String protocol = System.getenv().getOrDefault("POLYWIRE_OTEL_PROTOCOL", "grpc").toLowerCase(java.util.Locale.ROOT);
        // The default endpoint's port depends on which protocol is in play -- 4317 is gRPC's
        // OTLP convention, 4318 is HTTP's -- so this can't be one shared default computed before
        // the protocol is known.
        String defaultEndpoint = "http".equals(protocol) ? "http://localhost:4318" : "http://localhost:4317";
        String endpoint = System.getenv().getOrDefault("POLYWIRE_OTEL_ENDPOINT", defaultEndpoint);
        if ("disabled".equalsIgnoreCase(endpoint)) {
            return null;
        }
        long intervalMs = parseLongEnv("POLYWIRE_OTEL_EXPORT_INTERVAL_MS", 5_000);
        return new PolyWireTelemetry(protocol, endpoint, intervalMs, parseHeaders(System.getenv("POLYWIRE_OTEL_HEADERS")));
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    /**
     * {@code POLYWIRE_OTEL_HEADERS="api-key=NRAK-xxx,x-other=value"} -- comma-separated
     * {@code key=value} pairs sent as gRPC metadata on every export request. This is how a SaaS
     * OTLP endpoint that skips a local collector authenticates the export (New Relic's
     * {@code api-key} header, for instance); unset means no headers, appropriate when
     * POLYWIRE_OTEL_ENDPOINT points at a local collector/agent that needs none.
     */
    private static java.util.Map<String, String> parseHeaders(String spec) {
        if (spec == null || spec.isBlank()) {
            return java.util.Map.of();
        }
        java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                log.warn("POLYWIRE_OTEL_HEADERS: skipping malformed entry (expected key=value): {}", pair);
                continue;
            }
            headers.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return headers;
    }
}
