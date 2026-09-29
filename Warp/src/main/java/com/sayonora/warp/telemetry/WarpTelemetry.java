package com.sayonora.warp.telemetry;

import com.sayonora.warp.config.NodeRegistry;
import com.sayonora.warp.core.BackendConnectionPools;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WarpTelemetry {

    private static final Logger log = LoggerFactory.getLogger(WarpTelemetry.class);
    private static final AttributeKey<String> TENANT = AttributeKey.stringKey("tenant");
    private static final AttributeKey<String> POOL = AttributeKey.stringKey("pool");
    private static final AttributeKey<String> STATE = AttributeKey.stringKey("state");
    private static final AttributeKey<String> WORKLOAD_CLASS = AttributeKey.stringKey("workload_class");

    // The one live instance this process constructed (null when OTLP export is disabled) -- same
    // static-singleton-accessor pattern com.sayonora.warp.ab.AbRouting already uses in this
    // codebase, chosen over threading a WarpTelemetry reference through MetricsServer's several
    // constructors just so GET /api/observability can read its live export-health counters.
    private static volatile WarpTelemetry current;

    public static WarpTelemetry current() {
        return current;
    }

    private final LongCounter statementCounter;
    private final LongCounter errorCounter;
    private final DoubleHistogram latencyHistogram;
    private final LongCounter qosAdmittedCounter;
    private final LongCounter qosRejectedCounter;
    private final Meter meter;
    private final ExportHealthTrackingExporter trackedExporter;
    private final long exportIntervalMs;

    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("protocol");
    private static final AttributeKey<String> BACKEND = AttributeKey.stringKey("backend");
    private static final AttributeKey<String> KIND = AttributeKey.stringKey("kind");

    /**
     * Real per-export outcome tracking -- {@code attempts}/{@code successes}/{@code failures} are
     * cumulative counts, {@code lastAttemptAt}/{@code lastSuccessAt} the real timestamps of the
     * most recent export tick's start and most recent SUCCESSFUL export, {@code lastError} the
     * failure reason from the most recent unsuccessful export (cleared on the next success).
     * {@code verified} is the actual health verdict: at least one export has ever succeeded AND
     * the most recent success is no older than 3 export intervals -- a real destination that
     * accepted data once but has since gone quiet (collector restarted, network partition, auth
     * expired) reports {@code verified=false} again, not a stale permanent "yes". {@code
     * pausedByAdmin} is true when {@link ObservabilityToggles#otlpOverride()} is explicitly {@code
     * false} -- exports are skipped entirely (never sent to the real delegate) while paused, so
     * {@code verified} correctly decays to {@code false} the moment the pause outlasts 3 export
     * intervals, the same as a genuinely unreachable collector would.
     */
    public record ExportHealth(long attempts, long successes, long failures, Instant lastAttemptAt,
            Instant lastSuccessAt, String lastError, boolean verified, boolean pausedByAdmin) {
    }

    /**
     * Wraps the real OTLP exporter (grpc or http) so every {@code export()} call's real, async
     * {@link CompletableResultCode} outcome is recorded -- this is the actual delivery-confirmation
     * signal {@code ObservabilitySummary}'s {@code exportVerified} used to always report {@code
     * false} for, before this class existed. {@code whenComplete} fires once the SDK's own export
     * attempt genuinely finishes (success, failure or timeout), never assumed from having merely
     * been called -- calling {@code export()} only means an attempt started.
     */
    /** Package-private (not private) so a unit test can exercise its tracking logic directly
     * against a fake delegate, without constructing a real OTLP exporter/network client. */
    static final class ExportHealthTrackingExporter implements MetricExporter {
        private final MetricExporter delegate;
        private final AtomicLong attempts = new AtomicLong();
        private final AtomicLong successes = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private volatile Instant lastAttemptAt;
        private volatile Instant lastSuccessAt;
        private volatile String lastError;

        ExportHealthTrackingExporter(MetricExporter delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableResultCode export(java.util.Collection<io.opentelemetry.sdk.metrics.data.MetricData> metrics) {
            if (Boolean.FALSE.equals(ObservabilityToggles.otlpOverride())) {
                // Admin-paused: skip the real delegate entirely -- no network call, no attempt
                // recorded. Reported to the SDK as success so PeriodicMetricReader doesn't treat
                // this tick as a real failure; the pause itself is surfaced separately via
                // ExportHealth.pausedByAdmin(), not folded into the success/failure counters.
                return CompletableResultCode.ofSuccess();
            }
            attempts.incrementAndGet();
            lastAttemptAt = Instant.now();
            CompletableResultCode code = delegate.export(metrics);
            code.whenComplete(() -> {
                if (code.isSuccess()) {
                    successes.incrementAndGet();
                    lastSuccessAt = Instant.now();
                    lastError = null;
                } else {
                    failures.incrementAndGet();
                    Throwable t = code.getFailureThrowable();
                    lastError = t != null ? t.toString() : "export did not succeed (no exception reported)";
                }
            });
            return code;
        }

        @Override
        public CompletableResultCode flush() {
            return delegate.flush();
        }

        @Override
        public CompletableResultCode shutdown() {
            return delegate.shutdown();
        }

        @Override
        public io.opentelemetry.sdk.metrics.Aggregation getDefaultAggregation(io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
            return delegate.getDefaultAggregation(instrumentType);
        }

        @Override
        public io.opentelemetry.sdk.common.export.MemoryMode getMemoryMode() {
            return delegate.getMemoryMode();
        }

        @Override
        public io.opentelemetry.sdk.metrics.data.AggregationTemporality getAggregationTemporality(
                io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
            return delegate.getAggregationTemporality(instrumentType);
        }

        ExportHealth health(long exportIntervalMs) {
            boolean pausedByAdmin = Boolean.FALSE.equals(ObservabilityToggles.otlpOverride());
            Instant success = lastSuccessAt;
            boolean verified = !pausedByAdmin && success != null
                    && Duration.between(success, Instant.now()).toMillis() <= 3 * exportIntervalMs;
            return new ExportHealth(attempts.get(), successes.get(), failures.get(), lastAttemptAt, success, lastError,
                    verified, pausedByAdmin);
        }
    }

    /** The live export-health snapshot for this instance -- see {@link ExportHealth}'s own javadoc. */
    public ExportHealth exportHealth() {
        return trackedExporter.health(exportIntervalMs);
    }

    private final Resource resource;

    /** The resource attributes attached to every export from this instance -- lets {@code GET /api
     * /observability} disclose exactly what identity a multi-node deployment's metrics carry,
     * instead of the operator having to guess from an OTLP collector's own logs. */
    public Resource resourceAttributes() {
        return resource;
    }

    /** Real, disclosed limitation: {@code host.name} is the only per-process identity attached --
     * unlike {@code NodeRegistry}'s {@code node_id} (a stable UUID of {@code host:adminPort}),
     * this constructor has no admin port to include, so two Warp processes on the SAME host are
     * not distinguishable from their OTLP resource attributes alone. {@code warp.zone} reuses
     * {@link NodeRegistry#resolveZone} so the same "what zone is this node in" answer never drifts
     * between {@code warp_nodes} rows and OTLP resource attributes. Package-private (not private)
     * so a unit test can exercise it directly without constructing a real OTLP exporter. */
    static Resource buildResource() {
        String host = NodeRegistry.resolveHost();
        String serviceName = System.getenv().getOrDefault("WARP_OTEL_SERVICE_NAME", "warp");
        String serviceVersion = WarpTelemetry.class.getPackage().getImplementationVersion();
        var builder = Resource.builder()
                .put(AttributeKey.stringKey("service.name"), serviceName)
                .put(AttributeKey.stringKey("host.name"), host)
                .put(AttributeKey.stringKey("warp.zone"), NodeRegistry.resolveZone(host));
        if (serviceVersion != null && !serviceVersion.isBlank()) {
            builder.put(AttributeKey.stringKey("service.version"), serviceVersion);
        }
        return builder.build();
    }

    private WarpTelemetry(String protocol, String otlpEndpoint, long exportIntervalMs,
            java.util.Map<String, String> headers) {
        if (!"http".equalsIgnoreCase(protocol) && !"grpc".equalsIgnoreCase(protocol)) {
            log.warn("WARP_OTEL_PROTOCOL={} not recognized (expected 'grpc' or 'http'); defaulting to grpc", protocol);
            protocol = "grpc";
        }
        this.exportIntervalMs = exportIntervalMs;
        this.resource = buildResource();
        this.trackedExporter = new ExportHealthTrackingExporter(buildExporter(protocol, otlpEndpoint, headers));
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(trackedExporter)
                        .setInterval(Duration.ofMillis(exportIntervalMs))
                        .build())
                .build();
        Meter meter = meterProvider.meterBuilder("com.sayonora.warp").setInstrumentationVersion("0.1.0").build();

        this.statementCounter = meter.counterBuilder("warp.statements")
                .setDescription("Total statements executed.").build();
        this.errorCounter = meter.counterBuilder("warp.statement.errors")
                .setDescription("Total statements that raised a SQLException.").build();
        this.latencyHistogram = meter.histogramBuilder("warp.statement.duration")
                .setDescription("Per-statement execution time.").setUnit("s").build();
        this.qosAdmittedCounter = meter.counterBuilder("warp.qos.admitted")
                .setDescription("Statements admitted by QosControlStage.").build();
        this.qosRejectedCounter = meter.counterBuilder("warp.qos.rejected")
                .setDescription("Statements rejected by QosControlStage (rate limit or pool saturation).").build();

        meter.gaugeBuilder("warp.pool.connections").ofLongs()
                .setDescription("Physical backend connections per pool, by state.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.activeConnections(), Attributes.of(POOL, pool.poolKey(), STATE, "active"));
                        measurement.record(pool.idleConnections(), Attributes.of(POOL, pool.poolKey(), STATE, "idle"));
                    }
                });
        meter.gaugeBuilder("warp.pool.waiting").ofLongs()
                .setDescription("Frontend sessions currently blocked waiting for a pooled backend connection.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.threadsAwaitingConnection(), Attributes.of(POOL, pool.poolKey()));
                    }
                });
        meter.gaugeBuilder("warp.pool.max_size").ofLongs()
                .setDescription("Configured maximum physical connections for this pool.")
                .buildWithCallback(measurement -> {
                    for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
                        measurement.record(pool.maxPoolSize(), Attributes.of(POOL, pool.poolKey()));
                    }
                });

        this.meter = meter;
        log.info("OpenTelemetry metrics export enabled: OTLP/{} to {} every {}ms{}, resource attributes: {}",
                protocol.toUpperCase(), otlpEndpoint, exportIntervalMs,
                headers.isEmpty() ? "" : " (" + headers.size() + " header(s) attached)", resource.getAttributes());
        current = this;
    }

    /**
     * gRPC is the default -- it's what {@code WARP_OTEL_ENDPOINT}'s historical default
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
     * com.sayonora.warp.core.SqlMetricsCollector} as async OTel gauges, reporting whatever {@code
     * snapshotSupplier} returns at each export tick -- same pattern as the pool-stats gauges
     * above, and the same reasoning: these are cumulative counters read from a live collector,
     * not values this class accumulates itself, so an observable gauge reporting the current
     * total is simpler and just as correct as threading a LongCounter.add() call through every
     * call site that already has direct access to the collector.
     */
    public void attachSqlMetrics(java.util.function.Supplier<com.sayonora.warp.core.SqlMetricsCollector.Snapshot> snapshotSupplier) {
        meter.gaugeBuilder("warp.protocol.statements").ofLongs()
                .setDescription("Statements handled per wire protocol since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    snap.protocolCounts().forEach((protocol, count) -> measurement.record(count, Attributes.of(PROTOCOL, protocol)));
                });
        meter.gaugeBuilder("warp.statements.by_kind").ofLongs()
                .setDescription("Statements by read/write/other classification since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    measurement.record(snap.totalReads(), Attributes.of(KIND, "read"));
                    measurement.record(snap.totalWrites(), Attributes.of(KIND, "write"));
                    measurement.record(snap.totalOther(), Attributes.of(KIND, "other"));
                });
        meter.gaugeBuilder("warp.statements.rate")
                .setDescription("Reads/writes per second, computed since the previous export tick.")
                .setUnit("1/s")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    measurement.record(snap.readsPerSec(), Attributes.of(KIND, "read"));
                    measurement.record(snap.writesPerSec(), Attributes.of(KIND, "write"));
                });
        meter.gaugeBuilder("warp.backend.statements").ofLongs()
                .setDescription("Statements routed to each backend since process start.")
                .buildWithCallback(measurement -> {
                    var snap = snapshotSupplier.get();
                    for (var b : snap.byBackend()) {
                        measurement.record(b.calls(), Attributes.of(BACKEND, b.backend()));
                    }
                });
        meter.gaugeBuilder("warp.backend.statement_duration_total")
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

    public static WarpTelemetry fromEnv() {
        String protocol = System.getenv().getOrDefault("WARP_OTEL_PROTOCOL", "grpc").toLowerCase(java.util.Locale.ROOT);
        // The default endpoint's port depends on which protocol is in play -- 4317 is gRPC's
        // OTLP convention, 4318 is HTTP's -- so this can't be one shared default computed before
        // the protocol is known.
        String defaultEndpoint = "http".equals(protocol) ? "http://localhost:4318" : "http://localhost:4317";
        String endpoint = System.getenv().getOrDefault("WARP_OTEL_ENDPOINT", defaultEndpoint);
        if ("disabled".equalsIgnoreCase(endpoint)) {
            return null;
        }
        long intervalMs = parseLongEnv("WARP_OTEL_EXPORT_INTERVAL_MS", 5_000);
        return new WarpTelemetry(protocol, endpoint, intervalMs, parseHeaders(System.getenv("WARP_OTEL_HEADERS")));
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }

    /**
     * {@code WARP_OTEL_HEADERS="api-key=NRAK-xxx,x-other=value"} -- comma-separated
     * {@code key=value} pairs sent as gRPC metadata on every export request. This is how a SaaS
     * OTLP endpoint that skips a local collector authenticates the export (New Relic's
     * {@code api-key} header, for instance); unset means no headers, appropriate when
     * WARP_OTEL_ENDPOINT points at a local collector/agent that needs none.
     */
    private static java.util.Map<String, String> parseHeaders(String spec) {
        if (spec == null || spec.isBlank()) {
            return java.util.Map.of();
        }
        java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                log.warn("WARP_OTEL_HEADERS: skipping malformed entry (expected key=value): {}", pair);
                continue;
            }
            headers.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return headers;
    }
}
