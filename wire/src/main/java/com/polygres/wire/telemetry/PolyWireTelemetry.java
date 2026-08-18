package com.polygres.wire.telemetry;

import com.polygres.wire.core.BackendConnectionPools;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
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

    private PolyWireTelemetry(String otlpEndpoint, long exportIntervalMs) {
        OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(otlpEndpoint)
                .build();
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

        log.info("OpenTelemetry metrics export enabled: OTLP/gRPC to {} every {}ms", otlpEndpoint, exportIntervalMs);
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
        String endpoint = System.getenv().getOrDefault("POLYWIRE_OTEL_ENDPOINT", "http://localhost:4317");
        if ("disabled".equalsIgnoreCase(endpoint)) {
            return null;
        }
        long intervalMs = parseLongEnv("POLYWIRE_OTEL_EXPORT_INTERVAL_MS", 5_000);
        return new PolyWireTelemetry(endpoint, intervalMs);
    }

    private static long parseLongEnv(String name, long defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
    }
}
