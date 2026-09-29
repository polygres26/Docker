package com.sayonora.warp.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link WarpTelemetry.ExportHealthTrackingExporter} -- the real delivery-
 * confirmation tracking that replaced {@code ObservabilitySummary}'s previous "always false"
 * {@code exportVerified}. Exercised directly against a fake delegate (no real OTLP network client,
 * no PeriodicMetricReader background thread) so the tracking logic itself is deterministic.
 */
class ExportHealthTrackingExporterTest {

    @AfterEach
    void resetAdminOverride() {
        ObservabilityToggles.apply(null, null);
    }

    /** A fake exporter whose export() outcome is scripted per call. */
    private static final class ScriptedExporter implements MetricExporter {
        private final List<CompletableResultCode> results;
        int calls = 0;

        ScriptedExporter(List<CompletableResultCode> results) {
            this.results = results;
        }

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            return results.get(calls++);
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
            return Aggregation.defaultAggregation();
        }

        @Override
        public MemoryMode getMemoryMode() {
            return MemoryMode.IMMUTABLE_DATA;
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.CUMULATIVE;
        }
    }

    @Test
    void freshExporterHasNeverSucceededOrAttempted() {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(List.of()));
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertEquals(0, health.attempts());
        assertEquals(0, health.successes());
        assertEquals(0, health.failures());
        assertNull(health.lastAttemptAt());
        assertNull(health.lastSuccessAt());
        assertFalse(health.verified(), "never attempted -- must never report verified");
    }

    @Test
    void aSuccessfulExportIsRecordedAndVerified() {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(List.of(CompletableResultCode.ofSuccess())));
        tracked.export(new ArrayList<>());
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertEquals(1, health.attempts());
        assertEquals(1, health.successes());
        assertEquals(0, health.failures());
        assertNotNull(health.lastAttemptAt());
        assertNotNull(health.lastSuccessAt());
        assertNull(health.lastError());
        assertTrue(health.verified());
    }

    @Test
    void aFailedExportIsRecordedAsNotVerified() {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(List.of(CompletableResultCode.ofFailure())));
        tracked.export(new ArrayList<>());
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertEquals(1, health.attempts());
        assertEquals(0, health.successes());
        assertEquals(1, health.failures());
        assertNull(health.lastSuccessAt(), "no export has ever succeeded");
        assertNotNull(health.lastError());
        assertFalse(health.verified());
    }

    @Test
    void anExceptionalFailureRecordsTheRealThrowable() {
        RuntimeException boom = new RuntimeException("collector unreachable");
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(
                new ScriptedExporter(List.of(CompletableResultCode.ofExceptionalFailure(boom))));
        tracked.export(new ArrayList<>());
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertTrue(health.lastError().contains("collector unreachable"));
    }

    @Test
    void successThenFailureClearsSuccessCountButKeepsLastSuccessTimestamp() {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(
                List.of(CompletableResultCode.ofSuccess(), CompletableResultCode.ofFailure())));
        tracked.export(new ArrayList<>());
        tracked.export(new ArrayList<>());
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertEquals(2, health.attempts());
        assertEquals(1, health.successes());
        assertEquals(1, health.failures());
        assertNotNull(health.lastSuccessAt(), "a prior success is not erased by a later failure");
        assertNotNull(health.lastError(), "the most recent outcome was a failure");
        // A real success happened, and it's still within the freshness window (this test runs in
        // milliseconds), so verified stays true -- "verified" tracks recency of the last SUCCESS,
        // not whether the single most recent attempt succeeded.
        assertTrue(health.verified());
    }

    @Test
    void adminPauseSkipsTheRealDelegateEntirely() {
        var delegate = new ScriptedExporter(List.of());
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(delegate);
        ObservabilityToggles.apply("false", null);
        tracked.export(new ArrayList<>());
        assertEquals(0, delegate.calls, "a paused exporter must never call the real delegate");
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertEquals(0, health.attempts(), "a paused export isn't counted as an attempt");
        assertTrue(health.pausedByAdmin());
        assertFalse(health.verified());
    }

    @Test
    void aPriorSuccessIsNoLongerVerifiedOnceAdminPauses() {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(List.of(CompletableResultCode.ofSuccess())));
        tracked.export(new ArrayList<>());
        assertTrue(tracked.health(5_000).verified());
        ObservabilityToggles.apply("false", null);
        WarpTelemetry.ExportHealth health = tracked.health(5_000);
        assertTrue(health.pausedByAdmin());
        assertFalse(health.verified(), "a genuine prior success doesn't count as currently healthy while paused");
    }

    @Test
    void resumingAfterAPauseAllowsRealExportsAgain() {
        var delegate = new ScriptedExporter(List.of(CompletableResultCode.ofSuccess()));
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(delegate);
        ObservabilityToggles.apply("false", null);
        tracked.export(new ArrayList<>());
        assertEquals(0, delegate.calls);
        ObservabilityToggles.apply("true", null);
        tracked.export(new ArrayList<>());
        assertEquals(1, delegate.calls);
        assertTrue(tracked.health(5_000).verified());
    }

    @Test
    void aSuccessOlderThanThreeIntervalsIsNoLongerVerified() throws InterruptedException {
        var tracked = new WarpTelemetry.ExportHealthTrackingExporter(new ScriptedExporter(List.of(CompletableResultCode.ofSuccess())));
        tracked.export(new ArrayList<>());
        Thread.sleep(30);
        // A 5ms interval means the 3-interval freshness window (15ms) has already passed.
        WarpTelemetry.ExportHealth health = tracked.health(5);
        assertFalse(health.verified(), "a success older than 3 export intervals must not read as currently healthy");
    }
}
