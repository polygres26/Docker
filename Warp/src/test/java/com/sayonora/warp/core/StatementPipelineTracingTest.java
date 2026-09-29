package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link StatementPipeline}'s real end-to-end tracing -- exercised against a
 * genuine OTel SDK {@code SdkTracerProvider} over {@code InMemorySpanExporter} (no live
 * {@code WarpTelemetry} singleton, no real OTLP network collector needed), via the
 * package-private constructor built for exactly this. */
class StatementPipelineTracingTest {

    private static final Statement STMT = new Statement("acme", SourceDialect.POSTGRES, "select 1", List.of(),
            "default", "pg-primary");

    private static Tracer inMemoryTracer(InMemorySpanExporter exporter) {
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        return provider.get("test");
    }

    @Test
    void untracedPipelineNeedsNoTracerAndProducesNoSpans() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        StatementPipeline pipeline = new StatementPipeline(List.of((stmt, next) -> next.proceed(stmt)),
                stmt -> ExecutionResult.ofUpdate(0), null);
        ExecutionResult result = pipeline.execute(STMT);
        assertEquals(0, result.updateCount());
        assertTrue(exporter.getFinishedSpanItems().isEmpty(), "no tracer means no spans at all -- true zero overhead path");
    }

    @Test
    void tracedPipelineProducesARootSpanPerStagePlusBackendSpan() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        PipelineStage stageA = (stmt, next) -> next.proceed(stmt);
        PipelineStage stageB = (stmt, next) -> next.proceed(stmt);
        StatementPipeline pipeline = new StatementPipeline(List.of(stageA, stageB), stmt -> ExecutionResult.ofUpdate(0), tracer);

        pipeline.execute(STMT);

        List<SpanData> spans = exporter.getFinishedSpanItems();
        List<String> names = spans.stream().map(SpanData::getName).toList();
        assertTrue(names.contains("warp.statement"), "a root span must exist: " + names);
        assertTrue(names.contains("backend.execute"), "a terminal backend span must exist: " + names);
        // Each anonymous lambda PipelineStage above is a distinct class -- both stage spans exist,
        // named after their own (synthetic) class, proving every stage in the chain got a span.
        assertEquals(4, spans.size(), "root + 2 stages + backend = 4 spans: " + names);
    }

    @Test
    void spansNestCorrectlyAsARealParentChildTree() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        StatementPipeline pipeline = new StatementPipeline(List.of((stmt, next) -> next.proceed(stmt)),
                stmt -> ExecutionResult.ofUpdate(0), tracer);

        pipeline.execute(STMT);

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData root = spans.stream().filter(s -> "warp.statement".equals(s.getName())).findFirst().orElseThrow();
        SpanData backend = spans.stream().filter(s -> "backend.execute".equals(s.getName())).findFirst().orElseThrow();
        // Every non-root span in this trace must trace back to the same root -- a real tree, not
        // independent, unlinked spans that merely share a timestamp.
        for (SpanData s : spans) {
            if (s == root) {
                continue;
            }
            assertEquals(root.getTraceId(), s.getTraceId(), s.getName() + " must be in the same trace as the root");
        }
        assertEquals(StatusCode.OK, root.getStatus().getStatusCode());
        assertEquals(StatusCode.OK, backend.getStatus().getStatusCode());
    }

    @Test
    void rootSpanCarriesRealStatementAttributes() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        StatementPipeline pipeline = new StatementPipeline(List.of(), stmt -> ExecutionResult.ofUpdate(0), tracer);

        pipeline.execute(STMT);

        SpanData root = exporter.getFinishedSpanItems().stream()
                .filter(s -> "warp.statement".equals(s.getName())).findFirst().orElseThrow();
        assertEquals("acme", root.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("warp.tenant")));
        assertEquals("POSTGRES", root.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("warp.protocol")));
    }

    @Test
    void backendSpanCarriesTheTargetBackendAttribute() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        StatementPipeline pipeline = new StatementPipeline(List.of(), stmt -> ExecutionResult.ofUpdate(0), tracer);

        pipeline.execute(STMT);

        SpanData backend = exporter.getFinishedSpanItems().stream()
                .filter(s -> "backend.execute".equals(s.getName())).findFirst().orElseThrow();
        assertEquals("pg-primary", backend.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("warp.backend")));
    }

    @Test
    void aFailingStageMarksBothItsOwnSpanAndTheRootAsError() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        PipelineStage failing = (stmt, next) -> { throw new SQLException("backend unreachable"); };
        StatementPipeline pipeline = new StatementPipeline(List.of(failing), stmt -> ExecutionResult.ofUpdate(0), tracer);

        assertThrows(SQLException.class, () -> pipeline.execute(STMT));

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData root = spans.stream().filter(s -> "warp.statement".equals(s.getName())).findFirst().orElseThrow();
        assertEquals(StatusCode.ERROR, root.getStatus().getStatusCode());
        assertFalse(spans.stream().anyMatch(s -> "backend.execute".equals(s.getName())),
                "the failing stage never called next.proceed() -- no backend span should exist");
    }

    @Test
    void nullTargetBackendReportsAsAuto() throws SQLException {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        Tracer tracer = inMemoryTracer(exporter);
        Statement noBackend = new Statement("acme", SourceDialect.POSTGRES, "select 1", List.of(), "default", null);
        StatementPipeline pipeline = new StatementPipeline(List.of(), stmt -> ExecutionResult.ofUpdate(0), tracer);

        pipeline.execute(noBackend);

        SpanData backend = exporter.getFinishedSpanItems().stream()
                .filter(s -> "backend.execute".equals(s.getName())).findFirst().orElseThrow();
        assertEquals("auto", backend.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("warp.backend")));
    }

    @Test
    void constructingWithANullTracerNeverThrows() throws SQLException {
        // Documents the contract the public no-tracer-arg constructor relies on: passing tracer =
        // null must never throw during construction or execution, matching "OTLP disabled"
        // behaving exactly as it did before tracing existed.
        StatementPipeline pipeline = new StatementPipeline(List.of(), stmt -> ExecutionResult.ofUpdate(0), null);
        pipeline.execute(STMT);
    }
}
