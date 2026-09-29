package com.sayonora.warp.core;

import com.sayonora.warp.telemetry.WarpTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.sql.SQLException;
import java.util.List;

/**
 * The one real choke point every wire protocol's statement execution passes through (pgwire,
 * mywire, mssqlwire, orawire's RequestLoop, the gRPC QueryServiceImpl, and AdHocQueryRunner all
 * construct one of these and call {@link #execute}), which makes it the correct place to add
 * real end-to-end tracing across the whole pipeline -- protocol entry, through every {@link
 * PipelineStage} (firewall, router, QoS, dialect translation, rollup, cache, stats, query repair,
 * whichever are wired for this process), to the terminal backend execution -- without touching a
 * single existing stage's own code.
 *
 * <p>Tracing is opt-in ({@code WARP_OTEL_TRACES_ENABLED=true}, on top of OTLP being enabled at
 * all) and, when off, this class behaves EXACTLY as it did before tracing existed: {@link
 * #tracer()} returns {@code null} once at construction time, and the untraced {@link #buildChain}
 * path is used with zero added allocation or branching in the hot per-statement path. When traced,
 * one span is created per pipeline-stage invocation per statement, named after the stage's own
 * class, correctly nested as a real trace tree via {@link Span#makeCurrent()} -- not flattened,
 * not sampled-and-hoped, a genuine parent/child structure a collector can render directly.
 *
 * <p>Real, disclosed scope: spans exist for every stage IN this process's pipeline and for the
 * terminal backend call, correlated by tenant/protocol/workload-class/backend attributes already
 * on {@link Statement}. What this does NOT yet do: propagate a trace context INTO the backend JDBC
 * call itself (so a span for the backend's own execution time exists here, but nothing on the
 * backend side correlates back to it), and does NOT span anything before a {@code Statement} is
 * constructed (protocol handshake, auth) or after {@link ExecutionResult} is returned (response
 * serialization back to the client) -- both real, stated gaps, not silently implied away.
 */
public final class StatementPipeline {

    private final PipelineChain chain;
    private final Tracer tracer;

    public StatementPipeline(List<PipelineStage> stages, BackendExecutor terminal) {
        this(stages, terminal, liveTracer());
    }

    /** Package-private (not private) so a unit test can inject a real, in-memory-backed {@link
     * Tracer} (an OTel SDK {@code SdkTracerProvider} over {@code InMemorySpanExporter}) and assert
     * on the actual span tree produced, without needing a live {@code WarpTelemetry} singleton or
     * a real OTLP network collector. */
    StatementPipeline(List<PipelineStage> stages, BackendExecutor terminal, Tracer tracer) {
        this.tracer = tracer;
        this.chain = tracer != null
                ? buildTracedChain(List.copyOf(stages), 0, terminal, tracer)
                : buildChain(List.copyOf(stages), 0, terminal);
    }

    public ExecutionResult execute(Statement statement) throws SQLException {
        if (tracer == null) {
            return chain.proceed(statement);
        }
        Span root = tracer.spanBuilder("warp.statement")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("warp.tenant", statement.tenantId())
                .setAttribute("warp.protocol", statement.sourceDialect() == null ? "unknown" : statement.sourceDialect().toString())
                .setAttribute("warp.workload_class", statement.workloadClass())
                .startSpan();
        try (Scope scope = root.makeCurrent()) {
            ExecutionResult result = chain.proceed(statement);
            root.setStatus(StatusCode.OK);
            return result;
        } catch (SQLException | RuntimeException e) {
            root.recordException(e);
            root.setStatus(StatusCode.ERROR, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            throw e;
        } finally {
            root.end();
        }
    }

    private static Tracer liveTracer() {
        WarpTelemetry live = WarpTelemetry.current();
        return live != null ? live.tracer() : null;
    }

    private static PipelineChain buildChain(List<PipelineStage> stages, int index, BackendExecutor terminal) {
        if (index == stages.size()) {
            return terminal::execute;
        }
        PipelineStage stage = stages.get(index);
        PipelineChain next = buildChain(stages, index + 1, terminal);
        return statement -> stage.handle(statement, next);
    }

    private static PipelineChain buildTracedChain(List<PipelineStage> stages, int index, BackendExecutor terminal, Tracer tracer) {
        if (index == stages.size()) {
            return statement -> runSpan(tracer, "backend.execute",
                    span -> span.setAttribute("warp.backend", statement.targetBackend() == null ? "auto" : statement.targetBackend()),
                    () -> terminal.execute(statement));
        }
        PipelineStage stage = stages.get(index);
        PipelineChain next = buildTracedChain(stages, index + 1, terminal, tracer);
        String spanName = stage.getClass().getSimpleName();
        return statement -> runSpan(tracer, spanName, span -> { }, () -> stage.handle(statement, next));
    }

    @FunctionalInterface
    private interface ThrowingSupplier {
        ExecutionResult get() throws SQLException;
    }

    private static ExecutionResult runSpan(Tracer tracer, String name, java.util.function.Consumer<Span> attrs, ThrowingSupplier body)
            throws SQLException {
        Span span = tracer.spanBuilder(name).startSpan();
        attrs.accept(span);
        try (Scope scope = span.makeCurrent()) {
            ExecutionResult result = body.get();
            span.setStatus(StatusCode.OK);
            return result;
        } catch (SQLException | RuntimeException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
