package com.polygres.wire.core;

import com.polygres.wire.telemetry.PolyWireTelemetry;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real (not stubbed) pipeline stage: per-tenant/workload-class statement
 * counts, error counts, and cumulative latency, kept in memory. This is
 * ARCHITECTURE.md §5's "Stats Collector" — {@link #snapshot()} backs the
 * hand-rolled {@code /metrics} Prometheus endpoint, and (when {@code
 * telemetry} is non-null) the same numbers are also recorded as real
 * OpenTelemetry instruments and pushed via OTLP — see {@link
 * PolyWireTelemetry}. Both are populated from this single call site so they
 * can never drift from each other.
 */
public final class StatsCollectorStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(StatsCollectorStage.class);

    public record Counters(LongAdder statementCount, LongAdder errorCount, LongAdder totalLatencyNanos) {
        Counters() {
            this(new LongAdder(), new LongAdder(), new LongAdder());
        }
    }

    private final ConcurrentHashMap<String, Counters> byTenant = new ConcurrentHashMap<>();
    private final PolyWireTelemetry telemetry; // nullable — OTLP export is opt-in, see PolyWireTelemetry.fromEnv()

    public StatsCollectorStage() {
        this(null);
    }

    public StatsCollectorStage(PolyWireTelemetry telemetry) {
        this.telemetry = telemetry;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        Counters counters = byTenant.computeIfAbsent(statement.tenantId(), k -> new Counters());
        long start = System.nanoTime();
        try {
            ExecutionResult result = next.proceed(statement);
            long elapsedNanos = System.nanoTime() - start;
            counters.statementCount().increment();
            counters.totalLatencyNanos().add(elapsedNanos);
            record(statement.tenantId(), false, elapsedNanos);
            return result;
        } catch (SQLException e) {
            long elapsedNanos = System.nanoTime() - start;
            counters.statementCount().increment();
            counters.errorCount().increment();
            counters.totalLatencyNanos().add(elapsedNanos);
            record(statement.tenantId(), true, elapsedNanos);
            log.debug("statement failed: tenant={} dialect={} error={}",
                    statement.tenantId(), statement.sourceDialect(), e.getMessage());
            throw e;
        }
    }

    private void record(String tenant, boolean failed, long elapsedNanos) {
        if (telemetry != null) {
            telemetry.recordStatement(tenant, failed, elapsedNanos / 1_000_000_000.0);
        }
    }

    public java.util.Map<String, Counters> snapshot() {
        return java.util.Map.copyOf(byTenant);
    }
}
