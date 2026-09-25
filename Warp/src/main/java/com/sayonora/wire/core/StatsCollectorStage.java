package com.sayonora.wire.core;

import com.sayonora.wire.telemetry.WarpTelemetry;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StatsCollectorStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(StatsCollectorStage.class);

    public record Counters(LongAdder statementCount, LongAdder errorCount, LongAdder totalLatencyNanos) {
        Counters() {
            this(new LongAdder(), new LongAdder(), new LongAdder());
        }
    }

    private final ConcurrentHashMap<String, Counters> byTenant = new ConcurrentHashMap<>();
    // Real usage/cost gap, found auditing this against the product's own "Quality of Service"
    // pillar (traffic prioritization by workload class -- App/Analytics/AI, the exact
    // classification QosControlStage already rate-limits by): usage/cost was only ever visible
    // broken down by tenant or by backend, never by WORKLOAD CLASS, so there was no way to answer
    // "how much of our usage/cost is AI traffic vs. ordinary app traffic" -- the natural way a
    // customer running a shared gateway for both actually wants to see cost. Uses the exact same
    // Statement#workloadClass() RouterStage/QosControlStage already classify by, so it reflects
    // real routing/QoS decisions rather than a separate guess. See #usageByWorkloadClass below.
    private final ConcurrentHashMap<String, Counters> byWorkloadClass = new ConcurrentHashMap<>();
    private final WarpTelemetry telemetry;
    private final SqlMetricsCollector sqlMetrics;

    public StatsCollectorStage() {
        this(null);
    }

    public StatsCollectorStage(WarpTelemetry telemetry) {
        this(telemetry, new SqlMetricsCollector());
    }

    /**
     * @param sqlMetrics shared with mongowire/dynamowire in {@code Main} so every wire protocol
     *      feeds the same collector -- see that class's javadoc for why they can't go through
     *      this stage's own {@link #handle} the way the SQL protocols do.
     */
    public StatsCollectorStage(WarpTelemetry telemetry, SqlMetricsCollector sqlMetrics) {
        this.telemetry = telemetry;
        this.sqlMetrics = sqlMetrics;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        Counters counters = byTenant.computeIfAbsent(statement.tenantId(), k -> new Counters());
        // RouterStage runs before this stage and always resolves "default" into a real
        // classification (see its own classifyWorkload) before a statement reaches here -- so by
        // the time this stage sees it, statement.workloadClass() already reflects the SAME
        // App/Analytics/AI-shaped class QosControlStage rate-limited it by, not a placeholder.
        Counters workloadCounters = byWorkloadClass.computeIfAbsent(statement.workloadClass(), k -> new Counters());
        long start = System.nanoTime();
        try {
            ExecutionResult result = next.proceed(statement);
            long elapsedNanos = System.nanoTime() - start;
            counters.statementCount().increment();
            counters.totalLatencyNanos().add(elapsedNanos);
            workloadCounters.statementCount().increment();
            workloadCounters.totalLatencyNanos().add(elapsedNanos);
            sqlMetrics.record(statement.sourceDialect(), statement.targetBackend(), statement.sqlText(), elapsedNanos);
            recordRttOutcome(statement, elapsedNanos);
            record(statement.tenantId(), false, elapsedNanos);
            return result;
        } catch (SQLException e) {
            long elapsedNanos = System.nanoTime() - start;
            counters.statementCount().increment();
            counters.errorCount().increment();
            counters.totalLatencyNanos().add(elapsedNanos);
            workloadCounters.statementCount().increment();
            workloadCounters.errorCount().increment();
            workloadCounters.totalLatencyNanos().add(elapsedNanos);
            sqlMetrics.record(statement.sourceDialect(), statement.targetBackend(), statement.sqlText(), elapsedNanos);
            record(statement.tenantId(), true, elapsedNanos);
            log.debug("statement failed: tenant={} dialect={} error={}",
                    statement.tenantId(), statement.sourceDialect(), e.getMessage());
            throw e;
        }
    }

    public SqlMetricsCollector.Snapshot sqlMetricsSnapshot() {
        return sqlMetrics.snapshot();
    }

    /**
     * Every statement that reaches this stage already missed the cache (or was never cacheable in
     * the first place -- see {@code CacheStage}, which returns before this stage ever runs on a
     * hit), so {@code elapsedNanos} here is a real Postgres round trip, not a cache lookup.
     * Classifies it the same way {@link SqlMetricsCollector#record} already does internally, and
     * skips {@code OTHER} statements (BEGIN/COMMIT/DDL/etc.) -- "Postgres read" and "Postgres
     * write" timings are only meaningful for the statements that actually are one.
     */
    private void recordRttOutcome(Statement statement, long elapsedNanos) {
        SqlMetricsCollector.StatementKind kind = SqlMetricsCollector.classify(statement.sqlText());
        String outcome = switch (kind) {
            case READ -> SqlMetricsCollector.OUTCOME_PG_READ;
            case WRITE -> SqlMetricsCollector.OUTCOME_PG_WRITE;
            case OTHER -> null;
        };
        if (outcome != null) {
            sqlMetrics.recordRttOutcome(SqlMetricsCollector.protocolName(statement.sourceDialect()), outcome, elapsedNanos);
        }
    }

    /**
     * Lets a session handler find the shared collector by scanning {@code sharedStages} for this
     * stage, instead of threading a new constructor parameter through every protocol's session
     * handler and every {@code accept*Loop} in {@code Main} just to report RTT -- see {@code
     * SqlMetricsCollector}'s RTT section for why session handlers need this at all.
     */
    public SqlMetricsCollector sqlMetrics() {
        return sqlMetrics;
    }

    /**
     * The lookup every SQL-protocol session handler does in its constructor to get a
     * {@link SqlMetricsCollector} reference for RTT reporting. Returns {@code null} if
     * {@code stages} has no {@code StatsCollectorStage} (e.g. a test harness assembling its own
     * minimal stage list) -- callers treat that as "RTT reporting disabled for this session,"
     * never as a reason to fail the request.
     */
    public static SqlMetricsCollector findIn(java.util.List<PipelineStage> stages) {
        for (PipelineStage stage : stages) {
            if (stage instanceof StatsCollectorStage stats) {
                return stats.sqlMetrics();
            }
        }
        return null;
    }

    private void record(String tenant, boolean failed, long elapsedNanos) {
        if (telemetry != null) {
            telemetry.recordStatement(tenant, failed, elapsedNanos / 1_000_000_000.0);
        }
    }

    public java.util.Map<String, Counters> snapshot() {
        return java.util.Map.copyOf(byTenant);
    }

    /** Real, per-workload-class usage/cost counters -- see {@link #byWorkloadClass}'s own javadoc
     * for why this exists as its own breakdown, separate from {@link #snapshot()}'s per-tenant
     * one. Keys are whatever {@code RouterStage#classifyWorkload} (or an explicit router rule)
     * actually assigned -- "query"/"write"/etc. by default, or a custom class name if one's
     * configured. */
    public java.util.Map<String, Counters> usageByWorkloadClass() {
        return java.util.Map.copyOf(byWorkloadClass);
    }
}
