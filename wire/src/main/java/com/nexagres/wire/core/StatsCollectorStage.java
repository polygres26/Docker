package com.nexagres.wire.core;

import com.nexagres.wire.telemetry.PolyWireTelemetry;
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
    private final PolyWireTelemetry telemetry;
    private final SqlMetricsCollector sqlMetrics;

    public StatsCollectorStage() {
        this(null);
    }

    public StatsCollectorStage(PolyWireTelemetry telemetry) {
        this(telemetry, new SqlMetricsCollector());
    }

    /**
     * @param sqlMetrics shared with mongowire/dynamowire in {@code Main} so every wire protocol
     *      feeds the same collector -- see that class's javadoc for why they can't go through
     *      this stage's own {@link #handle} the way the SQL protocols do.
     */
    public StatsCollectorStage(PolyWireTelemetry telemetry, SqlMetricsCollector sqlMetrics) {
        this.telemetry = telemetry;
        this.sqlMetrics = sqlMetrics;
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
            sqlMetrics.record(statement.sourceDialect(), statement.targetBackend(), statement.sqlText(), elapsedNanos);
            recordRttOutcome(statement, elapsedNanos);
            record(statement.tenantId(), false, elapsedNanos);
            return result;
        } catch (SQLException e) {
            long elapsedNanos = System.nanoTime() - start;
            counters.statementCount().increment();
            counters.errorCount().increment();
            counters.totalLatencyNanos().add(elapsedNanos);
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
}
