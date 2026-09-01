package com.nexagres.wire.http.admin;

import com.nexagres.wire.core.BackendConnectionPools;
import com.nexagres.wire.core.QosControlStage;
import com.nexagres.wire.core.StatsCollectorStage;
import java.util.Locale;
import java.util.Map;

public final class MetricsRenderer {

    public static String render(StatsCollectorStage statsStage, QosControlStage qosStage) {
        return render(statsStage, qosStage, null);
    }

    public static String render(StatsCollectorStage statsStage, QosControlStage qosStage,
            com.nexagres.wire.mcp.McpMetricsCollector mcpMetrics) {
        Map<String, StatsCollectorStage.Counters> byTenant = statsStage.snapshot();
        StringBuilder out = new StringBuilder();

        out.append("# HELP warp_statements_total Total statements executed.\n");
        out.append("# TYPE warp_statements_total counter\n");
        byTenant.forEach((tenant, counters) ->
                appendSeries(out, "warp_statements_total", tenant, counters.statementCount().sum()));

        out.append("# HELP warp_statement_errors_total Total statements that raised a SQLException.\n");
        out.append("# TYPE warp_statement_errors_total counter\n");
        byTenant.forEach((tenant, counters) ->
                appendSeries(out, "warp_statement_errors_total", tenant, counters.errorCount().sum()));

        out.append("# HELP warp_statement_latency_seconds_total Cumulative statement execution time.\n");
        out.append("# TYPE warp_statement_latency_seconds_total counter\n");
        byTenant.forEach((tenant, counters) -> appendSeries(out, "warp_statement_latency_seconds_total", tenant,
                counters.totalLatencyNanos().sum() / 1_000_000_000.0));

        out.append("# HELP warp_pool_connections Physical backend connections per pool, by state.\n");
        out.append("# TYPE warp_pool_connections gauge\n");
        for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
            appendPoolSeries(out, pool.poolKey(), "active", pool.activeConnections());
            appendPoolSeries(out, pool.poolKey(), "idle", pool.idleConnections());
            out.append("warp_pool_max_size{pool=\"").append(escape(pool.poolKey())).append("\"} ")
                    .append(pool.maxPoolSize()).append('\n');
            out.append("warp_pool_waiting{pool=\"").append(escape(pool.poolKey())).append("\"} ")
                    .append(pool.threadsAwaitingConnection()).append('\n');
        }

        if (qosStage != null) {
            out.append("# HELP warp_qos_admitted_total Statements admitted by QosControlStage.\n");
            out.append("# TYPE warp_qos_admitted_total counter\n");
            qosStage.snapshot().forEach((key, counters) -> appendQosSeries(out, "warp_qos_admitted_total", key, counters.admitted().sum()));

            out.append("# HELP warp_qos_rejected_total Statements rejected by QosControlStage (rate limit or pool saturation).\n");
            out.append("# TYPE warp_qos_rejected_total counter\n");
            qosStage.snapshot().forEach((key, counters) -> appendQosSeries(out, "warp_qos_rejected_total", key, counters.rejected().sum()));
        }

        // Protocol/backend/read-write breakdown from SqlMetricsCollector -- the same numbers the
        // Advisor "Wire traffic" dashboard shows, now on the same path every hyperscaler and APM
        // platform already knows how to scrape. Deliberately does NOT expose the top-10-SQL data
        // here: normalized SQL text as a label value would be unbounded cardinality, which is
        // exactly what a Prometheus-style time series database is bad at -- that data stays in
        // /api/metrics/summary, where it's a bounded JSON list, not a metric series.
        var sqlSnap = statsStage.sqlMetricsSnapshot();

        out.append("# HELP warp_protocol_statements_total Statements handled per wire protocol.\n");
        out.append("# TYPE warp_protocol_statements_total counter\n");
        sqlSnap.protocolCounts().forEach((protocol, count) ->
                out.append("warp_protocol_statements_total{protocol=\"").append(escape(protocol)).append("\"} ")
                        .append(count).append('\n'));

        out.append("# HELP warp_statements_by_kind_total Statements by read/write/other classification.\n");
        out.append("# TYPE warp_statements_by_kind_total counter\n");
        out.append("warp_statements_by_kind_total{kind=\"read\"} ").append(sqlSnap.totalReads()).append('\n');
        out.append("warp_statements_by_kind_total{kind=\"write\"} ").append(sqlSnap.totalWrites()).append('\n');
        out.append("warp_statements_by_kind_total{kind=\"other\"} ").append(sqlSnap.totalOther()).append('\n');

        out.append("# HELP warp_statements_rate Reads/writes per second since the previous /metrics scrape.\n");
        out.append("# TYPE warp_statements_rate gauge\n");
        out.append("warp_statements_rate{kind=\"read\"} ")
                .append(String.format(Locale.ROOT, "%.4f", sqlSnap.readsPerSec())).append('\n');
        out.append("warp_statements_rate{kind=\"write\"} ")
                .append(String.format(Locale.ROOT, "%.4f", sqlSnap.writesPerSec())).append('\n');

        out.append("# HELP warp_backend_statements_total Statements routed to each backend.\n");
        out.append("# TYPE warp_backend_statements_total counter\n");
        out.append("# HELP warp_backend_statement_duration_seconds_total Cumulative execution time per backend.\n");
        out.append("# TYPE warp_backend_statement_duration_seconds_total counter\n");
        for (var b : sqlSnap.byBackend()) {
            out.append("warp_backend_statements_total{backend=\"").append(escape(b.backend())).append("\"} ")
                    .append(b.calls()).append('\n');
            out.append("warp_backend_statement_duration_seconds_total{backend=\"").append(escape(b.backend())).append("\"} ")
                    .append(String.format(Locale.ROOT, "%.6f", b.totalMillis() / 1000.0)).append('\n');
        }

        // Cache-hit vs. real-Postgres-read vs. real-Postgres-write timing, per protocol -- bounded
        // cardinality (a handful of protocols x 3 outcomes), unlike top-SQL above, so this is safe
        // as a Prometheus series and doesn't need the JSON-only treatment.
        out.append("# HELP warp_rtt_seconds_total Cumulative time by outcome (cache_hit, pg_read, pg_write), per protocol.\n");
        out.append("# TYPE warp_rtt_seconds_total counter\n");
        out.append("# HELP warp_rtt_calls_total Sample count backing warp_rtt_seconds_total, per protocol/outcome.\n");
        out.append("# TYPE warp_rtt_calls_total counter\n");
        for (var r : statsStage.sqlMetrics().rttOutcomeSnapshot()) {
            out.append("warp_rtt_seconds_total{protocol=\"").append(escape(r.protocol()))
                    .append("\",outcome=\"").append(escape(r.outcome())).append("\"} ")
                    .append(String.format(Locale.ROOT, "%.6f", r.totalMillis() / 1000.0)).append('\n');
            out.append("warp_rtt_calls_total{protocol=\"").append(escape(r.protocol()))
                    .append("\",outcome=\"").append(escape(r.outcome())).append("\"} ")
                    .append(r.calls()).append('\n');
        }

        if (mcpMetrics != null) {
            out.append("# HELP warp_mcp_tool_calls_total MCP tool invocations, per tool.\n");
            out.append("# TYPE warp_mcp_tool_calls_total counter\n");
            for (var t : mcpMetrics.snapshot()) {
                out.append("warp_mcp_tool_calls_total{tool=\"").append(escape(t.toolName())).append("\"} ")
                        .append(t.calls()).append('\n');
            }
            out.append("# HELP warp_mcp_tool_errors_total MCP tool invocations that returned an error, per tool.\n");
            out.append("# TYPE warp_mcp_tool_errors_total counter\n");
            for (var t : mcpMetrics.snapshot()) {
                out.append("warp_mcp_tool_errors_total{tool=\"").append(escape(t.toolName())).append("\"} ")
                        .append(t.errors()).append('\n');
            }
            out.append("# HELP warp_mcp_tool_latency_seconds_total Cumulative MCP tool server-side time, per tool.\n");
            out.append("# TYPE warp_mcp_tool_latency_seconds_total counter\n");
            for (var t : mcpMetrics.snapshot()) {
                out.append("warp_mcp_tool_latency_seconds_total{tool=\"").append(escape(t.toolName())).append("\"} ")
                        .append(String.format(Locale.ROOT, "%.6f", t.totalMillis() / 1000.0)).append('\n');
            }
        }

        return out.toString();
    }

    private static void appendQosSeries(StringBuilder out, String metric, String tenantAndClassKey, long value) {
        int split = tenantAndClassKey.indexOf(':');
        String tenant = split < 0 ? tenantAndClassKey : tenantAndClassKey.substring(0, split);
        String workloadClass = split < 0 ? "" : tenantAndClassKey.substring(split + 1);
        out.append(metric).append("{tenant=\"").append(escape(tenant)).append("\",workload_class=\"")
                .append(escape(workloadClass)).append("\"} ").append(value).append('\n');
    }

    private static void appendSeries(StringBuilder out, String metric, String tenant, long value) {
        out.append(metric).append("{tenant=\"").append(escape(tenant)).append("\"} ").append(value).append('\n');
    }

    private static void appendPoolSeries(StringBuilder out, String poolKey, String state, int value) {
        out.append("warp_pool_connections{pool=\"").append(escape(poolKey)).append("\",state=\"").append(state)
                .append("\"} ").append(value).append('\n');
    }

    private static void appendSeries(StringBuilder out, String metric, String tenant, double value) {
        out.append(metric).append("{tenant=\"").append(escape(tenant)).append("\"} ")
                .append(String.format(Locale.ROOT, "%.6f", value)).append('\n');
    }

    private static String escape(String label) {
        return label.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private MetricsRenderer() {
    }
}
