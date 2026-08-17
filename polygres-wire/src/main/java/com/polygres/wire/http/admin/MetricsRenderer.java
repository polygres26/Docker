package com.polygres.wire.http.admin;

import com.polygres.wire.core.BackendConnectionPools;
import com.polygres.wire.core.QosControlStage;
import com.polygres.wire.core.StatsCollectorStage;
import java.util.Locale;
import java.util.Map;

/** Renders {@link StatsCollectorStage#snapshot()}/{@link QosControlStage#snapshot()} as Prometheus text exposition format (one gauge/counter family per metric, one label series per tenant/class/pool). Ported from Omnigate's MetricsRenderer. */
public final class MetricsRenderer {

    public static String render(StatsCollectorStage statsStage, QosControlStage qosStage) {
        Map<String, StatsCollectorStage.Counters> byTenant = statsStage.snapshot();
        StringBuilder out = new StringBuilder();

        out.append("# HELP polywire_statements_total Total statements executed.\n");
        out.append("# TYPE polywire_statements_total counter\n");
        byTenant.forEach((tenant, counters) ->
                appendSeries(out, "polywire_statements_total", tenant, counters.statementCount().sum()));

        out.append("# HELP polywire_statement_errors_total Total statements that raised a SQLException.\n");
        out.append("# TYPE polywire_statement_errors_total counter\n");
        byTenant.forEach((tenant, counters) ->
                appendSeries(out, "polywire_statement_errors_total", tenant, counters.errorCount().sum()));

        out.append("# HELP polywire_statement_latency_seconds_total Cumulative statement execution time.\n");
        out.append("# TYPE polywire_statement_latency_seconds_total counter\n");
        byTenant.forEach((tenant, counters) -> appendSeries(out, "polywire_statement_latency_seconds_total", tenant,
                counters.totalLatencyNanos().sum() / 1_000_000_000.0));

        out.append("# HELP polywire_pool_connections Physical backend connections per pool, by state.\n");
        out.append("# TYPE polywire_pool_connections gauge\n");
        for (BackendConnectionPools.PoolStats pool : BackendConnectionPools.snapshot()) {
            appendPoolSeries(out, pool.poolKey(), "active", pool.activeConnections());
            appendPoolSeries(out, pool.poolKey(), "idle", pool.idleConnections());
            out.append("polywire_pool_max_size{pool=\"").append(escape(pool.poolKey())).append("\"} ")
                    .append(pool.maxPoolSize()).append('\n');
            out.append("polywire_pool_waiting{pool=\"").append(escape(pool.poolKey())).append("\"} ")
                    .append(pool.threadsAwaitingConnection()).append('\n');
        }

        if (qosStage != null) {
            out.append("# HELP polywire_qos_admitted_total Statements admitted by QosControlStage.\n");
            out.append("# TYPE polywire_qos_admitted_total counter\n");
            qosStage.snapshot().forEach((key, counters) -> appendQosSeries(out, "polywire_qos_admitted_total", key, counters.admitted().sum()));

            out.append("# HELP polywire_qos_rejected_total Statements rejected by QosControlStage (rate limit or pool saturation).\n");
            out.append("# TYPE polywire_qos_rejected_total counter\n");
            qosStage.snapshot().forEach((key, counters) -> appendQosSeries(out, "polywire_qos_rejected_total", key, counters.rejected().sum()));
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
        out.append("polywire_pool_connections{pool=\"").append(escape(poolKey)).append("\",state=\"").append(state)
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
