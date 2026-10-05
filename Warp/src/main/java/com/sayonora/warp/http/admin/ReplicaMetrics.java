package com.sayonora.warp.http.admin;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendSetModel;
import com.sayonora.warp.core.FailoverMonitor;
import com.sayonora.warp.core.ReplicaRouter;
import java.util.Locale;
import java.util.Map;

/**
 * Prometheus series for replica read routing and failover. The {@code replica} label is the replica's
 * URL with credentials masked ({@link BackendSetModel#maskUrl}); {@code backend} is the primary's name.
 * Cardinality is bounded by the configured topology (replicas x a handful of reasons/event kinds).
 */
public final class ReplicaMetrics {

    private ReplicaMetrics() {
    }

    public static String render(BackendRegistry registry) {
        StringBuilder out = new StringBuilder();
        ReplicaRouter router = registry.replicaRouter();
        FailoverMonitor monitor = registry.failoverMonitor();

        head(out, "warp_replica_lag_seconds", "gauge",
                "Last measured replication lag of a replica (absent while it cannot be measured).");
        head(out, "warp_replica_sample_age_seconds", "gauge", "Age of the replica's last lag measurement.");
        head(out, "warp_replica_eligible", "gauge", "1 when the replica may serve reads right now, else 0.");
        head(out, "warp_replica_quarantined", "gauge", "1 while a replica is paused after a failed read.");
        head(out, "warp_replica_reads_routed_total", "counter", "Reads sent to this replica.");
        head(out, "warp_replica_read_decisions_total", "counter",
                "Read-routing decisions per primary, by outcome (reads that stayed on the primary, and why).");
        long now = System.currentTimeMillis();
        for (String primary : registry.allReplicaSpecs().keySet()) {
            for (ReplicaRouter.Replica r : router.replicasOf(primary)) {
                String labels = "backend=\"" + esc(primary) + "\",replica=\""
                        + esc(BackendSetModel.maskUrl(r.target().jdbcUrl())) + "\"";
                var sample = router.samplesSnapshot().get(r.key());
                if (sample != null) {
                    if (sample.ok() && sample.isReplica()) {
                        line(out, "warp_replica_lag_seconds", labels, sample.lagSeconds());
                    }
                    line(out, "warp_replica_sample_age_seconds", labels, (now - sample.takenAtMillis()) / 1000.0);
                }
                line(out, "warp_replica_eligible", labels, router.eligible(r) ? 1 : 0);
                line(out, "warp_replica_quarantined", labels, router.isQuarantined(r) ? 1 : 0);
                line(out, "warp_replica_reads_routed_total", labels, router.routedCount(r.key()));
            }
            for (var e : router.reasonCounts(primary).entrySet()) {
                line(out, "warp_replica_read_decisions_total", "backend=\"" + esc(primary) + "\",reason=\""
                        + e.getKey().name().toLowerCase(Locale.ROOT) + "\"", e.getValue());
            }
        }

        head(out, "warp_failover_primary_writable", "gauge",
                "1 when the last probe found the backend's primary writable, 0 when not, absent before the first probe.");
        head(out, "warp_failover_last_switch_timestamp_seconds", "gauge",
                "Unix time of the last primary switch this process applied (absent if none).");
        head(out, "warp_failover_events_total", "counter",
                "Failover events since start, by kind (switched, promoted, switchover, rejoined, promote-blocked, "
                        + "split-brain-suspected, rejoin-needed, repoint-failed...).");
        if (monitor != null) {
            for (String backend : registry.allReplicaSpecs().keySet()) {
                var role = monitor.observedRole(backend, registry.get(backend) == null ? "" : registry.get(backend).jdbcUrl());
                if (role != null) {
                    line(out, "warp_failover_primary_writable", "backend=\"" + esc(backend) + "\"",
                            role == FailoverMonitor.NodeRole.WRITABLE ? 1 : 0);
                }
                Long last = monitor.lastSwitchMillis(backend);
                if (last != null) {
                    line(out, "warp_failover_last_switch_timestamp_seconds", "backend=\"" + esc(backend) + "\"", last / 1000.0);
                }
            }
            for (Map.Entry<String, Long> e : monitor.eventCounts().entrySet()) {
                int sep = e.getKey().indexOf('\u0000');
                line(out, "warp_failover_events_total", "backend=\"" + esc(e.getKey().substring(0, sep))
                        + "\",kind=\"" + esc(e.getKey().substring(sep + 1)) + "\"", e.getValue());
            }
        }
        return out.toString();
    }

    private static void head(StringBuilder out, String name, String type, String help) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void line(StringBuilder out, String name, String labels, double v) {
        out.append(name).append('{').append(labels).append("} ").append(String.format(Locale.ROOT, "%s", v == Math.rint(v)
                && Math.abs(v) < 1e15 ? String.valueOf((long) v) : String.valueOf(v))).append('\n');
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
