package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import com.sayonora.warp.core.ReplicaRouter.LagSample;
import com.sayonora.warp.http.admin.ReplicaMetrics;
import org.junit.jupiter.api.Test;

class ReplicaMetricsTest {

    private static final String P = "jdbc:postgresql://p/db";
    private static final String R1 = "jdbc:postgresql://r1/db";
    private static final String R2 = "jdbc:postgresql://r2/db";

    @Test
    void rendersLagEligibilityRoutingAndFailoverSeries() {
        BackendRegistry reg = BackendRegistry.fromConfig("pg=" + P + "|u|secret||" + R1 + "~5^" + R2 + "~5|follow", null);
        long now = System.currentTimeMillis();
        reg.replicaRouter().recordSample(R1, new LagSample(true, true, 1.5, null, now));
        reg.replicaRouter().recordSample(R2, new LagSample(false, true, 0, "threads stopped", now));
        java.util.Map<String, NodeRole> roles = new java.util.HashMap<>();
        roles.put(P, NodeRole.UNREACHABLE);
        roles.put(R1, NodeRole.WRITABLE);
        roles.put(R2, NodeRole.READ_ONLY);
        FailoverMonitor monitor = new FailoverMonitor(reg, n -> roles.get(n.jdbcUrl()),
                (b, o, n, r) -> reg.applyFailoverLocally(b, o, n, r), null, () -> 1_000L, 1, 0, 5);
        reg.setFailoverMonitor(monitor);
        monitor.evaluateOnce(false); // primary down, r1 writable -> switch to r1

        String text = ReplicaMetrics.render(reg);
        assertTrue(text.contains("# TYPE warp_replica_lag_seconds gauge"), text);
        assertFalse(text.contains("secret"), "credentials never appear in labels");
        // after the switch r1 is the primary and the old primary p sits in the replica list
        assertTrue(text.contains("warp_failover_events_total{backend=\"pg\",kind=\"switched\"} 1"), text);
        assertTrue(text.contains("warp_failover_primary_writable{backend=\"pg\"}"), text);
        assertTrue(text.contains("warp_failover_last_switch_timestamp_seconds{backend=\"pg\"}"), text);
        assertTrue(text.contains("warp_replica_eligible{backend=\"pg\",replica=\"" + BackendSetModel.maskUrl(R2) + "\"} 0"), text);
        // every sample line is "name{labels} number"
        for (String l : text.split("\n")) {
            if (!l.startsWith("#") && !l.isBlank()) {
                assertTrue(l.matches("[a-z_]+\\{[^}]*\\} -?[0-9.eE+-]+"), "bad exposition line: " + l);
            }
        }
    }

    @Test
    void anUnmeasurableReplicaHasNoLagSeries() {
        BackendRegistry reg = BackendRegistry.fromConfig("pg=" + P + "|u|pw||" + R1 + "~5|off", null);
        reg.replicaRouter().recordSample(R1, new LagSample(false, true, 0, "stopped", System.currentTimeMillis()));
        String text = ReplicaMetrics.render(reg);
        assertFalse(text.contains("warp_replica_lag_seconds{"), "no lag value to report: " + text);
        assertTrue(text.contains("warp_replica_eligible{"), text);
    }
}
