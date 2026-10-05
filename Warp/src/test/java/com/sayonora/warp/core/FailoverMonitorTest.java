package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.FailoverMonitor.Action;
import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import com.sayonora.warp.core.FailoverMonitor.NodeState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FailoverMonitorTest {

    private static final String P = "jdbc:postgresql://p/db";
    private static final String R1 = "jdbc:postgresql://r1/db";
    private static final String R2 = "jdbc:postgresql://r2/db";
    private static final String SPEC = "pg=" + P + "|u|pw||" + R1 + "~3^" + R2 + "~7";

    // ---- pure decision rule -------------------------------------------------------------------

    private static NodeState n(String url, NodeRole role, int bad, int writable) {
        return new NodeState(url, role, bad, writable);
    }

    @Test
    void healthyPrimaryNeverSwitches() {
        var d = FailoverMonitor.decide(n(P, NodeRole.WRITABLE, 0, 9), List.of(n(R1, NodeRole.READ_ONLY, 1, 0)), 3);
        assertEquals(Action.NONE, d.action());
    }

    @Test
    void aWritableReplicaWhileThePrimaryIsWritableIsASuspectedSplitBrain() {
        var d = FailoverMonitor.decide(n(P, NodeRole.WRITABLE, 0, 9), List.of(n(R1, NodeRole.WRITABLE, 0, 9)), 3);
        assertEquals(Action.SPLIT_BRAIN, d.action());
    }

    @Test
    void primaryDownIsNotActedOnUntilConfirmed() {
        var d = FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 2, 0), List.of(n(R1, NodeRole.WRITABLE, 0, 5)), 3);
        assertEquals(Action.NONE, d.action());
        assertTrue(d.reason().contains("2/3"));
    }

    @Test
    void confirmedDownPrimaryWithExactlyOneWritableStableReplicaSwitches() {
        var d = FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 3, 0),
                List.of(n(R1, NodeRole.READ_ONLY, 1, 0), n(R2, NodeRole.WRITABLE, 0, 3)), 3);
        assertEquals(Action.SWITCH, d.action());
        assertEquals(1, d.targetReplicaIndex());
    }

    @Test
    void aPrimaryThatIsReachableButDemotedToReadOnlyCountsAsNotWritable() {
        var d = FailoverMonitor.decide(n(P, NodeRole.READ_ONLY, 3, 0), List.of(n(R1, NodeRole.WRITABLE, 0, 4)), 3);
        assertEquals(Action.SWITCH, d.action());
    }

    @Test
    void aFreshlyWritableCandidateMustAlsoBeStable() {
        var d = FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 5, 0), List.of(n(R1, NodeRole.WRITABLE, 0, 1)), 3);
        assertEquals(Action.NONE, d.action());
    }

    @Test
    void twoWritableReplicasWhilePrimaryDownIsAmbiguousSoNoSwitch() {
        var d = FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 5, 0),
                List.of(n(R1, NodeRole.WRITABLE, 0, 5), n(R2, NodeRole.WRITABLE, 0, 5)), 3);
        assertEquals(Action.SPLIT_BRAIN, d.action());
    }

    @Test
    void nothingWritableAnywhereMeansNothingToFollow() {
        var d = FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 5, 0), List.of(n(R1, NodeRole.READ_ONLY, 0, 0)), 3);
        assertEquals(Action.NO_WRITABLE_NODE, d.action());
    }

    // ---- the monitor, with a fake cluster and clock -------------------------------------------

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final List<String> persisted = new ArrayList<>();

    private BackendRegistry registry() {
        return BackendRegistry.fromConfig(SPEC, null);
    }

    private FailoverMonitor monitor(BackendRegistry reg, FailoverMonitor.Persister persister) {
        return new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE), persister,
                null, clock::get, 3, 60, 5);
    }

    private FailoverMonitor.Persister applyingPersister(BackendRegistry reg) {
        return (backend, oldUrl, newUrl, replicas) -> {
            persisted.add(backend + ":" + oldUrl + "->" + newUrl);
            return reg.applyFailoverLocally(backend, oldUrl, newUrl, replicas);
        };
    }

    @Test
    void switchesOnlyAfterTheConfirmationWindowThenSwapsPrimaryAndKeepsLagAllowances() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, applyingPersister(reg));
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        cluster.put(R2, NodeRole.READ_ONLY);
        m.evaluateOnce(false);
        m.evaluateOnce(false);
        assertEquals(P, reg.get("pg").jdbcUrl(), "two probes are not enough");
        assertTrue(persisted.isEmpty());
        m.evaluateOnce(false);
        assertEquals(R1, reg.get("pg").jdbcUrl(), "third consecutive probe confirms the failover");
        assertEquals(List.of("pg:" + P + "->" + R1), persisted);
        // old primary took the promoted node's slot, keeping that slot's lag allowance (3s); r2 untouched (7s)
        List<ReplicaSpec> replicas = reg.replicaSpecsOf("pg");
        assertEquals(P, replicas.get(0).url());
        assertEquals(3.0, replicas.get(0).maxLagSeconds());
        assertEquals(R2, replicas.get(1).url());
        assertEquals(7.0, replicas.get(1).maxLagSeconds());
        assertEquals("u", reg.get("pg").user(), "credentials carry over");
        assertTrue(m.recentEvents().stream().anyMatch(e -> e.kind().equals("switched")));
    }

    @Test
    void aSingleBlipNeverMovesTraffic() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, applyingPersister(reg));
        cluster.put(R1, NodeRole.WRITABLE);
        cluster.put(P, NodeRole.UNREACHABLE);
        m.evaluateOnce(false);
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        for (int i = 0; i < 5; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertTrue(persisted.isEmpty());
    }

    @Test
    void splitBrainIsReportedAndNothingChanges() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, applyingPersister(reg));
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        for (int i = 0; i < 5; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertEquals(1, m.recentEvents().stream().filter(e -> e.kind().equals("split-brain-suspected")).count(),
                "reported once, not once per probe");
    }

    @Test
    void cooldownStopsPingPongThenAllowsTheNextSwitch() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, applyingPersister(reg));
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        for (int i = 0; i < 3; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(R1, reg.get("pg").jdbcUrl());
        // now the NEW primary dies and the old one comes back writable
        cluster.put(R1, NodeRole.UNREACHABLE);
        cluster.put(P, NodeRole.WRITABLE);
        for (int i = 0; i < 4; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(R1, reg.get("pg").jdbcUrl(), "still inside the cooldown");
        assertTrue(m.recentEvents().stream().anyMatch(e -> e.kind().equals("switch-suppressed")));
        clock.addAndGet(61_000);
        m.evaluateOnce(false);
        assertEquals(P, reg.get("pg").jdbcUrl(), "cooldown over -> follows the writable node again");
    }

    @Test
    void whenTheConfigCannotBeWrittenTheSwitchStillHappensInMemoryAndSurvivesAStaleReload() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, (b, o, nu, r) -> {
            throw new java.sql.SQLException("config database is down");
        });
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        for (int i = 0; i < 3; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(R1, reg.get("pg").jdbcUrl());
        // a reload from a config that still names the old primary must not revert the failover
        reg.reload(SPEC, null, null, null);
        assertEquals(R1, reg.get("pg").jdbcUrl());
        assertEquals(P, reg.replicaSpecsOf("pg").get(0).url());
        // once the config itself names the new primary, the override retires and the config wins
        reg.reload("pg=" + R1 + "|u|pw||" + P + "~3^" + R2 + "~7", null, null, null);
        assertEquals(R1, reg.get("pg").jdbcUrl());
        reg.reload(SPEC, null, null, null);
        assertEquals(P, reg.get("pg").jdbcUrl(), "override is gone: a config naming P as primary is honoured again");
    }

    @Test
    void aLostCompareAndSetLeavesTheRegistryAloneForTheConfigReloadToFix() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, (b, o, nu, r) -> false);
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        for (int i = 0; i < 3; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(P, reg.get("pg").jdbcUrl());
    }

    @Test
    void manualEvaluateNeedsOnlyOneObservationButStillNeverSwitchesAHealthyPrimary() {
        BackendRegistry reg = registry();
        FailoverMonitor m = monitor(reg, applyingPersister(reg));
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        assertEquals(Action.NONE, m.evaluateNow("pg").action());
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        assertEquals(Action.SWITCH, m.evaluateNow("pg").action());
        assertEquals(R1, reg.get("pg").jdbcUrl());
    }

    @Test
    void offModeAndUnsupportedEnginesAreNeverTouched() {
        BackendRegistry off = BackendRegistry.fromConfig(SPEC + "|off", null);
        assertEquals("off", off.failoverModeOf("pg"));
        FailoverMonitor m = monitor(off, applyingPersister(off));
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.WRITABLE);
        for (int i = 0; i < 5; i++) {
            m.evaluateOnce(false);
        }
        assertEquals(P, off.get("pg").jdbcUrl());

        BackendRegistry mysql = BackendRegistry.fromConfig("m=jdbc:mysql://p/db|u|pw||jdbc:mysql://r/db", null);
        FailoverMonitor mm = monitor(mysql, applyingPersister(mysql));
        for (int i = 0; i < 5; i++) {
            mm.evaluateOnce(false);
        }
        assertEquals("jdbc:mysql://p/db", mysql.get("m").jdbcUrl());
    }

    @Test
    void backendsWithoutReplicasHaveFailoverOffAndFollowIsTheDefaultWithReplicas() {
        BackendRegistry reg = BackendRegistry.fromConfig("a=jdbc:postgresql://a/db;" + "b=jdbc:postgresql://b/db|u|p||" + R1, null);
        assertEquals("off", reg.failoverModeOf("a"));
        assertEquals("follow", reg.failoverModeOf("b"));
        assertFalse(reg.failoverModeOf("b").isEmpty());
    }
}
