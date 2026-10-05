package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Planned switchover: freeze, catch up, promote, repoint, demote -- and the abort paths. */
class FailoverSwitchoverTest {

    private static final String P = "jdbc:postgresql://p/db";
    private static final String R1 = "jdbc:postgresql://r1/db";
    private static final String R2 = "jdbc:postgresql://r2/db";

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final List<String> steps = new ArrayList<>();
    private final List<String> repointed = new ArrayList<>();
    private String failAt;
    private boolean demotable = true;
    private boolean promoteMakesWritable = true;

    private BackendRegistry reg;
    private FailoverMonitor monitor;

    private void setUp(String mode) {
        reg = BackendRegistry.fromConfig("pg=" + P + "|u|pw||" + R1 + "^" + R2 + "|" + mode, null);
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        monitor = new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE),
                (b, o, n, r) -> reg.applyFailoverLocally(b, o, n, r), null, () -> 1_000_000L, 3, 60, 5)
                .withRepointer((r, n) -> repointed.add(r.jdbcUrl() + "->" + n.jdbcUrl()))
                .withSwitchoverOps(new FailoverMonitor.SwitchoverOps() {
                    private void step(String s) throws Exception {
                        steps.add(s);
                        if (s.equals(failAt)) {
                            throw new java.sql.SQLException(s + " failed");
                        }
                    }

                    @Override
                    public void freeze(BackendTarget p) throws Exception {
                        step("freeze");
                    }

                    @Override
                    public void unfreeze(BackendTarget p) throws Exception {
                        step("unfreeze");
                    }

                    @Override
                    public void awaitCaughtUp(BackendTarget p, BackendTarget r, long t) throws Exception {
                        step("catchup");
                    }

                    @Override
                    public void promote(BackendTarget r) throws Exception {
                        step("promote");
                        if (promoteMakesWritable) {
                            cluster.put(r.jdbcUrl(), NodeRole.WRITABLE);
                        }
                    }

                    @Override
                    public boolean demote(BackendTarget o, BackendTarget n) throws Exception {
                        step("demote");
                        return demotable;
                    }
                });
    }

    @Test
    void happyPathRunsTheStepsInOrderAndSwitches() {
        setUp("follow");
        var res = monitor.switchover("pg", R2);
        assertTrue(res.ok(), res.message());
        assertEquals(List.of("freeze", "catchup", "promote", "demote"), steps);
        assertEquals(R2, reg.get("pg").jdbcUrl());
        assertEquals(List.of(R1 + "->" + R2), repointed);
        assertEquals(P, reg.replicaSpecsOf("pg").get(1).url(), "old primary takes the target's slot");
        assertTrue(res.message().contains("replicates from the new one"));
    }

    @Test
    void anEngineThatCannotDemoteLeavesTheOldPrimaryReadOnlyAndSaysSo() {
        setUp("follow");
        demotable = false;
        var res = monitor.switchover("pg", R1);
        assertTrue(res.ok());
        assertTrue(res.message().contains("left read-only"), res.message());
    }

    @Test
    void failureToCatchUpReenablesWritesAndChangesNothing() {
        setUp("follow");
        failAt = "catchup";
        var res = monitor.switchover("pg", R1);
        assertFalse(res.ok());
        assertEquals(List.of("freeze", "catchup", "unfreeze"), steps);
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertTrue(repointed.isEmpty());
    }

    @Test
    void failureToPromoteReenablesWritesOnTheOldPrimary() {
        setUp("follow");
        failAt = "promote";
        var res = monitor.switchover("pg", R1);
        assertFalse(res.ok());
        assertEquals(List.of("freeze", "catchup", "promote", "unfreeze"), steps);
        assertEquals(P, reg.get("pg").jdbcUrl());
    }

    @Test
    void ifThePromotedNodeIsNotWritableTheOldPrimaryStaysFrozen() {
        setUp("follow");
        promoteMakesWritable = false;
        var res = monitor.switchover("pg", R1);
        assertFalse(res.ok());
        assertFalse(steps.contains("unfreeze"), "never risk two writers");
        assertEquals(P, reg.get("pg").jdbcUrl());
    }

    @Test
    void refusesWhenOffUnknownTargetOrPrimaryNotWritable() {
        setUp("off");
        assertFalse(monitor.switchover("pg", R1).ok());
        setUp("follow");
        assertFalse(monitor.switchover("pg", "jdbc:postgresql://other/db").ok());
        cluster.put(P, NodeRole.UNREACHABLE);
        assertFalse(monitor.switchover("pg", R1).ok());
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.UNREACHABLE);
        assertFalse(monitor.switchover("pg", R1).ok());
        assertTrue(steps.isEmpty(), "no step ran for a refused request");
    }
}
