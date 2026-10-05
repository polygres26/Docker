package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.FailoverMonitor.Action;
import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import com.sayonora.warp.core.FailoverMonitor.NodeState;
import com.sayonora.warp.core.FailoverMonitor.PromoteHooks;
import com.sayonora.warp.core.ReplicaRouter.LagSample;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Promote mode: Warp itself promotes a replica, but only through a long list of refusals. */
class FailoverPromoteTest {

    private static final String P = "jdbc:postgresql://p/db";
    private static final String R1 = "jdbc:postgresql://r1/db";
    private static final String R2 = "jdbc:postgresql://r2/db";
    private static final String SPEC = "pg=" + P + "|u|pw||" + R1 + "~3^" + R2 + "~7|promote";

    // ---- decision rule ------------------------------------------------------------------------

    private static NodeState n(String url, NodeRole role, int bad, int writable) {
        return new NodeState(url, role, bad, writable);
    }

    @Test
    void promoteModeAsksForAPromotionOnlyWhenNothingIsWritableAndAReplicaIsReachable() {
        var down = n(P, NodeRole.UNREACHABLE, 3, 0);
        var ro = List.of(n(R1, NodeRole.READ_ONLY, 1, 0), n(R2, NodeRole.READ_ONLY, 1, 0));
        assertEquals(Action.PROMOTE, FailoverMonitor.decide(down, ro, 3, true).action());
        assertEquals(Action.NO_WRITABLE_NODE, FailoverMonitor.decide(down, ro, 3, false).action(),
                "follow mode never promotes");
        assertEquals(Action.NO_WRITABLE_NODE, FailoverMonitor.decide(down,
                List.of(n(R1, NodeRole.UNREACHABLE, 4, 0)), 3, true).action(), "no reachable replica -> nothing to promote");
        assertEquals(Action.NONE, FailoverMonitor.decide(n(P, NodeRole.UNREACHABLE, 2, 0), ro, 3, true).action());
        assertEquals(Action.SWITCH, FailoverMonitor.decide(down,
                List.of(n(R1, NodeRole.WRITABLE, 0, 4), n(R2, NodeRole.READ_ONLY, 1, 0)), 3, true).action(),
                "if someone else already promoted one, follow it instead of promoting a second");
    }

    // ---- the monitor with a fake cluster, coordination and promoter ----------------------------

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final Map<String, Long> lsn = new HashMap<>();
    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final List<String> promoted = new ArrayList<>();
    private final List<String> fenced = new ArrayList<>();
    private final List<String> persisted = new ArrayList<>();

    private int votes = 1;
    private int live = 1;
    private boolean leaseHeldByOther;
    private boolean coordinationDown;
    private int leaseReleases;
    private boolean fenceResult = true;
    private boolean promoterFails;
    private boolean promoteMakesWritable = true;

    private final FailoverCoordination coordination = new FailoverCoordination() {
        @Override
        public void publishObservation(String backend, String primaryUrl, boolean down) throws Exception {
            if (coordinationDown) {
                throw new java.sql.SQLException("config database unreachable");
            }
        }

        @Override
        public Optional<Long> tryAcquireLease(String backend, long ttlSeconds) throws Exception {
            if (coordinationDown) {
                throw new java.sql.SQLException("config database unreachable");
            }
            return leaseHeldByOther ? Optional.empty() : Optional.of(1L);
        }

        @Override
        public void releaseLease(String backend, long term) {
            leaseReleases++;
        }

        @Override
        public int votesPrimaryDown(String backend, String primaryUrl, long freshSeconds) throws Exception {
            if (coordinationDown) {
                throw new java.sql.SQLException("config database unreachable");
            }
            return votes;
        }

        @Override
        public int liveInstances() throws Exception {
            if (coordinationDown) {
                throw new java.sql.SQLException("config database unreachable");
            }
            return live;
        }
    };

    private BackendRegistry reg;
    private FailoverMonitor monitor;

    private void setUp(double maxLag, boolean withFencer) {
        reg = BackendRegistry.fromConfig(SPEC, null);
        lsn.put(R1, 100L);
        lsn.put(R2, 200L);
        PromoteHooks hooks = new PromoteHooks(coordination,
                replica -> lsn.containsKey(replica.jdbcUrl()) ? OptionalLong.of(lsn.get(replica.jdbcUrl())) : OptionalLong.empty(),
                replica -> {
                    promoted.add(replica.jdbcUrl());
                    if (promoterFails) {
                        throw new java.sql.SQLException("pg_promote refused");
                    }
                    if (promoteMakesWritable) {
                        cluster.put(replica.jdbcUrl(), NodeRole.WRITABLE);
                    }
                },
                withFencer ? url -> {
                    fenced.add(url);
                    return fenceResult;
                } : null, maxLag, 120, 30);
        monitor = new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE),
                (backend, oldUrl, newUrl, replicas) -> {
                    persisted.add(backend + ":" + oldUrl + "->" + newUrl);
                    return reg.applyFailoverLocally(backend, oldUrl, newUrl, replicas);
                }, null, clock::get, 3, 60, 5).withPromoteHooks(hooks);
    }

    /** One healthy pass (so lag-while-primary-up is recorded), then the primary dies. */
    private void healthyThenPrimaryDies(double lag1, double lag2) {
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        reg.replicaRouter().recordSample(R1, new LagSample(true, true, lag1, null, clock.get()));
        reg.replicaRouter().recordSample(R2, new LagSample(true, true, lag2, null, clock.get()));
        monitor.evaluateOnce(false);
        cluster.put(P, NodeRole.UNREACHABLE);
    }

    private void passes(int n) {
        for (int i = 0; i < n; i++) {
            monitor.evaluateOnce(false);
        }
    }

    private boolean blockedFor(String fragment) {
        return monitor.recentEvents().stream()
                .anyMatch(e -> e.kind().equals("promote-blocked") && e.detail().contains(fragment));
    }

    @Test
    void promotesTheReplicaWithTheMostWalAfterTheConfirmationWindowAndSwitchesToIt() {
        setUp(30, true);
        healthyThenPrimaryDies(1.0, 2.0);
        passes(2);
        assertTrue(promoted.isEmpty(), "two probes are not enough");
        passes(1);
        assertEquals(List.of(R2), promoted, "R2 received more WAL than R1");
        assertEquals(List.of(P), fenced);
        assertEquals(R2, reg.get("pg").jdbcUrl());
        assertEquals(List.of("pg:" + P + "->" + R2), persisted);
        assertEquals(1, leaseReleases, "the lease is released after the promotion");
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("promoted")));
        // the old primary takes the promoted node's slot (same 7s allowance), R1 untouched
        assertEquals(P, reg.replicaSpecsOf("pg").get(1).url());
        assertEquals(7.0, reg.replicaSpecsOf("pg").get(1).maxLagSeconds());
    }

    @Test
    void walTiesGoToTheEarlierReplica() {
        setUp(30, false);
        healthyThenPrimaryDies(0, 0);
        lsn.put(R1, 500L);
        lsn.put(R2, 500L);
        passes(3);
        assertEquals(List.of(R1), promoted);
    }

    @Test
    void noMajorityNoPromotion() {
        setUp(30, false);
        votes = 1;
        live = 3;
        healthyThenPrimaryDies(0, 0);
        passes(4);
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("no majority"));
        assertEquals(P, reg.get("pg").jdbcUrl());
        votes = 2; // 2 of 3 is a majority
        passes(1);
        assertFalse(promoted.isEmpty());
    }

    @Test
    void anotherInstanceHoldingTheLeaseMeansWeDoNotPromote() {
        setUp(30, false);
        leaseHeldByOther = true;
        healthyThenPrimaryDies(0, 0);
        passes(4);
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("lease"));
    }

    @Test
    void ifTheConfigDatabaseIsUnreachableWeRefuseToPromote() {
        setUp(30, false);
        coordinationDown = true;
        healthyThenPrimaryDies(0, 0);
        passes(4);
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("cannot coordinate"));
        assertEquals(P, reg.get("pg").jdbcUrl());
    }

    @Test
    void aFailedFenceStopsThePromotionAndTheLeaseIsStillReleased() {
        setUp(30, true);
        fenceResult = false;
        healthyThenPrimaryDies(0, 0);
        passes(3);
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("fencing"));
        assertEquals(1, leaseReleases);
    }

    @Test
    void ifAnythingIsWritableOnTheFreshRecheckWeDoNotAddASecondWriter() {
        setUp(30, false);
        healthyThenPrimaryDies(0, 0);
        passes(2);
        cluster.put(P, NodeRole.WRITABLE); // the primary recovers right at the decisive moment
        // the next pass's own probe sees it writable, so the decision is simply "healthy"
        passes(1);
        assertTrue(promoted.isEmpty());
        assertEquals(P, reg.get("pg").jdbcUrl());
    }

    @Test
    void dataLossGateRefusesALaggingCandidateAndAnUnknownOne() {
        setUp(5, false);
        healthyThenPrimaryDies(1.0, 60.0); // R2 has the most WAL but was 60s behind
        passes(3);
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("was 60.0s behind"));

        setUp(5, false);
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        passes(3); // never saw the primary alive, so no honest lag reading exists
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("data loss unknown"));

        setUp(-1, false); // gate disabled: promotes regardless
        cluster.put(P, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        passes(3);
        assertEquals(List.of(R2), promoted);
    }

    @Test
    void aPromoterFailureOrAReplicaThatDoesNotBecomeWritableDoesNotSwitch() {
        setUp(30, false);
        promoterFails = true;
        healthyThenPrimaryDies(0, 0);
        passes(3);
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertTrue(blockedFor("pg_promote refused"));
        assertEquals(1, leaseReleases);

        promoted.clear();
        promoterFails = false;
        promoteMakesWritable = false;
        clock.addAndGet(120_000);
        passes(1);
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertTrue(blockedFor("does not report itself writable"));
    }

    @Test
    void aManualEvaluateCannotShortenThePromotionConfirmationWindow() {
        setUp(30, false);
        healthyThenPrimaryDies(0, 0);
        var d = monitor.evaluateNow("pg"); // one observation only
        assertEquals(Action.PROMOTE, d.action());
        assertTrue(promoted.isEmpty());
        assertTrue(blockedFor("full 3-probe confirmation"));
    }

    @Test
    void cooldownAppliesToPromotionsToo() {
        setUp(30, false);
        healthyThenPrimaryDies(0, 0);
        passes(3);
        assertEquals(1, promoted.size());
        // the new primary (R2) now dies; R1 is read-only. Inside the cooldown nothing may be promoted again.
        cluster.put(R2, NodeRole.UNREACHABLE);
        cluster.put(R1, NodeRole.READ_ONLY);
        passes(4);
        assertEquals(1, promoted.size());
        assertTrue(monitor.recentEvents().stream().anyMatch(e -> e.kind().equals("switch-suppressed")));
    }

    @Test
    void withoutPromoteHooksAPromoteBackendIsMonitoredButNeverPromoted() {
        reg = BackendRegistry.fromConfig(SPEC, null);
        monitor = new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE),
                null, null, clock::get, 3, 60, 5);
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        passes(4);
        assertEquals(P, reg.get("pg").jdbcUrl());
        assertTrue(blockedFor("not available"));
    }

    @Test
    void followModeNeverPromotes() {
        reg = BackendRegistry.fromConfig(SPEC.replace("|promote", "|follow"), null);
        promoted.clear();
        monitor = new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE),
                null, null, clock::get, 3, 60, 5).withPromoteHooks(new PromoteHooks(coordination,
                r -> OptionalLong.of(1), r -> promoted.add(r.jdbcUrl()), null, 30, 120, 30));
        cluster.put(R1, NodeRole.READ_ONLY);
        cluster.put(R2, NodeRole.READ_ONLY);
        passes(5);
        assertTrue(promoted.isEmpty());
    }
}
