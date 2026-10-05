package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.EngineHa.RejoinOutcome;
import com.sayonora.warp.core.EngineHa.RejoinResult;
import com.sayonora.warp.core.FailoverMonitor.NodeRole;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** A returned old primary is rejoined as a replica only when the engine says that is safe. */
class FailoverRejoinTest {

    private static final String P = "jdbc:mysql://p/db";
    private static final String OLD = "jdbc:mysql://old/db";

    private final Map<String, NodeRole> cluster = new HashMap<>();
    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final List<String> attempts = new ArrayList<>();
    private final List<String> fenced = new ArrayList<>();
    private RejoinResult result = RejoinResult.of(RejoinOutcome.REJOINED, "now replicates");
    private BackendRegistry reg;
    private FailoverMonitor monitor;

    private void setUp() {
        reg = BackendRegistry.fromConfig("my=" + P + "|u|pw||" + OLD + "|follow", null);
        cluster.put(P, NodeRole.WRITABLE);
        cluster.put(OLD, NodeRole.WRITABLE); // came back still writable
        monitor = new FailoverMonitor(reg, node -> cluster.getOrDefault(node.jdbcUrl(), NodeRole.UNREACHABLE),
                (b, o, n, r) -> reg.applyFailoverLocally(b, o, n, r), null, clock::get, 3, 60, 5)
                .withRejoinExecutor(Runnable::run)
                .withStaleWriterFence(node -> fenced.add(node.jdbcUrl()))
                .withFrozenCheck(node -> false)
                .withRejoiner((node, primary) -> {
                    attempts.add(node.jdbcUrl() + "->" + primary.jdbcUrl());
                    if (result.outcome() == RejoinOutcome.REJOINED) {
                        cluster.put(node.jdbcUrl(), NodeRole.READ_ONLY);
                    }
                    return result;
                });
    }

    private boolean has(String kind) {
        return monitor.recentEvents().stream().anyMatch(e -> e.kind().equals(kind));
    }

    @Test
    void aWritableReturnedOldPrimaryIsRejoinedAndNoSplitBrainIsReported() {
        setUp();
        monitor.evaluateOnce(false);
        assertEquals(List.of(OLD + "->" + P), attempts);
        assertTrue(has("rejoined"));
        assertFalse(has("split-brain-suspected"), "a node that was just made a replica is not a second writer");
    }

    @Test
    void aNodeThatNeedsARebuildIsReportedOnceAndStillRaisesTheSplitBrainAlarm() {
        setUp();
        result = RejoinResult.of(RejoinOutcome.NEEDS_REBUILD, "has errant transactions");
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertTrue(has("rejoin-needed"));
        assertTrue(has("split-brain-suspected"));
        assertEquals(1, attempts.size(), "one attempt per cooldown, not one per probe");
        clock.addAndGet(61_000);
        monitor.evaluateOnce(false);
        assertEquals(2, attempts.size(), "tried again after the cooldown");
        assertEquals(1, monitor.recentEvents().stream().filter(e -> e.kind().equals("rejoin-needed")).count(),
                "the same finding is not re-announced");
    }

    @Test
    void aFailingRejoinIsReportedNotThrown() {
        setUp();
        monitor.withRejoiner((n, p) -> {
            throw new java.sql.SQLException("access denied");
        });
        monitor.evaluateOnce(false);
        assertTrue(has("rejoin-failed"));
    }

    @Test
    void disabledRejoinTouchesNothing() {
        setUp();
        monitor.withRejoiner(null);
        monitor.evaluateOnce(false);
        assertTrue(attempts.isEmpty());
        assertTrue(has("split-brain-suspected"));
    }

    @Test
    void anOrdinaryReadOnlyReplicaIsLeftAlone() {
        setUp();
        cluster.put(OLD, NodeRole.READ_ONLY);
        monitor.evaluateOnce(false);
        assertTrue(attempts.isEmpty());
    }

    @Test
    void aSlowRejoinRunsOffTheMonitorThreadAndIsNotStartedTwice() throws Exception {
        setUp();
        monitor.withRejoinExecutor(java.util.concurrent.Executors.newSingleThreadExecutor());
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        monitor.withRejoiner((node, primary) -> {
            attempts.add(node.jdbcUrl());
            started.countDown();
            release.await();
            cluster.put(node.jdbcUrl(), NodeRole.READ_ONLY);
            return RejoinResult.of(RejoinOutcome.REJOINED, "slow rebuild done");
        });
        long t0 = System.nanoTime();
        monitor.evaluateOnce(false);
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000, "evaluation does not wait for the rejoin");
        assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS));
        clock.addAndGet(61_000); // past the cooldown, but the first attempt is still running
        monitor.evaluateOnce(false);
        assertEquals(1, attempts.size(), "no second attempt while one is in flight");
        release.countDown();
        long deadline = System.currentTimeMillis() + 5000;
        while (!has("rejoined") && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(has("rejoined"));
    }

    @Test
    void aStaleWritableNodeIsFrozenOnceItHasBeenWritableForTheConfirmationWindow() {
        setUp();
        monitor.withRejoiner(null); // the freeze does not depend on a rejoin being configured
        monitor.evaluateOnce(false);
        monitor.evaluateOnce(false);
        assertTrue(fenced.isEmpty(), "two probes are not enough to call it stale");
        monitor.evaluateOnce(false);
        assertEquals(List.of(OLD), fenced);
        assertTrue(has("stale-writer-frozen"));
        monitor.evaluateOnce(false);
        assertEquals(1, fenced.size(), "once per cooldown, not once per probe");
        clock.addAndGet(61_000);
        monitor.evaluateOnce(false);
        assertEquals(2, fenced.size(), "frozen again after the cooldown, in case a session set it writable again");
        assertEquals(1, monitor.recentEvents().stream().filter(e -> e.kind().equals("stale-writer-frozen")).count(),
                "announced once");
    }

    @Test
    void anEngineThatCannotFreezeSaysSoInsteadOfFailing() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withStaleWriterFence(node -> {
            throw new UnsupportedOperationException("no safe SQL for it");
        });
        passes3();
        assertTrue(has("stale-writer-unfenced"));
    }

    @Test
    void aFreezeThatFailsIsReportedNotThrown() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withStaleWriterFence(node -> {
            throw new java.sql.SQLException("access denied");
        });
        passes3();
        assertTrue(has("stale-writer-freeze-failed"));
    }

    @Test
    void freezingCanBeSwitchedOffAndNeverTouchesAnOrdinaryReplica() {
        setUp();
        monitor.withStaleWriterFence(null);
        passes3();
        assertTrue(fenced.isEmpty());
        setUp();
        cluster.put(OLD, NodeRole.READ_ONLY);
        passes3();
        assertTrue(fenced.isEmpty(), "a replica in recovery is not a second writer");
    }

    private void passes3() {
        for (int i = 0; i < 3; i++) {
            monitor.evaluateOnce(false);
        }
    }

    @Test
    void anInstanceBehindTheSharedConfigDoesNotFreezeAnything() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withPrimaryView(backend -> OLD); // the shared config already names the other node as primary
        passes3();
        assertTrue(fenced.isEmpty(), "this instance's primary is stale: the 'second writer' might be the new real primary");
        assertTrue(has("stale-writer-deferred"));
    }

    @Test
    void anInstanceThatCannotReadTheSharedConfigDoesNotFreezeAnything() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withPrimaryView(backend -> {
            throw new java.sql.SQLException("config database unreachable");
        });
        passes3();
        assertTrue(fenced.isEmpty());
        assertTrue(has("stale-writer-deferred"));
    }

    @Test
    void anInstanceInAgreementWithTheSharedConfigFreezes() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withPrimaryView(backend -> P);
        passes3();
        assertEquals(List.of(OLD), fenced);
    }

    @Test
    void aFrozenNodeIsReadOnlyNotASecondWriter() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withFrozenCheck(node -> node.jdbcUrl().equals(OLD)); // e.g. the primary a planned switchover left read-only
        passes3();
        assertFalse(has("split-brain-suspected"), "a node that refuses writes is not a second writer");
        assertTrue(fenced.isEmpty(), "nothing to freeze: it already is");
        assertTrue(has("frozen-node"));
    }

    @Test
    void aFrozenCheckThatFailsLeavesTheNodeWritable() {
        setUp();
        monitor.withRejoiner(null);
        monitor.withFrozenCheck(node -> {
            throw new java.sql.SQLException("cannot query it");
        });
        passes3();
        assertTrue(has("split-brain-suspected"), "no evidence it is frozen: the alarm stays");
        assertFalse(has("frozen-node"));
    }

    @Test
    void theConfiguredPrimaryIsNeverTreatedAsFrozen() {
        setUp();
        cluster.put(OLD, NodeRole.READ_ONLY); // an ordinary replica
        monitor.withRejoiner(null);
        monitor.withFrozenCheck(node -> true); // would say frozen for everything, including the primary
        passes3();
        assertFalse(has("frozen-node"), "only a replica entry that probes as writable is checked");
        assertFalse(has("split-brain-suspected"));
    }
}
