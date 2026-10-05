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
}
