package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** One dead/hanging backend must not delay the probes of the others (100 backends scenario). */
class BackendHealthCheckerConcurrencyTest {

    private static BackendRegistry registry(int n) {
        Map<String, BackendTarget> targets = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            targets.put("b" + i, new BackendTarget("b" + i, "jdbc:postgresql://unreachable.invalid:1/b" + i, "u", "p"));
        }
        return BackendRegistry.fromConfig(null, null, null, null, targets);
    }

    @Test
    void aHangingProbeCostsTheCycleOnlyItsTimeoutAndIsMarkedDown() {
        BackendRegistry reg = registry(100);
        AtomicInteger probed = new AtomicInteger();
        BackendHealthChecker checker = new BackendHealthChecker(reg, 15, null, t -> {
            probed.incrementAndGet();
            if (t.name().equals("b7")) {
                try {
                    Thread.sleep(30_000); // a black-holed host: connect hangs
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (t.name().equals("b9")) {
                return new BackendConnectivityTest.Result(false, "connection refused", 1, null);
            }
            return new BackendConnectivityTest.Result(true, "Connected", 1, "16");
        }, 1_000);

        long start = System.nanoTime();
        checker.probeAllSafely();
        long ms = (System.nanoTime() - start) / 1_000_000;

        assertEquals(100, probed.get());
        assertTrue(ms < 5_000, "the cycle took " + ms + "ms; a sequential checker would have taken 30s+");
        assertEquals(BackendRegistry.BackendState.DOWN, reg.stateOf("b7"), "the hung probe counts as failed");
        assertEquals(BackendRegistry.BackendState.DOWN, reg.stateOf("b9"));
        assertEquals(BackendRegistry.BackendState.ACTIVE, reg.stateOf("b8"));
        assertEquals(BackendRegistry.BackendState.ACTIVE, reg.stateOf("b99"));
    }

    @Test
    void drainingBackendsAreNeverProbedOrChanged() {
        BackendRegistry reg = registry(3);
        reg.setState("b1", BackendRegistry.BackendState.DRAINING);
        AtomicInteger probedB1 = new AtomicInteger();
        BackendHealthChecker checker = new BackendHealthChecker(reg, 15, null, t -> {
            if (t.name().equals("b1")) {
                probedB1.incrementAndGet();
            }
            return new BackendConnectivityTest.Result(false, "down", 1, null);
        }, 1_000);
        checker.probeAllSafely();
        assertEquals(0, probedB1.get());
        assertEquals(BackendRegistry.BackendState.DRAINING, reg.stateOf("b1"));
        assertEquals(BackendRegistry.BackendState.DOWN, reg.stateOf("b0"));
    }

    @Test
    void aBackendComesBackActiveWhenItsProbeRecovers() {
        BackendRegistry reg = registry(2);
        reg.setState("b0", BackendRegistry.BackendState.DOWN);
        new BackendHealthChecker(reg, 15, null,
                t -> new BackendConnectivityTest.Result(true, "Connected", 1, "16"), 1_000).probeAllSafely();
        assertEquals(BackendRegistry.BackendState.ACTIVE, reg.stateOf("b0"));
    }
}
