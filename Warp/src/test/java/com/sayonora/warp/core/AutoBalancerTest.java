package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AutoBalancerTest {

    private static Map<String, Long> rows(Object... kv) {
        Map<String, Long> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], ((Number) kv[i + 1]).longValue());
        }
        return m;
    }

    private static Map<Integer, Long> slots(long each, int count) {
        Map<Integer, Long> m = new java.util.TreeMap<>();
        for (int i = 0; i < count; i++) {
            m.put(i, each);
        }
        return m;
    }

    @Test
    void aBalancedGroupAndAnEmptyOneAreLeftAlone() {
        assertNull(AutoBalancer.decide(rows("a", 1000, "b", 1100), s -> slots(10, 100), 0.2, 32));
        assertNull(AutoBalancer.decide(rows("a", 0, "b", 0), s -> slots(0, 10), 0.2, 32));
        assertNull(AutoBalancer.decide(rows("a", 1000), s -> slots(10, 100), 0.2, 32));
    }

    @Test
    void anEmptyNewShardTakesAboutHalfTheGapFromTheFullestOne() {
        var d = AutoBalancer.decide(rows("a", 6000, "b", 6000, "c", 0), s -> slots(100, 60), 0.2, 64);
        assertNotNull(d);
        assertEquals("c", d.to());
        assertEquals(30, d.slots().size(), "half of 6000 rows is 30 slots of 100");
        assertEquals(3000, d.rowsToMove());
    }

    @Test
    void aPassMovesNoMoreThanTheSlotCap() {
        var d = AutoBalancer.decide(rows("a", 6000, "b", 0), s -> slots(100, 60), 0.2, 8);
        assertEquals(8, d.slots().size());
    }

    @Test
    void aSlotThatWouldOvershootIsSkippedForSmallerOnesAndAHugeKeyIsNotBounced() {
        // 3 slots: one holds 5000 of the 6000 rows
        Map<Integer, Long> uneven = new java.util.TreeMap<>(Map.of(0, 5000L, 1, 500L, 2, 500L));
        var d = AutoBalancer.decide(rows("a", 6000, "b", 0), s -> uneven, 0.2, 32);
        assertNotNull(d);
        assertEquals(java.util.List.of(1, 2), d.slots(), "the big slot would swing the imbalance the other way");
        // only one slot, and moving it just mirrors the imbalance: nothing to do
        assertNull(AutoBalancer.decide(rows("a", 6000, "b", 0), s -> new java.util.TreeMap<>(Map.of(0, 6000L)), 0.2, 32));
        // a lone slot that is bigger than half the gap but still narrows it is moved
        var d2 = AutoBalancer.decide(rows("a", 6000, "b", 1000), s -> new java.util.TreeMap<>(Map.of(0, 3000L, 1, 3000L)), 0.2, 32);
        assertNotNull(d2);
        assertEquals(1, d2.slots().size());
    }

    @Test
    void theWindowMayCrossMidnight() {
        var day = new AutoBalancer.Settings(300, 0.2, 32, Set.of(), LocalTime.of(1, 0), LocalTime.of(5, 0), 60);
        assertTrue(day.inWindow(LocalTime.of(2, 0)));
        assertFalse(day.inWindow(LocalTime.of(12, 0)));
        var night = new AutoBalancer.Settings(300, 0.2, 32, Set.of(), LocalTime.of(22, 0), LocalTime.of(4, 0), 60);
        assertTrue(night.inWindow(LocalTime.of(23, 30)));
        assertTrue(night.inWindow(LocalTime.of(3, 0)));
        assertFalse(night.inWindow(LocalTime.of(12, 0)));
        assertTrue(new AutoBalancer.Settings(300, 0.2, 32, Set.of(), null, null, 60).inWindow(LocalTime.of(12, 0)));
    }

    @Test
    void settingsReadFromTheEnvironment() {
        var s = AutoBalancer.Settings.fromEnv(Map.of("WARP_AUTO_REBALANCE_INTERVAL_SECONDS", "10", "WARP_AUTO_REBALANCE_THRESHOLD", "0.5",
                "WARP_AUTO_REBALANCE_TABLES", "Orders, items", "WARP_AUTO_REBALANCE_WINDOW", "01:00-05:30"));
        assertEquals(10, s.intervalSeconds());
        assertEquals(0.5, s.threshold());
        assertEquals(Set.of("orders", "items"), s.tables());
        assertEquals(LocalTime.of(5, 30), s.windowEnd());
        assertEquals(32, s.maxSlotsPerRun());
    }
}
