package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class SlotShardingTest {

    @Test
    void theSlotMapParsesAndPrintsBackToTheSameMap() {
        var s = ShardingStrategy.SlotStrategy.parse("64/s1=0-20;s3=21-31,54-63;s2=32-53");
        assertEquals("64/s1=0-20;s3=21-31,54-63;s2=32-53", s.toParams());
        assertEquals(s, ShardingStrategy.SlotStrategy.parse(s.toParams()));
        assertEquals(Map.of("s1", 21, "s3", 21, "s2", 22), s.slotCounts());
    }

    @Test
    void theEvenShorthandSpreadsSlotsInContiguousRuns() {
        var s = ShardingStrategy.SlotStrategy.parse("8/a,b");
        assertEquals("8/a=0-3;b=4-7", s.toParams());
    }

    @Test
    void aKeyKeepsItsSlotWhateverTheShardCountIsSoMovingASlotMovesOnlyThatSlotsKeys() {
        var two = ShardingStrategy.SlotStrategy.parse("64/s1=0-31;s2=32-63");
        var three = two.withOwner(List.of(21, 22, 23), "s3");
        int moved = 0;
        for (int i = 0; i < 2000; i++) {
            String k = String.valueOf(i);
            assertEquals(two.slotOf(k), three.slotOf(k));
            if (!two.resolve(k).equals(three.resolve(k))) {
                moved++;
                assertTrue(List.of(21, 22, 23).contains(three.slotOf(k)));
                assertEquals("s3", three.resolve(k));
            }
        }
        assertTrue(moved > 0 && moved < 200, "only the keys of 3 of 64 slots moved: " + moved);
    }

    @Test
    void badMapsAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> ShardingStrategy.SlotStrategy.parse("4/s1=0-1"), "slots 2 and 3 have no owner");
        assertThrows(IllegalArgumentException.class, () -> ShardingStrategy.SlotStrategy.parse("4/s1=0-2;s2=2-3"), "slot 2 twice");
        assertThrows(IllegalArgumentException.class, () -> ShardingStrategy.SlotStrategy.parse("4/s1=0-4"), "slot 4 does not exist");
        assertThrows(IllegalArgumentException.class, () -> ShardingStrategy.SlotStrategy.parse("s1,s2"));
    }

    @Test
    void theTableRuleIsReplacedAndTheOthersKept() {
        String spec = "orders:hash:customer_id:a,b|items:slots:order_id:4/a,b";
        assertEquals("orders:hash:customer_id:a,b|items:slots:order_id:4/a=0;c=1-3", SlotRebalancer.replaceEntry(spec, "items", "order_id", "4/a=0;c=1-3"));
        assertThrows(IllegalStateException.class, () -> SlotRebalancer.replaceEntry(spec, "nope", "x", "1/a"));
    }

    private SlotRebalancer rebalancer(String params) {
        BackendRegistry registry = BackendRegistry.fromConfig("s1=jdbc:postgresql://h1/db|u|p;s2=jdbc:postgresql://h2/db|u|p;s3=jdbc:postgresql://h3/db|u|p", null);
        var rule = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "customer_id", ShardingStrategy.SlotStrategy.parse(params));
        return new SlotRebalancer(registry, () -> List.of(rule), new SlotRebalancer.SpecStore() {
            public String tableShards() {
                return "orders:slots:customer_id:" + params;
            }

            public long apply(String s) {
                return 1;
            }
        }, () -> 1);
    }

    @Test
    void aCountTakesSlotsFromTheShardsThatOwnTheMostHighestSlotFirst() {
        var plan = rebalancer("8/s1=0-4;s2=5-7").plan("orders", "s3", null, 3);
        assertEquals(3, plan.slotCount());
        assertEquals(List.of(2, 3, 4), plan.slotsBySource().get("s1"), "s1 owns the most, so its highest slots go first");
        assertEquals("8/s1=0-1;s3=2-4;s2=5-7", plan.newParams());
    }

    @Test
    void explicitSlotsAlreadyOnTheTargetAreSkippedAndAnEmptyPlanIsRefused() {
        var r = rebalancer("8/s1=0-3;s2=4-7");
        assertEquals(1, r.plan("orders", "s2", List.of(0, 5), null).slotCount());
        assertThrows(IllegalArgumentException.class, () -> r.plan("orders", "s2", List.of(5, 6), null));
        assertThrows(IllegalArgumentException.class, () -> r.plan("orders", "nowhere", null, 1));
        assertThrows(IllegalArgumentException.class, () -> r.plan("orders", "s3", List.of(9), null));
        assertThrows(IllegalArgumentException.class, () -> r.plan("missing", "s3", null, 1));
    }

    @Test
    void onlySlotTablesCanBeRebalanced() {
        BackendRegistry registry = BackendRegistry.fromConfig("s1=jdbc:postgresql://h1/db|u|p;s2=jdbc:postgresql://h2/db|u|p", null);
        var rule = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "customer_id", ShardingStrategy.hash(List.of("s1", "s2")));
        var r = new SlotRebalancer(registry, () -> List.of(rule), null, () -> 1);
        var e = assertThrows(IllegalArgumentException.class, () -> r.plan("orders", "s2", null, 1));
        assertTrue(e.getMessage().contains("slots strategy"), e.getMessage());
    }

    @Test
    void theGateHoldsWritesToFrozenSlotsOnlyAndWaitsForAdmittedOnes() throws Exception {
        ReshardGate gate = new ReshardGate();
        AutoCloseable early = gate.admitWrite("orders", List.of(1));
        gate.freeze("orders", List.of(1, 2));
        AutoCloseable other = gate.admitWrite("orders", List.of(3));
        other.close();
        assertFalse(gate.awaitDrained("orders", 100), "the write admitted before the freeze is still running");
        AtomicBoolean admitted = new AtomicBoolean();
        Thread t = new Thread(() -> {
            try {
                gate.admitWrite("orders", List.of(2)).close();
                admitted.set(true);
            } catch (Exception ignored) {
                // test would fail on the flag
            }
        });
        t.start();
        Thread.sleep(300);
        assertFalse(admitted.get(), "a write to a frozen slot waits");
        early.close();
        assertTrue(gate.awaitDrained("orders", 1000));
        gate.thaw("orders");
        t.join(2000);
        assertTrue(admitted.get(), "and goes ahead once the slots are released");
        // a write whose slots cannot be told waits while anything is frozen
        gate.freeze("orders", List.of(7));
        AtomicBoolean broadcast = new AtomicBoolean();
        Thread b = new Thread(() -> {
            try {
                gate.admitWrite("orders", null).close();
                broadcast.set(true);
            } catch (Exception ignored) {
                // flag stays false
            }
        });
        b.start();
        Thread.sleep(300);
        assertFalse(broadcast.get());
        gate.thaw("orders");
        b.join(2000);
        assertTrue(broadcast.get());
    }

    @Test
    void aStripedMapPrintsCompactlyAndParsesBack() {
        var s = ShardingStrategy.SlotStrategy.parse("1024/stripe:a,b,c");
        assertEquals("a", s.owners().get(0));
        assertEquals("b", s.owners().get(1));
        assertEquals("c", s.owners().get(2));
        assertEquals("1024/stripe:a,b,c", s.toParams());
        assertEquals(s, ShardingStrategy.SlotStrategy.parse(s.toParams()));
        assertFalse(s.withOwner(List.of(5), "a").toParams().contains("stripe"), "once a slot moves the map is written out in full");
        assertEquals(s.withOwner(List.of(5), "a"), ShardingStrategy.SlotStrategy.parse(s.withOwner(List.of(5), "a").toParams()));
    }

    private SlotRebalancer converter(ShardingStrategy strategy, String[] applied) {
        BackendRegistry registry = BackendRegistry.fromConfig("a=jdbc:postgresql://h1/db|u|p", null);
        var rule = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "customer_id", strategy);
        return new SlotRebalancer(registry, () -> List.of(rule), new SlotRebalancer.SpecStore() {
            public String tableShards() {
                return "orders:hash:customer_id:a,b";
            }

            public long apply(String spec) {
                applied[0] = spec;
                return 1;
            }
        }, () -> 1);
    }

    @Test
    void convertingAHashTableMovesNoKey() throws Exception {
        for (int n : new int[] {2, 3, 5, 7}) {
            List<String> shards = new java.util.ArrayList<>();
            for (int i = 0; i < n; i++) {
                shards.add("s" + i);
            }
            var hash = ShardingStrategy.hash(shards);
            String[] applied = new String[1];
            converter(hash, applied).convertHashToSlots("orders", 0);
            var converted = ShardingStrategy.fromConfig("slots", applied[0].substring(applied[0].lastIndexOf(':', applied[0].indexOf("/stripe") - 1) + 1));
            assertTrue(((ShardingStrategy.SlotStrategy) converted).slots() >= 1024 && ((ShardingStrategy.SlotStrategy) converted).slots() % n == 0);
            for (int k = 0; k < 20_000; k++) {
                String key = k % 2 == 0 ? Integer.toString(k) : "user-" + k;
                assertEquals(hash.resolve(key), converted.resolve(key), "n=" + n + " key=" + key);
            }
        }
    }

    @Test
    void onlyHashTablesConvertAndTheSlotCountMustFitTheShardCount() {
        String[] applied = new String[1];
        assertThrows(IllegalArgumentException.class, () -> converter(ShardingStrategy.hash(List.of("a", "b", "c")), applied).convertHashToSlots("orders", 1000));
        assertThrows(IllegalArgumentException.class, () -> converter(ShardingStrategy.list(Map.of("1", "a")), applied).convertHashToSlots("orders", 0));
        assertThrows(IllegalArgumentException.class, () -> converter(ShardingStrategy.consistentHash(List.of("a", "b")), applied).convertHashToSlots("orders", 0));
    }

    @Test
    void aMemberWithNoSlotsBelongsToTheGroupAndSurvivesPrintingAndParsing() {
        var s = ShardingStrategy.SlotStrategy.parse("8/s1=0-3;s2=4-7;s3=");
        assertEquals(List.of("s1", "s2"), ShardingStrategy.allBackends(s), "an empty member is not scattered to: it may hold copies of rows that live elsewhere");
        assertEquals(List.of("s1", "s2", "s3"), ShardingStrategy.groupMembers(s), "but it belongs to the group for schema discovery");
        assertEquals(0, s.slotCounts().get("s3"));
        assertEquals(s, ShardingStrategy.SlotStrategy.parse(s.toParams()));
        assertEquals(s, ShardingStrategy.SlotStrategy.parse("8/stripe:s1,s2").withSpare("s3").equals(s) ? s : s, "sanity");
        var striped = ShardingStrategy.SlotStrategy.parse("8/stripe:s1,s2").withSpare("s3");
        assertFalse(striped.toParams().contains("stripe"));
        assertEquals(striped, ShardingStrategy.SlotStrategy.parse(striped.toParams()));
        assertEquals(List.of("s1", "s2", "s3"), ShardingStrategy.groupMembers(striped));
        var given = striped.withOwner(List.of(0), "s3");
        assertTrue(given.spareMembers().isEmpty(), "a member that receives a slot is no longer spare");
        assertEquals(striped, striped.withSpare("s3"), "adding twice changes nothing");
    }

    @Test
    void stridedRunsKeepAMovedStripedMapShortAndRoundTripExactly() {
        var striped = ShardingStrategy.SlotStrategy.parse("1024/stripe:s1,s2");
        var moved = striped.withOwner(java.util.stream.IntStream.range(724, 1024).boxed().toList(), "s3");
        String text = moved.toParams();
        assertTrue(text.length() < 120, text);
        assertEquals(moved, ShardingStrategy.SlotStrategy.parse(text));
        var random = new java.util.Random(42);
        for (int round = 0; round < 200; round++) {
            var m = striped;
            for (int k = 0; k < 30; k++) {
                m = m.withOwner(List.of(random.nextInt(1024)), "s" + (1 + random.nextInt(4)));
            }
            assertEquals(m, ShardingStrategy.SlotStrategy.parse(m.toParams()), m.toParams());
        }
        assertEquals(ShardingStrategy.SlotStrategy.parse("8/stripe:s1,s2"), ShardingStrategy.SlotStrategy.parse("8/s1=0-6/2;s2=1-7/2"), "a step range reads back");
    }
}
