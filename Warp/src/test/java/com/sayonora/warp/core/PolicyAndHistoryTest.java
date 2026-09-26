package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class PolicyAndHistoryTest {

    @Test
    void firewallDenialIsRecordedInThePolicyDecisionLog() {
        PolicyDecisionLog.get().clearForTest();
        FirewallStage fw = new FirewallStage(List.of(new FirewallStage.Rule(7, 100, FirewallStage.Action.DENY, null, null,
                Pattern.compile("DROP", Pattern.CASE_INSENSITIVE), "no drops")));
        Statement s = new Statement("t", SourceDialect.POSTGRES, "DROP TABLE accounts", List.of(), "default", null,
                AccessContext.ANONYMOUS);
        assertThrows(SQLException.class, () -> fw.handle(s, x -> ExecutionResult.ofUpdate(0)));
        var recent = PolicyDecisionLog.get().recent(10);
        assertEquals(1, recent.size());
        assertEquals("SQL firewall", recent.get(0).policy());
        assertEquals("Block", recent.get(0).decision());
        assertTrue(recent.get(0).reason().contains("#7"));
        assertEquals("DROP TABLE accounts", recent.get(0).target());
    }

    @Test
    void policyLogIsBounded() {
        PolicyDecisionLog.get().clearForTest();
        for (int i = 0; i < PolicyDecisionLog.CAPACITY + 25; i++) {
            PolicyDecisionLog.get().record("p", "u", "t" + i, "Block", "r");
        }
        assertEquals(PolicyDecisionLog.CAPACITY, PolicyDecisionLog.get().recent(10_000).size());
        assertEquals("t" + (PolicyDecisionLog.CAPACITY + 24), PolicyDecisionLog.get().recent(1).get(0).target());
    }

    @Test
    void historyRingKeepsNewestSamplesOldestFirst() {
        AtomicLong n = new AtomicLong();
        MetricsHistory h = new MetricsHistory(3, 1000, () -> Map.of("x", n.incrementAndGet()));
        for (int i = 0; i < 5; i++) {
            h.sampleNow(1000L * i);
        }
        var samples = h.samples();
        assertEquals(3, samples.size());
        assertEquals(2000L, samples.get(0).timestampMillis());
        assertEquals(5L, samples.get(2).counters().get("x"));
    }

    @Test
    void translationCacheCountsHitsMissesAndEvictions() {
        TranslationCache c = new TranslationCache(2);
        assertEquals(null, c.get("select 1", SourceDialect.ORACLE, SourceDialect.POSTGRES));
        c.put("select 1", SourceDialect.ORACLE, SourceDialect.POSTGRES, "a");
        c.put("select 2", SourceDialect.ORACLE, SourceDialect.POSTGRES, "b");
        c.put("select 3", SourceDialect.ORACLE, SourceDialect.POSTGRES, "c");
        assertEquals("c", c.get("select 3", SourceDialect.ORACLE, SourceDialect.POSTGRES));
        assertEquals(1, c.hits());
        assertEquals(1, c.misses());
        assertEquals(1, c.evictions());
        assertEquals(2, c.maxEntries());
    }
}
