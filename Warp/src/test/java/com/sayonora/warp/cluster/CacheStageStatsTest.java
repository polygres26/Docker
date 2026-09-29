package com.sayonora.warp.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.AccessContext;
import com.sayonora.warp.core.ColumnInfo;
import com.sayonora.warp.core.ExecutionResult;
import com.sayonora.warp.core.SourceDialect;
import com.sayonora.warp.core.Statement;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The counters behind GET /api/cache/stats: hits, misses, invalidations and per-table attribution. */
class CacheStageStatsTest {

    private static WarpCluster cluster;

    @BeforeAll
    static void start() {
        cluster = WarpCluster.startSingleNodeForCacheOnly();
    }

    @AfterAll
    static void stop() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private static Statement stmt(String sql) {
        return new Statement("t", SourceDialect.POSTGRES, sql, List.of(), null, null, AccessContext.ANONYMOUS);
    }

    private static ExecutionResult rows() {
        return ExecutionResult.ofQuery(List.of(new ColumnInfo("v", Types.VARCHAR, 0, 0, 0, false)), List.of(List.of("x")));
    }

    @Test
    void countsHitsMissesAndInvalidationsPerTable() throws Exception {
        CacheStage stage = new CacheStage(cluster, List.of("statsorders"), 30_000);
        Statement read = stmt("select v from statsorders");
        stage.handle(read, s -> rows());
        stage.handle(read, s -> rows());
        stage.handle(read, s -> rows());
        CacheStats.Snapshot snap = stage.stats().snapshot();
        assertEquals(2, snap.resultHits());
        assertEquals(1, snap.resultMisses());
        assertEquals("statsorders", snap.byTable().get(0).table());
        assertEquals(2, snap.byTable().get(0).hits());
        assertEquals(1, stage.info().resultEntries());

        stage.handle(stmt("insert into statsorders values (1)"), s -> ExecutionResult.ofUpdate(1));
        snap = stage.stats().snapshot();
        assertEquals(1, snap.invalidationEvents());
        assertEquals(1, snap.invalidatedEntries());
        assertEquals("statsorders", snap.recent().get(0).target());
        assertEquals("write through Warp", snap.recent().get(0).source());
        assertEquals(0, stage.info().resultEntries());

        stage.handle(read, s -> rows());
        stage.invalidateEverything("test");
        snap = stage.stats().snapshot();
        assertEquals(1, snap.fullClears());
        assertEquals("clear", snap.recent().get(0).kind());
        assertTrue(stage.info().tablePatterns().contains("statsorders") || !stage.info().tablePatterns().isEmpty());
    }

    @Test
    void writesToUncachedTablesLeaveNoInvalidationNoise() throws Exception {
        CacheStage stage = new CacheStage(cluster, List.of("someothertable"), 30_000);
        stage.handle(stmt("update untracked set a = 1"), s -> ExecutionResult.ofUpdate(1));
        assertEquals(0, stage.stats().snapshot().invalidationEvents());
        assertTrue(stage.stats().snapshot().recent().isEmpty());
    }

    @Test
    void aSelectAgainstAnUncachedTableIsCountedAsARealBypassNotASilentFallthrough() throws Exception {
        // Distinct TTL (-> a distinct, uniquely-named Ignite cache; see cacheName()) so this test's
        // entries never share a cache instance with another test's, the same real isolation
        // constraint every test in this class must respect since CacheStage names its Ignite cache
        // purely by TTL value, cluster-wide, not per CacheStage instance.
        CacheStage stage = new CacheStage(cluster, List.of("bypassconfigured"), 30_001);
        stage.handle(stmt("select v from bypass_other_table"), s -> rows());
        CacheStats.Snapshot snap = stage.stats().snapshot();
        assertEquals(1, snap.bypassedTotal());
        assertEquals(0, snap.resultHits() + snap.resultMisses(), "a bypass must never also count as a hit or miss");
        assertEquals(1, snap.bypassByReason().size());
        assertEquals(CacheStats.BYPASS_TABLE_NOT_CACHED, snap.bypassByReason().get(0).reason());
        assertEquals("bypass_other_table", snap.bypassByTable().get(0).table());
    }

    @Test
    void aSelectWithNoCacheTablesConfiguredAtAllReportsTheDistinctNotConfiguredReason() throws Exception {
        CacheStage stage = new CacheStage(cluster, List.of(), 30_002);
        stage.handle(stmt("select v from anything"), s -> rows());
        CacheStats.Snapshot snap = stage.stats().snapshot();
        assertEquals(1, snap.bypassedTotal());
        assertEquals(CacheStats.BYPASS_NOT_CONFIGURED, snap.bypassByReason().get(0).reason());
    }

    @Test
    void aSelectThatIsActuallyCachedIsNeverCountedAsABypass() throws Exception {
        CacheStage stage = new CacheStage(cluster, List.of("bypasshitorders"), 30_003);
        stage.handle(stmt("select v from bypasshitorders"), s -> rows());
        assertEquals(0, stage.stats().snapshot().bypassedTotal());
    }

    @Test
    void reconfiguringToEmptyTablesLaterStillReportsNotConfigured() throws Exception {
        CacheStage stage = new CacheStage(cluster, List.of("bypassreconfig"), 30_004);
        stage.reconfigure(null, "30004");
        stage.handle(stmt("select v from bypassreconfig"), s -> rows());
        CacheStats.Snapshot snap = stage.stats().snapshot();
        assertEquals(1, snap.bypassedTotal());
        assertEquals(CacheStats.BYPASS_NOT_CONFIGURED, snap.bypassByReason().get(0).reason());
    }
}
