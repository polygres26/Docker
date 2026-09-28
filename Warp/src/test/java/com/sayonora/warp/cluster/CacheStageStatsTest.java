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
}
