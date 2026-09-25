package com.sayonora.wire.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.core.AccessContext;
import com.sayonora.wire.core.ColumnInfo;
import com.sayonora.wire.core.ExecutionResult;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Real regression guard for the generic (any-backend, any-table, any-PK-column-name) point-lookup
 * / point-write cache added to {@link CacheStage} -- the mechanism that gives ordinary tables the
 * same row-cache precision dynamowire/mongowire's own fixed physical shapes already have, for the
 * two deployment cases it targets: a driver talking through Warp's translated/native-pinned
 * pipeline to a same-dialect backend, or to a Postgres backend. Uses a real single-node Ignite
 * cluster (no Docker, no JDBC backend needed -- {@link CacheStage#handle} never touches a real
 * database in these tests; the "backend" is a fake {@code PipelineChain} counting calls).
 */
class CacheStageGenericPkTest {

    private static WarpCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = WarpCluster.startSingleNodeForCacheOnly();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private CacheStage newStage(Map<String, List<String>> pkCatalog) {
        CacheStage stage = new CacheStage(cluster, List.of(), 30_000);
        stage.setPrimaryKeyCatalog(pkCatalog);
        return stage;
    }

    private static Statement select(String sql, List<Object> binds) {
        return new Statement("t1", SourceDialect.ORACLE, sql, binds, null, null, AccessContext.ANONYMOUS);
    }

    private static ExecutionResult oneRow(Object value) {
        return ExecutionResult.ofQuery(List.of(new ColumnInfo("v", Types.VARCHAR, 0, 0, 0, false)),
                List.of(List.of(value)));
    }

    @Test
    void singleColumnPkSelectHitsCacheOnSecondCall() throws Exception {
        CacheStage stage = newStage(Map.of("orders", List.of("id")));
        AtomicInteger backendCalls = new AtomicInteger();
        Statement stmt = select("select id, status from orders where id = ?", List.of(42));

        ExecutionResult first = stage.handle(stmt, s -> {
            backendCalls.incrementAndGet();
            return oneRow("shipped");
        });
        ExecutionResult second = stage.handle(stmt, s -> {
            backendCalls.incrementAndGet();
            return oneRow("SHOULD_NOT_BE_CALLED");
        });

        assertEquals(1, backendCalls.get(), "second call must be served from the generic pk cache, not the backend");
        assertEquals("shipped", first.rows().get(0).get(0));
        assertEquals("shipped", second.rows().get(0).get(0));
    }

    @Test
    void updateOnExactPkInvalidatesOnlyThatRow() throws Exception {
        CacheStage stage = newStage(Map.of("orders", List.of("id")));
        AtomicInteger backendCalls = new AtomicInteger();
        Statement selectRow1 = select("select id, status from orders where id = ?", List.of(1));
        Statement selectRow2 = select("select id, status from orders where id = ?", List.of(2));

        stage.handle(selectRow1, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });
        stage.handle(selectRow2, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });
        assertEquals(2, backendCalls.get());

        // UPDATE's own SET-clause bind comes before the WHERE bind -- bindParams = [newStatus, pk].
        Statement update = new Statement("t1", SourceDialect.ORACLE,
                "update orders set status = ? where id = ?", List.of("shipped", 1), null, null, AccessContext.ANONYMOUS);
        stage.handle(update, s -> { backendCalls.incrementAndGet(); return ExecutionResult.ofUpdate(1); });

        // Row 1 was invalidated -- must re-hit the backend.
        stage.handle(selectRow1, s -> { backendCalls.incrementAndGet(); return oneRow("shipped-fresh"); });
        // Row 2 was NOT touched -- must still be served from cache, not the backend.
        ExecutionResult row2Again = stage.handle(selectRow2, s -> {
            backendCalls.incrementAndGet();
            throw new java.sql.SQLException("row 2's cache entry must not have been evicted by a write to row 1");
        });

        assertEquals(4, backendCalls.get(), "update(1) + refetch(1) on top of the initial 2 selects = 4; row 2 must stay cached");
        assertEquals("pending", row2Again.rows().get(0).get(0));
    }

    @Test
    void compositePkOrderIndependentInWhereClause() throws Exception {
        CacheStage stage = newStage(Map.of("line_items", List.of("order_id", "line_no")));
        AtomicInteger backendCalls = new AtomicInteger();
        // WHERE clause order (line_no, order_id) is the REVERSE of catalog PK order (order_id,
        // line_no) -- proves the match is column-name-based, not positional.
        Statement stmt = select("select qty from line_items where line_no = ? and order_id = ?", List.of(3, 100));

        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow(5); });
        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); throw new java.sql.SQLException("should be a cache hit"); });

        assertEquals(1, backendCalls.get());
    }

    @Test
    void partialPkPredicateNeverUsesGenericFastPath() throws Exception {
        // Composite PK (order_id, line_no); WHERE pins only order_id -- could match many rows, so
        // this must NOT be treated as a point lookup. Falls through to ordinary (uncached, no
        // WARP_CACHE_TABLES pattern configured) execution every time.
        CacheStage stage = newStage(Map.of("line_items", List.of("order_id", "line_no")));
        AtomicInteger backendCalls = new AtomicInteger();
        Statement stmt = select("select qty from line_items where order_id = ?", List.of(100));

        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow(5); });
        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow(5); });

        assertEquals(2, backendCalls.get(), "a partial-PK predicate must never be cached by the generic pk fast path");
    }

    @Test
    void extraPredicateBesidesPkNeverUsesGenericFastPath() throws Exception {
        CacheStage stage = newStage(Map.of("orders", List.of("id")));
        AtomicInteger backendCalls = new AtomicInteger();
        Statement stmt = select("select status from orders where id = ? and status = ?", List.of(1, "pending"));

        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });
        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });

        assertEquals(2, backendCalls.get(), "an extra non-PK predicate must never be treated as a safe point lookup");
    }

    @Test
    void tableWithNoDiscoveredPkNeverUsesGenericFastPath() throws Exception {
        CacheStage stage = newStage(Map.of()); // empty catalog -- no table has a known PK
        AtomicInteger backendCalls = new AtomicInteger();
        Statement stmt = select("select status from orders where id = ?", List.of(1));

        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });
        stage.handle(stmt, s -> { backendCalls.incrementAndGet(); return oneRow("pending"); });

        assertEquals(2, backendCalls.get());
    }

    @Test
    void exactPkBindPositionsRejectsOrPredicate() {
        assertNull(CacheStage.exactPkBindPositions("id = ? or id = ?", List.of("id")));
    }

    @Test
    void exactPkBindPositionsRejectsWrongColumnSet() {
        assertNull(CacheStage.exactPkBindPositions("other_col = ?", List.of("id")));
    }

    @Test
    void exactPkBindPositionsAcceptsQualifiedColumn() {
        List<Integer> positions = CacheStage.exactPkBindPositions("o.id = ?", List.of("id"));
        assertEquals(List.of(0), positions);
    }

    @Test
    void exactPkBindPositionsMapsCompositeOrderCorrectly() {
        List<Integer> positions = CacheStage.exactPkBindPositions(
                "line_no = ? and order_id = ?", List.of("order_id", "line_no"));
        // pkColumns = [order_id, line_no]; WHERE text order = [line_no, order_id].
        // order_id's value is the WHERE clause's 2nd (index 1) bind; line_no's is the 1st (index 0).
        assertEquals(List.of(1, 0), positions);
        assertTrue(true);
    }
}
