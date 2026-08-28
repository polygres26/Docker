package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the fix to the follow-up gap found after the aggregate-merge fix: scatter-
 * gather used to send the client's own ORDER BY/LIMIT/OFFSET to every shard unmodified and just
 * concatenate results, so a 3-shard "ORDER BY x LIMIT 10" could return up to 30 rows in
 * shard-arrival order instead of the correct globally-ordered top 10.
 */
class ScatterGatherOrderLimitTest {

    private static List<ColumnInfo> cols(String... names) {
        return java.util.Arrays.stream(names)
                .map(n -> new ColumnInfo(n, Types.VARCHAR, 0, 0, 0, true))
                .toList();
    }

    @Test
    void sqlWithNoOrderByLimitOrOffsetParsesAsTrivialAndIsUntouched() {
        ScatterGatherOrderLimit.Parsed parsed = ScatterGatherOrderLimit.parse("SELECT a, b FROM t WHERE a > 1");
        assertEquals("SELECT a, b FROM t WHERE a > 1", parsed.withoutOrderLimitOffset());
        assertTrue(parsed.spec().isTrivial());
    }

    @Test
    void limitAndOffsetAreParsedAndStrippedInEitherOrder() {
        ScatterGatherOrderLimit.Parsed a = ScatterGatherOrderLimit.parse("SELECT a FROM t LIMIT 10 OFFSET 5");
        assertEquals("SELECT a FROM t", a.withoutOrderLimitOffset());
        assertEquals(10, a.spec().limit());
        assertEquals(5, a.spec().offset());

        ScatterGatherOrderLimit.Parsed b = ScatterGatherOrderLimit.parse("SELECT a FROM t OFFSET 5 LIMIT 10");
        assertEquals("SELECT a FROM t", b.withoutOrderLimitOffset());
        assertEquals(10, b.spec().limit());
        assertEquals(5, b.spec().offset());
    }

    @Test
    void orderByInsideASubqueryIsNotMistakenForTheStatementsOwnClause() {
        String sql = "SELECT a FROM (SELECT a FROM inner_t ORDER BY a) x ORDER BY a DESC LIMIT 3";
        ScatterGatherOrderLimit.Parsed parsed = ScatterGatherOrderLimit.parse(sql);
        assertEquals("SELECT a FROM (SELECT a FROM inner_t ORDER BY a) x", parsed.withoutOrderLimitOffset());
        assertEquals(1, parsed.spec().orderBy().size());
        assertEquals("a", parsed.spec().orderBy().get(0).columnRef());
        assertTrue(parsed.spec().orderBy().get(0).descending());
        assertEquals(3, parsed.spec().limit());
    }

    @Test
    void multiColumnOrderByWithMixedDirectionsIsParsed() {
        ScatterGatherOrderLimit.Parsed parsed =
                ScatterGatherOrderLimit.parse("SELECT region, total FROM t ORDER BY region ASC, total DESC");
        List<ScatterGatherOrderLimit.SortKey> keys = parsed.spec().orderBy();
        assertEquals(2, keys.size());
        assertEquals("region", keys.get(0).columnRef());
        assertEquals(false, keys.get(0).descending());
        assertEquals("total", keys.get(1).columnRef());
        assertEquals(true, keys.get(1).descending());
    }

    @Test
    void applyOrderAndLimitSortsAcrossWhatWouldHaveBeenSeparateShardResults() throws SQLException {
        // Simulates 3 shards' worth of already-gathered, unsorted rows -- exactly what
        // RoutingBackendExecutor hands this class after fetching from every shard.
        ExecutionResult gathered = ExecutionResult.ofQuery(cols("name", "total"), List.of(
                List.of("west", 5),
                List.of("east", 20),
                List.of("east", 10),
                List.of("south", 100)));
        ScatterGatherOrderLimit.Spec spec = new ScatterGatherOrderLimit.Spec(
                List.of(new ScatterGatherOrderLimit.SortKey("total", true, false)), 2, null);

        ExecutionResult result = ScatterGatherOrderLimit.applyOrderAndLimit(gathered, spec);

        assertEquals(2, result.rows().size(), "LIMIT must cap the globally-sorted set, not each shard's contribution");
        assertEquals("south", result.rows().get(0).get(0), "highest total (100) must sort first with DESC");
        assertEquals("east", result.rows().get(1).get(0), "second-highest total (20) must sort second");
    }

    @Test
    void offsetSkipsFromTheGloballySortedSetNotPerShard() throws SQLException {
        ExecutionResult gathered = ExecutionResult.ofQuery(cols("n"), List.of(
                List.of(3), List.of(1), List.of(4), List.of(1), List.of(5)));
        ScatterGatherOrderLimit.Spec spec = new ScatterGatherOrderLimit.Spec(
                List.of(new ScatterGatherOrderLimit.SortKey("n", false, false)), 2, 2);

        ExecutionResult result = ScatterGatherOrderLimit.applyOrderAndLimit(gathered, spec);

        // sorted ascending: 1,1,3,4,5 -- offset 2, limit 2 -> [3,4]
        assertEquals(List.of(3, 4), List.of(result.rows().get(0).get(0), result.rows().get(1).get(0)));
    }

    @Test
    void limitWithNoOrderByStillCapsTheTotalRowCount() throws SQLException {
        // No ORDER BY means SQL doesn't guarantee which rows, but the row COUNT must still be
        // capped at LIMIT -- this is the core correctness bug: returning limit * numShards rows.
        ExecutionResult gathered = ExecutionResult.ofQuery(cols("n"),
                List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)));
        ScatterGatherOrderLimit.Spec spec = new ScatterGatherOrderLimit.Spec(List.of(), 2, null);

        ExecutionResult result = ScatterGatherOrderLimit.applyOrderAndLimit(gathered, spec);

        assertEquals(2, result.rows().size());
    }

    @Test
    void orderByOrdinalPositionIsSupported() throws SQLException {
        ExecutionResult gathered = ExecutionResult.ofQuery(cols("a", "b"), List.of(
                List.of("x", 2), List.of("y", 1)));
        ScatterGatherOrderLimit.Spec spec = new ScatterGatherOrderLimit.Spec(
                List.of(new ScatterGatherOrderLimit.SortKey("2", false, false)), null, null);

        ExecutionResult result = ScatterGatherOrderLimit.applyOrderAndLimit(gathered, spec);

        assertEquals("y", result.rows().get(0).get(0));
        assertEquals("x", result.rows().get(1).get(0));
    }

    @Test
    void orderByAnUnresolvableExpressionIsRefusedRatherThanSilentlyIgnored() {
        ExecutionResult gathered = ExecutionResult.ofQuery(cols("a", "b"), List.of(List.of("x", 1)));
        ScatterGatherOrderLimit.Spec spec = new ScatterGatherOrderLimit.Spec(
                List.of(new ScatterGatherOrderLimit.SortKey("a * b", false, false)), null, null);

        SQLException e = assertThrows(SQLException.class, () -> ScatterGatherOrderLimit.applyOrderAndLimit(gathered, spec));
        assertTrue(e.getMessage().contains("ORDER BY"));
    }

    @Test
    void nullsSortLastByDefaultForAscendingAndFirstForDescending() throws SQLException {
        ExecutionResult ascGathered = ExecutionResult.ofQuery(cols("n"),
                java.util.Arrays.asList(java.util.Arrays.asList((Object) null), List.of(1), List.of(2)));
        ScatterGatherOrderLimit.Spec ascSpec = new ScatterGatherOrderLimit.Spec(
                List.of(new ScatterGatherOrderLimit.SortKey("n", false, false)), null, null);
        ExecutionResult ascResult = ScatterGatherOrderLimit.applyOrderAndLimit(ascGathered, ascSpec);
        assertNull(ascResult.rows().get(ascResult.rows().size() - 1).get(0), "NULLS LAST is the default for ASC");
    }
}
