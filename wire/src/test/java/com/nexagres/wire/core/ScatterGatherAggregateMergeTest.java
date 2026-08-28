package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the fix to the highest-risk gap flagged in a competitive comparison against
 * ShardingSphere: scatter-gather used to always concatenate raw per-shard rows, correct for a
 * plain SELECT but silently wrong for an aggregate (a 3-shard COUNT(*) returned 3 rows, not one
 * summed total). No DB needed here -- {@link ScatterGatherAggregateMerge} is pure planning/merge
 * logic over synthetic {@link ExecutionResult}s standing in for what each shard would return.
 */
class ScatterGatherAggregateMergeTest {

    private static ExecutionResult oneRow(List<ColumnInfo> columns, List<Object> row) {
        return ExecutionResult.ofQuery(columns, List.of(row));
    }

    @Test
    void plainSelectWithNoAggregateReturnsNullPlan() throws SQLException {
        assertNull(ScatterGatherAggregateMerge.plan("SELECT id, name FROM customers"));
    }

    @Test
    void countStarMergesBySummingAcrossShards() throws SQLException {
        ScatterGatherAggregateMerge.Plan plan = ScatterGatherAggregateMerge.plan("SELECT COUNT(*) FROM orders");
        assertTrue(plan.rewrittenSql().toUpperCase(java.util.Locale.ROOT).contains("COUNT(*)"));

        List<ColumnInfo> shardCols = List.of(new ColumnInfo("__agg_0", Types.BIGINT, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(40)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(25)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(35)), acc);

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        assertEquals(1, merged.rows().size());
        assertEquals(100.0, ((Number) merged.rows().get(0).get(0)).doubleValue());
    }

    @Test
    void sumMergesBySummingAcrossShards() throws SQLException {
        ScatterGatherAggregateMerge.Plan plan = ScatterGatherAggregateMerge.plan("SELECT SUM(total) FROM orders");
        List<ColumnInfo> shardCols = List.of(new ColumnInfo("__agg_0", Types.DOUBLE, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(150.0)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(220.5)), acc);

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        assertEquals(370.5, ((Number) merged.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void avgMergesAsWeightedAverageNotAverageOfAverages() throws SQLException {
        // Shard A: avg=10 over 90 rows. Shard B: avg=100 over 10 rows. A naive average-of-averages
        // would wrongly give 55; the correct weighted merge is (10*90 + 100*10) / 100 = 19.
        ScatterGatherAggregateMerge.Plan plan = ScatterGatherAggregateMerge.plan("SELECT AVG(total) FROM orders");
        String upper = plan.rewrittenSql().toUpperCase(java.util.Locale.ROOT);
        assertTrue(upper.contains("SUM(TOTAL)"));
        assertTrue(upper.contains("COUNT(TOTAL)"));

        List<ColumnInfo> shardCols = List.of(
                new ColumnInfo("__avg_sum_0", Types.DOUBLE, 0, 0, 0, true),
                new ColumnInfo("__avg_cnt_0", Types.BIGINT, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(900.0, 90)), acc); // sum=10*90
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(1000.0, 10)), acc); // sum=100*10

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        assertEquals(19.0, ((Number) merged.rows().get(0).get(0)).doubleValue(), 0.0001);
    }

    @Test
    void minAndMaxMergeAcrossShards() throws SQLException {
        ScatterGatherAggregateMerge.Plan plan =
                ScatterGatherAggregateMerge.plan("SELECT MIN(total) AS lo, MAX(total) AS hi FROM orders");
        List<ColumnInfo> shardCols = List.of(
                new ColumnInfo("__agg_0", Types.DOUBLE, 0, 0, 0, true),
                new ColumnInfo("__agg_1", Types.DOUBLE, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(5.0, 40.0)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of(2.0, 99.0)), acc);

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        assertEquals(2.0, ((Number) merged.rows().get(0).get(0)).doubleValue());
        assertEquals(99.0, ((Number) merged.rows().get(0).get(1)).doubleValue());
        assertEquals("lo", merged.columns().get(0).name());
        assertEquals("hi", merged.columns().get(1).name());
    }

    @Test
    void groupByMergesMatchingGroupsAcrossShardsAndKeepsDistinctGroupsSeparate() throws SQLException {
        ScatterGatherAggregateMerge.Plan plan =
                ScatterGatherAggregateMerge.plan("SELECT region, COUNT(*) AS cnt FROM orders GROUP BY region");
        List<ColumnInfo> shardCols = List.of(
                new ColumnInfo("region", Types.VARCHAR, 0, 0, 0, true),
                new ColumnInfo("__agg_0", Types.BIGINT, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        // Shard 1: east=10, west=5
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("east", 10)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("west", 5)), acc);
        // Shard 2: east=3 (same group, must combine with shard 1's east), south=7 (new group)
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("east", 3)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("south", 7)), acc);

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        Map<Object, Double> byRegion = new LinkedHashMap<>();
        for (List<Object> row : merged.rows()) {
            byRegion.put(row.get(0), ((Number) row.get(1)).doubleValue());
        }
        assertEquals(3, byRegion.size());
        assertEquals(13.0, byRegion.get("east")); // 10 + 3, merged across shards
        assertEquals(5.0, byRegion.get("west"));
        assertEquals(7.0, byRegion.get("south"));
    }

    @Test
    void limitIsAppliedToTheMergedResultNotPerShard() throws SQLException {
        ScatterGatherAggregateMerge.Plan plan = ScatterGatherAggregateMerge.plan(
                "SELECT region, COUNT(*) AS cnt FROM orders GROUP BY region LIMIT 2");
        assertEquals(2, plan.limit());
        List<ColumnInfo> shardCols = List.of(
                new ColumnInfo("region", Types.VARCHAR, 0, 0, 0, true),
                new ColumnInfo("__agg_0", Types.BIGINT, 0, 0, 0, true));
        Map<List<Object>, Object[]> acc = new LinkedHashMap<>();
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("a", 1)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("b", 1)), acc);
        ScatterGatherAggregateMerge.mergeShardResult(plan, oneRow(shardCols, List.of("c", 1)), acc);

        ExecutionResult merged = ScatterGatherAggregateMerge.buildResult(plan, acc);
        assertEquals(2, merged.rows().size(), "LIMIT must truncate the merged set, not each shard's own contribution");
    }

    @Test
    void countDistinctIsRefusedRatherThanSilentlyMismerged() {
        SQLException e = assertThrows(SQLException.class,
                () -> ScatterGatherAggregateMerge.plan("SELECT COUNT(DISTINCT customer_id) FROM orders"));
        assertTrue(e.getMessage().contains("DISTINCT"));
    }

    @Test
    void unrecognizedAggregateFunctionIsRefusedRatherThanTreatedAsAGroupKey() {
        SQLException e = assertThrows(SQLException.class,
                () -> ScatterGatherAggregateMerge.plan("SELECT STRING_AGG(name, ',') FROM orders"));
        assertTrue(e.getMessage().contains("STRING_AGG"));
    }
}
