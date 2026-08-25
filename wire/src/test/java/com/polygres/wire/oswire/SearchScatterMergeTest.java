package com.polygres.wire.oswire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.JsonObject;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the OpenSearch-search analogue of ScatterGatherAggregateMerge/
 * ScatterGatherOrderLimit -- pure logic, no DB needed, over synthetic per-shard
 * {@link SearchResult}/{@link AggregationResult}s standing in for what each shard would return.
 */
class SearchScatterMergeTest {

    private static JsonObject doc(String field, Object value) {
        JsonObject o = new JsonObject();
        if (value instanceof String s) {
            o.addProperty(field, s);
        } else if (value instanceof Number n) {
            o.addProperty(field, n);
        }
        return o;
    }

    @Test
    void mergeHitsSortsByScoreDescendingWhenNoExplicitSort() {
        SearchResult shard1 = new SearchResult(List.of(
                new SearchHit("a", 0.5, doc("x", "a")),
                new SearchHit("b", 0.9, doc("x", "b"))), 2);
        SearchResult shard2 = new SearchResult(List.of(
                new SearchHit("c", 0.7, doc("x", "c"))), 1);

        Comparator<SearchHit> cmp = SearchScatterMerge.comparatorFor(List.of());
        SearchResult merged = SearchScatterMerge.mergeHits(List.of(shard1, shard2), cmp, 0, 10);

        assertEquals(3, merged.total(), "total must sum across shards");
        assertEquals(List.of("b", "c", "a"), merged.hits().stream().map(SearchHit::id).toList());
    }

    @Test
    void mergeHitsAppliesGlobalOffsetAndTopKNotPerShard() {
        SearchResult shard1 = new SearchResult(List.of(
                new SearchHit("a", 5, doc("x", "a")), new SearchHit("b", 4, doc("x", "b"))), 2);
        SearchResult shard2 = new SearchResult(List.of(
                new SearchHit("c", 3, doc("x", "c")), new SearchHit("d", 2, doc("x", "d"))), 2);

        Comparator<SearchHit> cmp = SearchScatterMerge.comparatorFor(List.of());
        SearchResult merged = SearchScatterMerge.mergeHits(List.of(shard1, shard2), cmp, 1, 2);

        // global order: a(5) b(4) c(3) d(2) -- offset 1, topK 2 -> [b, c]
        assertEquals(List.of("b", "c"), merged.hits().stream().map(SearchHit::id).toList());
    }

    @Test
    void mergeHitsSortsByExplicitFieldAcrossShards() {
        SearchResult shard1 = new SearchResult(List.of(new SearchHit("a", 1, doc("price", 30))), 1);
        SearchResult shard2 = new SearchResult(List.of(
                new SearchHit("b", 1, doc("price", 10)), new SearchHit("c", 1, doc("price", 20))), 2);

        Comparator<SearchHit> cmp = SearchScatterMerge.comparatorFor(List.of(new SortField("price", true)));
        SearchResult merged = SearchScatterMerge.mergeHits(List.of(shard1, shard2), cmp, 0, 10);

        assertEquals(List.of("b", "c", "a"), merged.hits().stream().map(SearchHit::id).toList());
    }

    @Test
    void avgMetricMergesAsWeightedAverageAcrossShardsNotAverageOfAverages() {
        // shard A: avg=10 over 90 docs (sum=900). shard B: avg=100 over 10 docs (sum=1000).
        // naive average-of-averages = 55; correct weighted merge = (900+1000)/100 = 19.
        Aggregation.Metric avgAgg = new Aggregation.Metric("avgPrice", Aggregation.MetricType.AVG, "price");
        List<Aggregation> expanded = SearchScatterMerge.expandForSharding(List.of(avgAgg));
        assertEquals(2, expanded.size(), "AVG must expand into a SUM+COUNT pair before hitting any shard");

        List<AggregationResult> shardA = List.of(
                new AggregationResult.SingleValue("avgPrice__sum", 900.0, "sum"),
                new AggregationResult.SingleValue("avgPrice__count", 90.0, "value_count"));
        List<AggregationResult> shardB = List.of(
                new AggregationResult.SingleValue("avgPrice__sum", 1000.0, "sum"),
                new AggregationResult.SingleValue("avgPrice__count", 10.0, "value_count"));

        List<AggregationResult> merged = SearchScatterMerge.mergeAcrossShards(List.of(avgAgg), List.of(shardA, shardB));

        assertEquals(1, merged.size());
        AggregationResult.SingleValue result = (AggregationResult.SingleValue) merged.get(0);
        assertEquals("avgPrice", result.name(), "the merged result must use the ORIGINAL name, not the expanded __sum/__count names");
        assertEquals(19.0, result.value(), 0.0001);
    }

    @Test
    void sumMinMaxCountMergeDirectlyAcrossShards() {
        Aggregation.Metric sumAgg = new Aggregation.Metric("total", Aggregation.MetricType.SUM, "price");
        Aggregation.Metric minAgg = new Aggregation.Metric("lo", Aggregation.MetricType.MIN, "price");
        Aggregation.Metric maxAgg = new Aggregation.Metric("hi", Aggregation.MetricType.MAX, "price");
        Aggregation.Metric countAgg = new Aggregation.Metric("n", Aggregation.MetricType.COUNT, "price");
        List<Aggregation> aggs = List.of(sumAgg, minAgg, maxAgg, countAgg);

        List<AggregationResult> shard1 = List.of(
                new AggregationResult.SingleValue("total", 100.0, "sum"),
                new AggregationResult.SingleValue("lo", 5.0, "min"),
                new AggregationResult.SingleValue("hi", 40.0, "max"),
                new AggregationResult.SingleValue("n", 3.0, "value_count"));
        List<AggregationResult> shard2 = List.of(
                new AggregationResult.SingleValue("total", 50.0, "sum"),
                new AggregationResult.SingleValue("lo", 2.0, "min"),
                new AggregationResult.SingleValue("hi", 100.0, "max"),
                new AggregationResult.SingleValue("n", 1.0, "value_count"));

        List<AggregationResult> merged = SearchScatterMerge.mergeAcrossShards(aggs, List.of(shard1, shard2));

        assertEquals(150.0, ((AggregationResult.SingleValue) merged.get(0)).value(), 0.0001);
        assertEquals(2.0, ((AggregationResult.SingleValue) merged.get(1)).value(), 0.0001);
        assertEquals(100.0, ((AggregationResult.SingleValue) merged.get(2)).value(), 0.0001);
        assertEquals(4.0, ((AggregationResult.SingleValue) merged.get(3)).value(), 0.0001);
    }

    @Test
    void termsBucketsMergeTheSameKeyAcrossShardsAndReCapAtSize() {
        Aggregation.Terms termsAgg = new Aggregation.Terms("byRegion", "region", 2, List.of());

        AggregationResult.Buckets shard1Buckets = new AggregationResult.Buckets("byRegion", List.of(
                new AggregationResult.Bucket("east", 10, List.of()),
                new AggregationResult.Bucket("west", 3, List.of())), 1);
        AggregationResult.Buckets shard2Buckets = new AggregationResult.Buckets("byRegion", List.of(
                new AggregationResult.Bucket("east", 5, List.of()),
                new AggregationResult.Bucket("south", 20, List.of())), 0);

        List<AggregationResult> merged = SearchScatterMerge.mergeAcrossShards(
                List.of(termsAgg), List.of(List.of(shard1Buckets), List.of(shard2Buckets)));

        AggregationResult.Buckets result = (AggregationResult.Buckets) merged.get(0);
        assertEquals(2, result.buckets().size(), "size=2 must cap the MERGED bucket set, not each shard's own");
        assertEquals("south", result.buckets().get(0).key(), "south(20) must outrank the merged east(15)");
        assertEquals("east", result.buckets().get(1).key());
        assertEquals(15, result.buckets().get(1).docCount(), "east appears on both shards (10+5) and must merge into one bucket");
        // west(3) fell out of the top-2 after global re-sort and must be folded into sumOtherDocCount,
        // along with the two shards' own sumOtherDocCount (1+0).
        assertEquals(4, result.sumOtherDocCount());
    }

    @Test
    void nestedAvgSubAggregationMergesCorrectlyOnlyAcrossShardsThatHadThatBucketKey() {
        Aggregation.Metric avgSub = new Aggregation.Metric("avgPrice", Aggregation.MetricType.AVG, "price");
        Aggregation.Terms termsAgg = new Aggregation.Terms("byRegion", "region", 10, List.of(avgSub));

        // "east" appears on both shards -- its avgPrice must be a real weighted merge across both.
        // "west" appears only on shard1 -- its avgPrice must pass through unchanged (nothing to merge).
        AggregationResult.Bucket shard1East = new AggregationResult.Bucket("east", 2, List.of(
                new AggregationResult.SingleValue("avgPrice__sum", 20.0, "sum"),
                new AggregationResult.SingleValue("avgPrice__count", 2.0, "value_count")));
        AggregationResult.Bucket shard1West = new AggregationResult.Bucket("west", 1, List.of(
                new AggregationResult.SingleValue("avgPrice__sum", 9.0, "sum"),
                new AggregationResult.SingleValue("avgPrice__count", 1.0, "value_count")));
        AggregationResult.Bucket shard2East = new AggregationResult.Bucket("east", 3, List.of(
                new AggregationResult.SingleValue("avgPrice__sum", 60.0, "sum"),
                new AggregationResult.SingleValue("avgPrice__count", 3.0, "value_count")));

        AggregationResult.Buckets shard1Buckets = new AggregationResult.Buckets("byRegion", List.of(shard1East, shard1West), 0);
        AggregationResult.Buckets shard2Buckets = new AggregationResult.Buckets("byRegion", List.of(shard2East), 0);

        List<AggregationResult> merged = SearchScatterMerge.mergeAcrossShards(
                List.of(termsAgg), List.of(List.of(shard1Buckets), List.of(shard2Buckets)));

        AggregationResult.Buckets result = (AggregationResult.Buckets) merged.get(0);
        Map<String, AggregationResult.Bucket> byKey = new java.util.LinkedHashMap<>();
        for (AggregationResult.Bucket b : result.buckets()) {
            byKey.put(b.key(), b);
        }
        AggregationResult.SingleValue eastAvg = (AggregationResult.SingleValue) byKey.get("east").subResults().get(0);
        assertEquals(16.0, eastAvg.value(), 0.0001, "(20+60)/(2+3) = 16, a real weighted merge across both shards");
        assertEquals("avgPrice", eastAvg.name());

        AggregationResult.SingleValue westAvg = (AggregationResult.SingleValue) byKey.get("west").subResults().get(0);
        assertEquals(9.0, westAvg.value(), 0.0001, "west only exists on shard1 -- its avg passes through as sum/count of 1 shard");
    }
}
