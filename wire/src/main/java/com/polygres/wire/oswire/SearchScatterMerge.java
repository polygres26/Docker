package com.polygres.wire.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cross-shard hit and aggregation merging for {@code PostgresSearchStore}'s sharded structured
 * search -- the OpenSearch-search analogue of {@code ScatterGatherOrderLimit}/
 * {@code ScatterGatherAggregateMerge} for SQL scatter-gather. Same underlying problem, same
 * chosen trade-off: fetch every shard's full matching set (no partial per-shard limit pushdown)
 * and merge/sort/aggregate once, centrally -- correctness over that optimization.
 *
 * <p>Aggregations need one extra step beyond SQL's: {@link Aggregation.MetricType#AVG} can't be
 * correctly merged from each shard's own average (that's an average-of-averages, wrong the same
 * way it was wrong for SQL -- see {@code ScatterGatherAggregateMerge}'s javadoc). {@link
 * #expandForSharding} rewrites every {@code AVG} metric (at any nesting depth under a
 * {@link Aggregation.Terms} bucket) into a {@code SUM}+{@code COUNT} pair before the request is
 * sent to each shard, using the *existing*, unmodified per-shard aggregation execution code --
 * {@link #mergeAcrossShards} recombines the expanded results back into the original shape,
 * dividing the summed sum by the summed count exactly once, globally.
 */
final class SearchScatterMerge {

    private SearchScatterMerge() {
    }

    // --- hit merging ---

    /** Builds the {@link Comparator} that must produce the same order the merged, global result
     * needs -- either by {@code request.sort()} fields (evaluated against each hit's
     * {@code source}), or by score descending when there's no explicit sort. Mirrors
     * {@code ScatterGatherOrderLimit}'s null-handling convention (nulls sort last). */
    static Comparator<SearchHit> comparatorFor(List<SortField> sort) {
        if (sort.isEmpty()) {
            return Comparator.comparingDouble(SearchHit::score).reversed();
        }
        Comparator<SearchHit> cmp = null;
        for (SortField field : sort) {
            Comparator<SearchHit> next = Comparator.comparing(
                    (SearchHit hit) -> extractField(hit.source(), field.field()),
                    SearchScatterMerge::compareFieldValues);
            if (!field.ascending()) {
                next = next.reversed();
            }
            cmp = cmp == null ? next : cmp.thenComparing(next);
        }
        return cmp;
    }

    /** Merges every shard's already-gathered (unpaginated) hits into one globally-sorted,
     * globally-paginated list, plus a summed total. */
    static SearchResult mergeHits(List<SearchResult> perShard, Comparator<SearchHit> comparator, int offset, int topK) {
        List<SearchHit> all = new ArrayList<>();
        long total = 0;
        for (SearchResult r : perShard) {
            all.addAll(r.hits());
            total += r.total();
        }
        all.sort(comparator);
        int from = Math.min(offset, all.size());
        int to = Math.min(all.size(), from + topK);
        return new SearchResult(from >= to ? List.of() : all.subList(from, to), total);
    }

    @SuppressWarnings("unchecked")
    private static Object extractField(JsonObject source, String field) {
        JsonElement current = source;
        for (String part : field.split("\\.")) {
            if (current == null || !current.isJsonObject()) {
                return null;
            }
            current = current.getAsJsonObject().get(part);
        }
        if (current == null || current.isJsonNull()) {
            return null;
        }
        if (current.isJsonPrimitive()) {
            JsonPrimitive p = current.getAsJsonPrimitive();
            if (p.isNumber()) {
                return p.getAsDouble();
            }
            if (p.isBoolean()) {
                return p.getAsBoolean();
            }
            return p.getAsString();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static int compareFieldValues(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1; // nulls last
        }
        if (b == null) {
            return -1;
        }
        if (a instanceof Double da && b instanceof Double db) {
            return Double.compare(da, db);
        }
        if (a instanceof Comparable && a.getClass().isInstance(b)) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    // --- aggregation expansion (before sending to each shard) ---

    static List<Aggregation> expandForSharding(List<Aggregation> aggregations) {
        List<Aggregation> out = new ArrayList<>();
        for (Aggregation agg : aggregations) {
            out.addAll(expandOne(agg));
        }
        return out;
    }

    private static List<Aggregation> expandOne(Aggregation agg) {
        return switch (agg) {
            case Aggregation.Metric m when m.type() == Aggregation.MetricType.AVG -> List.of(
                    new Aggregation.Metric(m.name() + "__sum", Aggregation.MetricType.SUM, m.field()),
                    new Aggregation.Metric(m.name() + "__count", Aggregation.MetricType.COUNT, m.field()));
            case Aggregation.Metric m -> List.of(m);
            case Aggregation.Terms t ->
                    List.of(new Aggregation.Terms(t.name(), t.field(), t.size(), expandForSharding(t.subAggs())));
        };
    }

    // --- aggregation merging (after gathering each shard's expanded-shape results) ---

    /** {@code perShardExpanded}: one list per shard, each the result of running
     * {@code runAggregations(..., expandForSharding(originalAggregations))} against that shard --
     * i.e. every shard's results are in the SAME expanded order/shape, since every shard ran the
     * identical expanded request. */
    static List<AggregationResult> mergeAcrossShards(List<Aggregation> originalAggregations,
            List<List<AggregationResult>> perShardExpanded) {
        int[] cursors = new int[perShardExpanded.size()];
        List<AggregationResult> merged = new ArrayList<>();
        for (Aggregation agg : originalAggregations) {
            merged.add(mergeOneAcrossShards(agg, perShardExpanded, cursors));
        }
        return merged;
    }

    private static AggregationResult mergeOneAcrossShards(Aggregation agg,
            List<List<AggregationResult>> perShardExpanded, int[] cursors) {
        return switch (agg) {
            case Aggregation.Metric m when m.type() == Aggregation.MetricType.AVG -> {
                double sum = 0;
                double count = 0;
                boolean anySum = false;
                for (int s = 0; s < perShardExpanded.size(); s++) {
                    AggregationResult.SingleValue sv = (AggregationResult.SingleValue) perShardExpanded.get(s).get(cursors[s]);
                    AggregationResult.SingleValue cv = (AggregationResult.SingleValue) perShardExpanded.get(s).get(cursors[s] + 1);
                    cursors[s] += 2;
                    if (sv.value() != null) {
                        sum += sv.value();
                        anySum = true;
                    }
                    if (cv.value() != null) {
                        count += cv.value();
                    }
                }
                Double avg = (!anySum || count == 0) ? null : sum / count;
                yield new AggregationResult.SingleValue(m.name(), avg, "avg");
            }
            case Aggregation.Metric m -> {
                List<Double> values = new ArrayList<>();
                for (int s = 0; s < perShardExpanded.size(); s++) {
                    AggregationResult.SingleValue sv = (AggregationResult.SingleValue) perShardExpanded.get(s).get(cursors[s]);
                    cursors[s] += 1;
                    if (sv.value() != null) {
                        values.add(sv.value());
                    }
                }
                yield new AggregationResult.SingleValue(m.name(), combineSimpleMetric(m.type(), values), openSearchMetricType(m.type()));
            }
            case Aggregation.Terms t -> {
                List<AggregationResult.Buckets> perShardBuckets = new ArrayList<>();
                for (int s = 0; s < perShardExpanded.size(); s++) {
                    perShardBuckets.add((AggregationResult.Buckets) perShardExpanded.get(s).get(cursors[s]));
                    cursors[s] += 1;
                }
                yield mergeTermsBuckets(t, perShardBuckets);
            }
        };
    }

    private static AggregationResult.Buckets mergeTermsBuckets(Aggregation.Terms t, List<AggregationResult.Buckets> perShardBuckets) {
        Map<String, Long> docCountByKey = new LinkedHashMap<>();
        Map<String, List<List<AggregationResult>>> subResultsByKey = new LinkedHashMap<>();
        long sumOther = 0;
        for (AggregationResult.Buckets b : perShardBuckets) {
            sumOther += b.sumOtherDocCount();
            for (AggregationResult.Bucket bucket : b.buckets()) {
                docCountByKey.merge(bucket.key(), bucket.docCount(), Long::sum);
                subResultsByKey.computeIfAbsent(bucket.key(), k -> new ArrayList<>()).add(bucket.subResults());
            }
        }
        List<Map.Entry<String, Long>> sorted = new ArrayList<>(docCountByKey.entrySet());
        sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

        List<AggregationResult.Bucket> finalBuckets = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++) {
            Map.Entry<String, Long> entry = sorted.get(i);
            if (i < t.size()) {
                List<AggregationResult> mergedSub = mergeSubAggsAcrossContributingShards(
                        t.subAggs(), subResultsByKey.get(entry.getKey()));
                finalBuckets.add(new AggregationResult.Bucket(entry.getKey(), entry.getValue(), mergedSub));
            } else {
                sumOther += entry.getValue();
            }
        }
        return new AggregationResult.Buckets(t.name(), finalBuckets, sumOther);
    }

    /** {@code perContributingShardSubResults}: each inner list is one shard's (already-expanded)
     * {@code bucket.subResults()} for this specific bucket key -- only shards whose result
     * actually contained this key contribute an entry, unlike the top-level merge where every
     * shard always contributes (every shard ran every top-level aggregation). */
    private static List<AggregationResult> mergeSubAggsAcrossContributingShards(List<Aggregation> subAggs,
            List<List<AggregationResult>> perContributingShardSubResults) {
        int[] cursors = new int[perContributingShardSubResults.size()];
        List<AggregationResult> merged = new ArrayList<>();
        for (Aggregation subAgg : subAggs) {
            merged.add(mergeOneAcrossShards(subAgg, perContributingShardSubResults, cursors));
        }
        return merged;
    }

    private static Double combineSimpleMetric(Aggregation.MetricType type, List<Double> values) {
        if (values.isEmpty()) {
            return type == Aggregation.MetricType.COUNT ? 0.0 : null;
        }
        return switch (type) {
            case SUM, COUNT -> values.stream().mapToDouble(Double::doubleValue).sum();
            case MIN -> values.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
            case MAX -> values.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
            case AVG -> throw new IllegalStateException("AVG is merged via sum+count, never as a simple metric");
        };
    }

    private static String openSearchMetricType(Aggregation.MetricType type) {
        return switch (type) {
            case AVG -> "avg";
            case SUM -> "sum";
            case MIN -> "min";
            case MAX -> "max";
            case COUNT -> "value_count";
        };
    }
}
