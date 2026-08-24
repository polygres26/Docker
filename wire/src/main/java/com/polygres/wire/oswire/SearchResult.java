package com.polygres.wire.oswire;

import java.util.List;

/** What {@code PostgresSearchStore} hands back to an adapter -- {@code total} is the count
 * before {@code topK}/{@code offset} were applied (OpenSearch's {@code hits.total.value}).
 * {@code aggregations} is empty unless the request asked for any (see
 * {@link SearchRequest#aggregations()}). */
public record SearchResult(List<SearchHit> hits, long total, List<AggregationResult> aggregations) {

    public SearchResult(List<SearchHit> hits, long total) {
        this(hits, total, List.of());
    }
}
