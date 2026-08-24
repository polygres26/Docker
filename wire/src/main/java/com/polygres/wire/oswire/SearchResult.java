package com.polygres.wire.oswire;

import java.util.List;

/** What {@code PostgresSearchPlanner} hands back to an adapter -- {@code total} is the count
 * before {@code topK}/{@code offset} were applied (OpenSearch's {@code hits.total.value}). */
public record SearchResult(List<SearchHit> hits, long total) {
}
