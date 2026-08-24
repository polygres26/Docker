package com.polygres.wire.oswire;

import java.util.List;

/**
 * The internal representation every search-style wire protocol compiles its own query DSL into,
 * before a single line of SQL is generated. Today {@link OpenSearchAdapter} is the only producer
 * (parsing OpenSearch's {@code _search} JSON body) and {@code PostgresSearchPlanner} is the only
 * consumer (turning it into a query against a real Postgres table) -- but that's the whole point
 * of having this type exist as its own class instead of inlining OpenSearch's JSON shape directly
 * into the planner: a second adapter (Qdrant's REST API is the one actually being planned for --
 * see {@code oswire}'s package javadoc) only has to translate Qdrant's request shape into this
 * same record and can reuse the planner, the store, and the metrics/firewall/ACL wiring verbatim.
 * Nothing downstream of this record is protocol-specific.
 *
 * @param collection   the index/collection name (OpenSearch calls it an index; Qdrant a
 *                     collection -- this field is deliberately protocol-neutral)
 * @param projection   fields to include in {@code _source}/payload; {@code null} means "all
 *                     fields", an empty list means "none" (OpenSearch's {@code _source: false})
 * @param filter       the compiled filter/query tree -- see {@link SearchFilter}
 * @param textQuery    a bare full-text query string not already folded into {@code filter} (kept
 *                     separate so a future hybrid-search planner can weight it independently of
 *                     structural filters, per this feature's V2 plan)
 * @param vector       the query vector for a k-NN search, or {@code null} for a non-vector query
 * @param vectorField  which field {@code vector} is compared against; required when
 *                     {@code vector} is non-null
 * @param distanceMetric how to score {@code vector} against stored vectors
 * @param topK         max results to return (OpenSearch's {@code size})
 * @param offset       results to skip (OpenSearch's {@code from})
 * @param sort         explicit sort fields; empty means "by relevance/distance score"
 * @param aggregations reserved for V2 (OpenSearch aggregations) -- always empty in V1, kept as a
 *                     field now so the planner's/adapter's public shape doesn't have to change
 *                     shape again when that lands
 */
public record SearchRequest(
        String collection,
        List<String> projection,
        SearchFilter filter,
        String textQuery,
        float[] vector,
        String vectorField,
        DistanceMetric distanceMetric,
        int topK,
        int offset,
        List<SortField> sort,
        List<String> aggregations) {

    public enum DistanceMetric { COSINE, L2, DOT_PRODUCT }

    public boolean isVectorSearch() {
        return vector != null;
    }
}
