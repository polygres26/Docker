package com.polygres.wire.oswire;

import java.util.List;

/** What {@code PostgresSearchStore} hands back for one {@link Aggregation} request -- protocol-
 * neutral, same as {@link SearchHit}; {@code OpenSearchAdapter} renders this into OpenSearch's
 * real {@code aggregations} response shape. {@code openSearchType} is real OpenSearch's own
 * aggregation-type tag (e.g. {@code "sterms"}, {@code "avg"}) -- needed because official clients
 * (confirmed live with {@code opensearch-java}) require the response's {@code typed_keys} format
 * ({@code "<type>#<name>"} as the JSON key, not a bare {@code "<name>"}), which is real
 * OpenSearch's own default response shape for aggregations, not an oswire-specific addition. */
public sealed interface AggregationResult {

    String name();

    String openSearchType();

    /** Result of a {@link Aggregation.Terms} request -- always {@code "sterms"} (string-valued
     * terms), since every field value here comes from JSONB text extraction.
     * {@code sumOtherDocCount} is real OpenSearch's own field: the number of matching documents
     * that fell into a bucket excluded by the {@code size} cap -- required in the response shape
     * (confirmed live: {@code opensearch-java} throws {@code Missing required property
     * 'StringTermsAggregate.sumOtherDocCount'} without it), and a real, computed count here (see
     * {@code PostgresSearchStore#runOneAggregation}), not a hardcoded {@code 0}. */
    record Buckets(String name, List<Bucket> buckets, long sumOtherDocCount) implements AggregationResult {
        public String openSearchType() {
            return "sterms";
        }
    }

    /** Result of a top-level {@link Aggregation.Metric} request (not nested under a bucket).
     * {@code openSearchType} is one of {@code avg}/{@code sum}/{@code min}/{@code max}/
     * {@code value_count}, matching {@link Aggregation.MetricType}. */
    record SingleValue(String name, Double value, String openSearchType) implements AggregationResult {
    }

    /** One bucket of a {@link Buckets} result -- {@code subResults} holds this bucket's own
     * nested aggregation results, if any were requested. */
    record Bucket(String key, long docCount, List<AggregationResult> subResults) {
    }
}
