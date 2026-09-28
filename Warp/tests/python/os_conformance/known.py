"""Documented differences from real OpenSearch that the differential corpus is allowed to keep showing.
case name -> reason. (Everything else must match the recorded oracle output exactly.)"""

KNOWN = {
    "q/geo_bounding_box": "Lucene encodes geo points to 32-bit ints, so a point exactly on the box boundary can fall on either side; Warp compares doubles",
    "a/percentile_ranks": "OpenSearch answers percentile_ranks from a t-digest whose interpolation differs slightly from Warp's exact-centroid cdf",
    "a/aggs_metric_with_subagg_error": "real OpenSearch answers this malformed request with a 500; Warp returns the proper aggregation_initialization_exception (400)",
    "doc/delete": "Warp keeps no delete tombstones: the version of a delete of a missing document is 1, real OpenSearch continues the version sequence",
    "idx/explain_api": "the _explain explanation tree is a one-line summary, not Lucene's full BM25 breakdown",
    "cat/count_and_health_json": "Warp always reports cluster health green (no replica shards exist to be unassigned)",
    "cat/aliases_shards_json": "Warp does not list the unassigned replica shard row real OpenSearch reports for number_of_replicas=1",
    "cat/unknown_endpoint_and_method": "the list of allowed methods in a 405 body differs (real OpenSearch aggregates every route on the path)",
}

# additionally, when the index is spread over SEVERAL hosts (relevance is scored per host, like per shard)
KNOWN_SHARDED = {
    "s/sort_score_then_field": "the _score echoed in `sort` is the per-host score",
    "s/terminate_after": "terminate_after applies per host (per shard, as in OpenSearch with several shards)",
    "doc/scroll_doc_sort_and_total": "_doc order is host-local: a scroll over several hosts returns them one host after the other",
}
