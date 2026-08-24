package com.polygres.wire.oswire;

/** One entry of a search request's sort list -- OpenSearch's {@code {"field": {"order": "asc"}}}. */
public record SortField(String field, boolean ascending) {
}
