package com.nexagres.wire.oswire;

import com.google.gson.JsonObject;

/** One result row, protocol-neutral -- {@code OpenSearchAdapter} renders this into OpenSearch's
 * {@code hits.hits[]} shape ({@code _id}/{@code _score}/{@code _source}). */
public record SearchHit(String id, double score, JsonObject source) {
}
