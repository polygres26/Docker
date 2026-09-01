package com.nexagres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

/** oswire: real OpenSearch HTTP/JSON API (_search/documents/_bulk), the real official
 * opensearch-java client, translated to Postgres underneath -- see
 * com.nexagres.wire.oswire's package for the internal Search IR this is staged around. No auth
 * configured in the test compose file. */
class OsWireTest {

    private static OpenSearchClient client() {
        ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder
                .builder(new org.apache.hc.core5.http.HttpHost("http", TestConfig.HOST, TestConfig.OSWIRE_PORT))
                .build();
        return new OpenSearchClient(transport);
    }

    @Test
    void indexAndSearchByTerm() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("name", "warp");
        doc1.put("category", "gateway");
        c.index(r -> r.index(index).id("1").document(doc1).refresh(
                org.opensearch.client.opensearch._types.Refresh.True));

        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("name", "polyadvisor");
        doc2.put("category", "assessment");
        c.index(r -> r.index(index).id("2").document(doc2).refresh(
                org.opensearch.client.opensearch._types.Refresh.True));

        SearchResponse<Map> response = c.search(s -> s.index(index)
                .query(q -> q.term(t -> t.field("category").value(v -> v.stringValue("gateway")))),
                Map.class);

        assertEquals(1, response.hits().hits().size());
        assertEquals("1", response.hits().hits().get(0).id());
        assertEquals("warp", response.hits().hits().get(0).source().get("name"));
    }

    @Test
    void getAndDeleteDocument() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "warp");
        c.index(r -> r.index(index).id("1").document(doc));

        var got = c.get(g -> g.index(index).id("1"), Map.class);
        assertTrue(got.found());
        assertEquals("warp", got.source().get("name"));

        c.delete(d -> d.index(index).id("1"));
        var afterDelete = c.get(g -> g.index(index).id("1"), Map.class);
        assertFalse(afterDelete.found());
    }

    @Test
    void termsAggregationWithNestedAvgMetric() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        index(c, index, "1", Map.of("category", "electronics", "price", 20.0));
        index(c, index, "2", Map.of("category", "electronics", "price", 40.0));
        index(c, index, "3", Map.of("category", "home", "price", 10.0));
        c.indices().refresh(r -> r.index(index));

        SearchResponse<Map> response = c.search(s -> s.index(index).size(0)
                .aggregations("by_category", a -> a
                        .terms(t -> t.field("category").size(10))
                        .aggregations("avg_price", sub -> sub.avg(avg -> avg.field("price")))),
                Map.class);

        var buckets = response.aggregations().get("by_category").sterms().buckets().array();
        assertEquals(2, buckets.size());
        for (var bucket : buckets) {
            double avgPrice = bucket.aggregations().get("avg_price").avg().value();
            if ("electronics".equals(bucket.key())) {
                assertEquals(2, bucket.docCount());
                assertEquals(30.0, avgPrice, 0.01);
            } else {
                assertEquals("home", bucket.key());
                assertEquals(1, bucket.docCount());
                assertEquals(10.0, avgPrice, 0.01);
            }
        }
    }

    @Test
    void hybridQueryFusesTextAndVectorScores() throws Exception {
        OpenSearchClient c = client();
        String index = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        index(c, index, "1", Map.of("description", "a warp gateway", "vector", List.of(0.1, 0.2, 0.3, 0.4)));
        index(c, index, "2", Map.of("description", "unrelated text", "vector", List.of(0.9, 0.8, 0.1, 0.05)));
        c.indices().refresh(r -> r.index(index));

        // opensearch-java 2.19's typed query DSL doesn't model the neural-search hybrid query
        // (it's a plugin extension, not core OpenSearch) -- sent via a plain HTTP client instead
        // for this one query shape, same raw wire request any real client (including
        // opensearch-py, used for this same scenario in docker/tests/python) sends underneath.
        String body = "{\"query\":{\"hybrid\":{\"queries\":["
                + "{\"match\":{\"description\":\"warp\"}},"
                + "{\"knn\":{\"vector\":{\"vector\":[0.1,0.2,0.3,0.4],\"k\":2}}}"
                + "]}}}";
        var httpClient = java.net.http.HttpClient.newHttpClient();
        var request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://" + TestConfig.HOST + ":" + TestConfig.OSWIRE_PORT + "/" + index + "/_search"))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                .build();
        var response = httpClient.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertTrue(response.body().contains("\"_id\":\"1\""), "expected doc 1 (matches both sub-queries) in: " + response.body());
    }

    private static void index(OpenSearchClient c, String index, String id, Map<String, Object> doc) throws Exception {
        c.index(r -> r.index(index).id(id).document(doc));
    }
}
