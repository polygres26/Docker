package com.nexagres.wire.oswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real HTTP client speaking OpenSearch's actual {@code _search}/
 * documents/{@code _bulk} wire shape gets correct results out of oswire's Postgres-backed
 * implementation -- a real subprocess of {@code Main}, a real disposable Postgres container, no
 * mocks. Same rigor as {@code PgWireIntegrationTest} et al.
 */
class OpenSearchWireIntegrationTest {

    private static RealPostgres postgres;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        polywire = PolyWireProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("oswire", "POLYWIRE_OSWIRE_PORT")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) {
            polywire.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void seedIndex() throws IOException {
        // Reset to exactly docs 1-3 before every test, since the store is a real Postgres table
        // shared across tests in this class (no per-test schema) -- without this, a test that
        // writes (bulkIndexesAndDeletesInOneRequest adds doc 4, deletes doc 3) would leak state
        // into whichever test runs next, since JUnit doesn't guarantee method order.
        send("DELETE", "/products/_doc/4", null);
        put("/products/_doc/1", "{\"name\":\"Wireless Mouse\",\"category\":\"electronics\",\"price\":25.99,"
                + "\"in_stock\":true,\"description\":\"A comfortable wireless mouse\","
                + "\"vector\":[0.1,0.2,0.3,0.4]}");
        put("/products/_doc/2", "{\"name\":\"Mechanical Keyboard\",\"category\":\"electronics\",\"price\":89.99,"
                + "\"in_stock\":true,\"description\":\"A responsive mechanical keyboard\","
                + "\"vector\":[0.15,0.25,0.28,0.42]}");
        put("/products/_doc/3", "{\"name\":\"Garden Hose\",\"category\":\"home\",\"price\":15.50,"
                + "\"in_stock\":false,\"description\":\"A durable rubber garden hose\","
                + "\"vector\":[0.9,0.8,0.1,0.05]}");
    }

    private String baseUrl() {
        return "http://localhost:" + polywire.port("oswire");
    }

    private JsonObject send(String method, String path, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(baseUrl() + path).toURL().openConnection();
        conn.setRequestMethod(method);
        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int status = conn.getResponseCode();
        var stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
        String responseBody = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        JsonObject json = JsonParser.parseString(responseBody).getAsJsonObject();
        json.addProperty("__status", status);
        return json;
    }

    private JsonObject put(String path, String body) throws IOException {
        return send("PUT", path, body);
    }

    private JsonObject search(String query) throws IOException {
        return send("POST", "/products/_search", query);
    }

    @Test
    void termQueryFindsExactMatches() throws IOException {
        JsonObject result = search("{\"query\":{\"term\":{\"category\":\"electronics\"}}}");
        assertEquals(2, result.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsLong());
    }

    @Test
    void rangeQueryComparesNumericFieldCorrectly() throws IOException {
        JsonObject result = search("{\"query\":{\"range\":{\"price\":{\"gte\":20,\"lte\":90}}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(2, hits.size());
        for (var h : hits) {
            double price = h.getAsJsonObject().getAsJsonObject("_source").get("price").getAsDouble();
            assertTrue(price >= 20 && price <= 90, "price " + price + " out of requested range");
        }
    }

    @Test
    void matchQueryUsesRealFullTextSearch() throws IOException {
        JsonObject result = search("{\"query\":{\"match\":{\"description\":\"wireless mouse\"}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(1, hits.size());
        assertEquals("1", hits.get(0).getAsJsonObject().get("_id").getAsString());
    }

    @Test
    void boolQueryCombinesMustAndMustNot() throws IOException {
        JsonObject result = search("{\"query\":{\"bool\":{\"must\":[{\"term\":{\"category\":\"electronics\"}}],"
                + "\"must_not\":[{\"term\":{\"name\":\"Mechanical Keyboard\"}}]}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(1, hits.size());
        assertEquals("1", hits.get(0).getAsJsonObject().get("_id").getAsString());
    }

    @Test
    void knnQueryRanksByVectorDistance() throws IOException {
        JsonObject result = search("{\"query\":{\"knn\":{\"vector\":{\"vector\":[0.11,0.19,0.31,0.39],\"k\":3}}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(3, hits.size());
        // doc 1's own vector [0.1,0.2,0.3,0.4] is nearly identical to the query vector -- it must
        // be the closest match, ranked first.
        assertEquals("1", hits.get(0).getAsJsonObject().get("_id").getAsString());
    }

    @Test
    void sortAndPaginationRespectOffsetAndSize() throws IOException {
        JsonObject result = search("{\"size\":1,\"from\":1,\"sort\":[{\"name\":{\"order\":\"asc\"}}]}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(1, hits.size());
        // Alphabetical: Garden Hose, Mechanical Keyboard, Wireless Mouse -- from=1 skips the first.
        assertEquals("Mechanical Keyboard", hits.get(0).getAsJsonObject().getAsJsonObject("_source").get("name").getAsString());
    }

    @Test
    void bulkIndexesAndDeletesInOneRequest() throws IOException {
        String ndjson = "{\"index\":{\"_index\":\"products\",\"_id\":\"4\"}}\n"
                + "{\"name\":\"USB Hub\",\"category\":\"electronics\",\"price\":19.99,\"in_stock\":true}\n"
                + "{\"delete\":{\"_index\":\"products\",\"_id\":\"3\"}}\n";
        JsonObject bulkResult = send("POST", "/_bulk", ndjson);
        assertEquals(false, bulkResult.get("errors").getAsBoolean());

        JsonObject doc4 = send("GET", "/products/_doc/4", null);
        assertEquals(true, doc4.get("found").getAsBoolean());

        // Real OpenSearch's GET returns HTTP 200 with found=false for a missing document, not a
        // 404 -- see OpenSearchWireServer#handleGetDoc's javadoc for why this matters live.
        JsonObject doc3 = send("GET", "/products/_doc/3", null);
        assertEquals(200, doc3.get("__status").getAsInt());
        assertEquals(false, doc3.get("found").getAsBoolean());
    }

    @Test
    void unrecognizedQueryClauseFailsLoudlyInsteadOfMatchingEverything() throws IOException {
        JsonObject result = search("{\"query\":{\"wildcard\":{\"name\":\"*mouse*\"}}}");
        assertEquals(400, result.get("__status").getAsInt());
        assertEquals("parsing_exception", result.getAsJsonObject("error").get("type").getAsString());
    }

    @Test
    void termsAggregationGroupsWithNestedAvgMetric() throws IOException {
        JsonObject result = search("{\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10},"
                + "\"aggs\":{\"avg_price\":{\"avg\":{\"field\":\"price\"}}}}}}");
        assertEquals(0, result.getAsJsonObject("hits").getAsJsonArray("hits").size());
        JsonArray buckets = result.getAsJsonObject("aggregations").getAsJsonObject("by_category").getAsJsonArray("buckets");
        assertEquals(2, buckets.size());
        for (var b : buckets) {
            JsonObject bucket = b.getAsJsonObject();
            double avgPrice = bucket.getAsJsonObject("avg_price").get("value").getAsDouble();
            if ("electronics".equals(bucket.get("key").getAsString())) {
                assertEquals(2, bucket.get("doc_count").getAsInt());
                // (25.99 + 89.99) / 2
                assertEquals(57.99, avgPrice, 0.01);
            } else {
                assertEquals("home", bucket.get("key").getAsString());
                assertEquals(1, bucket.get("doc_count").getAsInt());
                assertEquals(15.50, avgPrice, 0.01);
            }
        }
    }

    @Test
    void matchQueryReturnsRealRelevanceScoreNotFlatOne() throws IOException {
        JsonObject result = search("{\"query\":{\"match\":{\"description\":\"wireless\"}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(1, hits.size());
        double score = hits.get(0).getAsJsonObject().get("_score").getAsDouble();
        // A real ts_rank value for a single-term match against a short description is well under
        // 1.0 -- V1 always returned a flat 1.0 here regardless of the actual text.
        assertTrue(score > 0.0 && score < 1.0, "expected a real ts_rank score in (0, 1), got " + score);
    }

    @Test
    void knnScoreIsHigherIsBetterSimilarityNotRawDistance() throws IOException {
        // Query vector is doc 1's own vector -- an exact match, so its similarity score must be
        // (very close to) the maximum of 1.0, not V1's raw near-zero distance value.
        JsonObject result = search("{\"query\":{\"knn\":{\"vector\":{\"vector\":[0.1,0.2,0.3,0.4],\"k\":3}}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        JsonObject best = hits.get(0).getAsJsonObject();
        assertEquals("1", best.get("_id").getAsString());
        assertEquals(1.0, best.get("_score").getAsDouble(), 0.01);
        // Every subsequent hit must score lower (higher-is-better ordering).
        for (int i = 1; i < hits.size(); i++) {
            assertTrue(hits.get(i - 1).getAsJsonObject().get("_score").getAsDouble()
                    >= hits.get(i).getAsJsonObject().get("_score").getAsDouble());
        }
    }

    @Test
    void hybridQueryFusesTextAndVectorScores() throws IOException {
        // doc 3 (Garden Hose) matches neither "wireless" nor is close to doc 1's vector, so it
        // must rank behind doc 1, which is boosted by both sub-queries at once.
        JsonObject result = search("{\"query\":{\"hybrid\":{\"queries\":["
                + "{\"match\":{\"description\":\"wireless\"}},"
                + "{\"knn\":{\"vector\":{\"vector\":[0.1,0.2,0.3,0.4],\"k\":3}}}"
                + "]}}}");
        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertTrue(hits.size() >= 2, "expected at least 2 fused hits, got " + hits);
        assertEquals("1", hits.get(0).getAsJsonObject().get("_id").getAsString());
        double topScore = hits.get(0).getAsJsonObject().get("_score").getAsDouble();
        double lastScore = hits.get(hits.size() - 1).getAsJsonObject().get("_score").getAsDouble();
        assertTrue(topScore >= lastScore);
    }

    @Test
    void aggregationsWithVectorSearchFailsLoudlyInsteadOfSilentlyDroppingAggs() throws IOException {
        JsonObject result = search("{\"query\":{\"knn\":{\"vector\":{\"vector\":[0.1,0.2,0.3,0.4],\"k\":3}}},"
                + "\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\"}}}}");
        assertEquals(400, result.get("__status").getAsInt());
        assertEquals("action_request_validation_exception", result.getAsJsonObject("error").get("type").getAsString());
    }

    @Test
    void metricsEndpointReportsOswireTraffic() throws Exception {
        search("{\"query\":{\"match_all\":{}}}");
        HttpURLConnection metricsConn = (HttpURLConnection) URI
                .create("http://localhost:" + polywire.metricsPort() + "/metrics").toURL().openConnection();
        assertEquals(200, metricsConn.getResponseCode());
        String body;
        try (var in = metricsConn.getInputStream()) {
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertTrue(body.contains("protocol=\"oswire\""),
                "expected /metrics to report oswire traffic, got:\n" + body);
    }
}
