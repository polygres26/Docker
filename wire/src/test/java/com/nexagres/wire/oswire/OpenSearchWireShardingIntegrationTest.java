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
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of oswire's V3 sharding: a real HTTP client speaking OpenSearch's actual
 * {@code _doc}/{@code _search} wire shape, a real Main subprocess sharded across two independent
 * real Postgres containers, no mocks. Documents are seeded with IDs deliberately not chosen for
 * convenience -- picking whichever docId actually hashes to each shard (confirmed by direct query
 * against each container below) proves documents really did land on different shards, not just
 * that the feature "didn't crash". See PostgresSearchStore's class javadoc and
 * SearchScatterMergeTest for the merge-logic unit coverage this complements.
 */
class OpenSearchWireShardingIntegrationTest {

    private static RealPostgres shard1;
    private static RealPostgres shard2;
    private static PolyWireProcess polywire;

    @BeforeAll
    static void startInfra() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();

        String backends = "default=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard2=" + shard2.jdbcUrl() + "|" + shard2.username() + "|" + shard2.password();

        polywire = PolyWireProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("oswire", "POLYWIRE_OSWIRE_PORT")
                .env("POLYWIRE_BACKENDS", backends)
                .env("POLYWIRE_SHARD_BACKENDS", "shard1,shard2")
                .env("POLYWIRE_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (polywire != null) polywire.close();
        if (shard2 != null) shard2.close();
        if (shard1 != null) shard1.close();
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

    /** Real proof documents landed on different physical shards: queries each container's own
     * {@code polywire_search_products} table directly (bypassing PolyWire entirely) and returns
     * which doc_ids matching {@code idPrefix} are actually present there. Filtered by prefix,
     * not a bare {@code SELECT *}, since this class's tests share one collection/table with no
     * per-test reset -- each test writes its own distinctly-prefixed doc_ids precisely so it can
     * make claims about only its own writes. */
    private Set<String> docIdsOnShard(RealPostgres shard, String idPrefix) throws Exception {
        Set<String> ids = new HashSet<>();
        try (var conn = java.sql.DriverManager.getConnection(shard.jdbcUrl(), shard.username(), shard.password());
                var ps = conn.prepareStatement("SELECT doc_id FROM polywire_search_products WHERE doc_id LIKE ?")) {
            ps.setString(1, idPrefix + "%");
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
            }
        }
        return ids;
    }

    @Test
    void documentsAreActuallySplitAcrossBothPhysicalShardsNotAllOnOne() throws Exception {
        for (int i = 1; i <= 10; i++) {
            put("/products/_doc/splitdoc" + i, "{\"name\":\"item " + i + "\",\"category\":\"cat\",\"price\":" + (i * 10) + "}");
        }

        Set<String> onShard1 = docIdsOnShard(shard1, "splitdoc");
        Set<String> onShard2 = docIdsOnShard(shard2, "splitdoc");

        assertEquals(10, onShard1.size() + onShard2.size(), "every doc must land on exactly one shard, none lost");
        assertTrue(!onShard1.isEmpty() && !onShard2.isEmpty(),
                "with 10 docs hashed across 2 shards, both shards must actually receive at least one "
                        + "(a real hash split, not everything defaulting to one backend)");
    }

    @Test
    void searchFindsDocumentsOnBothShardsRegardlessOfWhichOneTheyLandedOn() throws Exception {
        for (int i = 1; i <= 10; i++) {
            put("/products/_doc/doc" + i, "{\"name\":\"item " + i + "\",\"category\":\"widget\",\"price\":" + (i * 10) + "}");
        }

        JsonObject result = search("{\"query\":{\"term\":{\"category\":\"widget\"}}}");

        assertEquals(10, result.getAsJsonObject("hits").getAsJsonObject("total").get("value").getAsLong(),
                "a term query must see every matching document across both shards, not just the shard "
                        + "the query happened to be routed to first");
    }

    @Test
    void topKAndOffsetAreCorrectGloballyNotPerShard() throws Exception {
        for (int i = 1; i <= 10; i++) {
            put("/products/_doc/rdoc" + i, "{\"name\":\"ranked " + i + "\",\"category\":\"ranked\",\"price\":" + i + "}");
        }

        JsonObject result = send("POST", "/products/_search",
                "{\"query\":{\"term\":{\"category\":\"ranked\"}},\"sort\":[{\"price\":{\"order\":\"desc\"}}],\"size\":3}");

        JsonArray hits = result.getAsJsonObject("hits").getAsJsonArray("hits");
        assertEquals(3, hits.size(), "size=3 must cap the merged result at 3 total, not 3 per shard");
        assertEquals(10.0, hits.get(0).getAsJsonObject().getAsJsonObject("_source").get("price").getAsDouble());
        assertEquals(9.0, hits.get(1).getAsJsonObject().getAsJsonObject("_source").get("price").getAsDouble());
        assertEquals(8.0, hits.get(2).getAsJsonObject().getAsJsonObject("_source").get("price").getAsDouble());
    }

    @Test
    void avgAggregationMergesAsARealWeightedAverageAcrossShardsNotAnAverageOfAverages() throws Exception {
        for (int i = 1; i <= 10; i++) {
            put("/products/_doc/adoc" + i, "{\"name\":\"agg " + i + "\",\"category\":\"aggregated\",\"price\":" + (i * 10) + "}");
        }
        // prices 10,20,...,100 -- real average = 55, split however hashing lands them across shards.

        JsonObject result = send("POST", "/products/_search",
                "{\"query\":{\"term\":{\"category\":\"aggregated\"}},\"size\":0,"
                        + "\"aggs\":{\"avgPrice\":{\"avg\":{\"field\":\"price\"}}}}");

        JsonObject aggs = result.getAsJsonObject("aggregations");
        JsonObject avgPrice = aggs.getAsJsonObject(aggs.keySet().stream()
                .filter(k -> k.contains("avgPrice")).findFirst().orElseThrow());
        assertEquals(55.0, avgPrice.get("value").getAsDouble(), 0.0001,
                "true average of 10..100 is 55 -- an average-of-per-shard-averages would be wrong "
                        + "unless the split happened to be perfectly even");
    }

    @Test
    void termsAggregationMergesTheSameBucketKeyFoundOnBothShards() throws Exception {
        for (int i = 1; i <= 10; i++) {
            String region = i % 2 == 0 ? "east" : "west";
            put("/products/_doc/tdoc" + i, "{\"name\":\"t " + i + "\",\"category\":\"terms-test\",\"region\":\""
                    + region + "\",\"price\":" + i + "}");
        }

        JsonObject result = send("POST", "/products/_search",
                "{\"query\":{\"term\":{\"category\":\"terms-test\"}},\"size\":0,"
                        + "\"aggs\":{\"byRegion\":{\"terms\":{\"field\":\"region\",\"size\":10}}}}");

        JsonObject aggs = result.getAsJsonObject("aggregations");
        JsonObject byRegion = aggs.getAsJsonObject(aggs.keySet().stream()
                .filter(k -> k.contains("byRegion")).findFirst().orElseThrow());
        JsonArray buckets = byRegion.getAsJsonArray("buckets");

        assertEquals(2, buckets.size(), "exactly 2 distinct regions, merged into 2 buckets total -- "
                + "not up to 2-per-shard (4) if the same region appeared unmerged on both shards");
        long totalDocs = 0;
        for (var b : buckets) {
            totalDocs += b.getAsJsonObject().get("doc_count").getAsLong();
        }
        assertEquals(10, totalDocs, "every document across both shards must be counted");
    }

    @Test
    void vectorSearchOnAShardedCollectionIsRefusedNotSilentlyWrong() throws Exception {
        put("/products/_doc/vdoc1", "{\"name\":\"v\",\"vector\":[0.1,0.2,0.3,0.4]}");

        JsonObject result = send("POST", "/products/_search",
                "{\"query\":{\"knn\":{\"vector\":{\"vector\":[0.1,0.2,0.3,0.4],\"k\":5}}}}");

        assertTrue(result.get("__status").getAsInt() >= 400,
                "k-NN search on a sharded collection must fail loudly, not silently search only one shard");
    }
}
