package com.sayonora.wire.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@link RowCache} is actually SHARED across mongowire and the SQL
 * frontends, not just present in both -- the same real, no-mock discipline as {@code
 * RowCacheSharingIntegrationTest} (dynamowire's own version of this test), but for mongowire: a
 * real MongoDB Java driver client and a real pgwire JDBC connection, against the same Warp
 * subprocess and the same real Postgres backend.
 *
 * <p>Deliberately does NOT set {@code WARP_MONGOWIRE_CACHE_ENABLED=false} the way the
 * mongowire error-mapping tests do (they're testing error paths that don't want the cache in the
 * way) -- this test's whole point needs the row cache on, which is the default.
 */
class MongoRowCacheSharingIntegrationTest {

    private static final String ADMIN_TOKEN = "mongo-row-cache-sharing-test-token";

    /** The physical Postgres table mongowire creates for database {@code test} / collection
     * {@code orders} -- see {@code PostgresDocumentStore.qualifiedTable}: always
     * double-quoted, case-preserving {@code "db"."collection"}. Deliberately hardcoded here
     * rather than discovered, the same way a real SQL client pointed at a mongowire-backed
     * collection would have to know this name -- it's mongowire's own documented physical naming
     * convention, not an implementation detail this test reaches into. */
    private static final String PHYSICAL_TABLE = "\"test\".\"orders\"";

    private static String metricsSummary(WarpProcess warp) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/metrics/summary"))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    @Test
    void aMongoInsertAndFindIsVisibleAsASqlCacheHitOnTheExactSameRow() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_CACHE_TABLES", PHYSICAL_TABLE)
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create("mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("orders");
                coll.insertOne(new Document("_id", "item-42").append("amount", 129.99));

                // Real find, real Postgres round trip -- populates RowCache under the PHYSICAL
                // table name ("test"."orders"|"item-42"|), not any mongowire-only key shape.
                Document found = coll.find(new Document("_id", "item-42")).first();
                assertTrue(found != null && "item-42".equals(found.getString("_id")));
            }

            // The actual cross-protocol claim: a real SQL SELECT-by-id over real pgwire/JDBC,
            // against the physical table mongowire's find above already populated the row cache
            // for -- CacheStage's own SQL-side fast path (tryRowCacheLookup) must find that exact
            // entry and never touch Postgres for this read at all.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    PreparedStatement ps = conn.prepareStatement(
                            "SELECT doc FROM " + PHYSICAL_TABLE + " WHERE id = ?")) {
                ps.setString(1, "\"item-42\"");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "expected one row for id=\"item-42\"");
                    String docJson = rs.getString(1);
                    assertTrue(docJson.contains("item-42"),
                            "the SQL-visible row must be the same document MongoDB wrote -- got: " + docJson);
                    assertTrue(docJson.contains("129.99"),
                            "the SQL-visible row must carry the same field MongoDB wrote -- got: " + docJson);
                }
            }

            // Not "it returned the right data" (Postgres would also return the right data on a
            // real, uncached round trip) -- specifically that it came from the shared row cache,
            // under the pgwire protocol label, proving CacheStage's fast path is what served it.
            String summary = metricsSummary(warp);
            assertTrue(summary.contains("\"protocol\":\"pgwire\"") && summary.contains("\"outcome\":\"cache_hit\""),
                    "expected a pgwire cache_hit in rttByOutcome (the SQL side hitting mongowire's "
                            + "own populated row-cache entry) -- got: " + summary);
        }
    }

    @Test
    void aSqlUpdateByIdInvalidatesTheRowMongoDbSeesNext() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_CACHE_TABLES", PHYSICAL_TABLE)
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create("mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("orders");
                coll.insertOne(new Document("_id", "item-77").append("status", "pending"));
                // Warm the row cache the same way the other test does.
                coll.find(new Document("_id", "item-77")).first();

                // A real SQL UPDATE against the same id, over real pgwire/JDBC -- not through
                // mongowire at all. CacheStage#invalidateRowCacheForPointWrite must drop the
                // exact row-cache entry mongowire's own find above just populated. A real bind
                // parameter, not an inlined literal: the invalidation regex (like the lookup one)
                // only recognizes the `?`-placeholder shape a real prepared statement sends.
                try (Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "UPDATE " + PHYSICAL_TABLE + " SET doc = ?::jsonb WHERE id = ?")) {
                    ps.setString(1, "{\"_id\":\"item-77\",\"status\":\"shipped\"}");
                    ps.setString(2, "\"item-77\"");
                    ps.executeUpdate();
                }

                Document found = coll.find(new Document("_id", "item-77")).first();
                assertEquals("shipped", found.getString("status"),
                        "mongowire's find must see the fresh row the SQL UPDATE wrote, not a "
                                + "stale row-cache entry from before the SQL-side write");
            }
        }
    }
}
