package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of real {@code aggregate} pipeline support -- a real, high-impact gap found
 * auditing this frontend for GA transparency: {@code aggregate} wasn't handled at all before this
 * (fell straight through to "command not found"), and it's the mechanism a typical app uses for
 * grouped reports (totals/counts by some key). Real MongoDB client (real OP_MSG wire encoding),
 * real Postgres backend.
 */
class MongoAggregateIntegrationTest {

    private MongoCollection<Document> setUp(WarpProcess warp, String collectionName) {
        MongoClient client = MongoClients.create(
                "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true");
        MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName);
        coll.insertMany(List.of(
                new Document("_id", "o1").append("customer", "alice").append("status", "done").append("amount", 10.0),
                new Document("_id", "o2").append("customer", "alice").append("status", "done").append("amount", 25.0),
                new Document("_id", "o3").append("customer", "bob").append("status", "done").append("amount", 5.0),
                new Document("_id", "o4").append("customer", "bob").append("status", "pending").append("amount", 100.0)));
        return coll;
    }

    @Test
    void matchGroupSortLimitProducesARealGroupedReport() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "agg_it");

            List<Document> pipeline = List.of(
                    new Document("$match", new Document("status", "done")),
                    new Document("$group", new Document("_id", "$customer")
                            .append("total", new Document("$sum", "$amount"))
                            .append("count", new Document("$sum", 1))),
                    new Document("$sort", new Document("total", -1)));

            List<Document> results = new ArrayList<>();
            coll.aggregate(pipeline).into(results);

            assertEquals(2, results.size(), "expected one grouped row per distinct customer with a 'done' order");
            Document top = results.get(0);
            assertEquals("alice", top.getString("_id"), "alice has the higher total (35) and must sort first");
            assertEquals(35.0, top.getDouble("total"), 0.001);
            assertEquals(2, top.getInteger("count"));

            Document second = results.get(1);
            assertEquals("bob", second.getString("_id"));
            assertEquals(5.0, second.getDouble("total"), 0.001);
            assertEquals(1, second.getInteger("count"));
        }
    }

    @Test
    void matchSortLimitWithNoGroupWorksLikeAFilteredSortedFind() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "agg_nogroup_it");

            List<Document> pipeline = List.of(
                    new Document("$match", new Document("customer", "bob")),
                    new Document("$sort", new Document("amount", -1)),
                    new Document("$limit", 1));

            List<Document> results = new ArrayList<>();
            coll.aggregate(pipeline).into(results);

            assertEquals(1, results.size());
            assertEquals("o4", results.get(0).getString("_id"), "bob's highest-amount order must come first");
        }
    }

    @Test
    void countViaSumOneAcrossTheWholeCollectionWorksWithANullGroupId() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "agg_count_it");

            List<Document> pipeline = List.of(
                    new Document("$group", new Document("_id", null)
                            .append("total", new Document("$sum", 1))));

            List<Document> results = new ArrayList<>();
            coll.aggregate(pipeline).into(results);

            assertEquals(1, results.size());
            assertTrue(results.get(0).get("_id") == null);
            assertEquals(4, results.get(0).getInteger("total"), "expected all 4 inserted documents counted");
        }
    }
}
