package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Updates;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Real proof of real {@code $inc}/{@code $push}/{@code $pull}/{@code $addToSet} update operator
 * support -- counters and array manipulation used in nearly every non-trivial real app, refused
 * outright before this fix.
 */
class MongoUpdateOperatorsIntegrationTest {

    private MongoCollection<Document> setUp(WarpProcess warp, String collectionName) {
        MongoClient client = MongoClients.create(
                "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true");
        MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName);
        coll.insertOne(new Document("_id", "u1").append("views", 10).append("tags", List.of("a", "b")));
        return coll;
    }

    @Test
    void incIncrementsARealCounter() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "inc_it");
            coll.updateOne(new Document("_id", "u1"), Updates.inc("views", 5));

            Document found = coll.find(new Document("_id", "u1")).first();
            assertEquals(15, found.getInteger("views"));
        }
    }

    @Test
    void pushAppendsToARealArray() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "push_it");
            coll.updateOne(new Document("_id", "u1"), Updates.push("tags", "c"));

            Document found = coll.find(new Document("_id", "u1")).first();
            assertEquals(List.of("a", "b", "c"), found.getList("tags", String.class));
        }
    }

    @Test
    void pullRemovesAMatchingArrayElement() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "pull_it");
            coll.updateOne(new Document("_id", "u1"), Updates.pull("tags", "a"));

            Document found = coll.find(new Document("_id", "u1")).first();
            assertEquals(List.of("b"), found.getList("tags", String.class));
        }
    }

    @Test
    void addToSetDoesNotDuplicateAnExistingElement() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> coll = setUp(warp, "addtoset_it");
            coll.updateOne(new Document("_id", "u1"), Updates.addToSet("tags", "a"));
            coll.updateOne(new Document("_id", "u1"), Updates.addToSet("tags", "c"));

            Document found = coll.find(new Document("_id", "u1")).first();
            assertEquals(List.of("a", "b", "c"), found.getList("tags", String.class));
        }
    }
}
