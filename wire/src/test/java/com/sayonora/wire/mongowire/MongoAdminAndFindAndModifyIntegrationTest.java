package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Real proof of four more real MongoDB commands, closing real gaps found auditing this frontend
 * for GA transparency: {@code listCollections} (mongoose's default {@code autoIndex} startup
 * behavior calls this before a query ever runs -- its absence broke CONNECTION setup, not just
 * queries), {@code countDocuments}, {@code distinct}, and {@code findOneAndUpdate}/{@code
 * findOneAndDelete} (both compile to the single {@code findAndModify} wire command).
 */
class MongoAdminAndFindAndModifyIntegrationTest {

    private WarpProcess start(RealPostgres postgres) throws Exception {
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @Test
    void listCollectionsReportsRealCollectionsCreatedByInserts() throws Exception {
        try (RealPostgres postgres = RealPostgres.start(); WarpProcess warp = start(postgres);
                MongoClient client = MongoClients.create(
                        "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
            MongoDatabase database = client.getDatabase("admin_it");
            database.getCollection("widgets").insertOne(new Document("_id", "w1").append("name", "gadget"));
            database.getCollection("gizmos").insertOne(new Document("_id", "g1").append("name", "thing"));

            List<String> names = database.listCollectionNames().into(new java.util.ArrayList<>());
            assertTrue(names.contains("widgets") && names.contains("gizmos"),
                    "expected both real collections listed -- got: " + names);
        }
    }

    @Test
    void countDocumentsAndDistinctReturnRealValues() throws Exception {
        try (RealPostgres postgres = RealPostgres.start(); WarpProcess warp = start(postgres);
                MongoClient client = MongoClients.create(
                        "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("count_it");
            coll.insertMany(List.of(
                    new Document("_id", "a").append("category", "x"),
                    new Document("_id", "b").append("category", "x"),
                    new Document("_id", "c").append("category", "y")));

            assertEquals(3, coll.countDocuments());
            assertEquals(2, coll.countDocuments(new Document("category", "x")));

            List<String> distinctCategories = coll.distinct("category", String.class)
                    .into(new java.util.ArrayList<>()).stream().sorted().collect(Collectors.toList());
            assertEquals(List.of("x", "y"), distinctCategories);
        }
    }

    @Test
    void findOneAndUpdateAtomicallyUpdatesAndReturnsTheNewDocument() throws Exception {
        try (RealPostgres postgres = RealPostgres.start(); WarpProcess warp = start(postgres);
                MongoClient client = MongoClients.create(
                        "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("fam_it");
            coll.insertOne(new Document("_id", "job1").append("status", "pending"));

            Document updated = coll.findOneAndUpdate(new Document("_id", "job1"),
                    Updates.set("status", "claimed"),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

            assertTrue(updated != null);
            assertEquals("claimed", updated.getString("status"));

            Document stored = coll.find(new Document("_id", "job1")).first();
            assertEquals("claimed", stored.getString("status"), "the update must actually have persisted");
        }
    }

    @Test
    void findOneAndDeleteRemovesTheDocumentAndReturnsItsPriorState() throws Exception {
        try (RealPostgres postgres = RealPostgres.start(); WarpProcess warp = start(postgres);
                MongoClient client = MongoClients.create(
                        "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("famdel_it");
            coll.insertOne(new Document("_id", "job2").append("status", "pending"));

            Document removed = coll.findOneAndDelete(new Document("_id", "job2"));
            assertTrue(removed != null);
            assertEquals("pending", removed.getString("status"));

            assertNull(coll.find(new Document("_id", "job2")).first(), "the document must actually be gone");
        }
    }
}
