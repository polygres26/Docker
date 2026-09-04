package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@code $set}/{@code $unset} with dotted (nested-field) paths -- the
 * ordinary way a real MongoDB client updates one field inside a subdocument without clobbering
 * its siblings -- works through mongowire, via a real MongoDB client (real OP_MSG wire encoding).
 * Real gap found and fixed: {@link UpdateApplier} used to refuse any {@code $set} key containing
 * a {@code "."} outright.
 */
class MongoDottedUpdateIntegrationTest {

    @Test
    void dottedSetUpdatesOneNestedFieldWithoutClobberingItsSiblings() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create(
                    "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("dotted_it");
                coll.insertOne(new Document("_id", "u1")
                        .append("address", new Document("city", "Springfield").append("zip", "00000")));

                coll.updateOne(new Document("_id", "u1"),
                        new Document("$set", new Document("address.city", "Shelbyville")));

                Document found = coll.find(new Document("_id", "u1")).first();
                assertTrue(found != null);
                Document address = (Document) found.get("address");
                assertEquals("Shelbyville", address.getString("city"), "the targeted nested field must update");
                assertEquals("00000", address.getString("zip"), "a sibling nested field must be untouched");
            }
        }
    }

    @Test
    void dottedSetCreatesIntermediateSubdocumentsThatDidNotExistYet() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create(
                    "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("dotted_create_it");
                coll.insertOne(new Document("_id", "u2"));

                coll.updateOne(new Document("_id", "u2"),
                        new Document("$set", new Document("profile.settings.theme", "dark")));

                Document found = coll.find(new Document("_id", "u2")).first();
                Document profile = (Document) found.get("profile");
                Document settings = (Document) profile.get("settings");
                assertEquals("dark", settings.getString("theme"));
            }
        }
    }

    @Test
    void dottedUnsetRemovesOnlyTheTargetedNestedFieldMissingPathIsANoOp() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create(
                    "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("dotted_unset_it");
                coll.insertOne(new Document("_id", "u3")
                        .append("address", new Document("city", "Springfield").append("zip", "00000")));

                // Unsetting a path that doesn't exist at all must be a silent no-op, real MongoDB's
                // own semantics -- proves the missing-intermediate-segment branch doesn't throw.
                coll.updateOne(new Document("_id", "u3"),
                        new Document("$unset", new Document("nope.nested", "")));

                coll.updateOne(new Document("_id", "u3"),
                        new Document("$unset", new Document("address.zip", "")));

                Document found = coll.find(new Document("_id", "u3")).first();
                Document address = (Document) found.get("address");
                assertEquals("Springfield", address.getString("city"));
                assertFalse(address.containsKey("zip"), "the unset nested field must be gone");
            }
        }
    }
}
