package com.nexagres.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real MongoDB client (the official Java driver -- real OP_MSG wire
 * encoding and BSON error decoding, not a hand-written check) gets a genuine {@code DuplicateKey}
 * error out of a real Postgres backend failure, via {@link MongoErrorMapper}. Real subprocess,
 * real Postgres container, no mocks -- the FIRST real MongoDB client library any mongowire test
 * has been verified against; mongowire had no integration test at all before this (the existing
 * {@code org.mongodb:bson} dependency is explicitly BSON-codec-only, not the client driver).
 */
class MongoErrorMappingIntegrationTest {

    /** A duplicate {@code _id} insert is the one genuinely-triggerable unique-violation path a
     * real client can hit through mongowire's actual insert command -- {@code PostgresDocumentStore}
     * uses {@code _id} as the real Postgres primary key, so inserting the same {@code _id} twice
     * is a real {@code 23505 unique_violation}, not simulated. */
    @Test
    void aDuplicateIdInsertReturnsARealDuplicateKeyWriteError() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create("mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("dupe_it");
                coll.insertOne(new Document("_id", "x").append("field", "first"));

                MongoWriteException thrown = assertThrows(MongoWriteException.class,
                        () -> coll.insertOne(new Document("_id", "x").append("field", "second")),
                        "inserting a document with an _id that already exists must be a genuine "
                                + "DuplicateKey write error, not the generic UnknownError default");
                // com.mongodb.WriteError only exposes the numeric code (no getCodeName() on this
                // driver version) -- the codeName field's real presence on the wire is still
                // verified in the second test below, via MongoCommandException.getErrorCodeName().
                assertEquals(11000, thrown.getError().getCode());
            }
        }
    }

    /** Same real-outage discipline as the other three protocols' equivalent tests -- a genuinely
     * killed backend connection must surface as MongoDB's own real {@code ShutdownInProgress}
     * error, not the generic {@code UnknownError} default. */
    @Test
    void aGenuinelyDeadBackendConnectionReturnsARealShutdownInProgressError() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (MongoClient client = MongoClients.create("mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
                MongoCollection<Document> coll = client.getDatabase("test").getCollection("t");
                coll.insertOne(new Document("_id", "warmup").append("field", "x"));

                primary.stop();
                try {
                    // A find (not insertOne) here -- the outer dispatch() catch, not the per-doc
                    // insert writeErrors path, is what's under test on this side.
                    com.mongodb.MongoCommandException thrown = assertThrows(com.mongodb.MongoCommandException.class,
                            () -> coll.find().first(),
                            "a command against a genuinely dead backend connection must fail with a real error");
                    assertEquals(91, thrown.getErrorCode());
                    assertEquals("ShutdownInProgress", thrown.getErrorCodeName());
                } finally {
                    primary.resume();
                }
            }
        }
    }
}
