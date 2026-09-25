package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.List;
import java.util.regex.Pattern;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Real proof of real {@code $exists}/{@code $regex} filter operator support -- both near-
 * universal in real query filters (optional-field checks, partial text search), refused outright
 * before this fix.
 */
class MongoExistsAndRegexIntegrationTest {

    @Test
    void existsFiltersOnRealKeyPresenceNotValueNullness() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoClient client = MongoClients.create(
                    "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true");
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("exists_it");
            coll.insertMany(List.of(
                    new Document("_id", "e1").append("nickname", "Bob"),
                    new Document("_id", "e2").append("nickname", null),
                    new Document("_id", "e3")));

            List<Document> withField = coll.find(Filters.exists("nickname", true)).into(new java.util.ArrayList<>());
            assertEquals(2, withField.size(), "explicit null still counts as \"exists\" -- got: " + withField);

            List<Document> withoutField = coll.find(Filters.exists("nickname", false)).into(new java.util.ArrayList<>());
            assertEquals(1, withoutField.size());
            assertEquals("e3", withoutField.get(0).getString("_id"));
        }
    }

    @Test
    void regexFiltersByPartialCaseInsensitiveMatch() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoClient client = MongoClients.create(
                    "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true");
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("regex_it");
            coll.insertMany(List.of(
                    new Document("_id", "r1").append("name", "Alice Smith"),
                    new Document("_id", "r2").append("name", "bob jones"),
                    new Document("_id", "r3").append("name", "Carol Lee")));

            List<Document> matches = coll.find(Filters.regex("name",
                    Pattern.compile("smith", Pattern.CASE_INSENSITIVE))).into(new java.util.ArrayList<>());
            assertEquals(1, matches.size());
            assertEquals("r1", matches.get(0).getString("_id"));
        }
    }
}
