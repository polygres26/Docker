package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Real proof of real {@code $or}/{@code $and}/{@code $nor} filter support -- an extremely common
 * real filter shape ("status = active OR priority = high"), previously refused outright.
 */
class MongoLogicalOperatorIntegrationTest {

    @Test
    void orAndAndAndNorAllFilterCorrectly() throws Exception {
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
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("logic_it");
            coll.insertMany(List.of(
                    new Document("_id", "t1").append("status", "active").append("priority", "low"),
                    new Document("_id", "t2").append("status", "closed").append("priority", "high"),
                    new Document("_id", "t3").append("status", "closed").append("priority", "low")));

            // $or: active OR high-priority -> t1, t2
            List<Document> orResult = coll.find(Filters.or(
                    Filters.eq("status", "active"), Filters.eq("priority", "high")))
                    .into(new java.util.ArrayList<>());
            assertEquals(2, orResult.size());

            // $and: closed AND high-priority -> t2 only
            List<Document> andResult = coll.find(Filters.and(
                    Filters.eq("status", "closed"), Filters.eq("priority", "high")))
                    .into(new java.util.ArrayList<>());
            assertEquals(1, andResult.size());
            assertEquals("t2", andResult.get(0).getString("_id"));

            // $nor: neither active NOR high-priority -> t3 only
            List<Document> norResult = coll.find(Filters.nor(
                    Filters.eq("status", "active"), Filters.eq("priority", "high")))
                    .into(new java.util.ArrayList<>());
            assertEquals(1, norResult.size());
            assertEquals("t3", norResult.get(0).getString("_id"));
        }
    }
}
