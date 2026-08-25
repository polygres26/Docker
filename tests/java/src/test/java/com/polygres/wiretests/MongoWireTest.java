package com.polygres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.UUID;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

/** mongowire: real MongoDB wire protocol, real MongoDB Java driver, translated to Postgres
 * underneath. Covers find/insert/update/delete only -- mongowire doesn't implement the
 * aggregation pipeline, so this test doesn't reach for one. */
class MongoWireTest {

    private static MongoClient client() {
        return MongoClients.create("mongodb://" + TestConfig.HOST + ":" + TestConfig.MONGOWIRE_PORT);
    }

    @Test
    void insertAndFindById() {
        try (MongoClient c = client()) {
            MongoDatabase db = c.getDatabase("smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8));
            MongoCollection<Document> coll = db.getCollection("widgets");
            Document doc = new Document("name", "polywire");
            coll.insertOne(doc);
            ObjectId id = doc.getObjectId("_id");
            Document found = coll.find(new Document("_id", id)).first();
            assertEquals("polywire", found.getString("name"));
        }
    }

    @Test
    void updateAndDelete() {
        try (MongoClient c = client()) {
            MongoDatabase db = c.getDatabase("smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8));
            MongoCollection<Document> coll = db.getCollection("widgets");
            Document doc = new Document("name", "before");
            coll.insertOne(doc);
            ObjectId id = doc.getObjectId("_id");
            coll.updateOne(new Document("_id", id), new Document("$set", new Document("name", "after")));
            assertEquals("after", coll.find(new Document("_id", id)).first().getString("name"));
            coll.deleteOne(new Document("_id", id));
            assertNull(coll.find(new Document("_id", id)).first());
        }
    }
}
