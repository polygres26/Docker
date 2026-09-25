package com.sayonora.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a real mongo-java-driver client can authenticate via a genuine SCRAM-SHA-256
 * handshake -- from the "yes scope it" audit item: Mongo SCRAM auth was entirely unimplemented
 * (every connection was accepted unauthenticated, and any client configured with credentials at
 * all failed to connect). Uses CredentialStore's single-shared-credential fallback, which
 * WarpProcess#pgBackend already seeds from the real Postgres username/password.
 */
class MongoScramAuthIntegrationTest {

    private static RealPostgres postgres;
    private static WarpProcess warp;

    @org.junit.jupiter.api.BeforeAll
    static void startInfra() throws Exception {
        postgres = RealPostgres.start();
        warp = WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                .start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    @Test
    void correctCredentialsAuthenticateAndCanReadAndWrite() {
        MongoCredential credential = MongoCredential.createScramSha256Credential(
                postgres.username(), "admin", postgres.password().toCharArray());
        try (MongoClient client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyToClusterSettings(b -> b.hosts(List.of(new ServerAddress("localhost", warp.port("mongowire"))))
                        .mode(com.mongodb.connection.ClusterConnectionMode.SINGLE))
                .credential(credential)
                .build())) {
            MongoDatabase db = client.getDatabase("testdb");
            MongoCollection<Document> coll = db.getCollection("scram_auth_it");
            coll.insertOne(new Document("_id", 1).append("name", "authenticated"));
            Document found = coll.find(Filters.eq("_id", 1)).first();
            assertTrue(found != null, "the authenticated client must be able to read back what it wrote");
            assertEquals("authenticated", found.getString("name"));
        }
    }

    @Test
    void wrongPasswordFailsAuthenticationRatherThanSilentlyConnecting() {
        MongoCredential credential = MongoCredential.createScramSha256Credential(
                postgres.username(), "admin", "definitely-the-wrong-password".toCharArray());
        try (MongoClient client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyToClusterSettings(b -> b.hosts(List.of(new ServerAddress("localhost", warp.port("mongowire"))))
                        .mode(com.mongodb.connection.ClusterConnectionMode.SINGLE))
                .credential(credential)
                .build())) {
            MongoDatabase db = client.getDatabase("testdb");
            assertThrows(MongoSecurityException.class,
                    () -> db.getCollection("scram_auth_should_not_be_reached").insertOne(new Document("_id", 1)));
        }
    }
}
