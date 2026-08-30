package com.nexagres.wire.mongowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.nexagres.wire.migration.CdcCheckpointStore;
import com.nexagres.wire.testsupport.RealMongo;
import com.nexagres.wire.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of {@link MongoChangeStreamCdcWorker} against a REAL source MongoDB (a genuine
 * single-node replica set, real change stream) and a REAL target Postgres, using {@link
 * PostgresDocumentStore} directly -- the exact same class mongowire itself uses to serve live
 * traffic, so there's no separate "migration" schema to drift from the real one. No mocks.
 *
 * <p>Three things proved, in order: (1) the initial snapshot plus the live stream both actually
 * land real inserts/updates/deletes in the target table; (2) stopping the worker and restarting a
 * NEW instance against the SAME checkpoint store resumes from the saved token rather than
 * re-running the snapshot; (3) a write that happens to the source WHILE the worker is stopped is
 * not lost -- it shows up once the new worker resumes, proving the resume token genuinely didn't
 * skip ahead of it.
 */
class MongoChangeStreamCdcWorkerIntegrationTest {

    private static String targetDocJson(RealPostgres postgres, String idJson) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT doc FROM \"db\".\"orders\" WHERE id = ?")) {
            ps.setString(1, idJson);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static void waitUntil(Duration timeout, java.util.concurrent.Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(150);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    @Test
    void snapshotAndLiveChangesReplicateAndSurviveARestart() throws Exception {
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient sourceClient = MongoClients.create(mongo.connectionString())) {

            MongoCollection<Document> source = sourceClient.getDatabase("src").getCollection("orders");
            source.insertOne(new Document("_id", "order-1").append("amount", 129.99));
            source.insertOne(new Document("_id", "order-2").append("amount", 44.50));

            PostgresDocumentStore targetStore = new PostgresDocumentStore(
                    () -> DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password()));
            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            MongoChangeStreamCdcWorker worker = new MongoChangeStreamCdcWorker(
                    sourceClient, "src", "orders", targetStore, "db", "orders", checkpoints);
            worker.start();
            try {
                // Proof #1a: the initial snapshot copies pre-existing documents.
                waitUntil(Duration.ofSeconds(15), () -> worker.snapshotCopied() == 2);
                String order1Json = targetDocJson(postgres, PostgresDocumentStore.idJsonFor("order-1"));
                assertTrue(order1Json != null && order1Json.contains("129.99"),
                        "expected the snapshot to have copied order-1 -- got: " + order1Json);

                // Proof #1b: a real insert after the snapshot replicates via the live stream.
                source.insertOne(new Document("_id", "order-3").append("amount", 9.99));
                waitUntil(Duration.ofSeconds(15),
                        () -> targetDocJson(postgres, PostgresDocumentStore.idJsonFor("order-3")) != null);

                // Proof #1c: a real update replicates too.
                source.updateOne(new Document("_id", "order-1"), new Document("$set", new Document("amount", 200.00)));
                waitUntil(Duration.ofSeconds(15), () -> {
                    String json = targetDocJson(postgres, PostgresDocumentStore.idJsonFor("order-1"));
                    return json != null && json.contains("200.0");
                });

                // Proof #1d: a real delete replicates too.
                source.deleteOne(new Document("_id", "order-2"));
                waitUntil(Duration.ofSeconds(15),
                        () -> targetDocJson(postgres, PostgresDocumentStore.idJsonFor("order-2")) == null);

                assertTrue(worker.eventsApplied() >= 3, "expected at least the insert/update/delete counted -- got: " + worker.eventsApplied());
                assertEquals(null, worker.failure(), "the worker must not have died from an unexpected exception");
            } finally {
                worker.stop();
            }

            // Simulate real downtime: a write to the source WHILE no worker is running at all.
            source.insertOne(new Document("_id", "order-4").append("amount", 77.00));

            // Proof #2 + #3: a brand-new worker instance, same checkpoint store, resumes from the
            // saved token (no second snapshot) and still picks up the write that happened while
            // stopped -- the resume token genuinely didn't skip ahead of it.
            MongoChangeStreamCdcWorker resumedWorker = new MongoChangeStreamCdcWorker(
                    sourceClient, "src", "orders", targetStore, "db", "orders", checkpoints);
            resumedWorker.start();
            try {
                waitUntil(Duration.ofSeconds(15),
                        () -> targetDocJson(postgres, PostgresDocumentStore.idJsonFor("order-4")) != null);
                assertEquals(0, resumedWorker.snapshotCopied(),
                        "resuming from a checkpoint must not re-run the initial snapshot");
            } finally {
                resumedWorker.stop();
            }
        }
    }
}
