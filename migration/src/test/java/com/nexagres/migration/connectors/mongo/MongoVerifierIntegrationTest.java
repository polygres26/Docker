package com.nexagres.migration.connectors.mongo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.PolywireGrpcSink;
import com.nexagres.migration.testsupport.PolyWireProcess;
import com.nexagres.migration.testsupport.RealMongo;
import com.nexagres.migration.testsupport.RealPostgres;
import com.nexagres.migration.verify.VerificationResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 of this session's migration plan: real, not simulated, verification and lag reporting.
 * Runs a real migration end to end (real Mongo, real Polywire gRPC, real target Postgres), then
 * proves {@link MongoVerifier} correctly reports a clean match, correctly DETECTS a real
 * introduced mismatch (a target row hand-edited to differ from its source), and that {@link
 * CdcCheckpointStore}'s new {@code last_event_at} column is actually populated with a real,
 * recent, source-side event timestamp after a live change replicates.
 */
class MongoVerifierIntegrationTest {

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

    private static Long targetRowCount(RealPostgres postgres, String table) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM " + table);
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (java.sql.SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return null;
            }
            throw e;
        }
    }

    @Test
    void verifierMatchesAfterACleanMigrationAndDetectsAnIntroducedMismatch() throws Exception {
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient sourceClient = MongoClients.create(mongo.connectionString());
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "POLYWIRE_GRPC_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> source = sourceClient.getDatabase("src").getCollection("orders");
            for (int i = 0; i < 20; i++) {
                source.insertOne(new Document("_id", "order-" + i).append("amount", 10.0 + i));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            MongoSource migrationSource = new MongoSource(sourceClient, "src", "orders", "db", "orders", 3, "_id");
            PolywireGrpcSink sink = new PolywireGrpcSink("localhost", polywire.port("grpc"), postgres.username(), postgres.password());
            Coordinator coordinator = new Coordinator(migrationSource, sink, checkpoints, 3);
            Thread coordinatorThread = new Thread(() -> {
                try {
                    coordinator.run();
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-coordinator");
            coordinatorThread.start();
            try {
                waitUntil(Duration.ofSeconds(20), () -> {
                    Long count = targetRowCount(postgres, "\"db\".\"orders\"");
                    return count != null && count == 20;
                });

                // Proof #1: a clean, fully-replicated migration verifies as a match.
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password())) {
                    VerificationResult clean = MongoVerifier.verify(sourceClient, "src", "orders", targetConn, "db", "orders");
                    assertTrue(clean.matches(), "a clean migration should verify as matching: " + clean);
                }

                // Proof #2: hand-corrupt one target row (simulating a silent replication bug) and
                // confirm the verifier actually detects it -- not just a check that always passes.
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        PreparedStatement ps = targetConn.prepareStatement(
                                "UPDATE \"db\".\"orders\" SET doc = '{\"_id\":\"order-0\",\"amount\":999999.0}'::jsonb WHERE id = ?")) {
                    ps.setString(1, MongoBsonJson.valueToJson("order-0"));
                    int updated = ps.executeUpdate();
                    assertTrue(updated == 1, "the hand-corruption itself should have hit exactly the one row");
                }
                try (Connection targetConn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password())) {
                    VerificationResult corrupted = MongoVerifier.verify(sourceClient, "src", "orders", targetConn, "db", "orders");
                    assertFalse(corrupted.matches(), "an introduced mismatch must be DETECTED, not silently passed: " + corrupted);
                    assertTrue(corrupted.sourceCount() == corrupted.targetCount(),
                            "counts still match -- this must be a checksum-only mismatch, proving the checksum path itself catches it");
                }

                // Proof #3: a real live change's own source-side timestamp lands in last_event_at,
                // recent relative to now -- the actual lag signal Advisor's Data Sync report reads.
                source.insertOne(new Document("_id", "order-late").append("amount", 1.0));
                waitUntil(Duration.ofSeconds(15), () -> targetRowCount(postgres, "\"db\".\"orders\"") == 21);
                waitUntil(Duration.ofSeconds(10), () -> lastEventAgeSeconds(postgres, "mongo:src.orders") != null);
                Long ageSeconds = lastEventAgeSeconds(postgres, "mongo:src.orders");
                assertTrue(ageSeconds != null && ageSeconds < 30,
                        "a just-replicated event's last_event_at should be recent (within a wide margin for test-env clock skew), was: " + ageSeconds + "s");
            } finally {
                migrationSource.close();
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(10).toMillis());
                sink.close();
            }
        }
    }

    private static Long lastEventAgeSeconds(RealPostgres postgres, String sourceKey) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT EXTRACT(EPOCH FROM (now() - last_event_at))::bigint FROM polywire_cdc_checkpoints WHERE source_key = ?")) {
            ps.setString(1, sourceKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || rs.getObject(1) == null) {
                    return null;
                }
                return rs.getLong(1);
            }
        }
    }
}
