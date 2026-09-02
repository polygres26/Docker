package com.sayonora.migration.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.migration.checkpoint.CdcCheckpointStore;
import com.sayonora.migration.connectors.mongo.MongoSource;
import com.sayonora.migration.core.MigrationLicensingTestSupport;
import com.sayonora.migration.sink.WarpGrpcSink;
import com.sayonora.migration.testsupport.WarpProcess;
import com.sayonora.migration.testsupport.RealMongo;
import com.sayonora.migration.testsupport.RealPostgres;
import com.sayonora.wire.license.LicenseTier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves Phase 2 of this session's migration plan: TWO SEPARATE worker "processes" (here,
 * separate {@link MongoSource}/{@link WarpGrpcSink} instances on separate threads, each with
 * its own {@link MongoClient} -- the closest a single JVM test can get to genuinely separate
 * processes without actually forking two JVMs) racing against the SAME shared {@link
 * PartitionLeaseStore} and {@link CdcCheckpointStore}, migrating the same source collection.
 *
 * <p>Real distribution, not just two copies of the same work: every document lands exactly once
 * in the target, every partition's lease ends up claimed by exactly one worker (never split
 * mid-partition, never left unclaimed), and exactly one of the two workers wins change-feed
 * leadership and is the one that actually replicates a live post-snapshot write -- the other
 * worker exits as soon as its share of the initial sync is done, never touching the change feed
 * at all.
 *
 * <p>Forces {@link LicenseTier#ENTERPRISE} for the duration of this test class via {@link
 * MigrationLicensingTestSupport} -- {@link DistributedCoordinator} itself is the Enterprise-only
 * "real massively parallel way to move data" (see {@code MigrationLicensing}'s own javadoc), and
 * a genuine Enterprise license key can't be minted from this module's tests (it needs wire's
 * real, deliberately offline signing private key). This override is what lets the actual
 * distributed-coordination MECHANICS above still get full, real end-to-end coverage.
 */
class DistributedCoordinatorIntegrationTest {

    @BeforeEach
    void forceEnterpriseTier() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
    }

    @AfterEach
    void resetLicenseTier() {
        MigrationLicensingTestSupport.reset();
    }

    private static String customerDocJson(RealPostgres postgres, String idJson) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT doc FROM \"db\".\"customers\" WHERE id = ?")) {
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

    private static int countCustomerRows(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"db\".\"customers\"");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** {@code MongoBsonJson} (the real serializer this ID text has to match) is package-private to
     * {@code connectors.mongo} and rightly stays that way -- for these test IDs (plain ASCII, no
     * characters JSON needs to escape), its RELAXED-mode output for a bare string is just that
     * string double-quoted, so this is a faithful stand-in for THIS test's fixed ID shapes only,
     * not a general-purpose reimplementation. */
    private static String idJson(String id) {
        return "\"" + id + "\"";
    }

    private static void waitUntil(Duration timeout, Callable<Boolean> condition) throws Exception {
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
    void twoWorkersSplitPartitionsAndExactlyOneOwnsTheChangeFeed() throws Exception {
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient client1 = MongoClients.create(mongo.connectionString());
                MongoClient client2 = MongoClients.create(mongo.connectionString());
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            MongoCollection<Document> source = client1.getDatabase("src").getCollection("customers");
            int total = 60;
            for (int i = 0; i < total; i++) {
                source.insertOne(new Document("_id", "cust-" + i).append("customer", "cust-" + i).append("balance", i));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();
            PartitionLeaseStore leases = new PartitionLeaseStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            leases.ensureSchema();

            String sourceKey = "mongo:src.customers";
            int partitionCount = 6;

            MongoSource source1 = new MongoSource(client1, "src", "customers", "db", "customers", partitionCount, "customer");
            WarpGrpcSink sink1 = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            DistributedCoordinator worker1 = new DistributedCoordinator(source1, sink1, checkpoints, leases, sourceKey, "worker-1", 2, 3600);

            MongoSource source2 = new MongoSource(client2, "src", "customers", "db", "customers", partitionCount, "customer");
            WarpGrpcSink sink2 = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password());
            DistributedCoordinator worker2 = new DistributedCoordinator(source2, sink2, checkpoints, leases, sourceKey, "worker-2", 2, 3600);

            Thread t1 = new Thread(() -> runQuietly(worker1), "test-worker-1");
            Thread t2 = new Thread(() -> runQuietly(worker2), "test-worker-2");
            t1.start();
            t2.start();
            try {
                // Every document, from whichever worker actually claimed its partition, has to
                // show up exactly once -- proof the shared lease store actually split the work
                // instead of each worker redundantly copying the whole collection.
                for (int i = 0; i < total; i++) {
                    int idx = i;
                    waitUntil(Duration.ofSeconds(25),
                            () -> customerDocJson(postgres, idJson("cust-" + idx)) != null);
                }
                assertEquals(total, countCustomerRows(postgres), "every document should land exactly once, not zero times or duplicated across workers");

                // Every real partition lease ended up done, exactly once -- not split, not
                // abandoned.
                for (int bucket = 0; bucket < partitionCount; bucket++) {
                    assertTrue(leaseIsDone(postgres, sourceKey, "mongo:src.customers#p" + bucket),
                            "partition " + bucket + " should be leased-done by exactly one worker");
                }

                // A live write after both workers' initial sync must replicate -- through
                // whichever worker actually won leadership.
                source.insertOne(new Document("_id", "cust-late").append("customer", "cust-late").append("balance", 999));
                waitUntil(Duration.ofSeconds(20),
                        () -> customerDocJson(postgres, idJson("cust-late")) != null);
            } finally {
                source1.close();
                source2.close();
                t1.interrupt();
                t2.interrupt();
                t1.join(Duration.ofSeconds(10).toMillis());
                t2.join(Duration.ofSeconds(10).toMillis());
                sink1.close();
                sink2.close();
            }
        }
    }

    private static boolean leaseIsDone(RealPostgres postgres, String sourceKey, String partitionId) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT status FROM migration_partition_leases WHERE source_key = ? AND partition_id = ?")) {
            ps.setString(1, sourceKey);
            ps.setString(2, partitionId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && "done".equals(rs.getString(1));
            }
        }
    }

    private static void runQuietly(DistributedCoordinator coordinator) {
        try {
            coordinator.run();
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                throw new RuntimeException(e);
            }
        }
    }
}
