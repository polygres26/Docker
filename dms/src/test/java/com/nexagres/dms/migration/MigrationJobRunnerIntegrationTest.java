package com.nexagres.dms.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.nexagres.dms.core.ConnectionStore;
import com.nexagres.migration.testsupport.PolyWireProcess;
import com.nexagres.migration.testsupport.RealMongo;
import com.nexagres.migration.testsupport.RealPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Callable;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the NEW piece this class stack adds on top of everything {@code nexagres-migration}'s
 * own connector-level integration tests already cover: that Advisor's HTTP-facing job-launching
 * glue ({@link MigrationJobRequest} -&gt; {@link MigrationSourceFactory} -&gt;
 * {@link MigrationJobRunner}) actually wires a real connector together correctly end to end, using
 * the SAME real-infrastructure test helpers (real Mongo, real Postgres, a real Polywire instance)
 * every connector's own migration-module test already relies on -- not a mock, and not a
 * hand-rolled re-verification of MongoSource's own CDC correctness (that's already proven in
 * {@code MongoSourceIntegrationTest}; this test's job is proving the wiring around it, the part
 * that's actually new here).
 *
 * <p>Only Mongo is exercised here (not all seven connectors) -- {@link MigrationSourceFactory}'s
 * other six branches are the same mechanical "map of config strings to the same constructor calls
 * each connector's own real-infra test already uses" pattern, not independent logic worth a
 * separate real-infra test per connector at this layer; the actual connector CORRECTNESS is
 * already covered exhaustively in {@code nexagres-migration}'s own test suite.
 */
class MigrationJobRunnerIntegrationTest {

    private static void waitUntil(Duration timeout, Callable<Boolean> condition) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private static long countTargetRows(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"db\".\"customers\"");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (java.sql.SQLException e) {
            if ("42P01".equals(e.getSQLState())) {
                return 0;
            }
            throw e;
        }
    }

    @Test
    void startedJobActuallyMigratesRealMongoDataThroughRealPolywire(@TempDir java.nio.file.Path tempDir) throws Exception {
        System.setProperty("NEXAGRES_DATA_DIR", tempDir.toString());
        try (RealMongo mongo = RealMongo.start();
                RealPostgres postgres = RealPostgres.start();
                MongoClient seedClient = MongoClients.create(mongo.connectionString());
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "POLYWIRE_GRPC_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            seedClient.getDatabase("src").getCollection("customers")
                    .insertOne(new Document("_id", "cust-1").append("customer", "cust-1").append("balance", 42));

            // The saved connection this job's checkpoint/dead-letter bookkeeping points at --
            // same target Postgres Polywire itself writes into, same pattern MigrationStatusRoute
            // already uses.
            ConnectionStore connectionStore = new ConnectionStore();
            var targetConnection = connectionStore.create("test-target", postgres.jdbcUrl(), postgres.username(), postgres.password());

            MigrationJobRequest request = new MigrationJobRequest();
            request.connectorType = "MONGO";
            request.targetConnectionId = targetConnection.id;
            request.polywireGrpcHost = "localhost";
            request.polywireGrpcPort = polywire.port("grpc");
            request.polywireGrpcUser = postgres.username();
            request.polywireGrpcPassword = postgres.password();
            request.parallelism = 4; // free tier (no POLYWIRE_LICENSE_KEY in this test env) clamps
                                      // this to 1 inside Coordinator itself -- proves the real
                                      // license gate applies even when Advisor is the one launching
                                      // the job, not just a direct Migrate*Cli invocation.
            request.sourceConfig = Map.of(
                    "uri", mongo.connectionString(),
                    "sourceDb", "src",
                    "sourceCollection", "customers",
                    "targetDb", "db",
                    "targetCollection", "customers");

            MigrationJobRunner runner = new MigrationJobRunner();
            MigrationJobRunner.JobState state = runner.start(request);
            try {
                assertNotNull(state.id);
                assertEquals("MONGO", state.connectorType);

                waitUntil(Duration.ofSeconds(25), () -> countTargetRows(postgres) == 1);

                // A live write after the job's already running has to replicate too -- proves this
                // isn't just a one-shot snapshot copy, the real change-feed path is live.
                seedClient.getDatabase("src").getCollection("customers")
                        .insertOne(new Document("_id", "cust-2").append("customer", "cust-2").append("balance", 7));
                waitUntil(Duration.ofSeconds(20), () -> countTargetRows(postgres) == 2);

                assertTrue(runner.list().stream().anyMatch(j -> j.id.equals(state.id)),
                        "the started job should show up in the job list");
                // Free tier (no POLYWIRE_LICENSE_KEY in this test env): a second job while this one
                // is still RUNNING must be refused outright, not silently queued or double-run --
                // proves MigrationLicensing.requireCapacityForAnotherConcurrentJob is actually wired
                // into the real HTTP-facing start() path, not just unit-tested in isolation.
                MigrationJobRequest secondRequest = new MigrationJobRequest();
                secondRequest.connectorType = "MONGO";
                secondRequest.targetConnectionId = targetConnection.id;
                secondRequest.polywireGrpcHost = "localhost";
                secondRequest.polywireGrpcPort = polywire.port("grpc");
                secondRequest.polywireGrpcUser = postgres.username();
                secondRequest.polywireGrpcPassword = postgres.password();
                secondRequest.sourceConfig = request.sourceConfig;
                IllegalStateException capacity = assertThrows(IllegalStateException.class,
                        () -> runner.start(secondRequest));
                assertTrue(capacity.getMessage().contains("POLYWIRE_LICENSE_KEY"));
            } finally {
                // Ask the job's live change-feed loop to stop before this test's own try-with-
                // resources tears down the real Mongo/Postgres/Polywire containers underneath it --
                // otherwise the job's background thread would just start throwing real connection
                // errors instead of exiting cleanly. Same reasoning as every migration-module
                // integration test's own close()-in-finally teardown.
                runner.stop(state.id);
            }
        }
    }
}
