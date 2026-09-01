package com.nexagres.migration.connectors.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.WarpGrpcSink;
import com.nexagres.migration.testsupport.WarpProcess;
import com.nexagres.migration.testsupport.RealPostgres;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * End-to-end proof, real infrastructure throughout, using this session's own suggested testing
 * approach: since sqswire is API-compatible with real AWS SQS, a real running Warp instance
 * fronting sqswire stands in as a genuine SQS-API SOURCE -- no real AWS account/credentials
 * needed, and it's the exact same client code a real migration would use against real AWS. TWO
 * separate Warp instances are used, exactly like migrating between two real, independent
 * systems: a "source" instance (sqswire only, its own backend Postgres) and a "target" instance
 * (both grpc, for {@link WarpGrpcSink}'s real write path, AND sqswire, so the migrated queue
 * can be read back through a real SQS API round-trip -- the strongest possible proof this
 * connector actually produces a working, live-queryable queue, not just correct-looking rows).
 */
class SqsSourceIntegrationTest {

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

    private static SqsClient sqsClientFor(int port) {
        return SqsClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build()))
                .build();
    }

    private static Long targetRowCount(RealPostgres postgres) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM \"sqs_queue_orders\"");
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
    void backlogAndLiveMessagesReplicateAndAreRetrievableThroughARealSqsRoundTrip() throws Exception {
        try (RealPostgres sourcePostgres = RealPostgres.start();
                // Every wire protocol Warp supports binds its own fixed-default-port listener
                // regardless of which frontends are explicitly requested (confirmed live: running
                // a second Warp process alongside the first, without this, fails with real
                // BindException/Address-already-in-use on every OTHER protocol's shared default
                // port) -- this test is the first in this session to run two Warp processes
                // at once, so every listener neither side actually needs still has to be given
                // its own free port explicitly.
                WarpProcess sourceWarp = WarpProcess.builder()
                        .pgBackend(sourcePostgres.host(), sourcePostgres.port(), sourcePostgres.database(), sourcePostgres.username(), sourcePostgres.password())
                        .frontend("sqswire", "WARP_SQSWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .frontend("boltwire", "WARP_BOLTWIRE_PORT")
                        .frontend("orawire", "WARP_ORAWIRE_PORT")
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .frontend("oswire", "WARP_OSWIRE_PORT")
                        .frontend("influxwire", "WARP_INFLUXWIRE_PORT")
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start();
                RealPostgres targetPostgres = RealPostgres.start();
                WarpProcess targetWarp = WarpProcess.builder()
                        .pgBackend(targetPostgres.host(), targetPostgres.port(), targetPostgres.database(), targetPostgres.username(), targetPostgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .frontend("sqswire", "WARP_SQSWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .frontend("mywire", "WARP_MYWIRE_PORT")
                        .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                        .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                        .frontend("boltwire", "WARP_BOLTWIRE_PORT")
                        .frontend("orawire", "WARP_ORAWIRE_PORT")
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .frontend("oswire", "WARP_OSWIRE_PORT")
                        .frontend("influxwire", "WARP_INFLUXWIRE_PORT")
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start();
                SqsClient sourceSqs = sqsClientFor(sourceWarp.port("sqswire"))) {

            String sourceQueueUrl = sourceSqs.createQueue(r -> r.queueName("orders")).queueUrl();
            List<String> backlogBodies = List.of("order-A", "order-B", "order-C", "order-D", "order-E",
                    "order-F", "order-G", "order-H", "order-I", "order-J");
            for (String body : backlogBodies) {
                sourceSqs.sendMessage(r -> r.queueUrl(sourceQueueUrl).messageBody(body));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
            checkpoints.ensureSchema();

            SqsSource source = new SqsSource(sourceSqs, sourceQueueUrl, "orders");
            WarpGrpcSink sink = new WarpGrpcSink("localhost", targetWarp.port("grpc"), targetPostgres.username(), targetPostgres.password());
            Coordinator coordinator = new Coordinator(source, sink, checkpoints, 1);
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
                // Proof #1: the pre-existing backlog (10 messages sent before migration started)
                // fully replicates.
                waitUntil(Duration.ofSeconds(30), () -> {
                    Long count = targetRowCount(targetPostgres);
                    return count != null && count == 10;
                });

                // Proof #2: a LIVE message, sent after the coordinator is already running, also
                // replicates (the "streamChanges" half, not just the initial backlog drain).
                sourceSqs.sendMessage(r -> r.queueUrl(sourceQueueUrl).messageBody("live-order"));
                waitUntil(Duration.ofSeconds(30), () -> {
                    Long count = targetRowCount(targetPostgres);
                    return count != null && count == 11;
                });

                // Proof #3: every real body landed correctly, and the source queue was actually
                // drained (messages deleted after successful migration -- a real SQS ReceiveMessage
                // against the SOURCE now returns nothing).
                Set<String> targetBodies;
                try (Connection conn = DriverManager.getConnection(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
                        PreparedStatement ps = conn.prepareStatement("SELECT body FROM \"sqs_queue_orders\"");
                        ResultSet rs = ps.executeQuery()) {
                    targetBodies = new java.util.HashSet<>();
                    while (rs.next()) {
                        targetBodies.add(rs.getString(1));
                    }
                }
                assertTrue(targetBodies.containsAll(backlogBodies), "every backlog message body should have replicated: " + targetBodies);
                assertTrue(targetBodies.contains("live-order"), "the live message body should have replicated: " + targetBodies);

                var remainingOnSource = sourceSqs.receiveMessage(r -> r.queueUrl(sourceQueueUrl).waitTimeSeconds(1)).messages();
                assertTrue(remainingOnSource.isEmpty(), "the source queue should be fully drained after migration: " + remainingOnSource);

                // Proof #4: the catalog row landed correctly.
                try (Connection conn = DriverManager.getConnection(targetPostgres.jdbcUrl(), targetPostgres.username(), targetPostgres.password());
                        PreparedStatement ps = conn.prepareStatement("SELECT visibility_timeout, is_fifo FROM sqs_queues_catalog WHERE queue_name = 'orders'");
                        ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(30, rs.getInt("visibility_timeout"));
                    assertEquals(false, rs.getBoolean("is_fifo"));
                }

                // Proof #5, the strongest one: a REAL SQS client, speaking the REAL SQS API,
                // against the TARGET Warp instance's own sqswire frontend, can actually
                // receive the migrated messages -- not just "the rows look right in Postgres," a
                // genuine round-trip through both wire protocols.
                try (SqsClient targetSqs = sqsClientFor(targetWarp.port("sqswire"))) {
                    String targetQueueUrl = targetSqs.getQueueUrl(r -> r.queueName("orders")).queueUrl();
                    Set<String> receivedViaRealSqsApi = new java.util.HashSet<>();
                    waitUntil(Duration.ofSeconds(15), () -> {
                        var messages = targetSqs.receiveMessage(r -> r.queueUrl(targetQueueUrl).maxNumberOfMessages(10).waitTimeSeconds(1)).messages();
                        messages.forEach(m -> receivedViaRealSqsApi.add(m.body()));
                        return receivedViaRealSqsApi.size() >= 11;
                    });
                    assertTrue(receivedViaRealSqsApi.containsAll(backlogBodies) && receivedViaRealSqsApi.contains("live-order"),
                            "every migrated message should be retrievable through a REAL SQS ReceiveMessage call "
                                    + "against the target, not just present as raw Postgres rows: " + receivedViaRealSqsApi);
                }
            } finally {
                source.close();
                coordinatorThread.interrupt();
                coordinatorThread.join(Duration.ofSeconds(25).toMillis());
                sink.close();
            }
        }
    }
}
