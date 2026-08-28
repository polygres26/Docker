package com.nexagres.wire.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * End-to-end proof that a real SQS client (AWS SDK v2, real SigV4 signing -- not a hand-written
 * HTTP check) gets a genuine {@code QueueDoesNotExistException} out of a real Postgres backend
 * failure, via {@link SqsErrorMapper}. Real subprocess, real Postgres container, no mocks.
 *
 * <p>The exact wire-level {@code __type} string this exercises (the short Smithy shape name, NOT
 * the legacy dotted form documentation initially suggested -- see {@link SqsErrorMapper}'s
 * javadoc) was determined empirically against this real SDK version, not assumed: the dotted form
 * looked plausible from AWS's own docs and a filed AWS-SDK GitHub issue, but turned out to
 * describe a real AWS-side bug that breaks client-side unmarshalling, not the working case.
 */
class SqsErrorMappingIntegrationTest {

    private static SqsClient client(int port) {
        return SqsClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(
                        software.amazon.awssdk.core.retry.RetryPolicy.builder().numRetries(0).build()))
                .build();
    }

    /** The queue's catalog metadata ({@code _sqs_queues}) still lists the queue, but its real
     * backing Postgres table ({@code sqs_queue_<name>}, see {@code PgQueueStore.safeTableName})
     * was dropped directly against Postgres, bypassing sqswire entirely -- a genuine, unexpected
     * {@code 42P01 undefined_table}, not an app-level pre-validation. */
    @Test
    void aQueueWhoseTableWasDroppedUnderneathSqswireReturnsARealQueueDoesNotExistException() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("sqswire", "POLYWIRE_SQSWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (SqsClient sqs = client(polywire.port("sqswire"))) {
                String queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName("orphaned_queue").build())
                        .queueUrl();

                // Drop the REAL underlying table directly against Postgres -- _sqs_queues (the
                // catalog sqswire's own lookups check) still lists "orphaned_queue" as present.
                try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        Statement st = admin.createStatement()) {
                    st.execute("DROP TABLE sqs_queue_orphaned_queue");
                }

                SqsException thrown = assertThrows(QueueDoesNotExistException.class,
                        () -> sqs.sendMessage(SendMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .messageBody("hello")
                                .build()),
                        "a SendMessage against a queue whose real Postgres table is gone must be a genuine "
                                + "QueueDoesNotExistException, not the generic InternalError default");
                assertEquals(400, thrown.statusCode());
            }
        }
    }

    /** Same real-outage discipline as {@code BackendConnectionLostIntegrationTest}/{@code
     * DynamoDbErrorMappingIntegrationTest} -- SQS has no more specific real error name than the
     * common-AWS {@code InternalError} for a genuinely dead backend connection, so this just
     * confirms the client still gets a clean 500 rather than a hang or a malformed response. */
    @Test
    void aGenuinelyDeadBackendConnectionReturnsACleanInternalError() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("sqswire", "POLYWIRE_SQSWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (SqsClient sqs = client(polywire.port("sqswire"))) {
                String queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName("q").build()).queueUrl();
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("warmup").build());

                primary.stop();
                try {
                    SqsException thrown = assertThrows(SqsException.class,
                            () -> sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("x").build()),
                            "an operation against a genuinely dead backend connection must fail cleanly");
                    assertEquals(500, thrown.statusCode());
                } finally {
                    primary.resume();
                }
            }
        }
    }
}
