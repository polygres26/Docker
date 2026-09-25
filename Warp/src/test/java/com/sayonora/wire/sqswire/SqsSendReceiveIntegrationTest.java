package com.sayonora.wire.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Both existing sqswire tests ({@code SqsErrorMappingIntegrationTest}, {@code
 * PgQueueStoreDrainRoutingTest}) only cover error/routing paths -- neither proves the basic
 * happy-path round trip (send a real message, receive it back, delete it) works at all against a
 * real backend. Real AWS SDK v2 client, real SigV4 signing, real Postgres-backed queue table
 * (see {@code PgQueueStore}) -- no mocks.
 */
class SqsSendReceiveIntegrationTest {

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

    @Test
    void aSentMessageIsReceivedAndCanBeDeleted() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("sqswire", "WARP_SQSWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (SqsClient sqs = client(warp.port("sqswire"))) {
                String queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName("real_orders_queue").build())
                        .queueUrl();

                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody("order-42 shipped")
                        .build());

                List<Message> received = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(2)
                        .build()).messages();

                assertEquals(1, received.size(), "expected exactly the one real message sent");
                Message message = received.get(0);
                assertEquals("order-42 shipped", message.body());
                assertTrue(message.messageId() != null && !message.messageId().isBlank(),
                        "a real SQS response always carries a real message id");

                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());

                List<Message> afterDelete = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(1)
                        .build()).messages();
                assertTrue(afterDelete.isEmpty(), "the deleted message must not be received again");
            }
        }
    }
}
