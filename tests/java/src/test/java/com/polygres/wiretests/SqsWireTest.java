package com.polygres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** sqswire: real Amazon SQS HTTP/JSON API, real AWS SDK v2 client, translated to Postgres
 * underneath (pgmq-style storage -- no {@code pgmq} extension needed). Dummy static credentials
 * are enough since AWS SigV4 request verification is opt-in. */
class SqsWireTest {

    private static SqsClient client() {
        return SqsClient.builder()
                .endpointOverride(URI.create("http://" + TestConfig.HOST + ":" + TestConfig.SQSWIRE_PORT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Test
    void createSendReceiveDelete() {
        SqsClient c = client();
        String queueName = "smoke-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String queueUrl = c.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();
        try {
            c.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("hello from polywire").build());
            List<Message> messages = c.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(2).build()).messages();
            assertEquals(1, messages.size());
            assertEquals("hello from polywire", messages.get(0).body());
            c.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl).receiptHandle(messages.get(0).receiptHandle()).build());
        } finally {
            c.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
    }
}
