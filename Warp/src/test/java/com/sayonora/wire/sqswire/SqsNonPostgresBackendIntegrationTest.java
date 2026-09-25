package com.sayonora.wire.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sayonora.wire.testsupport.RealAzureSqlEdge;
import com.sayonora.wire.testsupport.RealMySql;
import com.sayonora.wire.testsupport.RealOracle;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Closes the same real, previously-disclosed gap {@code DynamowireNonPostgresBackendIntegrationTest}
 * closed for dynamowire: {@code sqswire_catalog.sql}/{@code sqswire_queue_table.sql} and {@link
 * SqswireDialect}'s own per-engine claim/upsert SQL have looked real and carefully written for a
 * while, but no test in this codebase ever ran them against anything but Postgres.
 * {@code WARP_BACKENDS=default=<url>|user|pass} overrides the registry's own {@code
 * DEFAULT_BACKEND_NAME} target to point sqswire's queue storage at a real Oracle/MySQL/SQL Server
 * instance instead of the config-primary Postgres.
 */
class SqsNonPostgresBackendIntegrationTest {

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

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

    private void sendAndReceiveRoundTrip(int port, String queueName) {
        try (SqsClient sqs = client(port)) {
            String queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();

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
            assertEquals("order-42 shipped", received.get(0).body());
        }
    }

    @Test
    void sendAndReceiveWorkAgainstARealOracleBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealOracle oracle = RealOracle.start()) {
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("sqswire", "WARP_SQSWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + oracle.jdbcUrl() + "|" + oracle.sysUsername() + "|" + oracle.sysPassword())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            sendAndReceiveRoundTrip(warp.port("sqswire"), "oracle_backed_queue");
        }
    }

    @Test
    void sendAndReceiveWorkAgainstARealMySqlBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealMySql mysql = RealMySql.start()) {
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("sqswire", "WARP_SQSWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + mysql.jdbcUrl() + "|" + mysql.username() + "|" + mysql.password())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            sendAndReceiveRoundTrip(warp.port("sqswire"), "mysql_backed_queue");
        }
    }

    @Test
    void sendAndReceiveWorkAgainstARealSqlServerBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealAzureSqlEdge mssql = RealAzureSqlEdge.start()) {
            String escapedUrl = mssql.masterJdbcUrl().replace(";", "%3B");
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("sqswire", "WARP_SQSWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + escapedUrl + "|" + mssql.username() + "|" + mssql.password())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            sendAndReceiveRoundTrip(warp.port("sqswire"), "sqlserver_backed_queue");
        }
    }
}
