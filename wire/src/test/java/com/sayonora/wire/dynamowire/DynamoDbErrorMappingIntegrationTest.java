package com.sayonora.wire.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * End-to-end proof that a real DynamoDB client (AWS SDK v2, real SigV4 request signing and JSON
 * parsing -- not a hand-written HTTP check) gets genuine DynamoDB service exceptions out of a real
 * Postgres backend failure, via {@link DynamoDbErrorMapper}, the same discipline used for orawire/
 * mywire/mssqlwire/pgwire's SQLSTATE translation. Real subprocess, real Postgres container, no
 * mocks -- the AWS SDK's own exception classes (e.g. {@code ResourceNotFoundException}) are what
 * get asserted on, proving the client library itself recognizes the response as that real error
 * type, not just that our own JSON string happens to contain the right substring.
 */
class DynamoDbErrorMappingIntegrationTest {

    private static DynamoDbClient client(int port) {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                // The SDK's default retry policy auto-retries a real InternalServerError (correct
                // real-world behavior), but after exhausting retries it wraps everything into a
                // generic DynamoDbException rather than surfacing the specific exception subtype
                // from the actual last attempt -- disabled here so what the test asserts on is the
                // real, direct response to a single request, not retry-exhaustion behavior.
                .overrideConfiguration(o -> o.retryPolicy(
                        software.amazon.awssdk.core.retry.RetryPolicy.builder().numRetries(0).build()))
                .build();
    }

    /** The table's catalog metadata ({@code _dynamo_tables}) still says the table exists, but the
     * real underlying Postgres table was dropped out from under dynamowire directly -- exactly
     * the shape of failure {@code describeTable}'s catalog-only check can't catch in advance, so
     * this genuinely reaches a real Postgres {@code 42P01 undefined_table} SQLException, not an
     * app-level pre-validation. Proves the client gets a real {@code ResourceNotFoundException},
     * not the generic {@code InternalFailure} the un-mapped path used to return. */
    @Test
    void aTableDroppedUnderneathDynamowireReturnsARealResourceNotFoundException() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = client(warp.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("orphaned_table")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());

                // Drop the REAL underlying table directly against Postgres, bypassing dynamowire
                // entirely -- _dynamo_tables (the catalog dynamowire's own describeTable checks)
                // still lists "orphaned_table" as present.
                try (Connection admin = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                        Statement st = admin.createStatement()) {
                    st.execute("SELECT pg_table FROM _dynamo_tables WHERE table_name = 'orphaned_table'");
                    var rs = st.getResultSet();
                    rs.next();
                    String pgTableName = rs.getString(1);
                    st.execute("DROP TABLE \"" + pgTableName + "\"");
                }

                DynamoDbException thrown = assertThrows(ResourceNotFoundException.class,
                        () -> dynamo.getItem(GetItemRequest.builder()
                                .tableName("orphaned_table")
                                .key(java.util.Map.of("id", AttributeValue.builder().s("x").build()))
                                .build()),
                        "a GetItem against a table whose real Postgres table is gone must be a genuine "
                                + "ResourceNotFoundException, not the generic InternalFailure default");
                assertEquals(400, thrown.statusCode());
            }
        }
    }

    /** Same real-outage discipline as {@code BackendConnectionLostIntegrationTest} -- a genuinely
     * killed backend Postgres connection ({@link RealPostgres#stop()}, not a mock) must surface as
     * DynamoDB's own real {@code InternalServerError}, not the generic common-AWS default. */
    @Test
    void aGenuinelyDeadBackendConnectionReturnsARealInternalServerError() throws Exception {
        try (RealPostgres primary = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(primary.host(), primary.port(), primary.database(), primary.username(), primary.password())
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = client(warp.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("t")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());
                dynamo.putItem(PutItemRequest.builder().tableName("t")
                        .item(java.util.Map.of("id", AttributeValue.builder().s("warmup").build()))
                        .build());

                primary.stop();
                try {
                    DynamoDbException thrown = assertThrows(InternalServerErrorException.class,
                            () -> dynamo.getItem(GetItemRequest.builder()
                                    .tableName("t")
                                    .key(java.util.Map.of("id", AttributeValue.builder().s("warmup").build()))
                                    .build()),
                            "an operation against a genuinely dead backend connection must be a real "
                                    + "InternalServerError, not the generic InternalFailure default");
                    assertEquals(500, thrown.statusCode());
                } finally {
                    primary.resume();
                }
            }
        }
    }
}
