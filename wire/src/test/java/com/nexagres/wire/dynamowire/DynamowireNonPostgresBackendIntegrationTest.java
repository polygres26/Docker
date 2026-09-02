package com.nexagres.wire.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nexagres.wire.testsupport.RealAzureSqlEdge;
import com.nexagres.wire.testsupport.RealMySql;
import com.nexagres.wire.testsupport.RealOracle;
import com.nexagres.wire.testsupport.RealPostgres;
import com.nexagres.wire.testsupport.WarpProcess;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Closes a real, previously-disclosed gap: {@code PgItemStore}'s own {@code CREATE TABLE} DDL has
 * had real Oracle/SQL Server/MySQL variants in {@code src/main/resources/ddl/} for a while (see
 * {@link com.nexagres.wire.core.DdlTemplates}), but every dynamowire test in this codebase before
 * this one only ever ran against Postgres -- the DDL was real and carefully written, but
 * unverified against the engines it targets. {@code WARP_BACKENDS=default=<url>|user|pass}
 * overrides the registry's own {@code DEFAULT_BACKEND_NAME} target to point dynamowire's item
 * storage at a real Oracle/MySQL/SQL Server instance instead of the config-primary Postgres.
 */
class DynamowireNonPostgresBackendIntegrationTest {

    private RealPostgres postgres;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (postgres != null) postgres.close();
    }

    private static DynamoDbClient client(int port) {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(
                        software.amazon.awssdk.core.retry.RetryPolicy.builder().numRetries(0).build()))
                .build();
    }

    private void putAndGetRoundTrip(int port, String tableName) {
        try (DynamoDbClient dynamo = client(port)) {
            dynamo.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .build());

            dynamo.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of(
                            "id", AttributeValue.builder().s("row-1").build(),
                            "status", AttributeValue.builder().s("pending").build(),
                            "amount", AttributeValue.builder().n("99.5").build()))
                    .build());

            GetItemResponse got = dynamo.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("id", AttributeValue.builder().s("row-1").build()))
                    .build());
            assertEquals("pending", got.item().get("status").s(),
                    "the item written directly through PutItem must read back from the real non-Postgres backend");
            assertEquals("99.5", got.item().get("amount").n());
        }
    }

    /** Hash-and-range, not hash-only -- Oracle specifically has a real, already-disclosed gap
     * with a hash-only table (see ddl/oracle/dynamowire_item_table.sql's own comment: Oracle
     * treats an empty string as NULL, and a hash-only table's sk_value is written as ''), a
     * separate, deliberately-deferred issue from what this test is proving (the DDL/catalog/
     * upsert path this session's own testing found and fixed). A real hash+range table sidesteps
     * it entirely with a genuinely common, real DynamoDB usage shape. */
    private void putAndGetRoundTripWithSortKey(int port, String tableName) {
        try (DynamoDbClient dynamo = client(port)) {
            dynamo.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("ts").attributeType(ScalarAttributeType.N).build())
                    .keySchema(
                            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("ts").keyType(KeyType.RANGE).build())
                    .build());

            dynamo.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of(
                            "id", AttributeValue.builder().s("row-1").build(),
                            "ts", AttributeValue.builder().n("1").build(),
                            "status", AttributeValue.builder().s("pending").build()))
                    .build());

            GetItemResponse got = dynamo.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of(
                            "id", AttributeValue.builder().s("row-1").build(),
                            "ts", AttributeValue.builder().n("1").build()))
                    .build());
            assertEquals("pending", got.item().get("status").s(),
                    "the item written directly through PutItem must read back from the real non-Postgres backend");
        }
    }

    @Test
    void putAndGetItemWorkAgainstARealOracleBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealOracle oracle = RealOracle.start()) {
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + oracle.jdbcUrl() + "|" + oracle.sysUsername() + "|" + oracle.sysPassword())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            putAndGetRoundTripWithSortKey(warp.port("dynamowire"), "oracle_backed_table");
        }
    }

    @Test
    void putAndGetItemWorkAgainstARealMySqlBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealMySql mysql = RealMySql.start()) {
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + mysql.jdbcUrl() + "|" + mysql.username() + "|" + mysql.password())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            putAndGetRoundTrip(warp.port("dynamowire"), "mysql_backed_table");
        }
    }

    @Test
    void putAndGetItemWorkAgainstARealSqlServerBackend() throws Exception {
        postgres = RealPostgres.start();
        try (RealAzureSqlEdge mssql = RealAzureSqlEdge.start()) {
            // masterJdbcUrl() contains real semicolons of its own (;encrypt=false;...) which would
            // otherwise collide with WARP_BACKENDS' own ;-delimited entry separator -- the
            // existing %3B escape (BackendRegistry.fromConfig) is exactly for this.
            String escapedUrl = mssql.masterJdbcUrl().replace(";", "%3B");
            warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                    .env("WARP_BACKENDS", "default=" + escapedUrl + "|" + mssql.username() + "|" + mssql.password())
                    .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start();
            putAndGetRoundTrip(warp.port("dynamowire"), "sqlserver_backed_table");
        }
    }
}
