package com.nexagres.wire.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchExecuteStatementResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * End-to-end proof that dynamowire's PartiQL subset (ExecuteStatement, BatchExecuteStatement --
 * see {@link PartiQlParser}'s own javadoc for exactly what shape is and isn't supported) actually
 * works against a real Postgres backend, driven by the real AWS SDK v2 {@code DynamoDbClient} --
 * not a hand-written HTTP/JSON check, so a passing test here means the SDK's own PartiQL request
 * builders and response parsers agree this is a real, well-formed DynamoDB service response.
 */
class PartiQlIntegrationTest {

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

    @Test
    void insertSelectUpdateDeleteAllRoundTripThroughPartiQl() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "POLYWIRE_DYNAMOWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = client(polywire.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("partiql_orders")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());

                // INSERT ... VALUE ? -- the real, common shape: a whole item bound as one Map
                // parameter, not a literal PartiQL tuple typed into the statement text.
                dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("INSERT INTO \"partiql_orders\" VALUE ?")
                        .parameters(AttributeValue.builder().m(Map.of(
                                "id", AttributeValue.builder().s("p-1").build(),
                                "status", AttributeValue.builder().s("pending").build(),
                                "amount", AttributeValue.builder().n("42.50").build()))
                                .build())
                        .build());

                // SELECT ... WHERE <partition key> = ? -- routed through the same Query path
                // GetItem/Query already use (KeyConditionParser, unchanged).
                ExecuteStatementResponse selected = dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("SELECT * FROM \"partiql_orders\" WHERE id = ?")
                        .parameters(AttributeValue.builder().s("p-1").build())
                        .build());
                assertEquals(1, selected.items().size(), "expected exactly one item for id=p-1");
                assertEquals("pending", selected.items().get(0).get("status").s());
                assertEquals("42.50", selected.items().get(0).get("amount").n());

                // UPDATE ... SET status = ? WHERE id = ? -- PartiQL's SET clause is the same
                // grammar as UpdateExpression's own SET clause, reused unchanged.
                dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("UPDATE \"partiql_orders\" SET status = ? WHERE id = ?")
                        .parameters(
                                AttributeValue.builder().s("shipped").build(),
                                AttributeValue.builder().s("p-1").build())
                        .build());

                ExecuteStatementResponse afterUpdate = dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("SELECT * FROM \"partiql_orders\" WHERE id = ?")
                        .parameters(AttributeValue.builder().s("p-1").build())
                        .build());
                assertEquals("shipped", afterUpdate.items().get(0).get("status").s(),
                        "PartiQL UPDATE must actually persist against the real Postgres backend");
                // The amount attribute PartiQL's UPDATE never touched must survive the
                // read-modify-write untouched -- proves this isn't overwriting the whole item.
                assertEquals("42.50", afterUpdate.items().get(0).get("amount").n());

                // DELETE FROM ... WHERE id = ?
                dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("DELETE FROM \"partiql_orders\" WHERE id = ?")
                        .parameters(AttributeValue.builder().s("p-1").build())
                        .build());

                ExecuteStatementResponse afterDelete = dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("SELECT * FROM \"partiql_orders\" WHERE id = ?")
                        .parameters(AttributeValue.builder().s("p-1").build())
                        .build());
                assertEquals(0, afterDelete.items().size(), "PartiQL DELETE must actually remove the row");
            }
        }
    }

    @Test
    void batchExecuteStatementRunsEachStatementIndependentlyAndReportsPerStatementErrors() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "POLYWIRE_DYNAMOWIRE_PORT")
                        .env("POLYWIRE_DYNAMOWIRE_CACHE_ENABLED", "false")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = client(polywire.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("partiql_batch")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());
                dynamo.executeStatement(ExecuteStatementRequest.builder()
                        .statement("INSERT INTO \"partiql_batch\" VALUE ?")
                        .parameters(AttributeValue.builder().m(Map.of(
                                "id", AttributeValue.builder().s("b-1").build()))
                                .build())
                        .build());

                BatchExecuteStatementResponse resp = dynamo.batchExecuteStatement(BatchExecuteStatementRequest.builder()
                        .statements(
                                BatchStatementRequest.builder()
                                        .statement("SELECT * FROM \"partiql_batch\" WHERE id = ?")
                                        .parameters(AttributeValue.builder().s("b-1").build())
                                        .build(),
                                // A second, genuinely malformed statement in the SAME batch --
                                // proves one bad statement doesn't fail the whole batch or get
                                // silently dropped, it comes back as its own per-statement error.
                                BatchStatementRequest.builder()
                                        .statement("SELECT * FROM \"partiql_batch\" WHERE nonexistent_attr = ?")
                                        .parameters(AttributeValue.builder().s("x").build())
                                        .build())
                        .build());

                assertEquals(2, resp.responses().size());
                assertEquals("b-1", resp.responses().get(0).item().get("id").s());
                assertEquals("ValidationException", resp.responses().get(1).error().codeAsString());
            }
        }
    }

    @Test
    void executeTransactionIsADisclosedGapNotASilentOne() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "POLYWIRE_DYNAMOWIRE_PORT")
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = client(polywire.port("dynamowire"))) {
                DynamoDbException thrown = assertThrows(DynamoDbException.class, () ->
                        dynamo.executeTransaction(b -> b.transactStatements(
                                software.amazon.awssdk.services.dynamodb.model.ParameterizedStatement.builder()
                                        .statement("SELECT * FROM \"nope\" WHERE id = ?")
                                        .parameters(AttributeValue.builder().s("x").build())
                                        .build())));
                assertEquals(400, thrown.statusCode());
            }
        }
    }
}
