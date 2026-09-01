package com.nexagres.wiretests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** dynamowire: real DynamoDB HTTP/JSON API, real AWS SDK v2 client, translated to Postgres
 * underneath. Dummy static credentials are enough since AWS SigV4 request verification is
 * opt-in (WARP_AWS_IAM_CREDENTIALS, unset in the test compose file). */
class DynamoWireTest {

    private static DynamoDbClient client() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://" + TestConfig.HOST + ":" + TestConfig.DYNAMOWIRE_PORT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Test
    void createTablePutGetItem() {
        DynamoDbClient c = client();
        String table = "smoke_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        c.createTable(CreateTableRequest.builder()
                .tableName(table)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        try {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s("1").build());
            item.put("name", AttributeValue.builder().s("warp").build());
            c.putItem(PutItemRequest.builder().tableName(table).item(item).build());

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id", AttributeValue.builder().s("1").build());
            Map<String, AttributeValue> found = c.getItem(
                    GetItemRequest.builder().tableName(table).key(key).build()).item();
            assertEquals("warp", found.get("name").s());
        } finally {
            c.deleteTable(DeleteTableRequest.builder().tableName(table).build());
        }
    }
}
