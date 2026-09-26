package com.sayonora.warp.core.connector.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.connector.ConnectorOperands;
import com.sayonora.warp.testsupport.RealDynamoDb;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Real DynamoDB Local container: real table, real items with heterogeneous attribute types, read
 * back through the real connector built from a real {@code dynamodb://} pseudo-URL. Also forces a
 * multi-page scan (> 1MB) to prove the paginator really walks every page. */
class DynamoSchemaTest {

    private static RealDynamoDb dynamo;
    private static final int BULK_ROWS = 1200;

    @BeforeAll
    static void start() throws Exception {
        dynamo = RealDynamoDb.start();
        try (DynamoDbClient c = dynamo.client()) {
            createTable(c, "Widgets");
            c.putItem(b -> b.tableName("Widgets").item(Map.of(
                    "id", AttributeValue.fromS("w1"), "qty", AttributeValue.fromN("3"),
                    "active", AttributeValue.fromBool(true))));
            c.putItem(b -> b.tableName("Widgets").item(Map.of(
                    "id", AttributeValue.fromS("w2"), "qty", AttributeValue.fromS("not-a-number"))));
            createTable(c, "Bulk");
            String padding = "x".repeat(1000);
            for (int i = 0; i < BULK_ROWS; i++) {
                String id = "b" + i;
                c.putItem(b -> b.tableName("Bulk").item(Map.of(
                        "id", AttributeValue.fromS(id), "pad", AttributeValue.fromS(padding))));
            }
        }
    }

    private static void createTable(DynamoDbClient c, String name) {
        c.createTable(b -> b.tableName(name)
                .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));
    }

    @AfterAll
    static void stop() {
        if (dynamo != null) dynamo.close();
    }

    private static Map<String, Object> operand(String tables) {
        return ConnectorOperands.parse("dynamodb://" + dynamo.region() + "?endpoint=" + dynamo.endpoint() + tables,
                dynamo.accessKeyId(), dynamo.secretAccessKey());
    }

    @Test
    void scansRealTableWithStringifiedHeterogeneousAttributes() {
        try (DynamoSchema schema = DynamoSchemaFactory.createSchema(
                operand("&table.widgets=id,qty,active&source.widgets=Widgets"))) {
            assertEquals(java.util.Set.of("widgets"), schema.tables().getNames(LikePattern.any()));
            var table = schema.tables().get("widgets");
            assertNotNull(table);
            assertInstanceOf(ScannableTable.class, table);
            List<List<Object>> rows = new ArrayList<>();
            ((ScannableTable) table).scan(null).forEach(r -> rows.add(java.util.Arrays.asList(r)));
            rows.sort(java.util.Comparator.comparing(r -> (String) r.get(0)));
            assertEquals(List.of(
                    java.util.Arrays.asList("w1", "3", "true"),
                    java.util.Arrays.asList("w2", "not-a-number", null)), rows);
        }
    }

    @Test
    void paginatesPastOneMegabyteScanPage() {
        try (DynamoSchema schema = DynamoSchemaFactory.createSchema(operand("&table.bulk=id&source.bulk=Bulk"))) {
            int[] count = {0};
            ((ScannableTable) schema.tables().get("bulk")).scan(null).forEach(r -> count[0]++);
            assertEquals(BULK_ROWS, count[0], "~1.2MB of items spans more than one 1MB Scan page");
        }
    }

    @Test
    void rejectsBackendWithNoDeclaredTables() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DynamoSchemaFactory.createSchema(operand("")));
        assertTrue(e.getMessage().contains("at least one table"), e.getMessage());
    }
}
