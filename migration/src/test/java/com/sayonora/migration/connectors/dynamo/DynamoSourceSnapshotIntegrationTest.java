package com.sayonora.migration.connectors.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.migration.checkpoint.CdcCheckpointStore;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.sink.WarpGrpcSink;
import com.sayonora.migration.testsupport.WarpProcess;
import com.sayonora.migration.testsupport.RealDynamoDb;
import com.sayonora.migration.testsupport.RealPostgres;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/**
 * Real, not simulated, snapshot (parallel {@code Scan}) verification: real DynamoDB Local, real
 * Warp gRPC subprocess, real target Postgres. Drives {@link DynamoSource}'s snapshot-related
 * methods directly (not through {@link com.sayonora.migration.coordinator.Coordinator}) --
 * deliberately, since {@code Coordinator#run} also calls {@code prepareChangeFeed}/{@code
 * streamChanges}, which need real DynamoDB Streams support DynamoDB Local does not have (see
 * {@link RealDynamoDb}'s own javadoc); the Streams path is verified separately with a fake client
 * in {@code DynamoSourceStreamsTest}.
 */
class DynamoSourceSnapshotIntegrationTest {

    private static void createTableWithCompositeKey(DynamoDbClient client, String tableName) {
        client.createTable(r -> r.tableName(tableName)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("customerId").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("orderId").attributeType(ScalarAttributeType.N).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName("customerId").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("orderId").keyType(KeyType.RANGE).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));
    }

    private static Map<String, AttributeValue> item(String customerId, int orderId, String amount) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("customerId", AttributeValue.builder().s(customerId).build());
        item.put("orderId", AttributeValue.builder().n(String.valueOf(orderId)).build());
        item.put("amount", AttributeValue.builder().n(amount).build());
        return item;
    }

    @Test
    void snapshotCopiesEveryItemAcrossParallelSegmentsAndRegistersTheCatalogRow() throws Exception {
        try (RealDynamoDb dynamo = RealDynamoDb.start();
                RealPostgres postgres = RealPostgres.start();
                DynamoDbClient sourceClient = dynamo.newClient();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("grpc", "WARP_GRPC_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            createTableWithCompositeKey(sourceClient, "Orders");
            int total = 25;
            for (int i = 0; i < total; i++) {
                Map<String, AttributeValue> row = item("cust-" + (i % 5), i, String.valueOf(10 + i));
                sourceClient.putItem(r -> r.tableName("Orders").item(row));
            }

            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();

            // Never actually called in this test (see this class's own javadoc) -- just needs to
            // exist to satisfy DynamoSource's constructor.
            DynamoDbStreamsClient unusedStreamsClient = DynamoDbStreamsClient.builder()
                    .endpointOverride(URI.create(dynamo.endpoint()))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                    .build();

            int partitionCount = 3;
            DynamoSource source = new DynamoSource(sourceClient, unusedStreamsClient, "Orders", partitionCount);
            try (WarpGrpcSink sink = new WarpGrpcSink("localhost", warp.port("grpc"), postgres.username(), postgres.password())) {
                source.ensureTargetSchema(sink);
                long copied = 0;
                for (Partition partition : source.listPartitions()) {
                    source.readPartition(partition, sink, checkpoints);
                }
            }

            String targetTable = "\"dynamo_item_orders\"";
            try (Connection conn = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password())) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM " + targetTable);
                        ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(total, rs.getLong(1), "every item across all parallel segments should have landed exactly once");
                }
                // Spot-check one row's actual content, including the numeric sort key's parallel
                // sk_num column (populated because orderId is type N).
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT sk_num, item FROM " + targetTable + " WHERE pk_value = ? AND sk_value = ?")) {
                    ps.setString(1, "cust-0");
                    ps.setString(2, "0");
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "order 0 for cust-0 should exist in the target");
                        assertEquals(0, rs.getBigDecimal("sk_num").intValue());
                        String itemJson = rs.getString("item");
                        assertTrue(itemJson.contains("\"amount\""), "item jsonb should contain the amount attribute: " + itemJson);
                        assertTrue(itemJson.contains("\"N\""), "numeric attributes should use DynamoDB's typed N encoding, matching dynamowire's own convention: " + itemJson);
                    }
                }
                // The catalog row is what makes this table actually discoverable/usable by a live
                // dynamowire client afterward -- without it, dynamo_item_orders exists physically
                // but no real dynamowire operation would ever find it.
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT pg_table, pk_name, pk_type, sk_name, sk_type FROM _dynamo_tables WHERE table_name = ?")) {
                    ps.setString(1, "Orders");
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "the catalog row for Orders should have been registered");
                        assertEquals("dynamo_item_orders", rs.getString("pg_table"));
                        assertEquals("customerId", rs.getString("pk_name"));
                        assertEquals("S", rs.getString("pk_type"));
                        assertEquals("orderId", rs.getString("sk_name"));
                        assertEquals("N", rs.getString("sk_type"));
                    }
                }
            }
        }
    }
}
