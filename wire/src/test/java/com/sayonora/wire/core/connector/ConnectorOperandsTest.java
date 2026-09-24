package com.sayonora.wire.core.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.SourceDialect;
import com.sayonora.wire.core.TrustedBackendHosts;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorOperandsTest {

    @Test
    @SuppressWarnings("unchecked")
    void parsesDynamoRegionEndpointCredentialsAndTables() {
        Map<String, Object> op = ConnectorOperands.parse(
                "dynamodb://eu-west-1?endpoint=http://localhost:8000&table.orders=order_id,customer_id,amount"
                        + "&source.orders=Orders&table.items=id,sku",
                "AKID", "vault:secret/aws#key");
        assertEquals("eu-west-1", op.get("region"));
        assertEquals("http://localhost:8000", op.get("endpoint"));
        assertEquals("AKID", op.get("accessKeyId"));
        assertEquals("vault:secret/aws#key", op.get("secretAccessKey"), "secret refs stay unresolved at parse time");
        Map<String, Map<String, Object>> tables = (Map<String, Map<String, Object>>) op.get("tables");
        assertEquals(List.of("orders", "items"), List.copyOf(tables.keySet()));
        assertEquals("Orders", tables.get("orders").get("table"));
        assertEquals(List.of("order_id", "customer_id", "amount"), tables.get("orders").get("fields"));
        assertEquals("items", tables.get("items").get("table"), "no source.<name> -> real name = sql name");
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesMongoDatabaseStripsGrammarKeysAndKeepsDriverOptions() {
        Map<String, Object> op = ConnectorOperands.parse(
                "mongodb://h1:27017,h2:27017/shop?replicaSet=rs0&table.orders=_id,customer_id&source.orders=order_docs"
                        + "&authSource=admin",
                "reader", "pw");
        assertEquals("mongodb://h1:27017,h2:27017/shop?replicaSet=rs0&authSource=admin", op.get("connectionString"));
        assertEquals("shop", op.get("database"));
        assertEquals("reader", op.get("user"));
        Map<String, Map<String, Object>> tables = (Map<String, Map<String, Object>>) op.get("tables");
        assertEquals("order_docs", tables.get("orders").get("collection"));
        assertEquals("shop", tables.get("orders").get("database"));
        assertEquals(List.of("_id", "customer_id"), tables.get("orders").get("fields"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void missingTablesParsesToEmptyRatherThanFailingRegistration() {
        Map<String, Object> op = ConnectorOperands.parse("dynamodb://us-east-1", null, null);
        assertTrue(((Map<String, Object>) op.get("tables")).isEmpty());
        assertNull(op.get("accessKeyId"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesS3BucketKeyFormatAndPushdownColumns() {
        Map<String, Object> op = ConnectorOperands.parse(
                "s3://my-bucket?key=path/to/file.parquet&format=parquet&region=us-west-2"
                        + "&pushdownColumns=id,ts&provider=s3-compatible&endpoint=http://localhost:9000"
                        + "&pathStyleAccess=true&table=widgets",
                "AKID", "vault:secret/aws#key");
        assertEquals("my-bucket", op.get("bucket"));
        assertEquals("us-west-2", op.get("region"));
        assertEquals("s3-compatible", op.get("provider"));
        assertEquals("http://localhost:9000", op.get("endpoint"));
        assertEquals(Boolean.TRUE, op.get("pathStyleAccess"));
        assertEquals("AKID", op.get("accessKeyId"));
        assertEquals("vault:secret/aws#key", op.get("secretAccessKey"));
        Map<String, Map<String, String>> tables = (Map<String, Map<String, String>>) op.get("tables");
        assertEquals(List.of("widgets"), List.copyOf(tables.keySet()));
        assertEquals("path/to/file.parquet", tables.get("widgets").get("key"));
        assertEquals("parquet", tables.get("widgets").get("format"));
        assertEquals("id,ts", tables.get("widgets").get("pushdownColumns"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesKafkaBrokerListTopicAndFields() {
        Map<String, Object> op = ConnectorOperands.parse(
                "kafka://broker1:9092,broker2:9092/orders-topic?format=json&fields=orderId,customer,amount"
                        + "&table=orders",
                null, null);
        assertEquals("broker1:9092,broker2:9092", op.get("bootstrapServers"));
        Map<String, Map<String, Object>> tables = (Map<String, Map<String, Object>>) op.get("tables");
        assertEquals(List.of("orders"), List.copyOf(tables.keySet()));
        assertEquals("orders-topic", tables.get("orders").get("topic"));
        assertEquals("json", tables.get("orders").get("format"));
        assertEquals(List.of("orderId", "customer", "amount"), tables.get("orders").get("fields"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesCassandraContactPointsKeyspaceTableAndGuardFlags() {
        Map<String, Object> op = ConnectorOperands.parse(
                "cassandra://host1:9042,host2:9042/mykeyspace.orders?localDc=dc1"
                        + "&partitionKeyEquals=order_id&table=orders",
                "cassuser", "vault:secret/cass#pw");
        assertEquals(List.of("host1:9042", "host2:9042"), op.get("contactPoints"));
        assertEquals("dc1", op.get("localDatacenter"));
        assertEquals("cassuser", op.get("username"));
        assertEquals("vault:secret/cass#pw", op.get("password"));
        Map<String, Map<String, Object>> tables = (Map<String, Map<String, Object>>) op.get("tables");
        assertEquals("mykeyspace", tables.get("orders").get("keyspace"));
        assertEquals("orders", tables.get("orders").get("table"));
        assertEquals("order_id", tables.get("orders").get("partitionKeyEquals"));
        assertEquals(Boolean.FALSE, tables.get("orders").get("allowFullScan"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesSplunkEndpointSearchAndAuthHeader() {
        Map<String, Object> op = ConnectorOperands.parse(
                "splunk://splunkhost:8089?search=index%3Dmain&pushdownColumns=host,sourcetype"
                        + "&pollTimeoutMs=45000&table=events",
                null, "Splunk abc123");
        assertEquals("https://splunkhost:8089", op.get("endpoint"));
        assertEquals("Splunk abc123", op.get("authHeader"));
        assertEquals(45000L, op.get("pollTimeoutMs"));
        Map<String, Map<String, Object>> tables = (Map<String, Map<String, Object>>) op.get("tables");
        assertEquals("index=main", tables.get("events").get("search"));
        assertEquals(List.of("host", "sourcetype"), tables.get("events").get("pushdownColumns"));
    }

    @Test
    void dialectRecognizesConnectorUrls() {
        assertEquals(SourceDialect.DYNAMODB, new BackendTarget("d", "dynamodb://us-east-1", null, null).dialect());
        assertEquals(SourceDialect.MONGODB, new BackendTarget("m", "mongodb://h:1/db", null, null).dialect());
        assertEquals(SourceDialect.MONGODB, new BackendTarget("m", "mongodb+srv://cluster.example/db", null, null).dialect());
        assertEquals(SourceDialect.POSTGRES, new BackendTarget("p", "jdbc:postgresql://h/db", null, null).dialect());
        assertTrue(new BackendTarget("d", "dynamodb://us-east-1", null, null).isFederationOnlyConnector());
    }

    @Test
    void openOnAConnectorTargetFailsWithActionableErrorInsteadOfReachingJdbc() {
        SQLException dynamo = assertThrows(SQLException.class,
                () -> new BackendTarget("dyn", "dynamodb://us-east-1", null, null).open());
        assertTrue(dynamo.getMessage().contains("DynamoDB") && dynamo.getMessage().contains("WARP_ROUTER_SCHEMA_RULES"),
                dynamo.getMessage());
        SQLException mongo = assertThrows(SQLException.class,
                () -> new BackendTarget("mg", "mongodb://h:1/db", null, null).open());
        assertTrue(mongo.getMessage().contains("MongoDB"), mongo.getMessage());
    }

    @Test
    void registryPopulatesConnectorOperandOnlyForConnectorBackends() {
        BackendRegistry registry = BackendRegistry.fromConfig(
                "pg=jdbc:postgresql://h/db|u|p;dyn=dynamodb://us-east-1?table.t=a,b|AK|SK", null);
        assertNull(registry.get("pg").connectorOperand());
        assertEquals("us-east-1", registry.get("dyn").connectorOperand().get("region"));
        assertEquals("AK", registry.get("dyn").connectorOperand().get("accessKeyId"));
    }

    @Test
    void trustedHostsExemptsDynamoAndChecksMongoHost() {
        TrustedBackendHosts trusted = TrustedBackendHosts.parse("db.internal");
        assertTrue(trusted.isTrusted("dynamodb://us-east-1?table.t=a"), "a region is not a host -- nothing to allowlist");
        assertTrue(trusted.isTrusted("mongodb://db.internal:27017/shop"));
        assertTrue(!trusted.isTrusted("mongodb://evil.example:27017/shop"));
    }
}
