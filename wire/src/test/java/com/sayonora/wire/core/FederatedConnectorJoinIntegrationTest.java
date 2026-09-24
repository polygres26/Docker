package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.mongodb.client.MongoClient;
import com.sayonora.wire.testsupport.RealCassandra;
import com.sayonora.wire.testsupport.RealDynamoDb;
import com.sayonora.wire.testsupport.RealKafka;
import com.sayonora.wire.testsupport.RealMinio;
import com.sayonora.wire.testsupport.RealMongo;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.RealSplunk;
import com.sayonora.wire.testsupport.WarpProcess;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * The headline proof for the DynamoDB/MongoDB connectors: a real Postgres {@code customers} table
 * JOINed with a real DynamoDB table (and, separately, a real MongoDB collection) in ONE federated
 * SQL statement sent by a real pgwire JDBC client through a real Warp process -- same
 * WarpProcess/RealPostgres/{@code WARP_ROUTER_SCHEMA_RULES} setup as {@link
 * FederatedNativeRlsIntegrationTest}. The connector side's rows exist only in the DynamoDB/Mongo
 * container, so a correct joined row can only come from real data crossing into the join.
 */
class FederatedConnectorJoinIntegrationTest {

    private RealPostgres pg;
    private RealDynamoDb dynamo;
    private RealMongo mongo;
    private RealMinio minio;
    private RealKafka kafka;
    private RealCassandra cassandra;
    private RealSplunk splunk;
    private WarpProcess warp;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (dynamo != null) dynamo.close();
        if (mongo != null) mongo.close();
        if (minio != null) minio.close();
        if (kafka != null) kafka.close();
        if (cassandra != null) cassandra.close();
        if (splunk != null) splunk.close();
        if (pg != null) pg.close();
    }

    private void startPostgresWithCustomers() throws Exception {
        pg = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(), pg.username(), pg.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA customers_db");
            st.execute("CREATE TABLE customers_db.customers (customer_id TEXT PRIMARY KEY, name TEXT)");
            st.execute("INSERT INTO customers_db.customers VALUES ('c1', 'Ada'), ('c2', 'Grace'), ('c3', 'Linus')");
        }
    }

    private WarpProcess startWarp(String connectorSchema, String connectorBackendEntry) throws Exception {
        String backends = "default=" + pg.jdbcUrl() + "|" + pg.username() + "|" + pg.password() + ";" + connectorBackendEntry;
        return WarpProcess.builder()
                .pgBackend(pg.host(), pg.port(), pg.database(), pg.username(), pg.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                .env("WARP_ROUTER_SCHEMA_RULES", "customers_db:default," + connectorSchema + ":conn")
                // Same pre-existing port-7070 collision FederatedNativeRlsIntegrationTest documents.
                .env("WARP_GRPC_PORT", String.valueOf(findFreePort()))
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @Test
    void postgresJoinsRealDynamoDbTableInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        dynamo = RealDynamoDb.start();
        try (DynamoDbClient c = dynamo.client()) {
            c.createTable(b -> b.tableName("Orders")
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("order_id").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder().attributeName("order_id").keyType(KeyType.HASH).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST));
            putOrder(c, "o1", "c1", "30");
            putOrder(c, "o2", "c2", "45");
            putOrder(c, "o3", "c1", "12");
            putOrder(c, "o4", "c9", "99"); // no matching customer -- must not appear in an inner join
        }
        warp = startWarp("orders_dyn", "conn=dynamodb://" + dynamo.region() + "?endpoint=" + dynamo.endpoint()
                + "&table.orders=order_id,customer_id,amount&source.orders=Orders"
                + "|" + dynamo.accessKeyId() + "|" + dynamo.secretAccessKey());

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_dyn.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x DYNAMODB ROWS: " + rows);
        assertEquals(List.of("Ada|o1|30", "Grace|o2|45", "Ada|o3|12"), rows);

        // A filter on the connector side runs as a Calcite post-filter (no pushdown by design).
        List<String> filtered = query("SELECT c.name, o.order_id FROM customers_db.customers c "
                + "JOIN orders_dyn.orders o ON c.customer_id = o.customer_id WHERE o.amount = '45'");
        System.out.println("FEDERATED POSTGRES x DYNAMODB FILTERED ROWS: " + filtered);
        assertEquals(List.of("Grace|o2"), filtered);

        // Single-backend routing to the DynamoDB backend alone is out of scope -> clear error.
        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_dyn.orders"));
        System.out.println("SINGLE-BACKEND DYNAMODB ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("DynamoDB backend"), e.getMessage());
    }

    @Test
    void postgresJoinsRealMongoCollectionInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        mongo = RealMongo.start();
        try (MongoClient c = mongo.client()) {
            c.getDatabase("shop").getCollection("order_docs").insertMany(List.of(
                    new Document("order_id", "m1").append("customer_id", "c3").append("amount", 7),
                    new Document("order_id", "m2").append("customer_id", "c2").append("amount", 8.5),
                    new Document("order_id", "m3").append("customer_id", "c404").append("amount", 1)));
        }
        warp = startWarp("orders_mongo", "conn=" + mongo.connectionString()
                + "/shop?table.orders=order_id,customer_id,amount&source.orders=order_docs");

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_mongo.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x MONGODB ROWS: " + rows);
        assertEquals(List.of("Linus|m1|7", "Grace|m2|8.5"), rows);

        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_mongo.orders"));
        System.out.println("SINGLE-BACKEND MONGODB ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("MongoDB backend"), e.getMessage());
    }

    @Test
    void postgresJoinsRealS3CsvObjectInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        minio = RealMinio.start();
        String csv = "order_id,customer_id,amount\n"
                + "s1,c1,30\n"
                + "s2,c2,45\n"
                + "s3,c1,12\n"
                + "s4,c9,99\n"; // no matching customer -- must not appear in an inner join
        minio.putObject("orders-bucket", "orders.csv", csv.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        warp = startWarp("orders_s3", "conn=s3://orders-bucket?key=orders.csv&format=csv"
                + "&provider=s3-compatible&endpoint=" + minio.endpoint() + "&pathStyleAccess=true&table=orders"
                + "|" + minio.accessKeyId() + "|" + minio.secretAccessKey());

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_s3.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x S3 ROWS: " + rows);
        assertEquals(List.of("Ada|s1|30", "Grace|s2|45", "Ada|s3|12"), rows);

        // Single-backend routing to the S3 backend alone is out of scope -> clear error.
        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_s3.orders"));
        System.out.println("SINGLE-BACKEND S3 ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("S3 backend"), e.getMessage());
    }

    @Test
    void postgresJoinsRealKafkaTopicInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        kafka = RealKafka.start();
        kafka.produce("orders-topic", "o1", "{\"order_id\":\"k1\",\"customer_id\":\"c1\",\"amount\":\"30\"}");
        kafka.produce("orders-topic", "o2", "{\"order_id\":\"k2\",\"customer_id\":\"c2\",\"amount\":\"45\"}");
        kafka.produce("orders-topic", "o3", "{\"order_id\":\"k3\",\"customer_id\":\"c1\",\"amount\":\"12\"}");
        kafka.produce("orders-topic", "o4", "{\"order_id\":\"k4\",\"customer_id\":\"c9\",\"amount\":\"99\"}"); // no matching customer

        warp = startWarp("orders_kafka", "conn=kafka://" + kafka.bootstrapServers() + "/orders-topic"
                + "?format=json&fields=order_id,customer_id,amount&table=orders");

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_kafka.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x KAFKA ROWS: " + rows);
        assertEquals(List.of("Ada|k1|30", "Grace|k2|45", "Ada|k3|12"), rows);

        // Single-backend routing to the Kafka backend alone is out of scope -> clear error.
        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_kafka.orders"));
        System.out.println("SINGLE-BACKEND KAFKA ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("Kafka backend"), e.getMessage());
    }

    @Test
    void postgresJoinsRealCassandraTableInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        cassandra = RealCassandra.start();
        cassandra.createKeyspace("shop");
        try (CqlSession session = cassandra.newSession()) {
            session.execute("CREATE TABLE shop.orders (order_id text PRIMARY KEY, customer_id text, amount text)");
            session.execute("INSERT INTO shop.orders (order_id, customer_id, amount) VALUES ('ca1', 'c1', '30')");
            session.execute("INSERT INTO shop.orders (order_id, customer_id, amount) VALUES ('ca2', 'c2', '45')");
            session.execute("INSERT INTO shop.orders (order_id, customer_id, amount) VALUES ('ca3', 'c1', '12')");
            session.execute("INSERT INTO shop.orders (order_id, customer_id, amount) VALUES ('ca4', 'c9', '99')"); // no matching customer
        }

        warp = startWarp("orders_cass", "conn=cassandra://" + cassandra.contactPoint() + "/shop.orders"
                + "?localDc=" + cassandra.localDatacenter() + "&partitionKeyEquals=order_id&table=orders");

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_cass.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x CASSANDRA ROWS: " + rows);
        assertEquals(List.of("Ada|ca1|30", "Grace|ca2|45", "Ada|ca3|12"), rows);

        // Single-backend routing to the Cassandra backend alone is out of scope -> clear error.
        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_cass.orders"));
        System.out.println("SINGLE-BACKEND CASSANDRA ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("Cassandra backend"), e.getMessage());
    }

    // Disabled, not deleted: real, confirmed-live blocker on this dev machine, not a guess.
    // splunk/splunk publishes NO arm64 image (docker manifest inspect confirms amd64-only), so on
    // this Apple Silicon host it only runs under `--platform linux/amd64` QEMU emulation. Under
    // that emulation splunkd started once (confirmed via `docker exec ... pgrep splunkd`) and then
    // died within about a minute -- every subsequent HTTPS request to the management port failed
    // with `SSL_ERROR_SYSCALL` (curl exit 35, connection reset during the TLS handshake), and a
    // follow-up `pgrep splunkd` found no process at all. Re-verified live rather than assumed. See
    // RealSplunk's own javadoc for the trust-all-TLS rationale that would otherwise be needed once
    // this becomes runnable. Re-enable once this environment gets a working amd64 Docker path (e.g.
    // a Linux CI runner) or Splunk ships an arm64 image.
    @org.junit.jupiter.api.Disabled("splunk/splunk has no arm64 image; under amd64 QEMU emulation on this "
            + "host splunkd starts then crashes within ~1 minute -- confirmed live, not flaky-skip-by-guess")
    @Test
    void postgresJoinsRealSplunkSearchInOneFederatedStatement() throws Exception {
        startPostgresWithCustomers();
        splunk = RealSplunk.start();
        splunk.indexEvent("order_id=sp1 customer_id=c1 amount=30");
        splunk.indexEvent("order_id=sp2 customer_id=c2 amount=45");
        splunk.indexEvent("order_id=sp3 customer_id=c1 amount=12");
        splunk.indexEvent("order_id=sp4 customer_id=c9 amount=99"); // no matching customer
        splunk.waitForIndexedEvents("index=main order_id=sp*", 4, java.time.Duration.ofMinutes(3));

        warp = startWarp("orders_splunk", "conn=splunk://" + splunk.endpoint()
                + "?search=" + java.net.URLEncoder.encode("index=main order_id=sp*", java.nio.charset.StandardCharsets.UTF_8)
                + "&table=orders||" + splunk.authHeader());

        List<String> rows = query("SELECT c.name, o.order_id, o.amount FROM customers_db.customers c "
                + "JOIN orders_splunk.orders o ON c.customer_id = o.customer_id ORDER BY o.order_id");
        System.out.println("FEDERATED POSTGRES x SPLUNK ROWS: " + rows);
        assertEquals(List.of("Ada|sp1|30", "Grace|sp2|45", "Ada|sp3|12"), rows);

        // Single-backend routing to the Splunk backend alone is out of scope -> clear error.
        SQLException e = assertThrows(SQLException.class, () -> query("SELECT order_id FROM orders_splunk.orders"));
        System.out.println("SINGLE-BACKEND SPLUNK ROUTING ERROR: " + e.getMessage());
        assertTrue(e.getMessage().contains("Splunk backend"), e.getMessage());
    }

    private static void putOrder(DynamoDbClient c, String orderId, String customerId, String amount) {
        c.putItem(b -> b.tableName("Orders").item(Map.of(
                "order_id", AttributeValue.fromS(orderId),
                "customer_id", AttributeValue.fromS(customerId),
                "amount", AttributeValue.fromN(amount))));
    }

    private List<String> query(String sql) throws SQLException {
        String url = "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres";
        List<String> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, pg.username(), pg.password());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) row.append('|');
                    row.append(rs.getString(i));
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
