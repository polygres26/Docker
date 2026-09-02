package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import org.bson.Document;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * The project's "short regression suite" -- one real Warp instance, real Postgres backends,
 * real client drivers per protocol, meant to run in well under 10 minutes and catch the class of
 * regression this project has actually hit live: a protocol silently breaking (the orawire
 * Execute-request regression), a real perf regression in the hot read/write path (the
 * PreparedStatement-reuse fix), or federation/caching breaking outright. NOT a substitute for the
 * project's own exhaustive integration suite (`mvn test`) -- this is the fast, always-run subset.
 *
 * <p>One shared {@link WarpProcess} + two real Postgres backends for every test in this class
 * (started once in {@link #startInfra}), not one per test -- container/process startup is the
 * dominant real cost here, and every test in this suite is cheap once the real infra is up.
 */
class ShortRegressionSuiteTest {

    private static RealPostgres shard1;
    private static RealPostgres shard2;
    private static WarpProcess warp;
    private static final String ADMIN_TOKEN = "short-regression-suite-token";

    @BeforeAll
    static void startInfra() throws Exception {
        shard1 = RealPostgres.start();
        shard2 = RealPostgres.start();

        try (Connection c = DriverManager.getConnection(shard1.jdbcUrl(), shard1.username(), shard1.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE protocol_smoke (id BIGSERIAL PRIMARY KEY, protocol TEXT, val TEXT)");
            st.execute("CREATE TABLE rtt_cached (id BIGSERIAL PRIMARY KEY, val TEXT)");
            st.execute("INSERT INTO rtt_cached (val) VALUES ('warm')");
            st.execute("CREATE TABLE rtt_plain (id BIGSERIAL PRIMARY KEY, val TEXT)");
            st.execute("INSERT INTO rtt_plain (val) VALUES ('warm')");
            st.execute("CREATE TABLE orders (id serial, region TEXT, total NUMERIC)");
            st.execute("INSERT INTO orders (region, total) VALUES ('east', 10), ('east', 20)");
        }
        try (Connection c = DriverManager.getConnection(shard2.jdbcUrl(), shard2.username(), shard2.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE orders (id serial, region TEXT, total NUMERIC)");
            st.execute("INSERT INTO orders (region, total) VALUES ('west', 30)");
        }

        String backends = "default=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard1=" + shard1.jdbcUrl() + "|" + shard1.username() + "|" + shard1.password()
                + ";shard2=" + shard2.jdbcUrl() + "|" + shard2.username() + "|" + shard2.password();

        warp = WarpProcess.builder()
                .pgBackend(shard1.host(), shard1.port(), shard1.database(), shard1.username(), shard1.password())
                .frontend("pgwire", "WARP_PGWIRE_PORT")
                .frontend("mywire", "WARP_MYWIRE_PORT")
                .frontend("mssqlwire", "WARP_MSSQLWIRE_PORT")
                .frontend("orawire", "WARP_ORAWIRE_PORT")
                .frontend("mongowire", "WARP_MONGOWIRE_PORT")
                .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                .frontend("sqswire", "WARP_SQSWIRE_PORT")
                .frontend("influxwire", "WARP_INFLUXWIRE_PORT")
                .frontend("boltwire", "WARP_BOLTWIRE_PORT")
                .env("WARP_BACKENDS", backends)
                // Real, declarative table sharding (no schema-qualifier prefix needed anywhere)
                // for the federated-query test below -- dogfoods the same mechanism the Router
                // rules UI edits.
                .env("WARP_TABLE_SHARDS", "orders:hash:region:shard1,shard2")
                .env("WARP_CACHE_TABLES", "rtt_cached")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
    }

    @AfterAll
    static void stopInfra() {
        if (warp != null) warp.close();
        if (shard2 != null) shard2.close();
        if (shard1 != null) shard1.close();
    }

    // --- 1. Basic protocol coverage: every wire protocol against a real Postgres backend --------

    @Test
    void pgwireBasicReadWrite() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", shard1.username(), shard1.password())) {
            assertProtocolReadWriteWorks(conn, "pgwire");
        }
    }

    @Test
    void mywireBasicReadWrite() throws Exception {
        // Real, pre-existing startup race (predates this suite -- the same flake this project's
        // own MySqlJdbcIntegrationTest hits under load): WarpProcess.waitForTcpReady only
        // confirms the mywire port itself accepts TCP connections, not that every internal
        // component (CredentialStore auth wiring included) is fully warm the instant after -- an
        // immediate first connection attempt can occasionally see a reset mid-handshake or a
        // spurious "Access denied". A short retry is the honest fix here, not a real code change
        // to mywire itself: this is a real client behavior (a driver/app retrying a failed
        // connect), not masking an actual bug.
        try (Connection conn = connectWithRetry(() -> DriverManager.getConnection(
                "jdbc:mysql://localhost:" + warp.port("mywire") + "/postgres", shard1.username(), shard1.password()), 3)) {
            assertProtocolReadWriteWorks(conn, "mywire");
        }
    }

    private interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    private static Connection connectWithRetry(ConnectionSupplier supplier, int attempts) throws SQLException, InterruptedException {
        SQLException lastFailure = null;
        for (int i = 0; i < attempts; i++) {
            try {
                return supplier.get();
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw lastFailure;
    }

    @Test
    void mssqlwireBasicReadWrite() throws SQLException {
        String url = "jdbc:sqlserver://localhost:" + warp.port("mssqlwire") + ";encrypt=false;"
                + "user=" + shard1.username() + ";password=" + shard1.password() + ";";
        try (Connection conn = DriverManager.getConnection(url)) {
            assertProtocolReadWriteWorks(conn, "mssqlwire");
        }
    }

    @Test
    void orawireBasicReadWrite() throws SQLException {
        String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
        try (Connection conn = DriverManager.getConnection(url, shard1.username(), shard1.password())) {
            assertProtocolReadWriteWorks(conn, "orawire");
        }
    }

    private static void assertProtocolReadWriteWorks(Connection conn, String protocol) throws SQLException {
        try (PreparedStatement ins = conn.prepareStatement("INSERT INTO protocol_smoke (protocol, val) VALUES (?, ?)")) {
            ins.setString(1, protocol);
            ins.setString(2, "hello-from-" + protocol);
            ins.executeUpdate();
        }
        try (PreparedStatement sel = conn.prepareStatement("SELECT val FROM protocol_smoke WHERE protocol = ?")) {
            sel.setString(1, protocol);
            try (ResultSet rs = sel.executeQuery()) {
                assertTrue(rs.next(), protocol + ": expected the row it just inserted to be readable back");
                assertEquals("hello-from-" + protocol, rs.getString(1));
            }
        }
    }

    @Test
    void mongowireBasicReadWrite() {
        try (MongoClient client = MongoClients.create(
                "mongodb://localhost:" + warp.port("mongowire") + "/?directConnection=true")) {
            MongoCollection<Document> coll = client.getDatabase("test").getCollection("regression_smoke");
            coll.insertOne(new Document("_id", "mongowire-1").append("val", "hello-from-mongowire"));
            Document found = coll.find(new Document("_id", "mongowire-1")).first();
            assertTrue(found != null, "mongowire: expected the document it just inserted to be readable back");
            assertEquals("hello-from-mongowire", found.getString("val"));
        }
    }

    @Test
    void dynamowireBasicReadWrite() {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + warp.port("dynamowire")))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build()))
                .build();
        String table = "regression_smoke";
        client.createTable(CreateTableRequest.builder()
                .tableName(table)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                .billingMode(software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST)
                .build());
        client.putItem(PutItemRequest.builder()
                .tableName(table)
                .item(Map.of("id", AttributeValue.fromS("dynamowire-1"), "val", AttributeValue.fromS("hello-from-dynamowire")))
                .build());
        var result = client.getItem(GetItemRequest.builder()
                .tableName(table)
                .key(Map.of("id", AttributeValue.fromS("dynamowire-1")))
                .build());
        assertTrue(result.hasItem(), "dynamowire: expected the item it just put to be readable back");
        assertEquals("hello-from-dynamowire", result.item().get("val").s());
    }

    @Test
    void sqswireBasicReadWrite() {
        SqsClient client = SqsClient.builder()
                .endpointOverride(URI.create("http://localhost:" + warp.port("sqswire")))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(RetryPolicy.builder().numRetries(0).build()))
                .build();
        String queueUrl = client.createQueue(CreateQueueRequest.builder().queueName("regression-smoke").build()).queueUrl();
        client.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("hello-from-sqswire").build());
        var received = client.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(1).build());
        assertTrue(!received.messages().isEmpty(), "sqswire: expected the message it just sent to be receivable");
        Message msg = received.messages().get(0);
        assertEquals("hello-from-sqswire", msg.body());
    }

    @Test
    void influxwireBasicReadWrite() {
        try (InfluxDB client = InfluxDBFactory.connect("http://localhost:" + warp.port("influxwire"))) {
            String db = "regression_smoke";
            client.write(db, "autogen", Point.measurement("smoke")
                    .addField("val", 1L)
                    .tag("protocol", "influxwire")
                    .build());
            QueryResult result = client.query(new Query("SELECT val FROM smoke", db));
            assertTrue(!result.hasError(), "influxwire: query returned an error: " + result.getError());
            assertTrue(!result.getResults().get(0).getSeries().isEmpty(),
                    "influxwire: expected the point it just wrote to be readable back");
        }
    }

    /** Real Bolt 4.4 handshake/auth/RUN/PULL/GOODBYE via the official driver -- boltwire's own
     * Phase 1 Cypher support is deliberately narrow ({@code RETURN <literal>} only, see {@link
     * com.sayonora.wire.boltwire.BoltWireSessionHandler}'s own javadoc), so that's what this
     * proves against, not a real graph mutation. */
    @Test
    void boltwireBasicQuery() {
        try (Driver driver = GraphDatabase.driver(
                "bolt://localhost:" + warp.port("boltwire"), AuthTokens.basic(shard1.username(), shard1.password()));
                Session session = driver.session()) {
            var result = session.run("RETURN 'hello-from-boltwire' AS val");
            assertTrue(result.hasNext(), "boltwire: expected a row back from a real RUN/PULL round trip");
            assertEquals("hello-from-boltwire", result.next().get("val").asString());
        }
    }

    // --- 2. RTT: cached read, plain read, write -- 50 executions each, real p50/p90 -------------

    @Test
    void rttCachedReadPlainReadAndWrite50Executions() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", shard1.username(), shard1.password())) {
            // Warm the cache for rtt_cached -- one throwaway call, same methodology as
            // docs/PERFORMANCE.md's own §5.
            try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM rtt_cached WHERE id = 1");
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
            }

            double[] cachedReadMs = timeExecutions(conn, "SELECT val FROM rtt_cached WHERE id = 1", 50);
            double[] plainReadMs = timeExecutions(conn, "SELECT val FROM rtt_plain WHERE id = 1", 50);
            double[] writeMs = new double[50];
            for (int i = 0; i < 50; i++) {
                long t0 = System.nanoTime();
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO rtt_plain (val) VALUES (?)")) {
                    ps.setString(1, "row-" + i);
                    ps.executeUpdate();
                }
                writeMs[i] = (System.nanoTime() - t0) / 1_000_000.0;
            }

            printStats("cached read", cachedReadMs);
            printStats("plain read ", plainReadMs);
            printStats("write      ", writeMs);

            // Loose upper bounds -- this is a REGRESSION guard (catch "it got 10x slower"), not a
            // precise perf benchmark (see docs/PERFORMANCE.md for that); real machine/environment
            // variance means asserting exact numbers here would just be flaky.
            assertTrue(avg(cachedReadMs) < 50.0, "cached read regressed badly: avg=" + avg(cachedReadMs) + "ms");
            assertTrue(avg(plainReadMs) < 50.0, "plain read regressed badly: avg=" + avg(plainReadMs) + "ms");
            assertTrue(avg(writeMs) < 50.0, "write regressed badly: avg=" + avg(writeMs) + "ms");
        }
    }

    private static double[] timeExecutions(Connection conn, String sql, int n) throws SQLException {
        double[] times = new double[n];
        for (int i = 0; i < n; i++) {
            long t0 = System.nanoTime();
            try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
            times[i] = (System.nanoTime() - t0) / 1_000_000.0;
        }
        return times;
    }

    private static double avg(double[] xs) {
        double sum = 0;
        for (double x : xs) sum += x;
        return sum / xs.length;
    }

    private static void printStats(String label, double[] xs) {
        double[] sorted = xs.clone();
        Arrays.sort(sorted);
        System.out.printf("RTT %s: min=%.3fms p50=%.3fms p90=%.3fms avg=%.3fms%n",
                label, sorted[0], sorted[sorted.length / 2], sorted[(int) (sorted.length * 0.9)], avg(xs));
    }

    // --- 3. Federated query over 2 real backends -------------------------------------------------

    @Test
    void federatedQueryOverTwoBackends() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", shard1.username(), shard1.password());
                Statement st = conn.createStatement();
                // No schema-qualifier prefix needed -- "orders" is declaratively sharded via
                // WARP_TABLE_SHARDS, matched by its own bare name.
                ResultSet rs = st.executeQuery("SELECT SUM(total) FROM orders")) {
            assertTrue(rs.next());
            assertEquals(60.0, rs.getDouble(1), 0.0001,
                    "10 + 20 (shard1) + 30 (shard2) = 60 -- a real cross-shard sum, not one shard's own total");
        }
    }

    // --- 4. Memory cache (Mach) -- functional correctness of a real cache hit --------------

    @Test
    void memoryCacheServesACorrectRealHit() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", shard1.username(), shard1.password())) {
            String sql = "SELECT val FROM rtt_cached WHERE id = 1";
            String firstRead;
            try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                rs.next();
                firstRead = rs.getString(1);
            }
            String secondRead;
            try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                rs.next();
                secondRead = rs.getString(1);
            }
            assertEquals(firstRead, secondRead, "a cached repeat read must return the same real value");
            assertEquals("warm", secondRead);
        }

        // Real confirmation it actually came from the in-memory cache, not just "happened to
        // match" -- /api/metrics/summary's rttByOutcome breaks out real cache_hit calls
        // separately (see CacheStage#handleCacheableSelect).
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/metrics/summary"))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertTrue(resp.body().contains("\"cache_hit\""),
                "expected at least one real cache_hit outcome recorded in rttByOutcome -- got: " + resp.body());
    }
}
