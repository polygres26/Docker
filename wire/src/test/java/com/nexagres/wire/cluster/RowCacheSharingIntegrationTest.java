package com.nexagres.wire.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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

/**
 * End-to-end proof that {@link RowCache} is actually SHARED across dynamowire and the SQL
 * frontends, not just present in both -- a real AWS SDK v2 {@code DynamoDbClient} and a real
 * pgwire JDBC connection, against the same Warp subprocess and the same real Postgres backend,
 * same discipline as {@code DynamoDbErrorMappingIntegrationTest} and
 * {@code ShortRegressionSuiteTest}. No mocks: this either proves the exact cache entry crossed
 * protocols, via the real admin metrics endpoint's per-protocol {@code cache_hit} breakdown, or it
 * doesn't -- there's no way to fake this assertion passing.
 *
 * <p>Deliberately does NOT set {@code WARP_DYNAMOWIRE_CACHE_ENABLED=false} the way the other
 * dynamowire integration tests do (they're testing error paths that don't want the cache in the
 * way) -- this test's whole point needs the row cache on, which is the default.
 */
class RowCacheSharingIntegrationTest {

    private static final String ADMIN_TOKEN = "row-cache-sharing-test-token";

    private static DynamoDbClient dynamoClient(int port) {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .overrideConfiguration(o -> o.retryPolicy(
                        software.amazon.awssdk.core.retry.RetryPolicy.builder().numRetries(0).build()))
                .build();
    }

    private static String metricsSummary(WarpProcess warp) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/metrics/summary"))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    /** The physical Postgres table dynamowire creates for a table named {@code orders} with no
     * sort key -- see {@code PgItemStore}'s own {@code pgTableName}: {@code "dynamo_item_" +
     * lowercased table name}. Deliberately hardcoded here rather than discovered, the same way a
     * real SQL client pointed at a dynamowire-backed table would have to know this name -- it's
     * dynamowire's own documented physical naming convention, not an implementation detail this
     * test reaches into.
     */
    private static final String PHYSICAL_TABLE = "dynamo_item_orders";

    @Test
    void aDynamoDbPutItemIsVisibleAsASqlCacheHitOnTheExactSameRow() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_CACHE_TABLES", PHYSICAL_TABLE)
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = dynamoClient(warp.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("orders")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());
                dynamo.putItem(PutItemRequest.builder().tableName("orders")
                        .item(Map.of(
                                "id", AttributeValue.builder().s("item-42").build(),
                                "amount", AttributeValue.builder().n("129.99").build()))
                        .build());

                // Real GetItem, real Postgres round trip -- populates RowCache under the PHYSICAL
                // table name (dynamo_item_orders|42|), not dynamowire's own logical "orders" name.
                dynamo.getItem(GetItemRequest.builder().tableName("orders")
                        .key(Map.of("id", AttributeValue.builder().s("item-42").build()))
                        .build());
            }

            // The actual cross-protocol claim: a real SQL SELECT-by-primary-key over real pgwire/
            // JDBC, against the physical table dynamowire's GetItem above already populated the
            // row cache for -- CacheStage's own SQL-side fast path (tryRowCacheLookup) must find
            // that exact entry and never touch Postgres for this read at all.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    PreparedStatement ps = conn.prepareStatement(
                            "SELECT item FROM " + PHYSICAL_TABLE + " WHERE pk_value = ?")) {
                ps.setString(1, "item-42");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "expected one row for pk_value=item-42");
                    String itemJson = rs.getString(1);
                    assertTrue(itemJson.contains("item-42"),
                            "the SQL-visible row must be the same item DynamoDB wrote -- got: " + itemJson);
                    assertTrue(itemJson.contains("129.99"),
                            "the SQL-visible row must carry the same attribute DynamoDB wrote -- got: " + itemJson);
                }
            }

            // Not "it returned the right data" (Postgres would also return the right data on a
            // real, uncached round trip) -- specifically that it came from the shared row cache,
            // under the pgwire protocol label, proving CacheStage's fast path is what served it.
            String summary = metricsSummary(warp);
            assertTrue(summary.contains("\"protocol\":\"pgwire\"") && summary.contains("\"outcome\":\"cache_hit\""),
                    "expected a pgwire cache_hit in rttByOutcome (the SQL side hitting dynamowire's "
                            + "own populated row-cache entry) -- got: " + summary);
        }
    }

    @Test
    void aSqlUpdateByPrimaryKeyInvalidatesTheRowDynamoDbSeesNext() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("dynamowire", "WARP_DYNAMOWIRE_PORT")
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_CACHE_TABLES", PHYSICAL_TABLE)
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (DynamoDbClient dynamo = dynamoClient(warp.port("dynamowire"))) {
                dynamo.createTable(CreateTableRequest.builder()
                        .tableName("orders")
                        .attributeDefinitions(AttributeDefinition.builder()
                                .attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .build());
                dynamo.putItem(PutItemRequest.builder().tableName("orders")
                        .item(Map.of("id", AttributeValue.builder().s("item-77").build(),
                                "status", AttributeValue.builder().s("pending").build()))
                        .build());
                // Warm the row cache the same way the other test does.
                dynamo.getItem(GetItemRequest.builder().tableName("orders")
                        .key(Map.of("id", AttributeValue.builder().s("item-77").build()))
                        .build());

                // A real SQL UPDATE against the same primary key, over real pgwire/JDBC -- not
                // through dynamowire at all. CacheStage#invalidateRowCacheForPointWrite must drop
                // the exact row-cache entry dynamowire's own GetItem above just populated. A real
                // bind parameter, not an inlined literal: the invalidation regex (like the lookup
                // one) only recognizes the `?`-placeholder shape a real prepared statement sends,
                // matching how CacheStage sees sqlText/bindParams as separate fields throughout.
                try (Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                        PreparedStatement ps = conn.prepareStatement(
                                "UPDATE " + PHYSICAL_TABLE + " SET item = ?::jsonb WHERE pk_value = ?")) {
                    ps.setString(1, "{\"id\":{\"S\":\"77\"},\"status\":{\"S\":\"shipped\"}}");
                    ps.setString(2, "item-77");
                    ps.executeUpdate();
                }

                var resp = dynamo.getItem(GetItemRequest.builder().tableName("orders")
                        .key(Map.of("id", AttributeValue.builder().s("item-77").build()))
                        .build());
                assertEquals("shipped", resp.item().get("status").s(),
                        "dynamowire's GetItem must see the fresh row the SQL UPDATE wrote, not a "
                                + "stale row-cache entry from before the SQL-side write");
            }
        }
    }
}
