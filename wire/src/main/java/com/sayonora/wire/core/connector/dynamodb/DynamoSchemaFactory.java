package com.sayonora.wire.core.connector.dynamodb;

import java.net.URI;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Real DynamoDB access as a federated Warp schema -- a port of the sibling ThinkingSense project's
 * {@code com.omnigate.calcite.dynamodb.DynamoSchemaFactory} (real, tested there), so a DynamoDB
 * table can be JOINed against a Postgres/Oracle/etc. table in one statement through {@code
 * SchemaFederationStage}. Operand shape (built from a {@code dynamodb://} {@code WARP_BACKENDS}
 * entry by {@link com.sayonora.wire.core.connector.ConnectorOperands}, see its javadoc for the
 * text grammar):
 * <pre>{@code
 * {"region": "us-east-1", "endpoint": "http://localhost:8000" (optional),
 *  "accessKeyId": "...", "secretAccessKey": "..." (optional pair -- else the SDK default chain),
 *  "tables": {"orders": {"table": "Orders", "fields": ["orderId","customer","amount"]}}}
 * }</pre>
 *
 * <p>Why {@code fields} is required and explicit, why every column is {@code VARCHAR}, and why there
 * is no predicate pushdown: a DynamoDB item's non-key attributes are schemaless, and an attribute's
 * {@code AttributeValue} type can differ per item -- a string-typed equality filter pushed down as
 * a {@code FilterExpression} would silently miss items where the attribute is stored as {@code N}.
 * No pushdown is strictly better than a wrong answer; Calcite filters the stringified rows instead.
 * (Same reasoning as {@link com.sayonora.wire.core.connector.mongo.MongoTable}'s javadoc, which has
 * the full version.)
 *
 * <p>Implements Calcite's {@link SchemaFactory} for fidelity with the source, but Warp calls {@link
 * #createSchema} directly from {@code SchemaFederationStage} -- no model-file/reflection loading.
 * ThinkingSense's {@code statisticsFor} ({@code DescribeTable.itemCount}) isn't ported: Warp's
 * {@code StatisticsStore} is JDBC/{@code pg_class}-based, so there is no seam for it yet.
 */
public final class DynamoSchemaFactory implements SchemaFactory {

    public static final DynamoSchemaFactory INSTANCE = new DynamoSchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    /** Builds a fresh {@link DynamoSchema} (and its own {@link DynamoDbClient}) -- the caller owns
     * it and must {@link DynamoSchema#close()} it once the federated statement is done. {@code
     * secretAccessKey} is resolved through {@code SecretResolver} here, on every call, so a rotated
     * vault/CyberArk secret takes effect on the next mount -- same posture as {@code
     * BackendTarget.borrow()}'s JDBC password. */
    @SuppressWarnings("unchecked")
    public static DynamoSchema createSchema(Map<String, Object> operand) {
        Object tablesObj = operand.get("tables");
        if (!(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("DynamoSchemaFactory requires at least one table -- declare each "
                    + "exposed table and its explicit field list as table.<sqlName>=<field>,<field>,... on the "
                    + "dynamodb:// backend URL (DynamoDB is schemaless, there is nothing to auto-discover)");
        }
        return new DynamoSchema(buildClient(operand), (Map<String, Map<String, Object>>) tablesObj);
    }

    static DynamoDbClient buildClient(Map<String, Object> operand) {
        String region = stringOrNull(operand, "region");
        String endpoint = stringOrNull(operand, "endpoint");
        String accessKeyId = stringOrNull(operand, "accessKeyId");
        String secretAccessKey = com.sayonora.wire.secrets.SecretResolver.resolve(stringOrNull(operand, "secretAccessKey"));

        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of(region == null || region.isBlank() ? "us-east-1" : region))
                .credentialsProvider(credentialsProvider(accessKeyId, secretAccessKey));
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    private static AwsCredentialsProvider credentialsProvider(String accessKeyId, String secretAccessKey) {
        if (accessKeyId != null && !accessKeyId.isBlank() && secretAccessKey != null && !secretAccessKey.isBlank()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        // No explicit key pair: the SDK's own default chain (env vars, ~/.aws, instance/task role).
        return DefaultCredentialsProvider.builder().build();
    }

    private static String stringOrNull(Map<String, Object> operand, String key) {
        return operand.get(key) instanceof String s ? s : null;
    }
}
