package com.sayonora.wire.core.connector.s3;

import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Real S3 (and S3-compatible: MinIO/Wasabi/OCI/GCS-interop) access as a federated Warp schema -- a
 * port of the sibling ThinkingSense project's {@code com.omnigate.calcite.s3.S3SchemaFactory} (real,
 * MinIO-verified there), so a Parquet/CSV/XML object in a bucket can be JOINed against a
 * Postgres/Oracle/etc. table in one statement through {@code SchemaFederationStage}. Operand shape
 * (built from an {@code s3://} {@code WARP_BACKENDS} entry by {@link
 * com.sayonora.wire.core.connector.ConnectorOperands}, see its javadoc for the text grammar):
 * <pre>{@code
 * {"bucket": "my-bucket", "provider": "s3" (or omitted), "region": "...", "endpoint": "..." (optional,
 *  MinIO/S3-compatible), "pathStyleAccess": true/false, "accessKeyId": "...", "secretAccessKey": "..."
 *  (optional pair -- else the SDK default credential chain),
 *  "tables": {"object": {"key": "path/to/file.parquet", "format": "parquet",
 *                         "recordElement": null, "pushdownColumns": "id,ts"}}}
 * }</pre>
 *
 * <p><b>One exact object key per table, no glob/prefix-union</b> -- {@link
 * com.sayonora.wire.core.connector.ConnectorOperands} enforces this at the config grammar level (a
 * {@code key=} query parameter, not a prefix), matching this connector's approved scope.
 *
 * <p><b>Real, structural Parquet pushdown; CSV/XML full parse every time (cached)</b> -- see
 * {@link S3Table}'s own javadoc for exactly how and why. No S3 Select.
 *
 * <p>Implements Calcite's {@link SchemaFactory} for fidelity with the source, but Warp calls
 * {@link #createSchema} directly from {@code SchemaFederationStage} -- no model-file/reflection
 * loading, same posture as {@code DynamoSchemaFactory}/{@code MongoSchemaFactory}.
 */
public final class S3SchemaFactory implements SchemaFactory {

    public static final S3SchemaFactory INSTANCE = new S3SchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    /** Builds a fresh {@link S3Schema} (and its own {@link ObjectFetcher}/S3 client) -- the caller
     * owns it and must {@link S3Schema#close()} it once the federated statement is done. {@code
     * secretAccessKey} is resolved through {@code SecretResolver} here, on every call, so a rotated
     * vault/CyberArk secret takes effect on the next mount -- same posture as {@code
     * DynamoSchemaFactory#createSchema}. */
    @SuppressWarnings("unchecked")
    public static S3Schema createSchema(Map<String, Object> operand) {
        Object tablesObj = operand.get("tables");
        if (!(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("S3SchemaFactory requires at least one table -- declare its "
                    + "exact object 'key' and 'format' on the s3:// backend URL");
        }
        String bucket = stringOrNull(operand, "bucket");
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("S3SchemaFactory requires string operand 'bucket'");
        }
        ObjectFetcher fetcher = buildFetcher(bucket, operand);
        return new S3Schema(fetcher, (Map<String, Map<String, String>>) tablesObj);
    }

    /** An S3 client for the backend an operand map describes (same region/endpoint/path-style/
     * credential handling as the federated mount); also what the MCP endpoint's S3 tools use. The
     * caller owns and must close it. */
    public static software.amazon.awssdk.services.s3.S3Client openClient(Map<String, Object> operand) {
        return S3CompatibleObjectFetcher.buildClient(stringOrNull(operand, "region"),
                stringOrNull(operand, "endpoint"), Boolean.TRUE.equals(operand.get("pathStyleAccess")),
                stringOrNull(operand, "accessKeyId"),
                com.sayonora.wire.secrets.SecretResolver.resolve(stringOrNull(operand, "secretAccessKey")));
    }

    private static ObjectFetcher buildFetcher(String bucket, Map<String, Object> operand) {
        String provider = stringOrNull(operand, "provider");
        if (provider != null && !provider.equalsIgnoreCase("s3") && !provider.equalsIgnoreCase("s3-compatible")) {
            throw new IllegalArgumentException(
                    "S3SchemaFactory: unknown provider \"" + provider + "\" -- expected \"s3\" or \"s3-compatible\"");
        }
        String region = stringOrNull(operand, "region");
        String endpoint = stringOrNull(operand, "endpoint");
        boolean pathStyleAccess = Boolean.TRUE.equals(operand.get("pathStyleAccess"));
        String accessKeyId = stringOrNull(operand, "accessKeyId");
        String secretAccessKey = com.sayonora.wire.secrets.SecretResolver.resolve(stringOrNull(operand, "secretAccessKey"));
        return new S3CompatibleObjectFetcher(bucket, region, endpoint, pathStyleAccess, accessKeyId, secretAccessKey);
    }

    private static String stringOrNull(Map<String, Object> operand, String key) {
        return operand.get(key) instanceof String s ? s : null;
    }
}
