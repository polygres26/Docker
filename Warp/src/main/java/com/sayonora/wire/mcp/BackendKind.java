package com.sayonora.wire.mcp;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The tool FAMILY a backend belongs to. Every backend has a specific type string (postgres, mysql,
 * oracle, mongodb, dynamodb, s3, ... -- see {@link BackendTypes}); the family decides which tool
 * vocabulary applies to it. {@link #RELATIONAL} covers every JDBC dialect and is today's SQL
 * toolset, unchanged. Kafka/Cassandra/Splunk have no data tools of their own on the MCP endpoint
 * (they are reachable as federated SQL sources) but are still listed and described.
 *
 * <p>Tools are associated with backends automatically from the types in an endpoint's scope. The
 * legacy {@code WARP_MCP_KIND} environment variable survives only as an override/filter (see
 * {@code WarpMcpServer}): when set it restricts the endpoint to those families, and with more than
 * one family the non-relational tools are advertised prefixed with {@link #prefix()}. A request
 * path of {@code /kinds/<kind>} narrows one request to a single family.
 */
public enum BackendKind {
    RELATIONAL("relational", ""),
    DYNAMODB("dynamodb", "dynamodb_"),
    INFLUX("influx", "influx_"),
    MONGODB("mongodb", "mongodb_"),
    S3("s3", "s3_"),
    KAFKA("kafka", "kafka_"),
    CASSANDRA("cassandra", "cassandra_"),
    SPLUNK("splunk", "splunk_"),
    /** Warp-hosted stores with no MCP data tools of their own yet: listed and described only. */
    SQS("sqs", "sqs_"),
    OPENSEARCH("opensearch", "opensearch_"),
    NEO4J("neo4j", "neo4j_"),
    /** The Warp-hosted S3 object store (the {@code s3} store of a Postgres backend): described only. */
    S3STORE("s3store", "s3store_"),
    /** The Warp-hosted Redis store (the {@code redis} store of a Postgres backend): described only. */
    REDIS("redis", "redis_"),
    /** Warp-hosted Azure Storage stores (described only). */
    AZBLOB("azblob", "azblob_"),
    AZQUEUE("azqueue", "azqueue_"),
    AZTABLE("aztable", "aztable_"),
    /** The Warp-hosted Google Firestore and Datastore stores (described only). */
    FIRESTORE("firestore", "firestore_"),
    DATASTORE("datastore", "datastore_"),
    /** The Warp-hosted Google Cloud Storage store (described only). */
    GCS("gcs", "gcs_"),
    /** The Warp-hosted Google Pub/Sub store (described only). */
    PUBSUB("pubsub", "pubsub_"),
    /** The Warp-hosted SNS, Kinesis and Secrets/SSM/KMS/STS stores (described only). */
    SNS("sns", "sns_"),
    KINESIS("kinesis", "kinesis_"),
    AWSPARAMS("awsparams", "awsparams_");

    private final String id;
    private final String prefix;

    BackendKind(String id, String prefix) {
        this.id = id;
        this.prefix = prefix;
    }

    public String id() {
        return id;
    }

    /** Tool-name prefix used only in legacy multi-kind ({@code WARP_MCP_KIND=a,b}) mode. */
    public String prefix() {
        return prefix;
    }

    public static BackendKind parse(String name) {
        String n = name.trim().toLowerCase(Locale.ROOT);
        return switch (n) {
            case "relational", "sql", "postgres", "postgresql" -> RELATIONAL;
            case "dynamodb", "dynamo" -> DYNAMODB;
            case "influx", "influxdb" -> INFLUX;
            case "mongodb", "mongo" -> MONGODB;
            case "s3" -> S3;
            case "kafka" -> KAFKA;
            case "cassandra" -> CASSANDRA;
            case "splunk" -> SPLUNK;
            case "sqs" -> SQS;
            case "opensearch", "os" -> OPENSEARCH;
            case "neo4j" -> NEO4J;
            case "s3store" -> S3STORE;
            case "redis", "valkey" -> REDIS;
            case "azblob" -> AZBLOB;
            case "azqueue" -> AZQUEUE;
            case "aztable" -> AZTABLE;
            case "firestore" -> FIRESTORE;
            case "datastore" -> DATASTORE;
            case "gcs" -> GCS;
            case "pubsub" -> PUBSUB;
            case "sns" -> SNS;
            case "kinesis" -> KINESIS;
            case "awsparams", "secretsmanager", "ssm", "kms", "sts" -> AWSPARAMS;
            default -> throw new IllegalArgumentException("WARP_MCP_KIND has an unknown kind \"" + name
                    + "\" -- expected one or more of " + String.join(", ", ids()));
        };
    }

    /** Parses a comma-separated kind list; {@code null}/blank = every kind (the automatic default). */
    public static Set<BackendKind> parseList(String spec) {
        if (spec == null || spec.isBlank()) {
            return EnumSet.allOf(BackendKind.class);
        }
        Set<BackendKind> kinds = new LinkedHashSet<>();
        for (String part : spec.split(",")) {
            if (!part.isBlank()) {
                kinds.add(parse(part));
            }
        }
        return kinds.isEmpty() ? EnumSet.allOf(BackendKind.class) : kinds;
    }

    /** {@code true} when {@code WARP_MCP_KIND} is set (the backward-compatible override/filter). */
    public static boolean explicitFromEnv() {
        String v = System.getenv("WARP_MCP_KIND");
        return v != null && !v.isBlank();
    }

    public static Set<BackendKind> fromEnv() {
        return parseList(System.getenv("WARP_MCP_KIND"));
    }

    public static List<String> ids() {
        List<String> out = new ArrayList<>();
        for (BackendKind k : values()) {
            out.add(k.id);
        }
        return out;
    }
}
