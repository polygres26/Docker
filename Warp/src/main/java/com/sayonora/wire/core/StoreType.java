package com.sayonora.wire.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The protocol stores a Postgres backend can be asked to HOST. Enabling one on a Postgres backend
 * (see {@link BackendRegistry#storeHosts}) makes that protocol's frontend keep its data in that
 * backend; when several backends of one backend set enable the same store the frontend shards
 * across them with the same hash machinery ({@link ShardingStrategy}) the older
 * {@code WARP_SHARD_BACKENDS} mode used.
 *
 * @param id          wire/API identifier ({@code influxdb}, {@code mongodb}, ...)
 * @param label       human label for the admin UI
 * @param setEnvVar   env var naming the backend set the protocol frontend serves
 *                    ({@code WARP_<PROTO>_SET}); blank = the set holding the {@code default} backend
 * @param shardable   {@code false} when the store cannot be spread over several backends of one set
 *                    (Neo4j/graph: traversals across shards are not sound)
 * @param description one-line description shown in the admin UI
 */
public enum StoreType {
    INFLUXDB("influxdb", "InfluxDB", "WARP_INFLUXWIRE_SET", true,
            "InfluxDB line-protocol writes and InfluxQL reads; points are kept in one table per measurement."),
    MONGODB("mongodb", "MongoDB", "WARP_MONGOWIRE_SET", true,
            "MongoDB wire protocol; documents are kept as jsonb rows, one table per collection."),
    SQS("sqs", "SQS", "WARP_SQSWIRE_SET", true,
            "Amazon SQS API; each queue is one table and lives wholly on one backend."),
    NEO4J("neo4j", "Neo4j", "WARP_BOLTWIRE_SET", false,
            "Neo4j Bolt protocol and Cypher; the graph lives in two tables. One backend per set only."),
    OPENSEARCH("opensearch", "OpenSearch", "WARP_OSWIRE_SET", true,
            "OpenSearch documents, search and bulk API; one table per index."),
    DYNAMODB("dynamodb", "DynamoDB", "WARP_DYNAMOWIRE_SET", true,
            "DynamoDB API; items are kept as rows, one table per DynamoDB table."),
    S3("s3", "S3", "WARP_S3WIRE_SET", true,
            "Amazon S3 API; objects are stored as chunked rows and sharded by key."),
    REDIS("redis", "Redis", "WARP_REDISWIRE_SET", true,
            "Redis protocol (RESP2/RESP3); keys are spread over the hosts by Redis Cluster hash slot, so multi-key "
                    + "commands need all keys on one host (same {hash tag}) or fail with CROSSSLOT."),
    AZBLOB("azblob", "Azure Blob", "WARP_AZBLOBWIRE_SET", true,
            "Azure Blob Storage REST API; blobs are stored as chunked rows and sharded by container/blob name."),
    AZQUEUE("azqueue", "Azure Queue", "WARP_AZQUEUEWIRE_SET", true,
            "Azure Queue Storage REST API; each queue lives wholly on one backend."),
    AZTABLE("aztable", "Azure Table", "WARP_AZTABLEWIRE_SET", true,
            "Azure Table Storage REST API (OData JSON); entities are sharded by table and PartitionKey."),
    GCS("gcs", "Google Cloud Storage", "WARP_GCSWIRE_SET", true,
            "Google Cloud Storage JSON and XML APIs; objects are stored as chunked rows and sharded by bucket/object name."),
    SNS("sns", "SNS", "WARP_SNSWIRE_SET", true,
            "Amazon SNS API (Query and JSON); a topic and its subscriptions live wholly on one backend (hash of the topic name)."),
    KINESIS("kinesis", "Kinesis", "WARP_KINESISWIRE_SET", true,
            "Amazon Kinesis Data Streams API (JSON and CBOR); a stream, its shards and records live wholly on one backend."),
    AWSPARAMS("awsparams", "Secrets, SSM, KMS, STS", "WARP_AWSPARAMSWIRE_SET", true,
            "Secrets Manager, SSM Parameter Store, KMS keys and STS sessions; each secret, parameter and key is placed by "
                    + "hash of its name or key id, aliases and STS sessions on the first backend.");

    private final String id;
    private final String label;
    private final String setEnvVar;
    private final boolean shardable;
    private final String description;

    StoreType(String id, String label, String setEnvVar, boolean shardable, String description) {
        this.id = id;
        this.label = label;
        this.setEnvVar = setEnvVar;
        this.shardable = shardable;
        this.description = description;
    }

    public String id() {
        return id;
    }

    public String label() {
        return label;
    }

    public String setEnvVar() {
        return setEnvVar;
    }

    public boolean shardable() {
        return shardable;
    }

    public String description() {
        return description;
    }

    /** @throws IllegalArgumentException for an unknown store name (message lists the valid ones) */
    public static StoreType parse(String name) {
        String n = name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
        for (StoreType t : values()) {
            if (t.id.equals(n)) {
                return t;
            }
        }
        throw new IllegalArgumentException("unknown store \"" + name + "\" -- expected one of " + ids());
    }

    public static List<String> ids() {
        List<String> out = new ArrayList<>();
        for (StoreType t : values()) {
            out.add(t.id);
        }
        return out;
    }
}
