package com.sayonora.migration.cli;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.sayonora.migration.checkpoint.CdcCheckpointStore;
import com.sayonora.migration.checkpoint.DeadLetterStore;
import com.sayonora.migration.connectors.mongo.MongoSource;
import com.sayonora.migration.coordinator.Coordinator;
import com.sayonora.migration.sink.WarpGrpcSink;
import com.sayonora.migration.sink.ResilientSink;

/**
 * Standalone entry point: migrate one MongoDB collection to a running Warp instance, over
 * Warp's own gRPC driver -- never a direct JDBC connection to the target Postgres. Run as a
 * separate process from Warp itself, the same way any other native-driver client would be.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code SOURCE_MONGO_URI} -- connection string for the legacy MongoDB deployment</li>
 *   <li>{@code SOURCE_MONGO_DB}, {@code SOURCE_MONGO_COLLECTION}</li>
 *   <li>{@code WARP_GRPC_HOST}, {@code WARP_GRPC_PORT}</li>
 *   <li>{@code WARP_GRPC_USER}, {@code WARP_GRPC_PASSWORD} -- a real Warp identity
 *       (see {@code WARP_AUTH_CREDENTIALS}), so migration traffic is attributable in
 *       Warp's own audit log like any other client, not an anonymous backdoor</li>
 *   <li>{@code TARGET_MONGO_DB}, {@code TARGET_MONGO_COLLECTION} -- usually the same names as the
 *       source, but kept separate in case a migration deliberately renames on the way in</li>
 *   <li>{@code CHECKPOINT_JDBC_URL}/{@code _USER}/{@code _PASSWORD} -- direct Postgres connection
 *       for {@link CdcCheckpointStore} only (see its own javadoc for why that one store is exempt
     *   from the "always through gRPC" rule)</li>
 * </ul>
 */
public final class MigrateMongoCli {

    private MigrateMongoCli() {
    }

    public static void main(String[] args) throws Exception {
        String sourceUri = require("SOURCE_MONGO_URI");
        String sourceDb = require("SOURCE_MONGO_DB");
        String sourceCollection = require("SOURCE_MONGO_COLLECTION");
        String targetDb = System.getenv().getOrDefault("TARGET_MONGO_DB", sourceDb);
        String targetCollection = System.getenv().getOrDefault("TARGET_MONGO_COLLECTION", sourceCollection);

        String grpcHost = require("WARP_GRPC_HOST");
        int grpcPort = Integer.parseInt(require("WARP_GRPC_PORT"));
        String grpcUser = require("WARP_GRPC_USER");
        String grpcPassword = require("WARP_GRPC_PASSWORD");

        String checkpointJdbcUrl = require("CHECKPOINT_JDBC_URL");
        String checkpointUser = require("CHECKPOINT_JDBC_USER");
        String checkpointPassword = require("CHECKPOINT_JDBC_PASSWORD");

        int parallelism = Integer.parseInt(System.getenv().getOrDefault("MIGRATION_PARALLELISM", "4"));
        int partitionCount = Integer.parseInt(System.getenv().getOrDefault("MIGRATION_PARTITION_COUNT", "1"));
        String shardKeyField = System.getenv().getOrDefault("MIGRATION_SHARD_KEY_FIELD", "_id");
        int maxRetries = Integer.parseInt(System.getenv().getOrDefault("MIGRATION_MAX_RETRIES", "5"));
        long retryBackoffMillis = Long.parseLong(System.getenv().getOrDefault("MIGRATION_RETRY_BACKOFF_MS", "1000"));

        CdcCheckpointStore checkpoints = new CdcCheckpointStore(checkpointJdbcUrl, checkpointUser, checkpointPassword);
        checkpoints.ensureSchema();
        // Same target Postgres/credentials as the checkpoint store -- a dead letter is
        // migration-infrastructure bookkeeping, exactly like a checkpoint (see DeadLetterStore's
        // own javadoc), so it belongs in the same place, not a separately configured store.
        DeadLetterStore deadLetters = new DeadLetterStore(checkpointJdbcUrl, checkpointUser, checkpointPassword);
        deadLetters.ensureSchema();

        try (MongoClient sourceClient = MongoClients.create(sourceUri);
                MongoSource source = new MongoSource(sourceClient, sourceDb, sourceCollection, targetDb, targetCollection,
                        partitionCount, shardKeyField);
                WarpGrpcSink grpcSink = new WarpGrpcSink(grpcHost, grpcPort, grpcUser, grpcPassword)) {
            ResilientSink sink = new ResilientSink(grpcSink, deadLetters, maxRetries, retryBackoffMillis);
            new Coordinator(source, sink, checkpoints, parallelism).run();
        }
    }

    private static String require(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing required environment variable: " + envVar);
        }
        return value;
    }
}
