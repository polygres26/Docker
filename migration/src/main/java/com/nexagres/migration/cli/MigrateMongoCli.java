package com.nexagres.migration.cli;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.connectors.mongo.MongoSource;
import com.nexagres.migration.coordinator.Coordinator;
import com.nexagres.migration.sink.PolywireGrpcSink;

/**
 * Standalone entry point: migrate one MongoDB collection to a running Polywire instance, over
 * Polywire's own gRPC driver -- never a direct JDBC connection to the target Postgres. Run as a
 * separate process from Polywire itself, the same way any other native-driver client would be.
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code SOURCE_MONGO_URI} -- connection string for the legacy MongoDB deployment</li>
 *   <li>{@code SOURCE_MONGO_DB}, {@code SOURCE_MONGO_COLLECTION}</li>
 *   <li>{@code POLYWIRE_GRPC_HOST}, {@code POLYWIRE_GRPC_PORT}</li>
 *   <li>{@code POLYWIRE_GRPC_USER}, {@code POLYWIRE_GRPC_PASSWORD} -- a real Polywire identity
 *       (see {@code POLYWIRE_AUTH_CREDENTIALS}), so migration traffic is attributable in
 *       Polywire's own audit log like any other client, not an anonymous backdoor</li>
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

        String grpcHost = require("POLYWIRE_GRPC_HOST");
        int grpcPort = Integer.parseInt(require("POLYWIRE_GRPC_PORT"));
        String grpcUser = require("POLYWIRE_GRPC_USER");
        String grpcPassword = require("POLYWIRE_GRPC_PASSWORD");

        String checkpointJdbcUrl = require("CHECKPOINT_JDBC_URL");
        String checkpointUser = require("CHECKPOINT_JDBC_USER");
        String checkpointPassword = require("CHECKPOINT_JDBC_PASSWORD");

        int parallelism = Integer.parseInt(System.getenv().getOrDefault("MIGRATION_PARALLELISM", "4"));

        CdcCheckpointStore checkpoints = new CdcCheckpointStore(checkpointJdbcUrl, checkpointUser, checkpointPassword);
        checkpoints.ensureSchema();

        try (MongoClient sourceClient = MongoClients.create(sourceUri);
                MongoSource source = new MongoSource(sourceClient, sourceDb, sourceCollection, targetDb, targetCollection);
                PolywireGrpcSink sink = new PolywireGrpcSink(grpcHost, grpcPort, grpcUser, grpcPassword)) {
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
