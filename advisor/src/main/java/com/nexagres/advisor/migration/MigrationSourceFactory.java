package com.nexagres.advisor.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.nexagres.migration.connectors.dynamo.DynamoSource;
import com.nexagres.migration.connectors.influx.InfluxSource;
import com.nexagres.migration.connectors.mongo.MongoSource;
import com.nexagres.migration.connectors.mssql.SqlServerSource;
import com.nexagres.migration.connectors.mysql.MySqlSource;
import com.nexagres.migration.connectors.neo4j.Neo4jSource;
import com.nexagres.migration.connectors.oracle.OracleSource;
import com.nexagres.migration.connectors.sqs.SqsSource;
import com.nexagres.migration.core.Source;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Turns one {@link MigrationJobRequest}'s {@code connectorType} + {@code sourceConfig} into a
 * real, ready-to-run {@link Source} -- the piece that makes Advisor actually LAUNCH a migration
 * (via {@link MigrationJobRunner}) rather than only reporting on one already running elsewhere
 * (as {@code MigrationStatusStore} has done since this session's earlier Advisor UI work).
 *
 * <p>Every branch below constructs a connector EXACTLY the way its own real-infrastructure
 * integration test in {@code nexagres-migration} does (same client builders, same constructor
 * overloads) -- this class adds no new connector behavior, it's purely the map-of-strings-to-
 * constructor-arguments translation a generic HTTP job-launch endpoint needs that a fixed,
 * per-connector {@code Migrate*Cli} main method (reading fixed-name env vars) doesn't.
 *
 * <p>A connector needing an external client the connector itself doesn't own or close (a
 * {@link MongoClient}, the AWS SDK clients, a Neo4j {@link Driver}) has that client returned
 * alongside the {@link Source} in {@link Built#externalResources()} -- {@link MigrationJobRunner}
 * is responsible for closing them once the run ends, the same ownership contract every real
 * integration test in {@code nexagres-migration} already follows (see e.g. {@code
 * MongoSourceIntegrationTest}'s own try-with-resources).
 */
public final class MigrationSourceFactory {

    /** @param source the constructed connector, ready for {@code ensureTargetSchema}/{@code
     *     listPartitions}/etc.
     * @param externalResources client objects {@link MigrationJobRunner} must close once the run
     *     ends -- empty for a connector that owns its connections internally (MySQL, SQL Server,
     *     Oracle, InfluxDB all open plain JDBC/HTTP per operation from host/port/credentials
     *     fields, no separate client object to track). */
    public record Built(Source source, List<AutoCloseable> externalResources) {
    }

    private MigrationSourceFactory() {
    }

    public static Built build(MigrationConnectorType type, Map<String, String> cfg) {
        return switch (type) {
            case MONGO -> buildMongo(cfg);
            case MYSQL -> buildMySql(cfg);
            case SQLSERVER -> buildSqlServer(cfg);
            case ORACLE -> buildOracle(cfg);
            case DYNAMODB -> buildDynamo(cfg);
            case SQS -> buildSqs(cfg);
            case NEO4J -> buildNeo4j(cfg);
            case INFLUXDB -> buildInflux(cfg);
        };
    }

    private static Built buildMongo(Map<String, String> cfg) {
        String uri = require(cfg, "uri", MigrationConnectorType.MONGO);
        String sourceDb = require(cfg, "sourceDb", MigrationConnectorType.MONGO);
        String sourceCollection = require(cfg, "sourceCollection", MigrationConnectorType.MONGO);
        String targetDb = optional(cfg, "targetDb", sourceDb);
        String targetCollection = optional(cfg, "targetCollection", sourceCollection);
        int partitionCount = optionalInt(cfg, "partitionCount", 1);
        String shardKeyField = optional(cfg, "shardKeyField", "_id");

        MongoClient client = MongoClients.create(uri);
        MongoSource source = new MongoSource(client, sourceDb, sourceCollection, targetDb, targetCollection,
                partitionCount, shardKeyField);
        return new Built(source, List.of(client));
    }

    private static Built buildMySql(Map<String, String> cfg) {
        String host = require(cfg, "host", MigrationConnectorType.MYSQL);
        int port = Integer.parseInt(require(cfg, "port", MigrationConnectorType.MYSQL));
        String user = require(cfg, "user", MigrationConnectorType.MYSQL);
        String password = require(cfg, "password", MigrationConnectorType.MYSQL);
        String sourceDatabase = require(cfg, "sourceDatabase", MigrationConnectorType.MYSQL);
        String sourceTable = require(cfg, "sourceTable", MigrationConnectorType.MYSQL);
        int partitionCount = optionalInt(cfg, "partitionCount", 1);
        // Same deterministic derivation MySqlSource's own single-arg constructor uses internally
        // -- see that constructor's own javadoc for why a fixed value isn't safe across
        // concurrent migrations against the same source server (never applies to this job runner,
        // which only ever launches one Coordinator per job, but reusing the exact formula keeps
        // this factory from silently diverging from the connector's own documented default).
        long binlogServerId = 6_000_000_000L + (sourceDatabase + sourceTable).hashCode() % 1_000_000;

        MySqlSource source = new MySqlSource(host, port, user, password, sourceDatabase, sourceTable,
                partitionCount, binlogServerId);
        return new Built(source, List.of());
    }

    private static Built buildSqlServer(Map<String, String> cfg) {
        String host = require(cfg, "host", MigrationConnectorType.SQLSERVER);
        int port = Integer.parseInt(require(cfg, "port", MigrationConnectorType.SQLSERVER));
        String user = require(cfg, "user", MigrationConnectorType.SQLSERVER);
        String password = require(cfg, "password", MigrationConnectorType.SQLSERVER);
        String sourceDatabase = require(cfg, "sourceDatabase", MigrationConnectorType.SQLSERVER);
        String sourceSchema = require(cfg, "sourceSchema", MigrationConnectorType.SQLSERVER);
        String sourceTable = require(cfg, "sourceTable", MigrationConnectorType.SQLSERVER);
        int partitionCount = optionalInt(cfg, "partitionCount", 1);

        SqlServerSource source = new SqlServerSource(host, port, user, password, sourceDatabase, sourceSchema,
                sourceTable, partitionCount);
        return new Built(source, List.of());
    }

    private static Built buildOracle(Map<String, String> cfg) {
        String host = require(cfg, "host", MigrationConnectorType.ORACLE);
        int port = Integer.parseInt(require(cfg, "port", MigrationConnectorType.ORACLE));
        String serviceName = require(cfg, "serviceName", MigrationConnectorType.ORACLE);
        String user = require(cfg, "user", MigrationConnectorType.ORACLE);
        String password = require(cfg, "password", MigrationConnectorType.ORACLE);
        String sourceSchema = require(cfg, "sourceSchema", MigrationConnectorType.ORACLE);
        String sourceTable = require(cfg, "sourceTable", MigrationConnectorType.ORACLE);
        int partitionCount = optionalInt(cfg, "partitionCount", 1);

        OracleSource source = new OracleSource(host, port, serviceName, user, password, sourceSchema, sourceTable,
                partitionCount);
        return new Built(source, List.of());
    }

    private static Built buildDynamo(Map<String, String> cfg) {
        String endpoint = require(cfg, "endpoint", MigrationConnectorType.DYNAMODB);
        Region region = Region.of(optional(cfg, "region", "us-east-1"));
        String accessKey = require(cfg, "accessKey", MigrationConnectorType.DYNAMODB);
        String secretKey = require(cfg, "secretKey", MigrationConnectorType.DYNAMODB);
        String sourceTable = require(cfg, "sourceTable", MigrationConnectorType.DYNAMODB);
        int partitionCount = optionalInt(cfg, "partitionCount", 1);

        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey));
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(credentials)
                .build();
        DynamoDbStreamsClient streamsClient = DynamoDbStreamsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(credentials)
                .build();
        DynamoSource source = new DynamoSource(client, streamsClient, sourceTable, partitionCount);
        return new Built(source, List.of(client, streamsClient));
    }

    private static Built buildSqs(Map<String, String> cfg) {
        String endpoint = require(cfg, "endpoint", MigrationConnectorType.SQS);
        Region region = Region.of(optional(cfg, "region", "us-east-1"));
        String accessKey = require(cfg, "accessKey", MigrationConnectorType.SQS);
        String secretKey = require(cfg, "secretKey", MigrationConnectorType.SQS);
        String queueUrl = require(cfg, "queueUrl", MigrationConnectorType.SQS);
        String queueName = require(cfg, "queueName", MigrationConnectorType.SQS);

        SqsClient client = SqsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
        SqsSource source = new SqsSource(client, queueUrl, queueName);
        return new Built(source, List.of(client));
    }

    /** {@code relationshipSpecs} format: {@code "FromLabel:TYPE:ToLabel"} entries separated by
     * {@code ;} -- e.g. {@code "Person:WORKS_AT:Company;Person:KNOWS:Person"}. */
    private static Built buildNeo4j(Map<String, String> cfg) {
        String boltUri = require(cfg, "boltUri", MigrationConnectorType.NEO4J);
        String user = require(cfg, "user", MigrationConnectorType.NEO4J);
        String password = require(cfg, "password", MigrationConnectorType.NEO4J);
        List<String> nodeLabels = splitList(require(cfg, "nodeLabels", MigrationConnectorType.NEO4J), ",");
        List<Neo4jSource.RelationshipSpec> relationshipSpecs = new ArrayList<>();
        for (String entry : splitList(optional(cfg, "relationshipSpecs", ""), ";")) {
            String[] parts = entry.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("NEO4J relationshipSpecs entry must be "
                        + "\"FromLabel:TYPE:ToLabel\", got: " + entry);
            }
            relationshipSpecs.add(new Neo4jSource.RelationshipSpec(parts[0], parts[1], parts[2]));
        }

        Driver driver = GraphDatabase.driver(boltUri, AuthTokens.basic(user, password));
        Neo4jSource source = new Neo4jSource(driver, nodeLabels, relationshipSpecs);
        return new Built(source, List.of(driver));
    }

    private static Built buildInflux(Map<String, String> cfg) {
        String host = require(cfg, "host", MigrationConnectorType.INFLUXDB);
        int port = Integer.parseInt(require(cfg, "port", MigrationConnectorType.INFLUXDB));
        String database = require(cfg, "database", MigrationConnectorType.INFLUXDB);
        String measurement = require(cfg, "measurement", MigrationConnectorType.INFLUXDB);
        Set<String> tagKeys = Set.copyOf(splitList(require(cfg, "tagKeys", MigrationConnectorType.INFLUXDB), ","));

        InfluxSource source = new InfluxSource(host, port, database, measurement, tagKeys);
        return new Built(source, List.of());
    }

    private static List<String> splitList(String csv, String separator) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(separator)).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static String require(Map<String, String> cfg, String key, MigrationConnectorType type) {
        String value = cfg == null ? null : cfg.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(type + " migration requires sourceConfig[\"" + key + "\"]");
        }
        return value;
    }

    private static String optional(Map<String, String> cfg, String key, String defaultValue) {
        String value = cfg == null ? null : cfg.get(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int optionalInt(Map<String, String> cfg, String key, int defaultValue) {
        String value = cfg == null ? null : cfg.get(key);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }
}
