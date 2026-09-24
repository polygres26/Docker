package com.sayonora.wire.core;

public enum SourceDialect {
    ORACLE, POSTGRES, MYSQL, SNOWFLAKE, REDSHIFT, BIGQUERY, DATABRICKS, SQL_SERVER, GENERIC_REST, NL,
    /** gRPC's own native driver protocol (WarpGrpcServer/QueryServiceImpl) -- plain SQL text,
     * already in Postgres dialect, no translation needed. See {@link #MCP} for the sibling label
     * used by the separate MCP execute_sql path, which used to share this same constant and
     * therefore was indistinguishable from gRPC traffic in every metrics view. */
    WARP_NATIVE,
    /** The MCP execute_sql tool's own dispatch path (AdHocQueryRunner) -- split out from
     * {@link #WARP_NATIVE} so gRPC and MCP traffic show up separately in metrics instead of
     * both aggregating under one "warp_native" label. */
    MCP,
    /** A real DynamoDB table set, mounted only as a federated schema -- see {@link
     * com.sayonora.wire.core.connector.dynamodb.DynamoSchemaFactory}. Never a translation target
     * and never routable on its own (no JDBC driver); {@link BackendTarget#dialect()} returns this
     * for a {@code dynamodb://} backend URL. */
    DYNAMODB,
    /** As {@link #DYNAMODB}, for a {@code mongodb://}/{@code mongodb+srv://} backend -- see {@link
     * com.sayonora.wire.core.connector.mongo.MongoSchemaFactory}. */
    MONGODB,
    /** A real S3 (or S3-compatible: MinIO/Wasabi/etc.) object exposed as a federated table -- see
     * {@link com.sayonora.wire.core.connector.s3.S3SchemaFactory}. Federation-only, same as {@link
     * #DYNAMODB}/{@link #MONGODB}: no JDBC driver exists for object storage. */
    S3,
    /** A real Kafka topic read as a bounded snapshot -- see {@link
     * com.sayonora.wire.core.connector.kafka.KafkaSchemaFactory}. Federation-only. */
    KAFKA,
    /** A real Cassandra table -- see {@link
     * com.sayonora.wire.core.connector.cassandra.CassandraSchemaFactory}. Federation-only. A plain
     * {@code ScannableTable} full scan by default, guarded (see that package's javadoc) against an
     * un-opted-in full-cluster scan. */
    CASSANDRA,
    /** A real Splunk saved SPL search -- see {@link
     * com.sayonora.wire.core.connector.splunk.SplunkSchemaFactory}. Federation-only. */
    SPLUNK,
    /**
     * The broader JDBC-dialect catalog below -- {@code CLICKHOUSE}/{@code DRUID}/{@code PINOT}/
     * {@code DB2}/{@code VERTICA}/{@code SINGLESTORE}/{@code HANA}/{@code SYNAPSE}/{@code
     * TERADATA}/{@code SPANNER}/{@code TRINO}/{@code SQLITE}, joining the pre-existing {@code
     * SNOWFLAKE}/{@code REDSHIFT}/{@code BIGQUERY}/{@code DATABRICKS} values above -- ported from
     * the sibling Omnigate project's own {@code SourceDialect} javadoc, same real-bundled-driver
     * posture: each is reachable via a real JDBC driver ({@link BackendDriverRegistry
     * #driverClassNameFor}), but none has native-statistics or query-history support yet (the same
     * honest gap {@code SNOWFLAKE}/{@code REDSHIFT}/{@code BIGQUERY}/{@code DATABRICKS} already
     * have) -- real, separate follow-on work per dialect, not attempted here. All backend-only: no
     * wire-protocol frontend speaks any of these; a client only ever REACHES Warp as Postgres,
     * Oracle, MySQL, or SQL Server (or the native gRPC/MCP paths) -- these values only ever appear
     * as a {@link BackendTarget#dialect()} result.
     *
     * <p>{@code DRUID} connects over Avatica's own remote protocol ({@code
     * jdbc:avatica:remote:url=...}), not a Druid-specific driver -- {@code avatica-core} is already
     * a transitive dependency of {@code calcite-core} itself, so no new dependency is needed for it.
     *
     * <p>{@code SYNAPSE} needs no new driver at all: an Azure Synapse SQL pool speaks the identical
     * T-SQL/TDS protocol {@code mssql-jdbc} already handles for {@link #SQL_SERVER} -- {@link
     * BackendTarget#dialect()} distinguishes it from plain SQL Server only by the {@code
     * .sql.azuresynapse.net} hostname convention, purely so dialect-aware code can tell the two
     * apart; connectivity itself is identical.
     *
     * <p>Two connectors from the same upstream investigation were deliberately NOT added, and
     * should not be re-attempted without addressing the reason: Amazon Athena's only Maven Central
     * artifact under a plausible name ({@code com.amazonaws:athena-jdbc}) is AWS's own Athena Query
     * Federation SDK (for building a connector Athena calls INTO other databases), not a client
     * driver for connecting TO Athena -- the real client driver is Simba's, distributed outside
     * Maven Central under AWS's own terms, a genuine vendoring decision not made here. Dremio's
     * legacy driver is hosted only on Dremio's own Maven repository (not Central); the Maven
     * Central alternative, Apache Arrow's Flight SQL JDBC driver, was tried and found live to
     * self-register a global {@code java.security.Provider} via its own {@code META-INF/services}
     * entry -- a process-wide side effect that broke TLS/HTTP behavior for unrelated code the
     * moment it was merely on the classpath. Both remain real, separate follow-on decisions.
     */
    CLICKHOUSE, DRUID, PINOT, DB2, VERTICA, SINGLESTORE, HANA,
    SYNAPSE, TERADATA, SPANNER, TRINO, SQLITE
}
