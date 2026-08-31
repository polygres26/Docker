package com.nexagres.dms.migration;

import java.util.Map;

/**
 * The JSON body {@code POST /api/migration/jobs} accepts -- plain mutable fields, no-arg
 * constructor, same Gson-friendly shape {@link com.nexagres.dms.core.ConnectionRecord} uses
 * elsewhere in this module, deserialized straight off the request body.
 *
 * <p>{@code targetConnectionId} and the {@code polywireGrpc*} fields are deliberately separate:
 * the former is a saved {@link com.nexagres.dms.core.ConnectionRecord} pointing at the TARGET
 * Postgres directly (for {@code CdcCheckpointStore}/{@code DeadLetterStore} bookkeeping, the one
 * documented exception to "every write goes through gRPC" -- see those classes' own javadoc); the
 * latter is the actual Polywire gRPC endpoint every real data write goes through. Two different
 * things a real deployment usually points at the same physical Postgres, but Advisor has no
 * existing concept of "a Polywire instance" as a saved entity, only "a Postgres connection" --
 * asking for the gRPC coordinates explicitly here rather than inventing a second registry.
 *
 * <p>{@code sourceConfig} is a flat string map -- see {@link MigrationSourceFactory} for the exact
 * keys each {@link MigrationConnectorType} requires/accepts. Deliberately not a strongly-typed
 * field per connector: with 7 wildly different connectors (a Mongo URI vs. a Neo4j Bolt URI plus
 * label/relationship lists vs. AWS SDK endpoint/region/credentials), one flat map keeps this
 * request shape -- and the UI form that produces it -- from needing 7 different sub-schemas.
 */
public class MigrationJobRequest {
    public String connectorType;
    public String targetConnectionId;
    public String polywireGrpcHost;
    public int polywireGrpcPort;
    public String polywireGrpcUser;
    public String polywireGrpcPassword;
    public int parallelism = 1;
    /** Overrides {@code MigrationLicensing.DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND} -- {@code
     * null} (the default) means "use the default cap." Setting any non-null value requires
     * Enterprise (see {@code MigrationLicensing#requireEnterpriseForCustomThrottle}); the free
     * tier can't raise, lower, or disable the default, only run at it. */
    public Double maxEventsPerSecond;
    public Map<String, String> sourceConfig;
}
