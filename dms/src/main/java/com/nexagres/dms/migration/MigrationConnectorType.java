package com.nexagres.dms.migration;

/**
 * The connector roster {@code nexagres-migration} ships (Phase 4 of that module's own build) --
 * one entry per real {@link com.nexagres.migration.core.Source} implementation, matched 1:1 so
 * {@link MigrationSourceFactory} has no branch this enum doesn't cover and vice versa.
 */
public enum MigrationConnectorType {
    MONGO,
    MYSQL,
    SQLSERVER,
    ORACLE,
    DYNAMODB,
    SQS,
    NEO4J,
    INFLUXDB
}
