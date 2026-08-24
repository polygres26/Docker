package com.polygres.wire.core;

public enum SourceDialect {
    ORACLE, POSTGRES, MYSQL, SNOWFLAKE, REDSHIFT, BIGQUERY, DATABRICKS, SQL_SERVER, GENERIC_REST, NL,
    /** gRPC's own native driver protocol (PolyWireGrpcServer/QueryServiceImpl) -- plain SQL text,
     * already in Postgres dialect, no translation needed. See {@link #MCP} for the sibling label
     * used by the separate MCP execute_sql path, which used to share this same constant and
     * therefore was indistinguishable from gRPC traffic in every metrics view. */
    POLYWIRE_NATIVE,
    /** The MCP execute_sql tool's own dispatch path (AdHocQueryRunner) -- split out from
     * {@link #POLYWIRE_NATIVE} so gRPC and MCP traffic show up separately in metrics instead of
     * both aggregating under one "polywire_native" label. */
    MCP
}
