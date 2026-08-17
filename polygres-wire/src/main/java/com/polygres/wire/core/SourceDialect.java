package com.polygres.wire.core;

/**
 * Every SQL dialect PolyWire can either receive a statement in (a real frontend session) or route
 * a statement to (a registered {@link BackendTarget}). Not every value can be both: {@code NL} and
 * {@code POLYWIRE_NATIVE} only ever appear as {@link Statement#sourceDialect()} (no frontend
 * listener exists for a natural-language or native-driver *backend*), and {@code SNOWFLAKE}/
 * {@code REDSHIFT}/{@code BIGQUERY}/{@code DATABRICKS} only ever appear as a {@link
 * BackendTarget#dialect()} result (no wire-protocol frontend exists for any of them — clients only
 * ever reach PolyWire as Oracle, Postgres, or MySQL, or via the native gRPC driver/NL2SQL).
 *
 * <p>{@code GENERIC_REST} is the dialect of any {@code jdbc:calcite:...}-backed target — a
 * {@link BackendTarget} fronting {@code com.polygres.wire.calcite.RestSchemaFactory}'s minimal generic
 * REST adapter (ARCHITECTURE.md §5.5c). Like the other backend-only dialects, it only ever appears
 * as a {@link BackendTarget#dialect()} result, never as a {@link Statement#sourceDialect()} — no
 * frontend produces "generic REST"-dialected SQL.
 *
 * <p>{@code SQL_SERVER} joins {@code SNOWFLAKE}/{@code REDSHIFT}/{@code BIGQUERY}/{@code
 * DATABRICKS} as backend-only (ARCHITECTURE.md §41's SQL history harvesting extension) — no
 * wire-protocol frontend speaks SQL Server's TDS protocol, only {@code jdbc:sqlserver:...}
 * {@link BackendTarget}s reached via the bundled {@code mssql-jdbc} driver.
 *
 * <p>{@link DialectTranslations} looks up a normalizer/renderer pair keyed by a {@code (source,
 * target)} dialect drawn from this enum — see that class's javadoc for which pairs actually have
 * rules today.
 */
public enum SourceDialect {
    ORACLE, POSTGRES, MYSQL, SNOWFLAKE, REDSHIFT, BIGQUERY, DATABRICKS, SQL_SERVER, GENERIC_REST, NL, POLYWIRE_NATIVE
}
