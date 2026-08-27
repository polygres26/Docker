package com.polygres.wire.core;

/**
 * Real JDBC driver-class lookup for a {@link BackendTarget}'s own {@code jdbcUrl} -- the one thing
 * {@link ShardJoinExecutor}/{@link SchemaFederationStage}/{@link RollupStage} each need before they
 * can mount a backend as a Calcite {@code JdbcSchema} (Calcite's own {@code JdbcSchema.dataSource}
 * needs an explicit driver class name, not just a URL). Used to be a hardcoded
 * {@code "org.postgresql.Driver"} literal at each of those 3 call sites -- extracted here, once,
 * so a real second (Oracle) and later third/fourth (SQL Server, MySQL) backend engine is a one-line
 * addition in one place, not three.
 *
 * <p>Mirrors {@link BackendTarget#dialect()}'s own URL-prefix dispatch (same shape, same real
 * ordering of engine support) rather than introducing a second, competing way to detect what engine
 * a URL points at.
 */
final class BackendDriverRegistry {

    private BackendDriverRegistry() {
    }

    /** @return the real JDBC driver class name for {@code jdbcUrl}, or {@code null} for an
     *     unrecognized prefix -- callers throw their own real, specific error rather than this
     *     class guessing a fallback that would silently connect as the wrong engine. */
    static String driverClassNameFor(String jdbcUrl) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        }
        if (url.startsWith("jdbc:oracle:")) {
            return "oracle.jdbc.OracleDriver";
        }
        if (url.startsWith("jdbc:sqlserver:")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        if (url.startsWith("jdbc:mysql:") || url.startsWith("jdbc:mariadb:")) {
            return "com.mysql.cj.jdbc.Driver";
        }
        return null;
    }

    /** @return {@code schemaName} adjusted to match how {@code jdbcUrl}'s own engine really
     *     stores an unquoted identifier in ITS OWN catalog -- {@code JdbcSchema}'s backend-side
     *     introspection (the {@code catalog}/{@code schema} constructor args) needs an EXACT match
     *     against the real stored name, regardless of how case-insensitively Calcite's own SQL
     *     parser lets a client write the name in a query. Real bug, found live against a real
     *     Oracle backend: an unquoted {@code CREATE USER customers_db} (or any unquoted DDL) folds
     *     to uppercase in Oracle's own catalog (unlike Postgres, which folds unquoted identifiers
     *     to LOWERCASE) -- passing the lowercase config name straight through produced a real
     *     "Object 'customers' not found within 'customers_db'" from Calcite, since it never found
     *     a schema literally named lowercase {@code customers_db} to look inside. Postgres is a
     *     no-op here (already lowercase, matching the config convention every existing
     *     {@code POLYWIRE_ROUTER_SCHEMA_RULES}/{@code POLYWIRE_SHARD_BACKENDS} entry already
     *     uses). */
    static String realCatalogSchemaName(String jdbcUrl, String schemaName) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:oracle:")) {
            return schemaName == null ? null : schemaName.toUpperCase(java.util.Locale.ROOT);
        }
        return schemaName;
    }
}
