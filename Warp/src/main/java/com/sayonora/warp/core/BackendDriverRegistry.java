package com.sayonora.warp.core;

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
        // Broader JDBC-dialect catalog -- mirrors BackendTarget#dialect()'s own prefix list
        // exactly (same ordering, same set), ported from the sibling Omnigate project. Each class
        // name confirmed there via the driver jar's own bundled META-INF/services/java.sql.Driver
        // entry, not guessed -- see SourceDialect's own javadoc for the per-dialect dependency and
        // license notes (Databricks/Db2 in particular ship under a non-Apache license, the only
        // way to reach either engine over JDBC at all).
        if (url.startsWith("jdbc:snowflake:")) {
            return "net.snowflake.client.jdbc.SnowflakeDriver";
        }
        if (url.startsWith("jdbc:redshift:")) {
            // Not com.amazon.redshift.jdbc42.Driver (a thin legacy wrapper class).
            return "com.amazon.redshift.Driver";
        }
        if (url.startsWith("jdbc:bigquery:")) {
            return "com.google.cloud.bigquery.jdbc.BigQueryDriver";
        }
        if (url.startsWith("jdbc:databricks:")) {
            return "com.databricks.client.jdbc.Driver";
        }
        if (url.startsWith("jdbc:clickhouse:")) {
            return "com.clickhouse.jdbc.ClickHouseDriver";
        }
        if (url.startsWith("jdbc:avatica:remote:")) {
            // Druid's own SQL access is over the Avatica remote protocol -- avatica-core is
            // already a transitive dependency of calcite-core itself, no extra driver needed.
            return "org.apache.calcite.avatica.remote.Driver";
        }
        if (url.startsWith("jdbc:pinot:")) {
            return "org.apache.pinot.client.PinotDriver";
        }
        if (url.startsWith("jdbc:db2:")) {
            return "com.ibm.db2.jcc.DB2Driver";
        }
        if (url.startsWith("jdbc:vertica:")) {
            return "com.vertica.jdbc.Driver";
        }
        if (url.startsWith("jdbc:singlestore:")) {
            return "com.singlestore.jdbc.Driver";
        }
        if (url.startsWith("jdbc:sap:")) {
            return "com.sap.db.jdbc.Driver";
        }
        if (url.startsWith("jdbc:teradata:")) {
            return "com.teradata.jdbc.TeraDriver";
        }
        if (url.startsWith("jdbc:cloudspanner:")) {
            return "com.google.cloud.spanner.jdbc.JdbcDriver";
        }
        if (url.startsWith("jdbc:trino:")) {
            return "io.trino.jdbc.TrinoDriver";
        }
        if (url.startsWith("jdbc:sqlite:")) {
            return "org.sqlite.JDBC";
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
     *     {@code WARP_ROUTER_SCHEMA_RULES}/{@code WARP_SHARD_BACKENDS} entry already
     *     uses). */
    static String realCatalogSchemaName(String jdbcUrl, String schemaName) {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:oracle:")) {
            return schemaName == null ? null : schemaName.toUpperCase(java.util.Locale.ROOT);
        }
        return schemaName;
    }
}
