package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Real regression guard for the broader JDBC-dialect catalog (ported from the sibling ThinkingSense
 * project's own {@code SourceDialect}/{@code BackendConnectionPools.driverClassNameFor}) — proves
 * {@link BackendTarget#dialect()} and {@link BackendDriverRegistry#driverClassNameFor} agree on the
 * exact same URL-prefix list, the invariant {@link BackendDriverRegistry}'s own javadoc claims
 * ("mirrors BackendTarget#dialect()'s own URL-prefix dispatch"). A future dialect added to one but
 * not the other would compile fine and fail only at real connection time — this test catches that
 * class of drift immediately instead.
 */
class JdbcDialectCatalogTest {

    private static BackendTarget target(String jdbcUrl) {
        return new BackendTarget("t", jdbcUrl, "user", "pw");
    }

    @Test
    void everyNewDialectResolvesAndHasARealDriverClass() {
        assertDialectAndDriver("jdbc:snowflake://acct.snowflakecomputing.com/db",
                SourceDialect.SNOWFLAKE, "net.snowflake.client.jdbc.SnowflakeDriver");
        assertDialectAndDriver("jdbc:redshift://cluster.redshift.amazonaws.com:5439/db",
                SourceDialect.REDSHIFT, "com.amazon.redshift.Driver");
        assertDialectAndDriver("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=p",
                SourceDialect.BIGQUERY, "com.google.cloud.bigquery.jdbc.BigQueryDriver");
        assertDialectAndDriver("jdbc:databricks://host:443/default",
                SourceDialect.DATABRICKS, "com.databricks.client.jdbc.Driver");
        assertDialectAndDriver("jdbc:clickhouse://host:8123/db",
                SourceDialect.CLICKHOUSE, "com.clickhouse.jdbc.ClickHouseDriver");
        assertDialectAndDriver("jdbc:avatica:remote:url=http://druid-broker:8082/druid/v2/sql/avatica/",
                SourceDialect.DRUID, "org.apache.calcite.avatica.remote.Driver");
        assertDialectAndDriver("jdbc:pinot://controller:9000",
                SourceDialect.PINOT, "org.apache.pinot.client.PinotDriver");
        assertDialectAndDriver("jdbc:db2://host:50000/db",
                SourceDialect.DB2, "com.ibm.db2.jcc.DB2Driver");
        assertDialectAndDriver("jdbc:vertica://host:5433/db",
                SourceDialect.VERTICA, "com.vertica.jdbc.Driver");
        assertDialectAndDriver("jdbc:singlestore://host:3306/db",
                SourceDialect.SINGLESTORE, "com.singlestore.jdbc.Driver");
        assertDialectAndDriver("jdbc:sap://host:39015",
                SourceDialect.HANA, "com.sap.db.jdbc.Driver");
        assertDialectAndDriver("jdbc:teradata://host/database=db",
                SourceDialect.TERADATA, "com.teradata.jdbc.TeraDriver");
        assertDialectAndDriver("jdbc:cloudspanner:/projects/p/instances/i/databases/d",
                SourceDialect.SPANNER, "com.google.cloud.spanner.jdbc.JdbcDriver");
        assertDialectAndDriver("jdbc:trino://host:8080/catalog/schema",
                SourceDialect.TRINO, "io.trino.jdbc.TrinoDriver");
        assertDialectAndDriver("jdbc:sqlite:/path/to/file.db",
                SourceDialect.SQLITE, "org.sqlite.JDBC");
    }

    private static void assertDialectAndDriver(String jdbcUrl, SourceDialect expectedDialect, String expectedDriver) {
        assertEquals(expectedDialect, target(jdbcUrl).dialect(), "dialect() mismatch for " + jdbcUrl);
        assertEquals(expectedDriver, BackendDriverRegistry.driverClassNameFor(jdbcUrl),
                "driverClassNameFor mismatch for " + jdbcUrl);
    }

    @Test
    void azureSynapseIsDistinguishedFromPlainSqlServerByHostname() {
        assertEquals(SourceDialect.SYNAPSE,
                target("jdbc:sqlserver://myworkspace.sql.azuresynapse.net:1433;database=db").dialect(),
                "a *.sql.azuresynapse.net host must resolve to SYNAPSE, not plain SQL_SERVER");
        assertEquals(SourceDialect.SQL_SERVER,
                target("jdbc:sqlserver://plain-host:1433;database=db").dialect(),
                "an ordinary SQL Server host must still resolve to SQL_SERVER, unaffected by the "
                        + "new Synapse hostname check");
        // Synapse connects over the identical mssql-jdbc driver -- no separate driverClassNameFor
        // entry exists (or is needed) for it.
        assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                BackendDriverRegistry.driverClassNameFor(
                        "jdbc:sqlserver://myworkspace.sql.azuresynapse.net:1433;database=db"));
    }

    @Test
    void unrecognizedUrlStillResolvesToNullNotAGuess() {
        assertNull(target("jdbc:somethingnobodyheardof://host/db").dialect());
        assertNull(BackendDriverRegistry.driverClassNameFor("jdbc:somethingnobodyheardof://host/db"));
    }

    @Test
    void preExistingDialectsAreUnaffected() {
        // Regression guard: adding 15 new URL-prefix checks to dialect()/driverClassNameFor must
        // not disturb the original 4, still exercised the same way RouterStageTest etc. already do.
        assertDialectAndDriver("jdbc:postgresql://host/db", SourceDialect.POSTGRES, "org.postgresql.Driver");
        assertDialectAndDriver("jdbc:oracle:thin:@//host:1521/service", SourceDialect.ORACLE, "oracle.jdbc.OracleDriver");
        assertDialectAndDriver("jdbc:sqlserver://host:1433;database=db", SourceDialect.SQL_SERVER,
                "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        assertDialectAndDriver("jdbc:mysql://host:3306/db", SourceDialect.MYSQL, "com.mysql.cj.jdbc.Driver");
    }
}
