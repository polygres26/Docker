package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @param connectorOperand non-null only for a non-JDBC connector backend ({@link
 *     SourceDialect#DYNAMODB}/{@link SourceDialect#MONGODB}) -- the parsed operand map {@link
 *     com.sayonora.wire.core.connector.ConnectorOperands#parse} built from this target's own
 *     pseudo-URL, handed to the connector's schema factory at federated-mount time. Secret-bearing
 *     values in it are still unresolved {@code vault:}/{@code cyberark:} references at this point,
 *     resolved fresh per mount (see the factories), same "never cache a resolved secret on this
 *     record" posture {@link #borrow()} already has for a JDBC password. {@code null} for every
 *     JDBC backend.
 */
public record BackendTarget(String name, String jdbcUrl, String user, String password,
        com.sayonora.wire.server.ServerOptions failoverOptions, String fallbackName,
        java.util.Map<String, Object> connectorOperand) {

    public BackendTarget(String name, String jdbcUrl, String user, String password) {
        this(name, jdbcUrl, user, password, null, null, null);
    }

    public BackendTarget(String name, String jdbcUrl, String user, String password,
            com.sayonora.wire.server.ServerOptions failoverOptions, String fallbackName) {
        this(name, jdbcUrl, user, password, failoverOptions, fallbackName, null);
    }

    public BackendTarget(String name, String jdbcUrl, String user, String password,
            com.sayonora.wire.server.ServerOptions failoverOptions) {
        this(name, jdbcUrl, user, password, failoverOptions, null, null);
    }

    /** @return the real target dialect for this backend's own JDBC URL, driving both {@link
     *     DialectTranslationStage} (what a client's own source-dialect SQL gets translated INTO)
     *     and {@link BackendDriverRegistry} (which real driver class actually opens the
     *     connection) -- {@code null} for an unrecognized URL prefix, which {@link
     *     DialectTranslationStage} treats as "pass the SQL through untranslated," not an error. */
    public SourceDialect dialect() {
        String url = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return SourceDialect.POSTGRES;
        }
        if (url.startsWith("jdbc:oracle:")) {
            return SourceDialect.ORACLE;
        }
        if (url.startsWith("jdbc:sqlserver:")) {
            // Azure Synapse speaks identical T-SQL/TDS -- mssql-jdbc handles it exactly like plain
            // SQL Server, distinguished only by hostname convention, purely so dialect-aware code
            // (translation, future stats/history work) can tell the two apart. Checked BEFORE
            // returning the generic SQL_SERVER value below.
            if (url.contains(".sql.azuresynapse.net")) {
                return SourceDialect.SYNAPSE;
            }
            return SourceDialect.SQL_SERVER;
        }
        if (url.startsWith("jdbc:mysql:") || url.startsWith("jdbc:mariadb:")) {
            return SourceDialect.MYSQL;
        }
        // The broader JDBC-dialect catalog -- see SourceDialect's own javadoc for the full
        // reasoning (ported from the sibling Omnigate project). All real, bundled JDBC drivers
        // (BackendDriverRegistry.driverClassNameFor mirrors this exact prefix list); no wire
        // frontend speaks any of these, so they only ever appear as a dialect() result, never a
        // Statement#sourceDialect().
        if (url.startsWith("jdbc:snowflake:")) {
            return SourceDialect.SNOWFLAKE;
        }
        if (url.startsWith("jdbc:redshift:")) {
            return SourceDialect.REDSHIFT;
        }
        if (url.startsWith("jdbc:bigquery:")) {
            return SourceDialect.BIGQUERY;
        }
        if (url.startsWith("jdbc:databricks:")) {
            return SourceDialect.DATABRICKS;
        }
        if (url.startsWith("jdbc:clickhouse:")) {
            return SourceDialect.CLICKHOUSE;
        }
        if (url.startsWith("jdbc:avatica:remote:")) {
            // Druid's own SQL access is over the Avatica remote protocol, not a Druid-specific
            // driver -- see SourceDialect#DRUID's own javadoc.
            return SourceDialect.DRUID;
        }
        if (url.startsWith("jdbc:pinot:")) {
            return SourceDialect.PINOT;
        }
        if (url.startsWith("jdbc:db2:")) {
            return SourceDialect.DB2;
        }
        if (url.startsWith("jdbc:vertica:")) {
            return SourceDialect.VERTICA;
        }
        if (url.startsWith("jdbc:singlestore:")) {
            return SourceDialect.SINGLESTORE;
        }
        if (url.startsWith("jdbc:sap:")) {
            return SourceDialect.HANA;
        }
        if (url.startsWith("jdbc:teradata:")) {
            return SourceDialect.TERADATA;
        }
        if (url.startsWith("jdbc:cloudspanner:")) {
            return SourceDialect.SPANNER;
        }
        if (url.startsWith("jdbc:trino:")) {
            return SourceDialect.TRINO;
        }
        if (url.startsWith("jdbc:sqlite:")) {
            return SourceDialect.SQLITE;
        }
        // Non-JDBC connector backends -- federation-only (see isFederationOnlyConnector below).
        if (url.startsWith("dynamodb://")) {
            return SourceDialect.DYNAMODB;
        }
        if (url.startsWith("mongodb://") || url.startsWith("mongodb+srv://")) {
            return SourceDialect.MONGODB;
        }
        if (url.startsWith("s3://")) {
            return SourceDialect.S3;
        }
        if (url.startsWith("kafka://")) {
            return SourceDialect.KAFKA;
        }
        if (url.startsWith("cassandra://")) {
            return SourceDialect.CASSANDRA;
        }
        if (url.startsWith("splunk://")) {
            return SourceDialect.SPLUNK;
        }
        return null;
    }

    /** {@code true} for a backend with no JDBC driver at all (DynamoDB, MongoDB, S3, Kafka,
     * Cassandra, Splunk) -- these are only ever reachable as a mounted schema inside a {@link
     * SchemaFederationStage} federated query, never through {@link #open()}/{@link #borrow()}. */
    public boolean isFederationOnlyConnector() {
        SourceDialect d = dialect();
        return d == SourceDialect.DYNAMODB || d == SourceDialect.MONGODB || d == SourceDialect.S3
                || d == SourceDialect.KAFKA || d == SourceDialect.CASSANDRA || d == SourceDialect.SPLUNK;
    }

    public Connection open() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(true);
        return connection;
    }

    /** As {@link #open()}, but prefers this target's configured standby for the connection --
     * see {@link com.sayonora.wire.pgwire.PgConnections#openForRead}. Falls back to {@link #open()}
     * when this target has no {@code failoverOptions} (i.e. no standby concept at all, the plain
     * {@code BackendConnectionPools}-only path) -- there's nothing to prefer in that case. */
    public Connection openPreferringStandby() throws SQLException {
        if (failoverOptions == null) {
            return open();
        }
        Connection connection = com.sayonora.wire.pgwire.PgConnections.openForRead(failoverOptions);
        connection.setAutoCommit(true);
        return connection;
    }

    /** The key this target's connections are pooled under in {@link BackendConnectionPools} --
     * shared with {@link #borrow()} so a drain/undrain admin call can address the exact same pool
     * a live session would borrow from. Meaningless (and unused) for a {@code failoverOptions}
     * target, since that path doesn't go through {@code BackendConnectionPools} at all. */
    public String poolKey() {
        return BackendConnectionPools.poolKeyFor(jdbcUrl, user);
    }

    public Connection openManualCommit() throws SQLException {
        Connection connection = borrow();
        connection.setAutoCommit(false);
        return connection;
    }

    private Connection borrow() throws SQLException {
        // Defense in depth behind RoutingBackendExecutor's own check: a DynamoDB/Mongo target has
        // no JDBC driver, so letting it reach BackendConnectionPools/HikariCP would throw a raw
        // "No suitable driver" RuntimeException (fatal to a whole pgwire session, not an in-band
        // SQL error -- the real bug RoutingBackendExecutor's own comment describes). Any caller
        // that reaches here with one (admin test-connection, XA, stats) gets the same clear,
        // actionable error instead.
        if (isFederationOnlyConnector()) {
            throw ErrorCatalog.sqlException(routingUnsupportedErrorKey(dialect()), name);
        }
        if (failoverOptions != null) {
            return com.sayonora.wire.pgwire.PgConnections.open(failoverOptions);
        }

        // password may be a "vault:..."/"cyberark:..." reference, not a literal -- resolved on
        // every borrow (not cached on this record) so a rotated secret takes effect on the next
        // connection attempt rather than requiring a restart. A literal password round-trips
        // through SecretRef.parse/SecretResolver.resolve as a no-op.
        String resolvedPassword = com.sayonora.wire.secrets.SecretResolver.resolve(password);
        String poolKey = BackendConnectionPools.poolKeyFor(jdbcUrl, user);
        BackendConnectionPools.registerBackendAlias(name, poolKey);
        return BackendConnectionPools.borrow(poolKey, jdbcUrl, user, resolvedPassword);
    }

    /** The {@code ERR_*_ROUTING_UNSUPPORTED} catalog key for a given federation-only dialect --
     * shared by {@link #borrow()} and {@link RoutingBackendExecutor}, which hits the identical
     * "this backend has no JDBC driver, route it through a federated JOIN instead" case at a
     * different call site. */
    static String routingUnsupportedErrorKey(SourceDialect dialect) {
        return switch (dialect) {
            case MONGODB -> "ERR_MONGO_ROUTING_UNSUPPORTED";
            case DYNAMODB -> "ERR_DYNAMODB_ROUTING_UNSUPPORTED";
            case S3 -> "ERR_S3_ROUTING_UNSUPPORTED";
            case KAFKA -> "ERR_KAFKA_ROUTING_UNSUPPORTED";
            case CASSANDRA -> "ERR_CASSANDRA_ROUTING_UNSUPPORTED";
            case SPLUNK -> "ERR_SPLUNK_ROUTING_UNSUPPORTED";
            default -> throw new IllegalArgumentException("not a federation-only connector dialect: " + dialect);
        };
    }
}
