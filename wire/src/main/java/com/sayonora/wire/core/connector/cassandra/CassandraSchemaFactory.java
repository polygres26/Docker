package com.sayonora.wire.core.connector.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Real Cassandra access as a federated Warp schema -- a port of the sibling ThinkingSense project's
 * {@code com.omnigate.calcite.cassandra.CassandraSchemaFactory} (real, tested there), so a Cassandra
 * table can be JOINed against a Postgres/Oracle/etc. table in one statement through {@code
 * SchemaFederationStage}. Operand shape (built from a {@code cassandra://} {@code WARP_BACKENDS}
 * entry by {@link com.sayonora.wire.core.connector.ConnectorOperands}, see its javadoc for the text
 * grammar):
 * <pre>{@code
 * {"contactPoints": ["host:9042"], "localDatacenter": "datacenter1",
 *  "username": "..." (optional), "password": "..." (optional),
 *  "tables": {"orders": {"keyspace": "mykeyspace", "table": "orders",
 *                         "partitionKeyEquals": "order_id" (or "allowFullScan": true)}}}
 * }</pre>
 *
 * <p>Unlike Mongo, Cassandra tables have a real, typed schema (CQL DDL) -- {@link CassandraTable}
 * introspects real column definitions via the driver's own {@code TableMetadata}, so columns get
 * real Calcite types rather than {@code MongoTable}'s blanket {@code VARCHAR}.
 *
 * <p><b>No predicate pushdown, ever</b> -- CQL itself requires either a full partition-key equality
 * match or an explicit {@code ALLOW FILTERING} clause for any other {@code WHERE} clause; correctly
 * determining which pushed predicates satisfy the partition key is real, separate follow-on work,
 * not attempted here.
 *
 * <p><b>REQUIRED full-scan guard</b> -- see {@link CassandraTable}'s own javadoc for the full
 * mechanism and why a construction-time operator opt-in ({@code partitionKeyEquals} or {@code
 * allowFullScan}) is the guard this connector actually implements, given a plain {@link
 * org.apache.calcite.schema.ScannableTable}'s real interface constraints.
 *
 * <p>Read-only, matching every other connector in this codebase. Implements Calcite's {@link
 * SchemaFactory} for fidelity with the source, but Warp calls {@link #createSchema} directly from
 * {@code SchemaFederationStage}.
 */
public final class CassandraSchemaFactory implements SchemaFactory {

    public static final CassandraSchemaFactory INSTANCE = new CassandraSchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    @SuppressWarnings("unchecked")
    public static CassandraSchema createSchema(Map<String, Object> operand) {
        Object tablesObj = operand.get("tables");
        if (!(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("CassandraSchemaFactory requires at least one table -- declare "
                    + "'keyspace'/'table' (plus 'partitionKeyEquals' or 'allowFullScan=true') on the "
                    + "cassandra:// backend URL");
        }
        CqlSession session = buildSession(operand);
        return new CassandraSchema(session, (Map<String, Map<String, Object>>) tablesObj);
    }

    @SuppressWarnings("unchecked")
    private static CqlSession buildSession(Map<String, Object> operand) {
        Object contactPointsObj = operand.get("contactPoints");
        if (!(contactPointsObj instanceof List)) {
            throw new IllegalArgumentException("CassandraSchemaFactory requires list operand 'contactPoints'");
        }
        List<String> contactPoints = (List<String>) contactPointsObj;
        String localDatacenter = requireString(operand, "localDatacenter");

        CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter(localDatacenter);
        for (String contactPoint : contactPoints) {
            builder.addContactPoint(parseHostPort(contactPoint));
        }
        String username = stringOrNull(operand, "username");
        String password = com.sayonora.wire.secrets.SecretResolver.resolve(stringOrNull(operand, "password"));
        if (username != null && !username.isBlank() && password != null && !password.isBlank()) {
            builder.withAuthCredentials(username, password);
        }
        return builder.build();
    }

    private static InetSocketAddress parseHostPort(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException(
                    "CassandraSchemaFactory: contact point \"" + hostPort + "\" must be \"host:port\"");
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        return new InetSocketAddress(host, port);
    }

    private static String requireString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof String s) || s.isBlank()) {
            throw new IllegalArgumentException("CassandraSchemaFactory requires string operand '" + key + "'");
        }
        return s;
    }

    private static String stringOrNull(Map<String, Object> operand, String key) {
        return operand.get(key) instanceof String s ? s : null;
    }
}
