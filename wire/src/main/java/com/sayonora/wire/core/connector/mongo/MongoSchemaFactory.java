package com.sayonora.wire.core.connector.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Real MongoDB collection access as a federated Warp schema -- a port of the sibling ThinkingSense
 * project's {@code com.omnigate.calcite.mongo.MongoSchemaFactory} (real, tested there). Deliberately
 * NOT Calcite's own calcite-mongodb adapter: that one is compiled against mongodb-driver-sync 4.10.2
 * and throws a real NoSuchMethodError against the 5.x driver this project's mongowire/bson lockstep
 * requires (see pom.xml) -- this class only uses the sync driver's {@code find()}, stable across 5.x.
 * Operand shape (built from a {@code mongodb://} {@code WARP_BACKENDS} entry by {@link
 * com.sayonora.wire.core.connector.ConnectorOperands}):
 * <pre>{@code
 * {"connectionString": "mongodb://host:27017/mydb", "user": "...", "password": "..." (optional),
 *  "tables": {"orders": {"database": "mydb", "collection": "orders", "fields": ["_id","customer"]}}}
 * }</pre>
 * {@code fields} is required and explicit: collections are schemaless, so there's no catalog to
 * introspect columns from. Every field surfaces as {@code VARCHAR} -- see {@link MongoTable}.
 * ThinkingSense's {@code statisticsFor} ({@code estimatedDocumentCount}) isn't ported: Warp's
 * {@code StatisticsStore} is JDBC/{@code pg_class}-based, so there's no seam for it yet.
 */
public final class MongoSchemaFactory implements SchemaFactory {

    public static final MongoSchemaFactory INSTANCE = new MongoSchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return createSchema(operand);
    }

    /** Builds a fresh {@link MongoSchema} with its own {@link MongoClient}; the caller owns it and
     * must {@link MongoSchema#close()} it. The password is resolved through {@code SecretResolver}
     * on every call (vault:/cyberark: references), never cached. */
    @SuppressWarnings("unchecked")
    public static MongoSchema createSchema(Map<String, Object> operand) {
        Object tablesObj = operand.get("tables");
        if (!(tablesObj instanceof Map<?, ?> tables) || tables.isEmpty()) {
            throw new IllegalArgumentException("MongoSchemaFactory requires at least one collection -- declare each "
                    + "exposed collection and its explicit field list as table.<sqlName>=<field>,<field>,... on "
                    + "the mongodb:// backend URL (MongoDB is schemaless, there is nothing to auto-discover)");
        }
        for (Map.Entry<?, ?> entry : tables.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> def && def.get("database") == null) {
                throw new IllegalArgumentException("MongoSchemaFactory: collection \"" + entry.getKey() + "\" has no "
                        + "database -- put it in the connection string's path (mongodb://host:port/DATABASE_NAME)");
            }
        }
        return new MongoSchema(openClient(operand), (Map<String, Map<String, Object>>) tablesObj);
    }

    /** A client for the backend an operand map describes -- same connection-string + credential
     * (incl. vault:/cyberark: password resolution) handling the federated mount uses; also what the
     * MCP endpoint's MongoDB tools connect with. The caller owns and must close it. */
    public static MongoClient openClient(Map<String, Object> operand) {
        return openClient(operand, 0);
    }

    /** As {@link #openClient(Map)}, with a bounded server-selection timeout (unless the URL sets
     * its own {@code serverSelectionTimeoutMS}) so interactive callers fail fast on an unreachable
     * server instead of waiting the driver's 30 s default. {@code 0} keeps the driver default. */
    public static MongoClient openClient(Map<String, Object> operand, long serverSelectionTimeoutMs) {
        String connectionString = requireString(operand, "connectionString");
        String withCreds = withCredentials(connectionString,
                operand.get("user") instanceof String u ? u : null,
                com.sayonora.wire.secrets.SecretResolver.resolve(operand.get("password") instanceof String p ? p : null));
        if (serverSelectionTimeoutMs <= 0 || withCreds.toLowerCase(java.util.Locale.ROOT).contains("serverselectiontimeoutms")) {
            return MongoClients.create(withCreds);
        }
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(withCreds))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(serverSelectionTimeoutMs,
                        java.util.concurrent.TimeUnit.MILLISECONDS))
                .build());
    }

    /** Splices {@code user:password@} into the connection string (URL-encoded, per the Mongo
     * connection-string spec). A connection string that already carries its own credentials, or no
     * user configured, is returned unchanged. */
    static String withCredentials(String connectionString, String user, String password) {
        if (user == null || user.isBlank()) {
            return connectionString;
        }
        int schemeEnd = connectionString.indexOf("://") + 3;
        int hostEnd = connectionString.indexOf('/', schemeEnd);
        String authority = hostEnd < 0 ? connectionString.substring(schemeEnd) : connectionString.substring(schemeEnd, hostEnd);
        if (authority.contains("@")) {
            return connectionString;
        }
        String creds = URLEncoder.encode(user, StandardCharsets.UTF_8)
                + (password == null ? "" : ":" + URLEncoder.encode(password, StandardCharsets.UTF_8));
        return connectionString.substring(0, schemeEnd) + creds + "@" + connectionString.substring(schemeEnd);
    }

    private static String requireString(Map<String, Object> map, String key) {
        if (!(map.get(key) instanceof String s) || s.isBlank()) {
            throw new IllegalArgumentException("MongoSchemaFactory requires string field '" + key + "'");
        }
        return s;
    }
}
