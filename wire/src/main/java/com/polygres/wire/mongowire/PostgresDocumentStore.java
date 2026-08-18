package com.polygres.wire.mongowire;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.postgresql.util.PGobject;

/**
 * Maps MongoDB database -&gt; Postgres schema and collection -&gt; Postgres table, same top-level
 * mapping mongo-java-server's postgresql-backend module uses (its {@code PostgresqlDatabase}/
 * {@code PostgresqlCollection}, BSD-3-Clause) — a database name becomes a schema, a collection
 * name becomes a table in it, created lazily with {@code CREATE SCHEMA IF NOT EXISTS}/
 * {@code CREATE TABLE IF NOT EXISTS} on first use.
 *
 * <p><b>Table shape diverges from the reference on purpose</b>, per this task's explicit
 * requirement: the reference stores an opaque {@code json} column plus a synthetic serial
 * {@code id} unrelated to MongoDB's own {@code _id}, and does all filtering by streaming rows
 * back into the JVM. This class instead uses:
 * <pre>{@code CREATE TABLE "<db>"."<collection>" (id text PRIMARY KEY, doc jsonb NOT NULL)}</pre>
 * where {@code id} is a real, indexed column holding the extended-JSON text form of the
 * document's actual {@code _id} (so lookups/updates/deletes by {@code _id} — the overwhelming
 * common case for all four CRUD ops — hit a primary-key index instead of a full scan), and
 * {@code doc} is {@code jsonb} (not {@code json}) specifically so {@link MongoQueryTranslator}'s
 * generated {@code WHERE} clauses can use jsonb's native comparison operators.
 */
final class PostgresDocumentStore {

    private static final Pattern SAFE_NAME = Pattern.compile("^[A-Za-z0-9_]+$");
    private final ConnectionSupplier connections;
    private final ConcurrentHashMap<String, Boolean> ensuredTables = new ConcurrentHashMap<>();

    interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    PostgresDocumentStore(ConnectionSupplier connections) {
        this.connections = connections;
    }

    private static String quoteIdent(String name) {
        if (!SAFE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("mongowire: database/collection names must match [A-Za-z0-9_]+, got \""
                    + name + "\"");
        }
        return "\"" + name + "\"";
    }

    private static String qualifiedTable(String db, String collection) {
        return quoteIdent(db) + "." + quoteIdent(collection);
    }

    private void ensureTable(Connection conn, String db, String collection) throws SQLException {
        String key = db + "." + collection;
        if (ensuredTables.putIfAbsent(key, Boolean.TRUE) != null) {
            return;
        }
        try (var st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + quoteIdent(db));
            st.execute("CREATE TABLE IF NOT EXISTS " + qualifiedTable(db, collection)
                    + " (id text PRIMARY KEY, doc jsonb NOT NULL)");
        }
    }

    private static PGobject jsonb(String json) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        obj.setValue(json);
        return obj;
    }

    /** {@code count} rows affected and the extended-JSON {@code _id} text of each — the exact
     * shape {@code MongoCache} keys on, so a write can invalidate precisely the cache entries it
     * touched instead of the whole collection. */
    record WriteResult(int count, List<String> ids) {}

    /** The same extended-JSON {@code _id} text this store's {@code id} column holds — used by
     * {@code MongoCommandDispatcher} to build/invalidate {@code MongoCache} keys consistently. */
    static String idJsonFor(Object idValue) {
        return BsonJson.valueToJson(new BsonObjectIdOrPassthrough(idValue).toBson());
    }

    /** Inserts one document, generating an ObjectId {@code _id} if the document doesn't have one. */
    Document insertOne(String db, String collection, Document document) throws SQLException {
        if (!document.containsKey("_id")) {
            document.put("_id", new ObjectId());
        }
        String idJson = idJsonFor(document.get("_id"));
        String docJson = BsonJson.toJson(document);
        try (Connection conn = connections.get()) {
            ensureTable(conn, db, collection);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + qualifiedTable(db, collection) + " (id, doc) VALUES (?, ?)")) {
                ps.setString(1, idJson);
                ps.setObject(2, jsonb(docJson));
                ps.executeUpdate();
            }
        }
        return document;
    }

    List<Document> find(String db, String collection, MongoQueryTranslator.Where where, int limit) throws SQLException {
        List<Document> results = new ArrayList<>();
        try (Connection conn = connections.get()) {
            ensureTable(conn, db, collection);
            String sql = "SELECT doc FROM " + qualifiedTable(db, collection) + where.sql()
                    + (limit > 0 ? " LIMIT " + limit : "");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(BsonJson.fromJson(rs.getString(1)));
                    }
                }
            }
        }
        return results;
    }

    /** Full-document replace ({@code $set}-merged document already computed by the caller). */
    WriteResult updateMany(String db, String collection, MongoQueryTranslator.Where where, Document merger, int limit)
            throws SQLException {
        try (Connection conn = connections.get()) {
            ensureTable(conn, db, collection);
            String selectSql = "SELECT id, doc FROM " + qualifiedTable(db, collection) + where.sql()
                    + (limit > 0 ? " LIMIT " + limit : "");
            List<String> ids = new ArrayList<>();
            List<String> newDocs = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Document existing = BsonJson.fromJson(rs.getString(2));
                        UpdateApplier.apply(existing, merger);
                        ids.add(rs.getString(1));
                        newDocs.add(BsonJson.toJson(existing));
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + qualifiedTable(db, collection) + " SET doc = ? WHERE id = ?")) {
                for (int i = 0; i < ids.size(); i++) {
                    ps.setObject(1, jsonb(newDocs.get(i)));
                    ps.setString(2, ids.get(i));
                    ps.addBatch();
                }
                if (!ids.isEmpty()) {
                    ps.executeBatch();
                }
            }
            return new WriteResult(ids.size(), ids);
        }
    }

    /** Selects the affected {@code id}s first (needed both to scope {@code limit} and to report
     * exactly which cache keys a write touched), then deletes precisely those rows — rather than
     * a single filter-scoped {@code DELETE}, so the caller always learns which {@code _id}s were
     * removed regardless of what shape the filter was. */
    WriteResult deleteMany(String db, String collection, MongoQueryTranslator.Where where, int limit) throws SQLException {
        try (Connection conn = connections.get()) {
            ensureTable(conn, db, collection);
            String selectSql = "SELECT id FROM " + qualifiedTable(db, collection) + where.sql()
                    + (limit > 0 ? " LIMIT " + limit : "");
            List<String> ids = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                bindParams(ps, where.jsonbParams());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ids.add(rs.getString(1));
                    }
                }
            }
            if (ids.isEmpty()) {
                return new WriteResult(0, List.of());
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + qualifiedTable(db, collection) + " WHERE id = ANY(?)")) {
                ps.setArray(1, conn.createArrayOf("text", ids.toArray()));
                int n = ps.executeUpdate();
                return new WriteResult(n, ids);
            }
        }
    }

    private void bindParams(PreparedStatement ps, List<String> jsonbParams) throws SQLException {
        int i = 1;
        for (String p : jsonbParams) {
            ps.setObject(i++, jsonb(p));
        }
    }

    /** Tiny local helper so a raw {@code ObjectId}/String/Number stored under "_id" both serialize consistently. */
    private record BsonObjectIdOrPassthrough(Object value) {
        org.bson.BsonValue toBson() {
            if (value instanceof ObjectId oid) {
                return new BsonObjectId(oid);
            }
            return new Document("_id", value).toBsonDocument().get("_id");
        }
    }
}
