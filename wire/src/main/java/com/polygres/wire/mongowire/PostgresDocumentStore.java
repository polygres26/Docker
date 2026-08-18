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

    record WriteResult(int count, List<String> ids) {}

    static String idJsonFor(Object idValue) {
        return BsonJson.valueToJson(new BsonObjectIdOrPassthrough(idValue).toBson());
    }

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

    private record BsonObjectIdOrPassthrough(Object value) {
        org.bson.BsonValue toBson() {
            if (value instanceof ObjectId oid) {
                return new BsonObjectId(oid);
            }
            return new Document("_id", value).toBsonDocument().get("_id");
        }
    }
}
