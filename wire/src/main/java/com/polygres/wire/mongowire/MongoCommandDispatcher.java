package com.polygres.wire.mongowire;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MongoCommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MongoCommandDispatcher.class);
    private final PostgresDocumentStore store;
    private final MongoCache cache;

    MongoCommandDispatcher(PostgresDocumentStore store, MongoCache cache) {
        this.store = store;
        this.cache = cache;
    }

    BsonDocument dispatch(BsonDocument command) {
        String commandName = command.getFirstKey();
        String db = command.containsKey("$db") ? command.getString("$db").getValue() : "test";
        try {
            return switch (commandName.toLowerCase(java.util.Locale.ROOT)) {
                case "hello", "ismaster", "ismastercmd" -> hello();
                case "ping" -> ok();
                case "buildinfo" -> buildInfo();
                case "getparameter" -> ok();
                case "endsessions" -> ok();
                case "insert" -> insert(command, db);
                case "find" -> find(command, db);
                case "update" -> update(command, db);
                case "delete" -> delete(command, db);
                default -> commandNotFound(commandName);
            };
        } catch (IllegalArgumentException badFilter) {
            
            return error(badFilter.getMessage(), 9);
        } catch (SQLException e) {
            log.warn("mongowire: Postgres error servicing \"{}\": {}", commandName, e.getMessage());
            return error("Postgres error: " + e.getMessage(), 8);
        }
    }

    private BsonDocument hello() {
        BsonDocument reply = new BsonDocument();
        reply.put("ismaster", BsonBoolean.TRUE);
        reply.put("helloOk", BsonBoolean.TRUE);
        reply.put("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024));
        reply.put("maxMessageSizeBytes", new BsonInt32(48 * 1024 * 1024));
        reply.put("maxWriteBatchSize", new BsonInt32(100000));
        reply.put("localTime", new org.bson.BsonDateTime(System.currentTimeMillis()));
        reply.put("logicalSessionTimeoutMinutes", new BsonInt32(30));
        reply.put("connectionId", new BsonInt32(1));
        reply.put("minWireVersion", new BsonInt32(0));
        
        reply.put("maxWireVersion", new BsonInt32(17));
        reply.put("readOnly", BsonBoolean.FALSE);
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private BsonDocument buildInfo() {
        BsonDocument reply = ok();
        reply.put("version", new BsonString("7.0.0-polywire-mongowire"));
        reply.put("versionArray", new BsonArray(List.of(new BsonInt32(7), new BsonInt32(0), new BsonInt32(0))));
        reply.put("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024));
        return reply;
    }

    private static BsonDocument ok() {
        BsonDocument doc = new BsonDocument();
        doc.put("ok", new BsonDouble(1.0));
        return doc;
    }

    private static BsonDocument error(String message, int code) {
        BsonDocument doc = new BsonDocument();
        doc.put("ok", new BsonDouble(0.0));
        doc.put("errmsg", new BsonString(message));
        doc.put("code", new BsonInt32(code));
        return doc;
    }

    private static BsonDocument commandNotFound(String commandName) {
        return error("no such command: '" + commandName + "' (mongowire covers hello/ping/buildInfo/"
                + "getParameter/endSessions plus find/insert/update/delete — not the aggregation "
                + "pipeline or index/admin commands)", 59);
    }

    private BsonDocument insert(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("insert").getValue();
        BsonArray documents = command.getArray("documents");
        int inserted = 0;
        List<BsonDocument> writeErrors = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            Document doc = BsonJson.toDocument(documents.get(i).asDocument());
            try {
                Document stored = store.insertOne(db, collection, doc);
                inserted++;
                if (cache != null) {
                    
                    cache.invalidate(MongoCache.key(db, collection, PostgresDocumentStore.idJsonFor(stored.get("_id"))));
                }
            } catch (SQLException e) {
                BsonDocument werr = new BsonDocument();
                werr.put("index", new BsonInt32(i));
                werr.put("errmsg", new BsonString(e.getMessage()));
                writeErrors.add(werr);
            }
        }
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(inserted));
        if (!writeErrors.isEmpty()) {
            reply.put("writeErrors", new BsonArray(new ArrayList<>(writeErrors)));
        }
        return reply;
    }

    private BsonDocument find(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("find").getValue();
        BsonDocument filter = command.containsKey("filter") ? command.getDocument("filter") : new BsonDocument();
        int limit = command.containsKey("limit") ? command.getNumber("limit").intValue() : 0;
        List<Document> docs;
        
        String idJson = cache != null ? MongoQueryTranslator.exactIdEquality(filter) : null;
        if (idJson != null) {
            String cacheKey = MongoCache.key(db, collection, idJson);
            Document cached = cache.get(cacheKey);
            if (cached != null) {
                log.debug("mongowire cache hit: {}", cacheKey);
                docs = List.of(cached);
            } else {
                docs = store.find(db, collection, MongoQueryTranslator.translate(filter), limit);
                if (!docs.isEmpty()) {
                    cache.put(cacheKey, docs.get(0));
                }
            }
        } else {
            docs = store.find(db, collection, MongoQueryTranslator.translate(filter), limit);
        }

        BsonArray firstBatch = new BsonArray();
        for (Document d : docs) {
            firstBatch.add(d.toBsonDocument());
        }
        BsonDocument cursor = new BsonDocument();
        cursor.put("id", new BsonInt64(0));
        cursor.put("ns", new BsonString(db + "." + collection));
        cursor.put("firstBatch", firstBatch);

        BsonDocument reply = ok();
        reply.put("cursor", cursor);
        return reply;
    }

    private BsonDocument update(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("update").getValue();
        BsonArray updates = command.getArray("updates");
        int matched = 0;
        int modified = 0;
        for (BsonValue u : updates) {
            BsonDocument spec = u.asDocument();
            BsonDocument filter = spec.getDocument("q", new BsonDocument());
            Document updateDoc = BsonJson.toDocument(spec.getDocument("u"));
            boolean multi = spec.containsKey("multi") && spec.getBoolean("multi").getValue();
            MongoQueryTranslator.Where where = MongoQueryTranslator.translate(filter);
            PostgresDocumentStore.WriteResult result = store.updateMany(db, collection, where, updateDoc, multi ? 0 : 1);
            matched += result.count();
            modified += result.count();
            if (cache != null) {
                for (String idJson : result.ids()) {
                    cache.invalidate(MongoCache.key(db, collection, idJson));
                }
            }
        }
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(matched));
        reply.put("nModified", new BsonInt32(modified));
        return reply;
    }

    private BsonDocument delete(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("delete").getValue();
        BsonArray deletes = command.getArray("deletes");
        int deleted = 0;
        for (BsonValue d : deletes) {
            BsonDocument spec = d.asDocument();
            BsonDocument filter = spec.getDocument("q", new BsonDocument());
            int limit = spec.containsKey("limit") ? spec.getNumber("limit").intValue() : 0;
            MongoQueryTranslator.Where where = MongoQueryTranslator.translate(filter);
            PostgresDocumentStore.WriteResult result = store.deleteMany(db, collection, where, limit);
            deleted += result.count();
            if (cache != null) {
                for (String idJson : result.ids()) {
                    cache.invalidate(MongoCache.key(db, collection, idJson));
                }
            }
        }
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(deleted));
        return reply;
    }
}
