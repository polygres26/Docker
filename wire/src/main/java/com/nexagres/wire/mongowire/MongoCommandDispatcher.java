package com.nexagres.wire.mongowire;

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
    private final com.nexagres.wire.core.SqlMetricsCollector sqlMetrics;

    MongoCommandDispatcher(PostgresDocumentStore store, MongoCache cache) {
        this(store, cache, null);
    }

    MongoCommandDispatcher(PostgresDocumentStore store, MongoCache cache, com.nexagres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.cache = cache;
        this.sqlMetrics = sqlMetrics;
    }

    BsonDocument dispatch(BsonDocument command) {
        String commandName = command.getFirstKey();
        String db = command.containsKey("$db") ? command.getString("$db").getValue() : "test";
        String lower = commandName.toLowerCase(java.util.Locale.ROOT);
        long start = System.nanoTime();
        try {
            return switch (lower) {
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
            return error("Postgres error: " + e.getMessage(),
                    MongoErrorMapper.code(e.getSQLState()), MongoErrorMapper.codeName(e.getSQLState()));
        } finally {
            // Only the real data-plane commands -- hello/ping/buildinfo/etc. are driver handshake
            // noise on every connection, not something a traffic dashboard should show as
            // "operations", and would otherwise dominate the top-N-by-cost table with near-zero
            // latency entries.
            if (sqlMetrics != null) {
                var kind = switch (lower) {
                    case "find" -> com.nexagres.wire.core.SqlMetricsCollector.StatementKind.READ;
                    case "insert", "update", "delete" -> com.nexagres.wire.core.SqlMetricsCollector.StatementKind.WRITE;
                    default -> null;
                };
                if (kind != null) {
                    sqlMetrics.recordOperation("mongowire", resolveBackendLabel(command, lower), kind, db + "." + lower,
                            System.nanoTime() - start);
                }
            }
        }
    }

    /**
     * Best-effort, metrics-label-only re-derivation of which shard an operation resolved to --
     * mirrors dynamowire's {@code DynamoWireServer#resolveBackendLabel}. For find/update/delete
     * this is exact ({@code exactIdEquality} is the same check the store itself uses to route);
     * for insert it's a peek at the caller-supplied {@code _id} on the first document only --
     * an auto-generated one isn't known until {@code insertOne} runs, and that's fine, this is a
     * label, not a routing decision. Any failure just falls back to "default".
     */
    private String resolveBackendLabel(BsonDocument command, String lower) {
        try {
            BsonDocument filter = switch (lower) {
                case "find" -> command.containsKey("filter") ? command.getDocument("filter") : null;
                case "update" -> firstSpecFilter(command, "updates");
                case "delete" -> firstSpecFilter(command, "deletes");
                default -> null;
            };
            if (filter != null) {
                String idJson = MongoQueryTranslator.exactIdEquality(filter);
                return idJson == null ? "default" : store.resolveBackendFor(idJson);
            }
            if ("insert".equals(lower) && command.containsKey("documents")) {
                BsonArray docs = command.getArray("documents");
                if (!docs.isEmpty() && docs.get(0).asDocument().containsKey("_id")) {
                    String idJson = BsonJson.valueToJson(docs.get(0).asDocument().get("_id"));
                    return store.resolveBackendFor(idJson);
                }
            }
            return "default";
        } catch (RuntimeException e) {
            return "default";
        }
    }

    private static BsonDocument firstSpecFilter(BsonDocument command, String arrayField) {
        if (!command.containsKey(arrayField)) {
            return null;
        }
        BsonArray specs = command.getArray(arrayField);
        if (specs.isEmpty()) {
            return null;
        }
        return specs.get(0).asDocument().getDocument("q", new BsonDocument());
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
        return error(message, code, null);
    }

    /** {@code codeName} is real MongoDB's own second, string-typed identifier for the same error
     * -- a genuine command-error reply always carries both, not code alone (see {@link
     * MongoErrorMapper}'s javadoc). Callers that only have a bare numeric code (the handful of
     * fixed, hand-picked codes elsewhere in this class, like {@link #commandNotFound}) pass {@code
     * null} via the other overload rather than inventing a codeName that isn't real. */
    private static BsonDocument error(String message, int code, String codeName) {
        BsonDocument doc = new BsonDocument();
        doc.put("ok", new BsonDouble(0.0));
        doc.put("errmsg", new BsonString(message));
        doc.put("code", new BsonInt32(code));
        if (codeName != null) {
            doc.put("codeName", new BsonString(codeName));
        }
        return doc;
    }

    private static BsonDocument commandNotFound(String commandName) {
        return error("no such command: '" + commandName + "' (mongowire covers hello/ping/buildInfo/"
                + "getParameter/endSessions plus find/insert/update/delete — not the aggregation "
                + "pipeline or index/admin commands)", 59);
    }

    /** Shared by every read/write branch below -- one sample per Mongo command, same granularity
     * {@link #dispatch} already uses for {@code recordOperation}. No-op if metrics are disabled. */
    private void recordRttOutcome(String outcome, long elapsedNanos) {
        if (sqlMetrics != null) {
            sqlMetrics.recordRttOutcome("mongowire", outcome, elapsedNanos);
        }
    }

    private BsonDocument insert(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("insert").getValue();
        BsonArray documents = command.getArray("documents");
        int inserted = 0;
        List<BsonDocument> writeErrors = new ArrayList<>();
        long writeStart = System.nanoTime();
        for (int i = 0; i < documents.size(); i++) {
            Document doc = BsonJson.toDocument(documents.get(i).asDocument());
            try {
                // No cache.invalidate() here, deliberately -- found live costing ~150-270us on
                // every single insert (an Ignite cache op, not free even locally) to guard
                // against a case that can't happen: a freshly successful INSERT (not an upsert)
                // means this _id wasn't already in the table, and updateMany/deleteMany below
                // already invalidate their own touched keys on every write, so any *prior*
                // occupant of this _id (if it was ever deleted to free the id up for reuse) had
                // its cache entry cleared by that delete already. By the time a fresh insert can
                // reuse an _id, there is nothing stale left to invalidate.
                store.insertOne(db, collection, doc);
                inserted++;
            } catch (SQLException e) {
                BsonDocument werr = new BsonDocument();
                werr.put("index", new BsonInt32(i));
                // Real MongoDB per-item writeErrors always carry code/codeName, not just errmsg --
                // this was missing entirely before, so a real driver's own per-item error handling
                // (pymongo's BulkWriteError.details['writeErrors'][i]['code'], etc.) had nothing
                // to key off.
                werr.put("code", new BsonInt32(MongoErrorMapper.code(e.getSQLState())));
                werr.put("codeName", new BsonString(MongoErrorMapper.codeName(e.getSQLState())));
                werr.put("errmsg", new BsonString(e.getMessage()));
                writeErrors.add(werr);
            }
        }
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
            long cacheStart = System.nanoTime();
            Document cached = cache.get(cacheKey);
            if (cached != null) {
                log.debug("mongowire cache hit: {}", cacheKey);
                recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - cacheStart);
                docs = List.of(cached);
            } else {
                long readStart = System.nanoTime();
                docs = store.find(db, collection, filter, MongoQueryTranslator.translate(filter), limit);
                recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
                if (!docs.isEmpty()) {
                    cache.put(cacheKey, docs.get(0));
                }
            }
        } else {
            long readStart = System.nanoTime();
            docs = store.find(db, collection, filter, MongoQueryTranslator.translate(filter), limit);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
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
        long writeStart = System.nanoTime();
        for (BsonValue u : updates) {
            BsonDocument spec = u.asDocument();
            BsonDocument filter = spec.getDocument("q", new BsonDocument());
            Document updateDoc = BsonJson.toDocument(spec.getDocument("u"));
            boolean multi = spec.containsKey("multi") && spec.getBoolean("multi").getValue();
            MongoQueryTranslator.Where where = MongoQueryTranslator.translate(filter);
            PostgresDocumentStore.WriteResult result = store.updateMany(db, collection, filter, where, updateDoc, multi ? 0 : 1);
            matched += result.count();
            modified += result.count();
            if (cache != null) {
                for (String idJson : result.ids()) {
                    cache.invalidate(MongoCache.key(db, collection, idJson));
                }
            }
        }
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(matched));
        reply.put("nModified", new BsonInt32(modified));
        return reply;
    }

    private BsonDocument delete(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("delete").getValue();
        BsonArray deletes = command.getArray("deletes");
        int deleted = 0;
        long writeStart = System.nanoTime();
        for (BsonValue d : deletes) {
            BsonDocument spec = d.asDocument();
            BsonDocument filter = spec.getDocument("q", new BsonDocument());
            int limit = spec.containsKey("limit") ? spec.getNumber("limit").intValue() : 0;
            MongoQueryTranslator.Where where = MongoQueryTranslator.translate(filter);
            PostgresDocumentStore.WriteResult result = store.deleteMany(db, collection, filter, where, limit);
            deleted += result.count();
            if (cache != null) {
                for (String idJson : result.ids()) {
                    cache.invalidate(MongoCache.key(db, collection, idJson));
                }
            }
        }
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(deleted));
        return reply;
    }
}
