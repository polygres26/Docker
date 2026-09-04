package com.sayonora.wire.mongowire;

import com.sayonora.wire.auth.CredentialStore;
import com.sayonora.wire.mongowire.auth.MongoScramConversation;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBinary;
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
    private final com.sayonora.wire.cluster.RowCache cache;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;
    private final CredentialStore credentials = new CredentialStore();
    // SCRAM state for the single login attempt in flight on this connection -- mongowire is one
    // dispatcher instance per connection (see MongoWireSessionHandler), so instance fields are the
    // right scope, same as MongoScramConversation's own javadoc explains. conversationId is a
    // simple per-connection counter; real MongoDB drivers just echo back whatever the server sent
    // in saslStart's reply, they never invent their own.
    private MongoScramConversation pendingScram;
    private int scramConversationId;

    MongoCommandDispatcher(PostgresDocumentStore store, com.sayonora.wire.cluster.RowCache cache) {
        this(store, cache, null);
    }

    MongoCommandDispatcher(PostgresDocumentStore store, com.sayonora.wire.cluster.RowCache cache, com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
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
                case "hello", "ismaster", "ismastercmd" -> hello(command);
                case "ping" -> ok();
                case "buildinfo" -> buildInfo();
                case "getparameter" -> ok();
                case "endsessions" -> ok();
                case "saslstart" -> saslStart(command);
                case "saslcontinue" -> saslContinue(command);
                case "insert" -> insert(command, db);
                case "find" -> find(command, db);
                case "aggregate" -> aggregate(command, db);
                case "update" -> update(command, db);
                case "delete" -> delete(command, db);
                case "listcollections" -> listCollections(command, db);
                case "count" -> count(command, db);
                case "distinct" -> distinct(command, db);
                case "findandmodify" -> findAndModify(command, db);
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
                    case "find", "aggregate" -> com.sayonora.wire.core.SqlMetricsCollector.StatementKind.READ;
                    case "insert", "update", "delete" -> com.sayonora.wire.core.SqlMetricsCollector.StatementKind.WRITE;
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

    private BsonDocument hello(BsonDocument command) {
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
        // A client that's about to authenticate probes here first with saslSupportedMechs:
        // "<db>.<user>" (real mongo-java-driver behavior whenever a MongoCredential is
        // configured) to pick a mechanism before ever sending saslStart. Only SCRAM-SHA-256 is
        // implemented (see MongoScramConversation's javadoc), so that's the only one advertised --
        // a driver that only supports SCRAM-SHA-1 will fail to negotiate a mechanism and report
        // that clearly, rather than this server silently accepting a mechanism it can't actually
        // verify.
        if (command.containsKey("saslSupportedMechs")) {
            String spec = command.getString("saslSupportedMechs").getValue();
            int dot = spec.indexOf('.');
            String username = dot >= 0 ? spec.substring(dot + 1) : spec;
            if (credentials.lookupPassword(username) != null) {
                reply.put("saslSupportedMechs", new BsonArray(List.of(new BsonString("SCRAM-SHA-256"))));
            }
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private BsonDocument saslStart(BsonDocument command) {
        String mechanism = command.containsKey("mechanism") ? command.getString("mechanism").getValue() : "";
        if (!"SCRAM-SHA-256".equals(mechanism)) {
            return error("Unsupported mechanism '" + mechanism + "' -- only SCRAM-SHA-256 is implemented", 334, "MechanismUnavailable");
        }
        String clientFirstMessage = new String(command.getBinary("payload").getData(), StandardCharsets.UTF_8);
        MongoScramConversation conversation;
        try {
            conversation = MongoScramConversation.start(clientFirstMessage, credentials);
        } catch (IllegalArgumentException malformed) {
            return error("Invalid SCRAM client-first-message: " + malformed.getMessage(), 9);
        }
        if (conversation == null) {
            return error("Authentication failed.", 18, "AuthenticationFailed");
        }
        pendingScram = conversation;
        scramConversationId++;
        BsonDocument reply = ok();
        reply.put("conversationId", new BsonInt32(scramConversationId));
        reply.put("done", BsonBoolean.FALSE);
        reply.put("payload", new BsonBinary(conversation.serverFirstMessage().getBytes(StandardCharsets.UTF_8)));
        return reply;
    }

    private BsonDocument saslContinue(BsonDocument command) {
        int conversationId = command.getNumber("conversationId").intValue();
        if (pendingScram == null || conversationId != scramConversationId) {
            return error("Authentication failed.", 18, "AuthenticationFailed");
        }
        String clientFinalMessage = new String(command.getBinary("payload").getData(), StandardCharsets.UTF_8);
        String serverFinalMessage = pendingScram.verifyAndFinish(clientFinalMessage);
        if (serverFinalMessage == null) {
            pendingScram = null;
            return error("Authentication failed.", 18, "AuthenticationFailed");
        }
        pendingScram = null;
        BsonDocument reply = ok();
        reply.put("conversationId", new BsonInt32(conversationId));
        reply.put("done", BsonBoolean.TRUE);
        reply.put("payload", new BsonBinary(serverFinalMessage.getBytes(StandardCharsets.UTF_8)));
        return reply;
    }

    private BsonDocument buildInfo() {
        BsonDocument reply = ok();
        reply.put("version", new BsonString("7.0.0-warp-mongowire"));
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
                + "getParameter/endSessions plus find/insert/update/delete/aggregate — not "
                + "index/admin commands or auth)", 59);
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
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
            String physicalTable = db + "." + collection;
            String cacheKey = com.sayonora.wire.cluster.RowCache.key(physicalTable, idJson, null);
            long cacheStart = System.nanoTime();
            String cachedJson = cache.get(cacheKey);
            if (cachedJson != null) {
                log.debug("mongowire cache hit: {}", cacheKey);
                recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - cacheStart);
                docs = List.of(BsonJson.fromJson(cachedJson));
            } else {
                long readStart = System.nanoTime();
                docs = store.find(db, collection, filter, MongoQueryTranslator.translate(filter), limit);
                recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
                if (!docs.isEmpty()) {
                    cache.put(cacheKey, BsonJson.toJson(docs.get(0)));
                }
            }
        } else {
            long readStart = System.nanoTime();
            docs = store.find(db, collection, filter, MongoQueryTranslator.translate(filter), limit);
            recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
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

    /** Real {@code aggregate} support -- see {@link MongoAggregationTranslator}'s own javadoc for
     * the exact pipeline shape this understands ({@code [$match] [$group] [$sort] [$limit]
     * [$project]}, each optional). Reply shape mirrors {@link #find}'s cursor-with-firstBatch --
     * a real driver's {@code aggregate()} cursor iterator reads this identically either way. */
    private BsonDocument aggregate(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("aggregate").getValue();
        BsonArray pipeline = command.containsKey("pipeline") ? command.getArray("pipeline") : new BsonArray();
        String table = PostgresDocumentStore.qualifiedTable(db, collection);

        long readStart = System.nanoTime();
        List<Document> docs = store.aggregate(db, collection, MongoAggregationTranslator.translate(table, pipeline));
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);

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
                    cache.invalidate(com.sayonora.wire.cluster.RowCache.key(db + "." + collection, idJson, null));
                }
            }
        }
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
                    cache.invalidate(com.sayonora.wire.cluster.RowCache.key(db + "." + collection, idJson, null));
                }
            }
        }
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        BsonDocument reply = ok();
        reply.put("n", new BsonInt32(deleted));
        return reply;
    }

    /** Real {@code listCollections} support -- see {@link PostgresDocumentStore#listCollections}'s
     * own javadoc for why this closes a connection-SETUP gap, not just a query gap (mongoose's
     * default {@code autoIndex} behavior calls this before ever running a real query). Reply
     * shape mirrors a real server's own {@code {cursor: {firstBatch: [{name, type}]}}}. */
    private BsonDocument listCollections(BsonDocument command, String db) throws SQLException {
        List<String> names = store.listCollections(db);
        BsonArray firstBatch = new BsonArray();
        for (String name : names) {
            BsonDocument entry = new BsonDocument();
            entry.put("name", new BsonString(name));
            entry.put("type", new BsonString("collection"));
            firstBatch.add(entry);
        }
        BsonDocument cursor = new BsonDocument();
        cursor.put("id", new BsonInt64(0));
        cursor.put("ns", new BsonString(db + ".$cmd.listCollections"));
        cursor.put("firstBatch", firstBatch);
        BsonDocument reply = ok();
        reply.put("cursor", cursor);
        return reply;
    }

    /** Real {@code count}/{@code countDocuments} support. */
    private BsonDocument count(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("count").getValue();
        BsonDocument filter = command.containsKey("query") ? command.getDocument("query") : new BsonDocument();
        long n = store.count(db, collection, MongoQueryTranslator.translate(filter));
        BsonDocument reply = ok();
        reply.put("n", new BsonInt64(n));
        return reply;
    }

    /** Real {@code distinct} support. */
    private BsonDocument distinct(BsonDocument command, String db) throws SQLException {
        String collection = command.getString("distinct").getValue();
        String field = command.getString("key").getValue();
        BsonDocument filter = command.containsKey("query") ? command.getDocument("query") : new BsonDocument();
        List<org.bson.BsonValue> values = store.distinct(db, collection, field, MongoQueryTranslator.translate(filter));
        BsonDocument reply = ok();
        reply.put("values", new BsonArray(values));
        return reply;
    }

    /** Real {@code findAndModify} support -- the single wire command real drivers/ODMs use for
     * BOTH {@code findOneAndUpdate} and {@code findOneAndDelete}. See {@link
     * PostgresDocumentStore#findAndModify}'s own javadoc for the exact matching/return-value
     * semantics. Reply shape mirrors a real server's own {@code {value: <doc-or-null>}}. */
    private BsonDocument findAndModify(BsonDocument command, String db) throws SQLException {
        // The command's own first key IS its collection-name value, whatever casing the client
        // actually sent ("findAndModify" vs "findandmodify" -- driver-dependent) -- reading it
        // positionally like this sidesteps needing to guess which spelling to look up.
        String collection = command.get(command.getFirstKey()).asString().getValue();
        BsonDocument filter = command.containsKey("query") ? command.getDocument("query") : new BsonDocument();
        boolean remove = command.containsKey("remove") && command.getBoolean("remove").getValue();
        boolean returnNew = command.containsKey("new") && command.getBoolean("new").getValue();
        Document updateMerger = null;
        if (!remove) {
            if (!command.containsKey("update")) {
                throw new IllegalArgumentException("findAndModify: either \"remove\" or \"update\" must be given");
            }
            updateMerger = Document.parse(command.getDocument("update").toJson());
        }
        Document result = store.findAndModify(db, collection, filter, MongoQueryTranslator.translate(filter),
                updateMerger, remove, returnNew);
        BsonDocument reply = ok();
        reply.put("value", result != null ? result.toBsonDocument() : org.bson.BsonNull.VALUE);
        return reply;
    }
}
