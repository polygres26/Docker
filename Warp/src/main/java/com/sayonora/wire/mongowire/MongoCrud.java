package com.sayonora.wire.mongowire;

import com.sayonora.wire.cluster.RowCache;
import java.sql.SQLException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/** find / insert / update / delete / findAndModify / count / distinct / aggregate over the document store. */
final class MongoCrud {

    private static final Set<String> COMMON = Set.of("lsid", "txnNumber", "$clusterTime", "$db", "$readPreference", "readConcern",
            "writeConcern", "maxTimeMS", "comment", "apiVersion", "apiStrict", "apiDeprecationErrors", "autocommit", "startTransaction",
            "stmtId", "$configTime", "$topologyTime", "txnRetryCounter", "$audit", "$client", "$oplogQueryData", "$replData",
            "$readOnce", "serverApi", "encryptionInformation", "bypassDocumentValidation", "$queryOptions", "$mongoWireMetadata", "expectPrefix",
            "mayBypassWriteBlocking", "rawData");
    private static final Set<String> FIND_FIELDS = Set.of("find", "filter", "sort", "projection", "hint", "skip", "limit", "batchSize",
            "singleBatch", "max", "min", "returnKey", "showRecordId", "tailable", "oplogReplay", "noCursorTimeout", "awaitData",
            "allowPartialResults", "collation", "allowDiskUse", "let", "readOnce", "term", "lastKnownCommittedOpTime",
            "requestResumeToken", "resumeAfter", "$_requestResumeToken", "$_resumeAfter", "showExpandedEvents");
    private static final Set<String> INSERT_FIELDS = Set.of("insert", "documents", "ordered", "bypassDocumentValidation");
    private static final Set<String> UPDATE_FIELDS = Set.of("update", "updates", "ordered", "bypassDocumentValidation", "let");
    private static final Set<String> DELETE_FIELDS = Set.of("delete", "deletes", "ordered", "let");
    private static final Set<String> FAM_FIELDS = Set.of("findAndModify", "findandmodify", "query", "sort", "remove", "update", "new",
            "fields", "upsert", "bypassDocumentValidation", "arrayFilters", "collation", "hint", "let");
    private static final Set<String> COUNT_FIELDS = Set.of("count", "query", "limit", "skip", "hint", "collation", "fields");
    private static final Set<String> DISTINCT_FIELDS = Set.of("distinct", "key", "query", "collation", "hint");
    private static final Set<String> AGG_FIELDS = Set.of("aggregate", "pipeline", "cursor", "explain", "allowDiskUse", "collation",
            "hint", "let", "bypassDocumentValidation", "fromMongos", "needsMerge", "mergeByPBRT", "exchange", "isMapReduceCommand",
            "runtimeConstants", "$_requestReshardingResumeToken");

    private final PostgresDocumentStore store;
    private final RowCache cache;
    private final java.util.function.BiConsumer<String, Long> rtt;
    private static final ConcurrentTtl TTL_SWEEPS = new ConcurrentTtl();

    MongoCrud(PostgresDocumentStore store, RowCache cache, java.util.function.BiConsumer<String, Long> rtt) {
        this.store = store;
        this.cache = cache;
        this.rtt = rtt;
    }

    private static final class ConcurrentTtl {
        final Map<String, Long> last = new java.util.concurrent.ConcurrentHashMap<>();
    }

    // ------------------------------------------------------------------ argument helpers

    static void checkFields(BsonDocument cmd, String name, Set<String> allowed) {
        for (String k : cmd.keySet()) {
            if (!allowed.contains(k) && !COMMON.contains(k) && !k.startsWith("$")) {
                throw new MongoCmdException(40415, "BSON field '" + name + "." + k + "' is an unknown field.");
            }
        }
    }

    static String collName(BsonDocument cmd, String name) {
        BsonValue v = cmd.get(name);
        if (v == null || !v.isString()) {
            throw new MongoCmdException(2, "collection name has invalid type " + (v == null ? "missing" : MongoMatcher.typeName(v)));
        }
        String s = v.asString().getValue();
        if (s.isEmpty()) {
            throw new MongoCmdException(73, "Invalid namespace specified '" + "'");
        }
        if (s.indexOf('$') >= 0) {
            throw new MongoCmdException(73, "Invalid collection name specified '" + s + "'");
        }
        return s;
    }

    private static MongoCmdException wrongType(String cmd, String field, BsonValue v, String expected) {
        return new MongoCmdException(14, "BSON field '" + cmd + "." + field + "' is the wrong type '" + MongoMatcher.typeName(v)
                + "', expected " + expected);
    }

    static BsonDocument optDoc(BsonDocument c, String cmd, String key) {
        BsonValue v = c.get(key);
        if (v == null) {
            return null;
        }
        if (!v.isDocument()) {
            throw wrongType(cmd, key, v, "type 'object'");
        }
        return v.asDocument();
    }

    static long optLong(BsonDocument c, String cmd, String key, long def) {
        BsonValue v = c.get(key);
        if (v == null || v.isNull()) {
            return def;
        }
        if (!BsonCmp.isNumber(v)) {
            throw wrongType(cmd, key, v, "types '[long, int, decimal, double]'");
        }
        double d = MongoNum.toDouble(v);
        if (d != Math.floor(d) || Double.isNaN(d) || Double.isInfinite(d)) {
            throw new MongoCmdException(2, "Field '" + key + "' must be a whole number, got " + MongoFmt.value(v));
        }
        return MongoNum.truncLong(v);
    }

    static boolean optBool(BsonDocument c, String cmd, String key, boolean def) {
        BsonValue v = c.get(key);
        if (v == null) {
            return def;
        }
        if (v.isBoolean()) {
            return v.asBoolean().getValue();
        }
        if (BsonCmp.isNumber(v)) {
            return MongoExpr.truthy(v);
        }
        throw wrongType(cmd, key, v, "types '[bool, long, int, decimal, double]'");
    }

    static BsonCmp.Collation collation(BsonDocument cmd, String name) {
        BsonDocument c = optDoc(cmd, name, "collation");
        return collationFrom(c);
    }

    static BsonCmp.Collation collationFrom(BsonDocument c) {
        if (c == null || c.isEmpty()) {
            return null;
        }
        BsonValue loc = c.get("locale");
        if (loc == null) {
            throw new MongoCmdException(40414, "BSON field 'collation.locale' is missing but a required field");
        }
        if (!loc.isString()) {
            throw new MongoCmdException(14, "BSON field 'collation.locale' is the wrong type '" + MongoMatcher.typeName(loc) + "', expected type 'string'");
        }
        String locale = loc.asString().getValue();
        if (locale.equals("simple")) {
            for (Map.Entry<String, BsonValue> e : c.entrySet()) {
                if (!e.getKey().equals("locale")) {
                    throw new MongoCmdException(2, "Field '" + e.getKey() + "' is invalid in: { locale: \"simple\" ... }");
                }
            }
            return null;
        }
        Locale l = Locale.forLanguageTag(locale.replace('_', '-'));
        boolean valid = false;
        for (Locale a : Locale.getAvailableLocales()) {
            if (a.getLanguage().equals(l.getLanguage()) && !l.getLanguage().isEmpty()) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            throw new MongoCmdException(2, "Field 'locale' is invalid in: " + MongoFmt.doc(c));
        }
        Collator col = Collator.getInstance(l);
        long strength = c.containsKey("strength") ? MongoNum.truncLong(c.get("strength")) : 3;
        if (strength < 1 || strength > 5) {
            throw new MongoCmdException(51024, "BSON field 'strength' value must be <= 5, actual value '" + strength + "'");
        }
        boolean caseLevel = c.containsKey("caseLevel") && c.getBoolean("caseLevel").getValue();
        col.setStrength(strength == 1 ? (caseLevel ? Collator.SECONDARY : Collator.PRIMARY) : strength == 2 ? Collator.SECONDARY
                : strength == 3 ? Collator.TERTIARY : Collator.IDENTICAL);
        if (strength == 1 && caseLevel) {
            // primary strength plus case differences: approximate with tertiary comparison on the accent-stripped form
            col.setStrength(Collator.TERTIARY);
        }
        boolean numeric = c.containsKey("numericOrdering") && c.getBoolean("numericOrdering").getValue();
        return new BsonCmp.Collation(col, numeric, c);
    }

    /** Evaluates a command's {@code let} document into aggregation variables. */
    static Map<String, BsonValue> letVars(BsonDocument let, BsonCmp.Collation coll) {
        if (let == null || let.isEmpty()) {
            return Map.of();
        }
        Map<String, BsonValue> vars = new java.util.HashMap<>();
        MongoExpr.Scope sc = MongoExpr.Scope.root(new BsonDocument(), coll);
        let.forEach((k, v) -> {
            MongoExpr.checkVarName(k);
            BsonValue r = MongoExpr.parse(v).eval(sc);
            vars.put(k, r == MongoExpr.MISSING ? BsonNull.VALUE : r);
        });
        return vars;
    }

    // ------------------------------------------------------------------ hints / metadata

    private void checkHint(BsonValue hint, PostgresDocumentStore.CollMeta meta) {
        if (hint == null) {
            return;
        }
        if (hint.isString()) {
            String h = hint.asString().getValue();
            if (h.equals("_id_") || h.equals("$natural")) {
                return;
            }
            for (BsonDocument ix : meta.indexes) {
                if (ix.getString("name").getValue().equals(h)) {
                    return;
                }
            }
        } else if (hint.isDocument()) {
            BsonDocument h = hint.asDocument();
            if (h.containsKey("$natural") || h.equals(new BsonDocument("_id", new BsonInt32(1)))) {
                return;
            }
            for (BsonDocument ix : meta.indexes) {
                if (keyEquals(ix.getDocument("key"), h)) {
                    return;
                }
            }
        } else {
            throw new MongoCmdException(2, "hint provided must be a string or an object");
        }
        throw new MongoCmdException(2, "error processing query: hint provided does not correspond to an existing index");
    }

    private static boolean keyEquals(BsonDocument a, BsonDocument b) {
        if (a.size() != b.size()) {
            return false;
        }
        Iterator<Map.Entry<String, BsonValue>> i = a.entrySet().iterator();
        Iterator<Map.Entry<String, BsonValue>> j = b.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, BsonValue> x = i.next();
            Map.Entry<String, BsonValue> y = j.next();
            if (!x.getKey().equals(y.getKey()) || BsonCmp.compare(x.getValue(), y.getValue()) != 0) {
                return false;
            }
        }
        return true;
    }

    private PostgresDocumentStore.CollMeta metaFor(String db, String coll) throws SQLException {
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (m.exists) {
            maybeSweep(db, coll, m);
        }
        return m;
    }

    private void maybeSweep(String db, String coll, PostgresDocumentStore.CollMeta m) throws SQLException {
        boolean ttl = false;
        for (BsonDocument ix : m.indexes) {
            if (ix.containsKey("expireAfterSeconds")) {
                ttl = true;
            }
        }
        if (!ttl) {
            return;
        }
        String k = db + "." + coll;
        long now = System.currentTimeMillis();
        Long last = TTL_SWEEPS.last.get(k);
        if (last != null && now - last < 60_000) {
            return;
        }
        TTL_SWEEPS.last.put(k, now);
        store.sweepExpired(db, coll, m);
    }

    // ------------------------------------------------------------------ id routing hints

    private static List<String> idKeysFor(BsonDocument filter) {
        BsonValue eq = MongoMatcher.idEquality(filter);
        if (eq != null) {
            return List.of(PostgresDocumentStore.idKey(eq));
        }
        List<BsonValue> in = MongoMatcher.idIn(filter);
        if (in != null) {
            List<String> keys = new ArrayList<>();
            for (BsonValue v : in) {
                keys.add(PostgresDocumentStore.idKey(v));
            }
            return keys;
        }
        return null;
    }

    private void invalidate(String db, String coll, BsonValue id) {
        if (cache != null && id != null) {
            try {
                cache.invalidate(RowCache.key(db + "." + coll, PostgresDocumentStore.idKey(id), null));
            } catch (RuntimeException ignored) {
                // cache is best effort
            }
        }
    }

    private void checkTxn(BsonDocument cmd) {
        if (cmd.containsKey("txnNumber") || cmd.containsKey("startTransaction")) {
            throw new MongoCmdException(20, "Transaction numbers are only allowed on a replica set member or mongos");
        }
    }

    // ------------------------------------------------------------------ find

    BsonDocument find(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "find", FIND_FIELDS);
        checkTxn(cmd);
        String coll = collName(cmd, "find");
        PostgresDocumentStore.validateDb(db);
        BsonDocument filter = optDoc(cmd, "find", "filter");
        BsonDocument sort = optDoc(cmd, "find", "sort");
        BsonDocument projection = optDoc(cmd, "find", "projection");
        long skip = optLong(cmd, "find", "skip", 0);
        long limit = optLong(cmd, "find", "limit", 0);
        long batchSize = optLong(cmd, "find", "batchSize", -1);
        boolean singleBatch = optBool(cmd, "find", "singleBatch", false);
        BsonCmp.Collation coll0 = collation(cmd, "find");
        if (skip < 0) {
            throw new MongoCmdException(51024, "Skip value must be non-negative, but received: " + skip);
        }
        if (limit < 0) {
            throw new MongoCmdException(51024, "Limit value must be non-negative, but received: " + limit);
        }
        if (batchSize < -1 || cmd.containsKey("batchSize") && batchSize < 0) {
            throw new MongoCmdException(51024, "BatchSize value must be non-negative, but received: " + batchSize);
        }
        if (optBool(cmd, "find", "awaitData", false) && !optBool(cmd, "find", "tailable", false)) {
            throw new MongoCmdException(9, "Cannot set 'awaitData' without also setting 'tailable'");
        }
        if (cmd.containsKey("tailable") && optBool(cmd, "find", "tailable", false)) {
            throw new MongoCmdException(2, "error processing query: tailable cursors are only supported on capped collections");
        }
        if (filter == null) {
            filter = new BsonDocument();
        }
        MongoMatcher.Pred pred = MongoMatcher.compile(filter, coll0, letVars(optDoc(cmd, "find", "let"), coll0));
        if (sort != null && !sort.isEmpty()) {
            MongoSort.validate(sort, "find");
        }
        MongoProjection proj = projection == null ? null : MongoProjection.compile(projection, false, filter, coll0);
        PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
        checkHint(cmd.get("hint"), meta);
        String ns = db + "." + coll;
        if (!meta.exists) {
            return MongoCursors.cursorDoc(ns, 0, new BsonArray(), true);
        }
        long readStart = System.nanoTime();
        Stream<BsonDocument> s = store.scan(db, coll, idKeysFor(filter));
        s = s.filter(pred::test);
        if (sort != null && !sort.isEmpty()) {
            s = s.sorted(MongoSort.comparator(sort, coll0));
        }
        if (skip > 0) {
            s = s.skip(skip);
        }
        if (limit > 0) {
            s = s.limit(limit);
        }
        if (proj != null && !proj.isIdentity()) {
            s = s.map(proj::apply);
        }
        Stream<BsonDocument> finalStream = s;
        Iterator<BsonDocument> it = finalStream.iterator();
        BsonDocument reply;
        try {
            reply = MongoCursors.reply(ns, it, finalStream, batchSize >= 0 ? Long.valueOf(batchSize) : null, limit > 0 ? limit : -1,
                    singleBatch, true);
        } catch (RuntimeException e) {
            finalStream.close();
            throw unwrap(e);
        }
        rtt.accept("pg_read", System.nanoTime() - readStart);
        return reply;
    }

    static RuntimeException unwrap(RuntimeException e) {
        if (e instanceof java.io.UncheckedIOException u && u.getCause() != null && u.getCause().getCause() instanceof SQLException se) {
            return new SqlFailure(se);
        }
        return e;
    }

    /** Carries a SQLException out of stream lambdas. */
    static final class SqlFailure extends RuntimeException {
        final SQLException sql;

        SqlFailure(SQLException sql) {
            super(sql);
            this.sql = sql;
        }
    }

    BsonDocument getMore(BsonDocument cmd, String db) {
        long id = cmd.get("getMore").isNumber() ? MongoNum.truncLong(cmd.get("getMore")) : -1;
        if (!cmd.get("getMore").isInt64()) {
            throw new MongoCmdException(14, "BSON field 'getMore.getMore' is the wrong type '" + MongoMatcher.typeName(cmd.get("getMore"))
                    + "', expected type 'long'");
        }
        if (!cmd.containsKey("collection")) {
            throw new MongoCmdException(40414, "BSON field 'getMore.collection' is missing but a required field");
        }
        String coll = collName(cmd, "collection");
        long bs = optLong(cmd, "getMore", "batchSize", -1);
        if (cmd.containsKey("batchSize") && bs < 0) {
            throw new MongoCmdException(51024, "BatchSize value must be non-negative, but received: " + bs);
        }
        try {
            return MongoCursors.getMore(id, coll, db, bs > 0 ? Long.valueOf(bs) : null);
        } catch (RuntimeException e) {
            throw unwrap(e);
        }
    }

    BsonDocument killCursors(BsonDocument cmd, String db) {
        String coll = collName(cmd, "killCursors");
        if (!cmd.containsKey("cursors")) {
            throw new MongoCmdException(40414, "BSON field 'killCursors.cursors' is missing but a required field");
        }
        BsonValue cs = cmd.get("cursors");
        if (!cs.isArray()) {
            throw new MongoCmdException(14, "BSON field 'killCursors.cursors' is the wrong type '" + MongoMatcher.typeName(cs)
                    + "', expected type 'array'");
        }
        BsonArray killed = new BsonArray();
        BsonArray notFound = new BsonArray();
        for (BsonValue v : cs.asArray()) {
            if (!v.isInt64()) {
                throw new MongoCmdException(14, "Each element of the 'cursors' array must be of type long");
            }
            if (MongoCursors.kill(v.asInt64().getValue(), db + "." + coll)) {
                killed.add(v);
            } else {
                notFound.add(v);
            }
        }
        BsonDocument r = new BsonDocument();
        r.put("cursorsKilled", killed);
        r.put("cursorsNotFound", notFound);
        r.put("cursorsAlive", new BsonArray());
        r.put("cursorsUnknown", new BsonArray());
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    // ------------------------------------------------------------------ count / distinct

    BsonDocument count(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "count", COUNT_FIELDS);
        String coll = collName(cmd, "count");
        BsonDocument query = optDoc(cmd, "count", "query");
        long limit = optLong(cmd, "count", "limit", 0);
        long skip = optLong(cmd, "count", "skip", 0);
        if (skip < 0) {
            throw new MongoCmdException(51024, "Skip value must be non-negative, but received: " + skip);
        }
        BsonCmp.Collation coll0 = collation(cmd, "count");
        MongoMatcher.Pred pred = MongoMatcher.compile(query, coll0);
        PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
        checkHint(cmd.get("hint"), meta);
        long n = 0;
        if (meta.exists) {
            if ((query == null || query.isEmpty()) && skip == 0 && limit == 0) {
                n = store.countAll(db, coll);
            } else {
                try (Stream<BsonDocument> s = store.scan(db, coll, query == null ? null : idKeysFor(query))) {
                    Stream<BsonDocument> f = s.filter(pred::test);
                    if (skip > 0) {
                        f = f.skip(skip);
                    }
                    if (limit != 0) {
                        f = f.limit(Math.abs(limit));
                    }
                    n = f.count();
                } catch (RuntimeException e) {
                    throw unwrap(e);
                }
            }
        }
        BsonDocument r = new BsonDocument();
        r.put("n", MongoNum.fromLong(n));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    BsonDocument distinct(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "distinct", DISTINCT_FIELDS);
        String coll = collName(cmd, "distinct");
        if (!cmd.containsKey("key")) {
            throw new MongoCmdException(40414, "BSON field 'distinct.key' is missing but a required field");
        }
        if (!cmd.get("key").isString()) {
            throw wrongType("distinct", "key", cmd.get("key"), "type 'string'");
        }
        String key = cmd.getString("key").getValue();
        if (key.isEmpty()) {
            throw new MongoCmdException(40352, "FieldPath cannot be constructed with empty string");
        }
        if (key.indexOf('\0') >= 0) {
            throw new MongoCmdException(2, "Key field cannot contain an embedded null byte");
        }
        BsonDocument query = optDoc(cmd, "distinct", "query");
        BsonCmp.Collation coll0 = collation(cmd, "distinct");
        MongoMatcher.Pred pred = MongoMatcher.compile(query, coll0);
        PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
        LinkedHashMap<String, BsonValue> out = new LinkedHashMap<>();
        if (meta.exists) {
            try (Stream<BsonDocument> s = store.scan(db, coll, query == null ? null : idKeysFor(query))) {
                Iterator<BsonDocument> it = s.iterator();
                while (it.hasNext()) {
                    BsonDocument d = it.next();
                    if (!pred.test(d)) {
                        continue;
                    }
                    for (BsonValue c : MongoMatcher.resolve(d, key)) {
                        if (c == MongoMatcher.MISSING) {
                            continue;
                        }
                        if (c.isArray()) {
                            for (BsonValue e : c.asArray()) {
                                out.putIfAbsent(BsonCmp.key(e, coll0), e);
                            }
                        } else {
                            out.putIfAbsent(BsonCmp.key(c, coll0), c);
                        }
                    }
                }
            } catch (RuntimeException e) {
                throw unwrap(e);
            }
        }
        List<BsonValue> vals = new ArrayList<>(out.values());
        vals.sort((a, b) -> BsonCmp.compare(a, b, coll0));
        BsonDocument r = new BsonDocument();
        r.put("values", new BsonArray(vals));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    // ------------------------------------------------------------------ aggregate

    BsonDocument aggregate(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "aggregate", AGG_FIELDS);
        checkTxn(cmd);
        BsonValue target = cmd.get("aggregate");
        boolean collectionless = target != null && BsonCmp.isNumber(target) && MongoNum.truncLong(target) == 1;
        if (collectionless && cmd.containsKey("pipeline") && cmd.get("pipeline").isArray() && !cmd.getArray("pipeline").isEmpty()
                && cmd.getArray("pipeline").get(0).isDocument() && !cmd.getArray("pipeline").get(0).asDocument().isEmpty()) {
            String first = cmd.getArray("pipeline").get(0).asDocument().getFirstKey();
            if (!List.of("$documents", "$currentOp", "$listLocalSessions", "$listSessions", "$changeStream").contains(first)) {
                throw new MongoCmdException(73, "{aggregate: 1} is not valid for '" + first + "'; a collection is required.");
            }
        }
        String coll = collectionless ? "$cmd.aggregate" : collName(cmd, "aggregate");
        if (!cmd.containsKey("pipeline")) {
            throw new MongoCmdException(40414, "BSON field 'aggregate.pipeline' is missing but a required field");
        }
        if (!cmd.get("pipeline").isArray()) {
            throw wrongType("aggregate", "pipeline", cmd.get("pipeline"), "type 'array'");
        }
        BsonArray pipeline = cmd.getArray("pipeline");
        boolean explain = optBool(cmd, "aggregate", "explain", false);
        BsonDocument cursorOpt = optDoc(cmd, "aggregate", "cursor");
        if (cursorOpt == null && !explain) {
            throw new MongoCmdException(9, "The 'cursor' option is required, except for aggregate with the explain argument");
        }
        if (cursorOpt != null && explain) {
            throw new MongoCmdException(9, "The 'cursor' option is not allowed in conjunction with the explain option");
        }
        long batchSize = -1;
        if (cursorOpt != null && cursorOpt.containsKey("batchSize")) {
            batchSize = optLong(cursorOpt, "cursor", "batchSize", -1);
            if (batchSize < 0) {
                throw new MongoCmdException(51024, "BatchSize value must be non-negative, but received: " + batchSize);
            }
        }
        BsonCmp.Collation coll0 = collation(cmd, "aggregate");
        for (BsonValue st : pipeline) {
            if (st.isDocument() && st.asDocument().size() == 1 && st.asDocument().getFirstKey().equals("$changeStream")) {
                throw new MongoCmdException(40573, "The $changeStream stage is only supported on replica sets");
            }
        }
        BsonDocument let = optDoc(cmd, "aggregate", "let");
        checkHintForAgg(cmd, db, coll, collectionless);
        String ns = collectionless ? db + ".$cmd.aggregate" : db + "." + coll;
        if (explain) {
            return explainAggregate(cmd, db, coll, pipeline);
        }
        MongoAgg.Ctx ctx = new MongoAgg.Ctx();
        ctx.db = db;
        ctx.coll = coll;
        ctx.collation = coll0;
        ctx.source = (d, c) -> store.scan(d, c, null);
        ctx.sink = new StoreSink();
        if (let != null) {
            Map<String, BsonValue> vars = new java.util.HashMap<>();
            let.forEach((k, v) -> vars.put(k, MongoExpr.parse(v).eval(MongoExpr.Scope.root(new BsonDocument(), coll0))));
            ctx.vars = vars;
        }
        PostgresDocumentStore.CollMeta meta = collectionless ? null : metaFor(db, coll);
        List<String> idKeys = null;
        if (!pipeline.isEmpty() && pipeline.get(0).isDocument() && pipeline.get(0).asDocument().size() == 1
                && pipeline.get(0).asDocument().getFirstKey().equals("$match") && pipeline.get(0).asDocument().get("$match").isDocument()) {
            idKeys = idKeysFor(pipeline.get(0).asDocument().getDocument("$match"));
        }
        long readStart = System.nanoTime();
        Stream<BsonDocument> source;
        if (collectionless || !meta.exists) {
            source = Stream.empty();
        } else {
            source = store.scan(db, coll, idKeys);
        }
        Stream<BsonDocument> out;
        try {
            out = MongoAgg.run(pipeline, source, ctx);
        } catch (RuntimeException e) {
            source.close();
            throw unwrap(e);
        }
        Stream<BsonDocument> fout = out;
        BsonDocument reply;
        try {
            Iterator<BsonDocument> it = fout.iterator();
            reply = MongoCursors.reply(ns, it, () -> {
                fout.close();
                source.close();
            }, batchSize >= 0 ? Long.valueOf(batchSize) : null, -1, false, true);
        } catch (RuntimeException e) {
            fout.close();
            source.close();
            throw unwrap(e);
        }
        rtt.accept("pg_read", System.nanoTime() - readStart);
        return reply;
    }

    private void checkHintForAgg(BsonDocument cmd, String db, String coll, boolean collectionless) throws SQLException {
        if (!collectionless && cmd.containsKey("hint")) {
            PostgresDocumentStore.CollMeta m = store.meta(db, coll);
            if (m.exists) {
                checkHint(cmd.get("hint"), m);
            }
        }
    }

    private BsonDocument explainAggregate(BsonDocument cmd, String db, String coll, BsonArray pipeline) {
        BsonDocument r = new BsonDocument();
        r.put("explainVersion", new BsonString("1"));
        BsonDocument f = new BsonDocument();
        if (!pipeline.isEmpty() && pipeline.get(0).isDocument() && pipeline.get(0).asDocument().containsKey("$match")
                && pipeline.get(0).asDocument().get("$match").isDocument()) {
            f = pipeline.get(0).asDocument().getDocument("$match");
        }
        r.put("queryPlanner", planner(db + "." + coll, f));
        r.put("serverInfo", serverInfo());
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    static BsonDocument serverInfo() {
        return new BsonDocument("host", new BsonString("warp")).append("port", new BsonInt32(27017))
                .append("version", new BsonString("7.0.0")).append("gitVersion", new BsonString("warp-mongowire"));
    }

    static BsonDocument planner(String ns, BsonDocument filter) {
        BsonDocument winning = new BsonDocument("stage", new BsonString("COLLSCAN"));
        if (!filter.isEmpty()) {
            winning.put("filter", filter);
        }
        winning.put("direction", new BsonString("forward"));
        return new BsonDocument("namespace", new BsonString(ns)).append("indexFilterSet", BsonBoolean.FALSE)
                .append("parsedQuery", filter).append("queryHash", new BsonString("00000000"))
                .append("planCacheKey", new BsonString("00000000")).append("maxIndexedOrSolutionsReached", BsonBoolean.FALSE)
                .append("maxIndexedAndSolutionsReached", BsonBoolean.FALSE).append("maxScansToExplodeReached", BsonBoolean.FALSE)
                .append("winningPlan", winning).append("rejectedPlans", new BsonArray());
    }

    /** {@code $out}/{@code $merge} writes through the store. */
    private final class StoreSink implements MongoAgg.Sink {

        @Override
        public void replaceCollection(String db, String coll, List<BsonDocument> docs) {
            try {
                PostgresDocumentStore.CollMeta m = store.meta(db, coll);
                if (m.exists) {
                    store.truncate(db, coll);
                } else {
                    store.createCollection(db, coll, new BsonDocument());
                }
                for (BsonDocument d : docs) {
                    BsonDocument doc = ensureId(d);
                    store.insert(db, coll, doc, store.meta(db, coll));
                }
            } catch (SQLException e) {
                throw new SqlFailure(e);
            }
        }

        @Override
        public BsonDocument findOne(String db, String coll, BsonDocument filter) {
            MongoMatcher.Pred p = MongoMatcher.compile(filter);
            try (Stream<BsonDocument> s = store.scan(db, coll, idKeysFor(filter))) {
                return s.filter(p::test).findFirst().orElse(null);
            }
        }

        @Override
        public void insert(String db, String coll, BsonDocument doc) {
            try {
                store.ensureCollection(db, coll);
                store.insert(db, coll, ensureId(doc), store.meta(db, coll));
            } catch (SQLException e) {
                throw new SqlFailure(e);
            }
        }

        @Override
        public void replace(String db, String coll, BsonDocument oldDoc, BsonDocument newDoc) {
            try {
                store.replace(db, coll, oldDoc, newDoc, store.meta(db, coll));
                invalidate(db, coll, oldDoc.get("_id"));
            } catch (SQLException e) {
                throw new SqlFailure(e);
            }
        }
    }

    // ------------------------------------------------------------------ writes: shared

    static BsonDocument ensureId(BsonDocument d) {
        if (d.containsKey("_id")) {
            return d;
        }
        BsonDocument out = new BsonDocument("_id", PostgresDocumentStore.newId());
        d.forEach(out::put);
        return out;
    }

    private BsonDocument writeError(int index, RuntimeException e) {
        BsonDocument w = new BsonDocument();
        w.put("index", new BsonInt32(index));
        if (e instanceof MongoCmdException m) {
            w.put("code", new BsonInt32(m.code));
            w.put("errmsg", new BsonString(m.getMessage()));
            if (m.extra != null) {
                m.extra.forEach(w::put);
            }
        } else if (e instanceof SqlFailure sf) {
            w.put("code", new BsonInt32(MongoErrorMapper.code(sf.sql.getSQLState())));
            w.put("errmsg", new BsonString(sf.sql.getMessage()));
        } else {
            w.put("code", new BsonInt32(8));
            w.put("errmsg", new BsonString(String.valueOf(e.getMessage())));
        }
        return w;
    }

    private BsonDocument writeErrorSql(int index, SQLException e) {
        BsonDocument w = new BsonDocument();
        w.put("index", new BsonInt32(index));
        w.put("code", new BsonInt32(MongoErrorMapper.code(e.getSQLState())));
        w.put("errmsg", new BsonString(e.getMessage()));
        return w;
    }

    /** Validates the document against the collection validator (collMod/create validator). */
    private void validate(String db, String coll, PostgresDocumentStore.CollMeta meta, BsonDocument newDoc, BsonDocument oldDoc,
            boolean bypass) {
        MongoMatcher.Pred v = meta.validator();
        if (v == null || bypass) {
            return;
        }
        String action = meta.options.containsKey("validationAction") ? meta.options.getString("validationAction").getValue() : "error";
        String level = meta.options.containsKey("validationLevel") ? meta.options.getString("validationLevel").getValue() : "strict";
        if (level.equals("off")) {
            return;
        }
        if (oldDoc != null && level.equals("moderate") && !v.test(oldDoc)) {
            return;
        }
        if (!v.test(newDoc)) {
            if (action.equals("warn")) {
                return;
            }
            BsonDocument details = new BsonDocument("operatorName", new BsonString(
                    meta.options.getDocument("validator").containsKey("$jsonSchema") ? "$jsonSchema" : "$and"));
            BsonDocument info = new BsonDocument("failingDocumentId", newDoc.containsKey("_id") ? newDoc.get("_id") : BsonNull.VALUE)
                    .append("details", details);
            throw new MongoCmdException(121, "Document failed validation", new BsonDocument("errInfo", info));
        }
    }

    /** Standalone mongod semantics: w>1 and non-majority modes are rejected up front. */
    private void checkWriteConcern(BsonDocument cmd) {
        BsonDocument wc = cmd.containsKey("writeConcern") && cmd.get("writeConcern").isDocument() ? cmd.getDocument("writeConcern") : null;
        if (wc == null || !wc.containsKey("w")) {
            return;
        }
        BsonValue w = wc.get("w");
        if (BsonCmp.isNumber(w)) {
            long n = MongoNum.truncLong(w);
            if (n < 0 || n > 50) {
                throw new MongoCmdException(9, "w has to be a non-negative number and not greater than 50; found: " + n);
            }
            if (n > 1) {
                throw new MongoCmdException(2, "cannot use 'w' > 1 when a host is not replicated");
            }
        } else if (w.isString() && !w.asString().getValue().equals("majority")) {
            throw new MongoCmdException(2, "cannot use non-majority 'w' mode \"" + w.asString().getValue() + "\" when a host is not a member of a replica set");
        }
    }

    private static boolean unacknowledged(BsonDocument cmd) {
        BsonValue wc = cmd.get("writeConcern");
        return wc != null && wc.isDocument() && wc.asDocument().containsKey("w") && BsonCmp.isNumber(wc.asDocument().get("w"))
                && MongoNum.truncLong(wc.asDocument().get("w")) == 0;
    }

    private BsonDocument writeConcernError(BsonDocument cmd) {
        return null;
    }

    private static void checkBatchSize(int n) {
        if (n < 1 || n > 100000) {
            throw new MongoCmdException(16, "Write batch sizes must be between 1 and 100000. Got " + n + " operations.");
        }
    }

    // ------------------------------------------------------------------ insert

    BsonDocument insert(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "insert", INSERT_FIELDS);
        checkTxn(cmd);
        checkWriteConcern(cmd);
        String coll = collName(cmd, "insert");
        PostgresDocumentStore.validateDb(db);
        PostgresDocumentStore.validateColl(coll);
        if (coll.startsWith("system.") && !coll.equals("system.js")) {
            throw new MongoCmdException(73, "Invalid system namespace: " + db + "." + coll);
        }
        BsonValue docsV = cmd.get("documents");
        if (docsV == null) {
            throw new MongoCmdException(40414, "BSON field 'insert.documents' is missing but a required field");
        }
        if (!docsV.isArray()) {
            throw wrongType("insert", "documents", docsV, "type 'array'");
        }
        boolean ordered = optBool(cmd, "insert", "ordered", true);
        boolean bypass = optBool(cmd, "insert", "bypassDocumentValidation", false);
        BsonArray documents = docsV.asArray();
        checkBatchSize(documents.size());
        for (BsonValue d : documents) {
            if (!d.isDocument()) {
                throw new MongoCmdException(14, "BSON field 'insert.documents.0' is the wrong type '" + MongoMatcher.typeName(d)
                        + "', expected type 'object'");
            }
        }
        long writeStart = System.nanoTime();
        store.ensureCollection(db, coll);
        PostgresDocumentStore.CollMeta meta = store.meta(db, coll);
        int inserted = 0;
        BsonArray errors = new BsonArray();
        for (int i = 0; i < documents.size(); i++) {
            BsonDocument doc = documents.get(i).asDocument();
            try {
                doc = prepareInsert(doc);
                validate(db, coll, meta, doc, null, bypass);
                store.insert(db, coll, doc, meta);
                inserted++;
            } catch (SQLException e) {
                errors.add(writeErrorSql(i, e));
                if (ordered) {
                    break;
                }
            } catch (RuntimeException e) {
                errors.add(writeError(i, unwrap(e)));
                if (ordered) {
                    break;
                }
            }
        }
        rtt.accept("pg_write", System.nanoTime() - writeStart);
        BsonDocument reply = new BsonDocument();
        reply.put("n", new BsonInt32(unacknowledged(cmd) ? 0 : inserted));
        if (!errors.isEmpty()) {
            reply.put("writeErrors", errors);
        }
        BsonDocument wce = writeConcernError(cmd);
        if (wce != null) {
            reply.put("writeConcernError", wce);
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private BsonDocument prepareInsert(BsonDocument doc) {
        BsonDocument d = ensureId(doc);
        BsonValue id = d.get("_id");
        if (id.isArray()) {
            throw new MongoCmdException(53, "The '_id' value cannot be of type array");
        }
        if (id.isDocument()) {
            for (String k : id.asDocument().keySet()) {
                if (k.startsWith("$")) {
                    throw new MongoCmdException(2, "_id fields may not contain '$'-prefixed fields: " + k + " is not valid for storage.");
                }
            }
        }
        if (id.isRegularExpression() || BsonCmp.isUndef(id)) {
            throw new MongoCmdException(53, "_id fields may not be " + (BsonCmp.isUndef(id) ? "undefined" : "regex"));
        }
        int size = MongoBson.encode(d).length;
        if (size > 16 * 1024 * 1024) {
            throw new MongoCmdException(10334, "object to insert too large. size in bytes: " + size + ", max size: " + 16 * 1024 * 1024);
        }
        return d;
    }

    // ------------------------------------------------------------------ update

    private enum Outcome { NO_MATCH, UNCHANGED, CHANGED }

    BsonDocument update(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "update", UPDATE_FIELDS);
        checkTxn(cmd);
        checkWriteConcern(cmd);
        String coll = collName(cmd, "update");
        PostgresDocumentStore.validateDb(db);
        PostgresDocumentStore.validateColl(coll);
        BsonValue ups = cmd.get("updates");
        if (ups == null) {
            throw new MongoCmdException(40414, "BSON field 'update.updates' is missing but a required field");
        }
        if (!ups.isArray()) {
            throw wrongType("update", "updates", ups, "type 'array'");
        }
        boolean ordered = optBool(cmd, "update", "ordered", true);
        boolean bypass = optBool(cmd, "update", "bypassDocumentValidation", false);
        checkBatchSize(ups.asArray().size());
        long writeStart = System.nanoTime();
        long matched = 0;
        long modified = 0;
        BsonArray upserted = new BsonArray();
        BsonArray errors = new BsonArray();
        List<BsonDocument> specs = new ArrayList<>();
        for (BsonValue u : ups.asArray()) {
            if (!u.isDocument()) {
                throw new MongoCmdException(14, "BSON field 'updates.0' is the wrong type '" + MongoMatcher.typeName(u) + "', expected type 'object'");
            }
            specs.add(u.asDocument());
        }
        for (BsonDocument spec : specs) {
            validateUpdateSpec(spec);
        }
        for (int i = 0; i < specs.size(); i++) {
            BsonDocument spec = specs.get(i);
            try {
                long[] r = updateOne(db, coll, spec, bypass, i, upserted, cmd.get("let"));
                matched += r[0];
                modified += r[1];
            } catch (SQLException e) {
                errors.add(writeErrorSql(i, e));
                if (ordered) {
                    break;
                }
            } catch (RuntimeException e) {
                RuntimeException u = unwrap(e);
                errors.add(writeError(i, u));
                if (ordered) {
                    break;
                }
            }
        }
        rtt.accept("pg_write", System.nanoTime() - writeStart);
        BsonDocument reply = new BsonDocument();
        reply.put("n", MongoNum.fromLong(matched + upserted.size()));
        reply.put("nModified", MongoNum.fromLong(modified));
        if (!upserted.isEmpty()) {
            reply.put("upserted", upserted);
        }
        if (!errors.isEmpty()) {
            reply.put("writeErrors", errors);
        }
        BsonDocument wce = writeConcernError(cmd);
        if (wce != null) {
            reply.put("writeConcernError", wce);
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private static void validateUpdateSpec(BsonDocument spec) {
        for (String k : spec.keySet()) {
            if (!Set.of("q", "u", "upsert", "multi", "collation", "arrayFilters", "hint", "c", "sort").contains(k)) {
                throw new MongoCmdException(40415, "BSON field 'update.updates." + k + "' is an unknown field.");
            }
        }
        if (!spec.containsKey("q")) {
            throw new MongoCmdException(40414, "BSON field 'update.updates.q' is missing but a required field");
        }
        if (!spec.get("q").isDocument()) {
            throw new MongoCmdException(14, "BSON field 'update.updates.q' is the wrong type '" + MongoMatcher.typeName(spec.get("q")) + "', expected type 'object'");
        }
        if (!spec.containsKey("u")) {
            throw new MongoCmdException(40414, "BSON field 'update.updates.u' is missing but a required field");
        }
        BsonValue u = spec.get("u");
        if (!u.isDocument() && !u.isArray()) {
            throw new MongoCmdException(9, "Update argument must be either an object or an array");
        }
        if (u.isArray()) {
            try {
                MongoUpdate.validate(u, null);
            } catch (MongoCmdException e) {
                if (e.code != 72) {
                    throw e;
                }
            }
        }
    }

    /** Returns {matched, modified}; upserts append to {@code upserted}. */
    private long[] updateOne(String db, String coll, BsonDocument spec, boolean bypass, int index, BsonArray upserted, BsonValue let)
            throws SQLException {
        for (String k : spec.keySet()) {
            if (!Set.of("q", "u", "upsert", "multi", "collation", "arrayFilters", "hint", "c", "sort").contains(k)) {
                throw new MongoCmdException(40415, "BSON field 'updates." + k + "' is an unknown field.");
            }
        }
        if (!spec.containsKey("q")) {
            throw new MongoCmdException(40414, "BSON field 'updates.q' is missing but a required field");
        }
        if (!spec.containsKey("u")) {
            throw new MongoCmdException(40414, "BSON field 'updates.u' is missing but a required field");
        }
        if (!spec.get("q").isDocument()) {
            throw wrongType("updates", "q", spec.get("q"), "type 'object'");
        }
        BsonValue u = spec.get("u");
        if (!u.isDocument() && !u.isArray()) {
            throw new MongoCmdException(14, "Update argument must be either an object or an array");
        }
        BsonDocument q = spec.getDocument("q");
        boolean upsert = optBool(spec, "updates", "upsert", false);
        boolean multi = optBool(spec, "updates", "multi", false);
        BsonCmp.Collation coll0 = collationFrom(optDoc(spec, "updates", "collation"));
        BsonArray arrayFilters = null;
        if (spec.containsKey("arrayFilters")) {
            if (!spec.get("arrayFilters").isArray()) {
                throw wrongType("updates", "arrayFilters", spec.get("arrayFilters"), "type 'array'");
            }
            arrayFilters = spec.getArray("arrayFilters");
        }
        boolean replacement = u.isDocument() && MongoUpdate.isReplacement(u.asDocument());
        if (multi && replacement) {
            throw new MongoCmdException(9, "multi update is not supported for replacement-style update");
        }
        if (arrayFilters != null && replacement) {
            throw new MongoCmdException(9, "arrayFilters may not be specified for replacement-style updates");
        }
        MongoUpdate.validate(u, arrayFilters);
        Map<String, BsonValue> vars = letVars(let != null && let.isDocument() ? let.asDocument() : spec.containsKey("c") && spec.get("c").isDocument()
                ? spec.getDocument("c") : null, coll0);
        MongoMatcher.Pred pred = MongoMatcher.compile(q, coll0, vars);
        PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
        checkHint(spec.get("hint"), meta);
        MongoUpdate.Ctx uctx = new MongoUpdate.Ctx();
        uctx.vars = vars;
        uctx.filter = q;
        uctx.coll = coll0;
        uctx.arrayFilters = MongoUpdate.parseArrayFilters(arrayFilters);
        long matched = 0;
        long modified = 0;
        if (meta.exists) {
            try (Stream<BsonDocument> s = store.scan(db, coll, idKeysFor(q))) {
                Iterator<BsonDocument> it = s.filter(pred::test).iterator();
                while (it.hasNext()) {
                    BsonDocument cur = it.next();
                    Outcome o = applyToDoc(db, coll, meta, cur, u, uctx, pred, bypass);
                    if (o != Outcome.NO_MATCH) {
                        matched++;
                        if (o == Outcome.CHANGED) {
                            modified++;
                        }
                        if (!multi) {
                            break;
                        }
                    }
                }
            } catch (RuntimeException e) {
                throw unwrap(e);
            }
        }
        if (matched == 0 && upsert) {
            BsonDocument doc = buildUpsert(q, u, uctx, coll0);
            store.ensureCollection(db, coll);
            PostgresDocumentStore.CollMeta m2 = store.meta(db, coll);
            validate(db, coll, m2, doc, null, bypass);
            store.insert(db, coll, doc, m2);
            upserted.add(new BsonDocument("index", new BsonInt32(index)).append("_id", doc.get("_id")));
        }
        return new long[] {matched, modified};
    }

    private BsonDocument buildUpsert(BsonDocument q, BsonValue u, MongoUpdate.Ctx uctx, BsonCmp.Collation coll) {
        if (q.containsKey("$expr")) {
            throw new MongoCmdException(224, "$expr is not allowed in the query predicate for an upsert");
        }
        BsonDocument seed = MongoUpdate.upsertSeed(q);
        MongoUpdate.Ctx ctx = new MongoUpdate.Ctx();
        ctx.insert = true;
        ctx.filter = q;
        ctx.coll = coll;
        ctx.arrayFilters = uctx.arrayFilters;
        BsonDocument doc;
        if (u.isDocument() && MongoUpdate.isReplacement(u.asDocument())) {
            doc = new BsonDocument();
            BsonValue qid = seed.get("_id");
            BsonValue uid = u.asDocument().get("_id");
            if (uid != null && qid != null && BsonCmp.compare(uid, qid) != 0 && q.get("_id") != null && !q.get("_id").isDocument()) {
                throw new MongoCmdException(66, "The _id field cannot be changed from {_id: " + MongoFmt.value(qid) + "} to {_id: "
                        + MongoFmt.value(uid) + "}.");
            }
            BsonValue id = uid != null ? uid : qid;
            if (id != null) {
                doc.put("_id", id);
            }
            u.asDocument().forEach(doc::put);
        } else {
            doc = MongoUpdate.apply(seed, u, ctx);
        }
        if (!doc.containsKey("_id")) {
            BsonDocument withId = new BsonDocument("_id", PostgresDocumentStore.newId());
            doc.forEach(withId::put);
            doc = withId;
        } else if (!doc.getFirstKey().equals("_id")) {
            BsonDocument reordered = new BsonDocument("_id", doc.get("_id"));
            doc.forEach((k, v) -> {
                if (!k.equals("_id")) {
                    reordered.put(k, v);
                }
            });
            doc = reordered;
        }
        return doc;
    }

    private Outcome applyToDoc(String db, String coll, PostgresDocumentStore.CollMeta meta, BsonDocument cur, BsonValue update,
            MongoUpdate.Ctx uctx, MongoMatcher.Pred pred, boolean bypass) throws SQLException {
        for (int attempt = 0; attempt < 12; attempt++) {
            uctx.insert = false;
            BsonDocument next = MongoUpdate.apply(cur, update, uctx);
            byte[] a = MongoBson.encode(cur);
            byte[] b = MongoBson.encode(next);
            if (java.util.Arrays.equals(a, b)) {
                return update.isDocument() && MongoUpdate.isReplacement(update.asDocument()) ? Outcome.CHANGED : Outcome.UNCHANGED;
            }
            validate(db, coll, meta, next, cur, bypass);
            int size = b.length;
            if (size > 16 * 1024 * 1024) {
                throw new MongoCmdException(10334, "Resulting document after update is larger than 16777216");
            }
            if (store.replace(db, coll, cur, next, meta)) {
                invalidate(db, coll, cur.get("_id"));
                return Outcome.CHANGED;
            }
            BsonDocument fresh = store.fetch(db, coll, cur.get("_id"));
            if (fresh == null || !pred.test(fresh)) {
                return Outcome.NO_MATCH;
            }
            cur = fresh;
        }
        throw new MongoCmdException(112, "Caused by :: Write conflict during plan execution and yielding is disabled.");
    }

    // ------------------------------------------------------------------ delete

    BsonDocument delete(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "delete", DELETE_FIELDS);
        checkTxn(cmd);
        checkWriteConcern(cmd);
        String coll = collName(cmd, "delete");
        PostgresDocumentStore.validateDb(db);
        BsonValue dels = cmd.get("deletes");
        if (dels == null) {
            throw new MongoCmdException(40414, "BSON field 'delete.deletes' is missing but a required field");
        }
        if (!dels.isArray()) {
            throw wrongType("delete", "deletes", dels, "type 'array'");
        }
        boolean ordered = optBool(cmd, "delete", "ordered", true);
        checkBatchSize(dels.asArray().size());
        long writeStart = System.nanoTime();
        long deleted = 0;
        BsonArray errors = new BsonArray();
        int i = -1;
        for (BsonValue dv : dels.asArray()) {
            validateDeleteSpec(dv);
        }
        for (BsonValue dv : dels.asArray()) {
            i++;
            try {
                if (!dv.isDocument()) {
                    throw new MongoCmdException(14, "BSON field 'deletes.0' is the wrong type '" + MongoMatcher.typeName(dv) + "', expected type 'object'");
                }
                BsonDocument spec = dv.asDocument();
                for (String k : spec.keySet()) {
                    if (!Set.of("q", "limit", "collation", "hint").contains(k)) {
                        throw new MongoCmdException(40415, "BSON field 'deletes." + k + "' is an unknown field.");
                    }
                }
                if (!spec.containsKey("q")) {
                    throw new MongoCmdException(40414, "BSON field 'deletes.q' is missing but a required field");
                }
                if (!spec.containsKey("limit")) {
                    throw new MongoCmdException(40414, "BSON field 'deletes.limit' is missing but a required field");
                }
                if (!spec.get("q").isDocument()) {
                    throw wrongType("deletes", "q", spec.get("q"), "type 'object'");
                }
                BsonValue lv = spec.get("limit");
                long limit = BsonCmp.isNumber(lv) ? MongoNum.truncLong(lv) : lv.isBoolean() ? (lv.asBoolean().getValue() ? 1 : 0) : 0;
                BsonDocument q = spec.getDocument("q");
                BsonCmp.Collation coll0 = collationFrom(optDoc(spec, "deletes", "collation"));
                MongoMatcher.Pred pred = MongoMatcher.compile(q, coll0, letVars(optDoc(cmd, "delete", "let"), coll0));
                PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
                checkHint(spec.get("hint"), meta);
                if (!meta.exists) {
                    continue;
                }
                try (Stream<BsonDocument> s = store.scan(db, coll, idKeysFor(q))) {
                    Iterator<BsonDocument> it = s.filter(pred::test).iterator();
                    while (it.hasNext()) {
                        BsonDocument d = it.next();
                        if (store.delete(db, coll, d, meta)) {
                            deleted++;
                            invalidate(db, coll, d.get("_id"));
                            if (limit == 1) {
                                break;
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                errors.add(writeErrorSql(i, e));
                if (ordered) {
                    break;
                }
            } catch (RuntimeException e) {
                errors.add(writeError(i, unwrap(e)));
                if (ordered) {
                    break;
                }
            }
        }
        rtt.accept("pg_write", System.nanoTime() - writeStart);
        BsonDocument reply = new BsonDocument();
        reply.put("n", MongoNum.fromLong(deleted));
        if (!errors.isEmpty()) {
            reply.put("writeErrors", errors);
        }
        BsonDocument wce = writeConcernError(cmd);
        if (wce != null) {
            reply.put("writeConcernError", wce);
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }

    private static void validateDeleteSpec(BsonValue dv) {
        if (!dv.isDocument()) {
            throw new MongoCmdException(14, "BSON field 'delete.deletes.0' is the wrong type '" + MongoMatcher.typeName(dv) + "', expected type 'object'");
        }
        BsonDocument spec = dv.asDocument();
        for (String k : spec.keySet()) {
            if (!Set.of("q", "limit", "collation", "hint").contains(k)) {
                throw new MongoCmdException(40415, "BSON field 'delete.deletes." + k + "' is an unknown field.");
            }
        }
        if (!spec.containsKey("q")) {
            throw new MongoCmdException(40414, "BSON field 'delete.deletes.q' is missing but a required field");
        }
        if (!spec.containsKey("limit")) {
            throw new MongoCmdException(40414, "BSON field 'delete.deletes.limit' is missing but a required field");
        }
        if (!spec.get("q").isDocument()) {
            throw new MongoCmdException(14, "BSON field 'delete.deletes.q' is the wrong type '" + MongoMatcher.typeName(spec.get("q")) + "', expected type 'object'");
        }
        BsonValue l = spec.get("limit");
        if (BsonCmp.isNumber(l) && MongoNum.truncLong(l) != 0 && MongoNum.truncLong(l) != 1) {
            throw new MongoCmdException(9, "The limit field in delete objects must be 0 or 1. Got " + MongoNum.truncLong(l));
        }
    }

    // ------------------------------------------------------------------ findAndModify

    BsonDocument findAndModify(BsonDocument cmd, String db) throws SQLException {
        checkFields(cmd, "findAndModify", FAM_FIELDS);
        checkTxn(cmd);
        String coll = collName(cmd, cmd.getFirstKey());
        PostgresDocumentStore.validateDb(db);
        BsonDocument query = optDoc(cmd, "findAndModify", "query");
        BsonDocument sort = optDoc(cmd, "findAndModify", "sort");
        BsonDocument fields = optDoc(cmd, "findAndModify", "fields");
        boolean remove = optBool(cmd, "findAndModify", "remove", false);
        boolean returnNew = optBool(cmd, "findAndModify", "new", false);
        boolean upsert = optBool(cmd, "findAndModify", "upsert", false);
        boolean bypass = optBool(cmd, "findAndModify", "bypassDocumentValidation", false);
        BsonValue update = cmd.get("update");
        if (remove && update != null) {
            throw new MongoCmdException(9, "Cannot specify both an update and remove=true");
        }
        if (remove && upsert) {
            throw new MongoCmdException(9, "Cannot specify both upsert=true and remove=true");
        }
        if (remove && returnNew) {
            throw new MongoCmdException(9, "Cannot specify both new=true and remove=true; 'remove' always returns the deleted document");
        }
        if (!remove && update == null) {
            throw new MongoCmdException(9, "Either an update or remove=true must be specified");
        }
        if (update != null && !update.isDocument() && !update.isArray()) {
            throw new MongoCmdException(9, "Update argument must be either an object or an array");
        }
        BsonCmp.Collation coll0 = collation(cmd, "findAndModify");
        BsonArray arrayFilters = cmd.containsKey("arrayFilters") ? cmd.getArray("arrayFilters") : null;
        if (query == null) {
            query = new BsonDocument();
        }
        if (update != null) {
            MongoUpdate.validate(update, arrayFilters);
        }
        if (sort != null && !sort.isEmpty()) {
            MongoSort.validate(sort, "findAndModify");
        }
        MongoProjection proj = fields == null ? null : MongoProjection.compile(fields, false, query, coll0);
        Map<String, BsonValue> famVars = letVars(optDoc(cmd, "findAndModify", "let"), coll0);
        MongoMatcher.Pred pred = MongoMatcher.compile(query, coll0, famVars);
        PostgresDocumentStore.CollMeta meta = metaFor(db, coll);
        checkHint(cmd.get("hint"), meta);
        MongoUpdate.Ctx uctx = new MongoUpdate.Ctx();
        uctx.vars = famVars;
        uctx.filter = query;
        uctx.coll = coll0;
        uctx.arrayFilters = MongoUpdate.parseArrayFilters(arrayFilters);
        BsonDocument lastError = new BsonDocument();
        BsonValue value = BsonNull.VALUE;
        BsonDocument target = null;
        if (meta.exists) {
            try (Stream<BsonDocument> s = store.scan(db, coll, idKeysFor(query))) {
                Stream<BsonDocument> f = s.filter(pred::test);
                if (sort != null && !sort.isEmpty()) {
                    f = f.sorted(MongoSort.comparator(sort, coll0));
                }
                target = f.findFirst().orElse(null);
            } catch (RuntimeException e) {
                throw unwrap(e);
            }
        }
        if (target != null) {
            if (remove) {
                store.delete(db, coll, target, meta);
                invalidate(db, coll, target.get("_id"));
                value = target;
                lastError.put("n", new BsonInt32(1));
            } else {
                BsonDocument before = target;
                BsonDocument after = target;
                for (int attempt = 0; ; attempt++) {
                    uctx.insert = false;
                    after = MongoUpdate.apply(before, update, uctx);
                    if (java.util.Arrays.equals(MongoBson.encode(before), MongoBson.encode(after))) {
                        break;
                    }
                    validate(db, coll, meta, after, before, bypass);
                    if (store.replace(db, coll, before, after, meta)) {
                        invalidate(db, coll, before.get("_id"));
                        break;
                    }
                    BsonDocument fresh = store.fetch(db, coll, before.get("_id"));
                    if (fresh == null || !pred.test(fresh) || attempt > 10) {
                        throw new MongoCmdException(112, "Write conflict during plan execution and yielding is disabled.");
                    }
                    before = fresh;
                }
                value = returnNew ? after : before;
                lastError.put("n", new BsonInt32(1));
                lastError.put("updatedExisting", BsonBoolean.TRUE);
            }
        } else if (!remove) {
            lastError.put("n", new BsonInt32(upsert ? 1 : 0));
            lastError.put("updatedExisting", BsonBoolean.FALSE);
            if (upsert) {
                BsonDocument doc = buildUpsert(query, update, uctx, coll0);
                store.ensureCollection(db, coll);
                PostgresDocumentStore.CollMeta m2 = store.meta(db, coll);
                validate(db, coll, m2, doc, null, bypass);
                store.insert(db, coll, doc, m2);
                lastError.put("upserted", doc.get("_id"));
                value = returnNew ? doc : BsonNull.VALUE;
            }
        } else {
            lastError.put("n", new BsonInt32(0));
        }
        if (value.isDocument() && proj != null && !proj.isIdentity()) {
            value = proj.apply(value.asDocument());
        }
        BsonDocument reply = new BsonDocument();
        if (unacknowledged(cmd)) {
            reply.put("ok", new BsonDouble(1.0));
            return reply;
        }
        reply.put("lastErrorObject", lastError);
        reply.put("value", value);
        BsonDocument wce = writeConcernError(cmd);
        if (wce != null) {
            reply.put("writeConcernError", wce);
        }
        reply.put("ok", new BsonDouble(1.0));
        return reply;
    }
}
