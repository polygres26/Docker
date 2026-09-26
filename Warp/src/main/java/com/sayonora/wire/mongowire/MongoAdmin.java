package com.sayonora.wire.mongowire;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

/** Collection / index / database administration commands. */
final class MongoAdmin {

    private static final long START = System.currentTimeMillis();
    private final PostgresDocumentStore store;

    MongoAdmin(PostgresDocumentStore store) {
        this.store = store;
    }

    private static BsonDocument ok() {
        return new BsonDocument("ok", new BsonDouble(1.0));
    }

    // ------------------------------------------------------------------ collections

    private static final Set<String> CREATE_OPTIONS = Set.of("create", "capped", "autoIndexId", "size", "max", "storageEngine",
            "validator", "validationLevel", "validationAction", "indexOptionDefaults", "viewOn", "pipeline", "collation",
            "writeConcern", "expireAfterSeconds", "timeseries", "changeStreamPreAndPostImages", "clusteredIndex", "encryptedFields",
            "recordIdsReplicated", "flags", "temp", "comment", "lsid", "$db", "maxTimeMS", "$clusterTime", "txnNumber");

    BsonDocument create(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "create");
        PostgresDocumentStore.validateDb(db);
        PostgresDocumentStore.validateColl(coll);
        BsonDocument options = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : cmd.entrySet()) {
            String k = e.getKey();
            if (k.equals("create") || k.startsWith("$") || k.equals("lsid") || k.equals("writeConcern") || k.equals("comment")
                    || k.equals("maxTimeMS") || k.equals("txnNumber")) {
                continue;
            }
            if (!CREATE_OPTIONS.contains(k)) {
                throw new MongoCmdException(40415, "BSON field 'create." + k + "' is an unknown field.");
            }
            options.put(k, e.getValue());
        }
        if (options.containsKey("viewOn")) {
            throw new MongoCmdException(115, "views are not supported by Warp's MongoDB frontend");
        }
        if (options.containsKey("timeseries") || options.containsKey("clusteredIndex")) {
            throw new MongoCmdException(115, "time series and clustered collections are not supported by Warp's MongoDB frontend");
        }
        if (options.containsKey("validator")) {
            if (!options.get("validator").isDocument()) {
                throw new MongoCmdException(14, "BSON field 'create.validator' is the wrong type '" + MongoMatcher.typeName(options.get("validator"))
                        + "', expected type 'object'");
            }
            MongoMatcher.compile(options.getDocument("validator"));
        }
        if (options.containsKey("capped") && options.getBoolean("capped").getValue() && !options.containsKey("size")) {
            throw new MongoCmdException(72, "the 'size' field is required when 'capped' is true");
        }
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (m.exists) {
            if (m.options.equals(options) || options.isEmpty() && m.options.isEmpty()) {
                return ok();
            }
            throw new MongoCmdException(48, "Collection " + db + "." + coll + " already exists.");
        }
        store.createCollection(db, coll, options);
        return ok();
    }

    BsonDocument drop(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "drop");
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (!m.exists) {
            return ok();
        }
        int n = m.indexes.size() + 1;
        store.dropCollection(db, coll);
        BsonDocument r = new BsonDocument("nIndexesWas", new BsonInt32(n)).append("ns", new BsonString(db + "." + coll));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    BsonDocument dropDatabase(BsonDocument cmd, String db) throws SQLException {
        store.dropDatabase(db);
        return ok();
    }

    BsonDocument renameCollection(BsonDocument cmd, String db) throws SQLException {
        BsonValue f = cmd.get("renameCollection");
        BsonValue t = cmd.get("to");
        if (f == null || !f.isString()) {
            throw new MongoCmdException(14, "BSON field 'renameCollection.renameCollection' is the wrong type, expected type 'string'");
        }
        if (t == null) {
            throw new MongoCmdException(40414, "BSON field 'renameCollection.to' is missing but a required field");
        }
        if (!t.isString()) {
            throw new MongoCmdException(14, "BSON field 'renameCollection.to' is the wrong type '" + MongoMatcher.typeName(t) + "', expected type 'string'");
        }
        String from = f.asString().getValue();
        String to = t.asString().getValue();
        int fd = from.indexOf('.');
        int td = to.indexOf('.');
        if (fd <= 0 || td <= 0) {
            throw new MongoCmdException(73, "Invalid namespace specified '" + (fd <= 0 ? from : to) + "'");
        }
        String fdb = from.substring(0, fd);
        String fcoll = from.substring(fd + 1);
        String tdb = to.substring(0, td);
        String tcoll = to.substring(td + 1);
        boolean dropTarget = cmd.containsKey("dropTarget") && MongoExpr.truthy(cmd.get("dropTarget"));
        if (!fdb.equals(tdb)) {
            throw new MongoCmdException(115, "renameCollection across databases is not supported by Warp's MongoDB frontend");
        }
        PostgresDocumentStore.CollMeta src = store.meta(fdb, fcoll);
        if (!src.exists) {
            throw new MongoCmdException(26, "Source collection " + from + " does not exist");
        }
        PostgresDocumentStore.CollMeta dst = store.meta(tdb, tcoll);
        if (fcoll.equals(tcoll)) {
            throw new MongoCmdException(20, "Can't rename a collection to itself");
        }
        if (dst.exists && !dropTarget) {
            throw new MongoCmdException(48, "target namespace exists");
        }
        if (fcoll.equals(tcoll)) {
            throw new MongoCmdException(20, "Can't rename a collection to itself");
        }
        store.renameCollection(fdb, fcoll, tcoll, dropTarget);
        return ok();
    }

    BsonDocument listCollections(BsonDocument cmd, String db) throws SQLException {
        BsonDocument filter = MongoCrud.optDoc(cmd, "listCollections", "filter");
        boolean nameOnly = cmd.containsKey("nameOnly") && MongoExpr.truthy(cmd.get("nameOnly"));
        BsonDocument cursor = MongoCrud.optDoc(cmd, "listCollections", "cursor");
        long batch = cursor != null && cursor.containsKey("batchSize") ? MongoNum.truncLong(cursor.get("batchSize")) : -1;
        MongoMatcher.Pred pred = MongoMatcher.compile(filter);
        List<BsonDocument> out = new ArrayList<>();
        PostgresDocumentStore.validateDb(db);
        for (String name : store.listCollections(db)) {
            PostgresDocumentStore.CollMeta m = store.meta(db, name);
            BsonDocument e = new BsonDocument("name", new BsonString(name)).append("type", new BsonString("collection"));
            if (!nameOnly) {
                e.put("options", m.options);
                e.put("info", new BsonDocument("readOnly", BsonBoolean.FALSE).append("uuid", m.uuid));
                e.put("idIndex", new BsonDocument("v", new BsonInt32(2)).append("key", new BsonDocument("_id", new BsonInt32(1)))
                        .append("name", new BsonString("_id_")));
            }
            if (pred.test(e)) {
                out.add(e);
            }
        }
        out.sort((a, b) -> a.getString("name").getValue().compareTo(b.getString("name").getValue()));
        Stream<BsonDocument> s = out.stream();
        return MongoCursors.reply(db + ".$cmd.listCollections", s.iterator(), s, batch >= 0 ? Long.valueOf(batch) : null, -1, false, true, true);
    }

    BsonDocument listDatabases(BsonDocument cmd, String db) throws SQLException {
        boolean nameOnly = cmd.containsKey("nameOnly") && MongoExpr.truthy(cmd.get("nameOnly"));
        BsonDocument filter = MongoCrud.optDoc(cmd, "listDatabases", "filter");
        MongoMatcher.Pred pred = MongoMatcher.compile(filter);
        List<String> names = new ArrayList<>(List.of("admin", "config", "local"));
        for (String n : store.listDatabases()) {
            if (!names.contains(n)) {
                names.add(n);
            }
        }
        java.util.Collections.sort(names);
        BsonArray arr = new BsonArray();
        long total = 0;
        for (String n : names) {
            BsonDocument e = new BsonDocument("name", new BsonString(n));
            long size = 0;
            boolean empty = true;
            if (!List.of("admin", "config", "local").contains(n)) {
                for (String c : store.listCollections(n)) {
                    long[] st = store.storageStats(n, c);
                    size += st[1] + 8192;
                    if (st[0] > 0) {
                        empty = false;
                    }
                }
                empty = size == 0 || empty && false;
            }
            e.put("sizeOnDisk", new BsonInt64(size));
            e.put("empty", BsonBoolean.valueOf(size == 0));
            if (!pred.test(e)) {
                continue;
            }
            total += size;
            if (nameOnly) {
                arr.add(new BsonDocument("name", new BsonString(n)));
            } else {
                arr.add(e);
            }
        }
        BsonDocument r = new BsonDocument("databases", arr);
        if (!nameOnly) {
            r.put("totalSize", new BsonInt64(total));
            r.put("totalSizeMb", new BsonInt64(total / (1024 * 1024)));
        }
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    // ------------------------------------------------------------------ indexes

    private static BsonDocument idIndex() {
        return new BsonDocument("v", new BsonInt32(2)).append("key", new BsonDocument("_id", new BsonInt32(1)))
                .append("name", new BsonString("_id_"));
    }

    BsonDocument listIndexes(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "listIndexes");
        BsonDocument cursor = MongoCrud.optDoc(cmd, "listIndexes", "cursor");
        long batch = cursor != null && cursor.containsKey("batchSize") ? MongoNum.truncLong(cursor.get("batchSize")) : -1;
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (!m.exists) {
            throw new MongoCmdException(26, "ns does not exist: " + db + "." + coll);
        }
        List<BsonDocument> out = new ArrayList<>();
        out.add(idIndex());
        out.addAll(m.indexes);
        Stream<BsonDocument> s = out.stream();
        return MongoCursors.reply(db + "." + coll, s.iterator(), s, batch >= 0 ? Long.valueOf(batch) : null, -1, false, true, true);
    }

    static String indexName(BsonDocument key) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, BsonValue> e : key.entrySet()) {
            if (sb.length() > 0) {
                sb.append('_');
            }
            sb.append(e.getKey()).append('_');
            BsonValue v = e.getValue();
            sb.append(v.isString() ? v.asString().getValue() : BsonCmp.isNumber(v) ? MongoExpr.toStringValue(v) : v.toString());
        }
        return sb.toString();
    }

    private static final Set<String> INDEX_FIELDS = Set.of("v", "key", "name", "unique", "sparse", "expireAfterSeconds", "partialFilterExpression",
            "collation", "hidden", "background", "storageEngine", "weights", "default_language", "language_override", "textIndexVersion",
            "2dsphereIndexVersion", "bits", "min", "max", "bucketSize", "wildcardProjection", "dropDups", "ns", "prefixKey", "clustered");

    BsonDocument createIndexes(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "createIndexes");
        PostgresDocumentStore.validateDb(db);
        PostgresDocumentStore.validateColl(coll);
        BsonValue ixs = cmd.get("indexes");
        if (ixs == null) {
            throw new MongoCmdException(40414, "BSON field 'createIndexes.indexes' is missing but a required field");
        }
        if (!ixs.isArray()) {
            throw new MongoCmdException(14, "BSON field 'createIndexes.indexes' is the wrong type '" + MongoMatcher.typeName(ixs) + "', expected type 'array'");
        }
        if (ixs.asArray().isEmpty()) {
            throw new MongoCmdException(2, "Must specify at least one index to create");
        }
        List<BsonDocument> specs = new ArrayList<>();
        List<BsonDocument> specs0 = new ArrayList<>();
        for (BsonValue v : ixs.asArray()) {
            if (!v.isDocument()) {
                throw new MongoCmdException(14, "The 'indexes' array must contain objects");
            }
            specs0.add(v.asDocument());
            specs.add(normalizeIndex(v.asDocument()));
        }
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        boolean createdColl = false;
        if (!m.exists) {
            store.createCollection(db, coll, new BsonDocument());
            createdColl = true;
            m = store.meta(db, coll);
        }
        int before = m.indexes.size() + 1;
        List<BsonDocument> current = new ArrayList<>(m.indexes);
        boolean changed = false;
        List<BsonDocument> added = new ArrayList<>();
        for (BsonDocument spec : specs) {
            BsonDocument key = spec.getDocument("key");
            String name = spec.getString("name").getValue();
            if (key.size() == 1 && key.containsKey("_id") && BsonCmp.compare(key.get("_id"), new BsonInt32(1)) == 0) {
                if (rawUnique(specs0, spec)) {
                    throw new MongoCmdException(197, "The field 'unique' is not valid for an _id index specification. Specification: " + MongoFmt.doc(spec));
                }
                continue;
            }
            BsonDocument sameName = null;
            BsonDocument sameKey = null;
            for (BsonDocument c : current) {
                if (c.getString("name").getValue().equals(name)) {
                    sameName = c;
                }
                if (keyMatches(c.getDocument("key"), key) && sameCollation(c, spec)) {
                    sameKey = c;
                }
            }
            if (sameName != null && sameKey != null && sameName == sameKey && optionsEqual(sameName, spec)) {
                continue;
            }
            if (sameName != null && sameKey == sameName && !optionsEqual(sameName, spec)) {
                throw new MongoCmdException(86, "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: "
                        + MongoFmt.doc(spec) + ", existing index: " + MongoFmt.doc(sameName));
            }
            if (sameName != null && (sameKey == null || sameName != sameKey)) {
                if (keyMatches(sameName.getDocument("key"), key)) {
                    throw new MongoCmdException(85, "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: "
                            + MongoFmt.doc(spec) + ", existing index: " + MongoFmt.doc(sameName));
                }
                throw new MongoCmdException(86, "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: "
                        + MongoFmt.doc(spec) + ", existing index: " + MongoFmt.doc(sameName));
            }
            if (sameKey != null) {
                if (optionsEqual(sameKey, spec) && sameKey.getString("name").getValue().equals(name)) {
                    continue;
                }
                throw new MongoCmdException(85, "Index already exists with a different name: " + sameKey.getString("name").getValue());
            }
            if (current.size() + 1 >= 64) {
                throw new MongoCmdException(67, "add index fails, too many indexes for " + db + "." + coll + " key:" + MongoFmt.doc(key));
            }
            if (spec.containsKey("unique") && spec.getBoolean("unique").getValue()) {
                PostgresDocumentStore.UniqueIndex ux = new PostgresDocumentStore.UniqueIndex(spec);
                try {
                    store.buildUniqueIndex(db, coll, ux, m);
                } catch (MongoCmdException e) {
                    throw new MongoCmdException(11000, "Index build failed: " + java.util.UUID.randomUUID() + ": Collection " + db + "." + coll
                            + " ( " + m.uuid.asUuid() + " ) :: caused by :: " + e.getMessage(), e.extra);
                }
            }
            current.add(spec);
            added.add(spec);
            changed = true;
        }
        if (changed) {
            store.saveMeta(db, coll, m.options, current, m.uuid);
        }
        BsonDocument r = new BsonDocument();
        r.put("createdCollectionAutomatically", BsonBoolean.valueOf(createdColl));
        r.put("numIndexesBefore", new BsonInt32(before));
        r.put("numIndexesAfter", new BsonInt32(current.size() + 1));
        if (!changed) {
            r.put("note", new BsonString("all indexes already exist"));
        }
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    private static boolean rawUnique(List<BsonDocument> raws, BsonDocument normalized) {
        for (BsonDocument r : raws) {
            if (r.containsKey("key") && r.get("key").isDocument() && keyMatches(r.getDocument("key"), normalized.getDocument("key"))
                    && r.containsKey("unique")) {
                return true;
            }
        }
        return false;
    }

    private static boolean keyMatches(BsonDocument a, BsonDocument b) {
        if (a.size() != b.size()) {
            return false;
        }
        var i = a.entrySet().iterator();
        var j = b.entrySet().iterator();
        while (i.hasNext()) {
            var x = i.next();
            var y = j.next();
            if (!x.getKey().equals(y.getKey()) || BsonCmp.compare(x.getValue(), y.getValue()) != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameCollation(BsonDocument a, BsonDocument b) {
        BsonValue x = a.get("collation");
        BsonValue y = b.get("collation");
        return x == null && y == null || x != null && y != null && BsonCmp.compare(x, y) == 0;
    }

    private static boolean optionsEqual(BsonDocument a, BsonDocument b) {
        for (String k : List.of("unique", "sparse", "expireAfterSeconds", "partialFilterExpression", "collation", "hidden")) {
            BsonValue x = a.get(k);
            BsonValue y = b.get(k);
            if (k.equals("unique") || k.equals("sparse") || k.equals("hidden")) {
                boolean bx = x != null && MongoExpr.truthy(x);
                boolean by = y != null && MongoExpr.truthy(y);
                if (bx != by) {
                    return false;
                }
            } else if (x == null != (y == null) || x != null && BsonCmp.compare(x, y) != 0) {
                return false;
            }
        }
        return true;
    }

    private BsonDocument normalizeIndex(BsonDocument in) {
        for (String k : in.keySet()) {
            if (!INDEX_FIELDS.contains(k)) {
                throw new MongoCmdException(197, "The field '" + k + "' is not valid for an index specification. Specification: "
                        + MongoFmt.doc(in));
            }
        }
        if (!in.containsKey("key")) {
            throw new MongoCmdException(40414, "BSON field 'createIndexes.indexes.key' is missing but a required field");
        }
        if (!in.get("key").isDocument()) {
            throw new MongoCmdException(14, "The field 'key' must be an object");
        }
        BsonDocument key = in.getDocument("key");
        if (key.isEmpty()) {
            throw new MongoCmdException(67, "Index keys cannot be empty.");
        }
        for (Map.Entry<String, BsonValue> e : key.entrySet()) {
            BsonValue v = e.getValue();
            if (e.getKey().isEmpty() || e.getKey().startsWith(".") || e.getKey().endsWith(".") || e.getKey().contains("..")) {
                throw new MongoCmdException(67, "Index keys cannot contain an empty field name or empty path component");
            }
            if (v.isString()) {
                String t = v.asString().getValue();
                if (t.equals("text") || t.equals("2d") || t.equals("2dsphere")) {
                    throw new MongoCmdException(115, "index type '" + t + "' is not supported by Warp's MongoDB frontend");
                }
                if (!t.equals("hashed")) {
                    throw new MongoCmdException(67, "Unknown index plugin '" + t + "' in index " + MongoFmt.doc(key));
                }
            } else if (BsonCmp.isNumber(v)) {
                double d = MongoNum.toDouble(v);
                if (d == 0 || Double.isNaN(d)) {
                    throw new MongoCmdException(67, "Values in v:2 index key pattern cannot be 0");
                }
            } else {
                throw new MongoCmdException(67, "Values in v:2 index key pattern must be numbers or strings: " + MongoFmt.doc(key));
            }
        }
        BsonDocument out = new BsonDocument("v", new BsonInt32(2));
        if (!in.containsKey("name")) {
            throw new MongoCmdException(9, "Error in specification " + MongoFmt.doc(in) + " :: caused by :: The 'name' field is a required property");
        }
        String name = in.get("name").isString() ? in.getString("name").getValue() : null;
        if (name == null) {
            throw new MongoCmdException(14, "The field 'name' for an index must be a string");
        }
        if (name.isEmpty()) {
            throw new MongoCmdException(67, "index name cannot be empty");
        }
        out.put("key", key);
        out.put("name", new BsonString(name));
        if (in.containsKey("unique") && MongoExpr.truthy(in.get("unique"))) {
            out.put("unique", BsonBoolean.TRUE);
        }
        if (in.containsKey("sparse")) {
            out.put("sparse", BsonBoolean.valueOf(MongoExpr.truthy(in.get("sparse"))));
        }
        if (in.containsKey("expireAfterSeconds")) {
            BsonValue e = in.get("expireAfterSeconds");
            if (!BsonCmp.isNumber(e) || MongoNum.truncLong(e) < 0) {
                throw new MongoCmdException(67, "TTL index 'expireAfterSeconds' option must be within an acceptable range, try a non-negative number less than 2147483648.");
            }
            if (key.size() != 1) {
                throw new MongoCmdException(67, "TTL indexes are single-field indexes, compound indexes do not support TTL");
            }
            out.put("expireAfterSeconds", e);
        }
        if (in.containsKey("partialFilterExpression")) {
            if (!in.get("partialFilterExpression").isDocument()) {
                throw new MongoCmdException(14, "'partialFilterExpression' for an index has to be a document");
            }
            if (out.containsKey("sparse") && out.getBoolean("sparse").getValue()) {
                throw new MongoCmdException(67, "cannot mix \"partialFilterExpression\" and \"sparse\" options");
            }
            MongoMatcher.compile(in.getDocument("partialFilterExpression"));
            out.put("partialFilterExpression", in.get("partialFilterExpression"));
        }
        if (in.containsKey("collation")) {
            MongoCrud.collationFrom(in.getDocument("collation"));
            BsonDocument cin = in.getDocument("collation");
            BsonDocument full = new BsonDocument("locale", cin.get("locale"))
                    .append("caseLevel", cin.getOrDefault("caseLevel", BsonBoolean.FALSE))
                    .append("caseFirst", cin.getOrDefault("caseFirst", new BsonString("off")))
                    .append("strength", cin.getOrDefault("strength", new BsonInt32(3)))
                    .append("numericOrdering", cin.getOrDefault("numericOrdering", BsonBoolean.FALSE))
                    .append("alternate", cin.getOrDefault("alternate", new BsonString("non-ignorable")))
                    .append("maxVariable", cin.getOrDefault("maxVariable", new BsonString("punct")))
                    .append("normalization", cin.getOrDefault("normalization", BsonBoolean.FALSE))
                    .append("backwards", cin.getOrDefault("backwards", BsonBoolean.FALSE))
                    .append("version", new BsonString("57.1"));
            out.put("collation", full);
        }
        if (in.containsKey("hidden")) {
            out.put("hidden", BsonBoolean.valueOf(MongoExpr.truthy(in.get("hidden"))));
        }
        return out;
    }

    BsonDocument dropIndexes(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "dropIndexes");
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (!m.exists) {
            throw new MongoCmdException(26, "ns not found " + db + "." + coll);
        }
        BsonValue which = cmd.get("index");
        if (which == null) {
            throw new MongoCmdException(40414, "BSON field 'dropIndexes.index' is missing but a required field");
        }
        List<BsonDocument> current = new ArrayList<>(m.indexes);
        int before = current.size() + 1;
        BsonDocument r = new BsonDocument("nIndexesWas", new BsonInt32(before));
        List<String> names = new ArrayList<>();
        if (which.isString() && which.asString().getValue().equals("*")) {
            for (BsonDocument c : current) {
                names.add(c.getString("name").getValue());
            }
            r.put("msg", new BsonString("non-_id indexes dropped for collection"));
        } else if (which.isString()) {
            names.add(which.asString().getValue());
        } else if (which.isArray()) {
            for (BsonValue v : which.asArray()) {
                if (!v.isString()) {
                    throw new MongoCmdException(14, "dropIndexes 'index' array entries must be strings");
                }
                names.add(v.asString().getValue());
            }
        } else if (which.isDocument()) {
            String found = null;
            for (BsonDocument c : current) {
                if (keyMatches(c.getDocument("key"), which.asDocument())) {
                    found = c.getString("name").getValue();
                }
            }
            if (found == null) {
                if (keyMatches(idIndex().getDocument("key"), which.asDocument())) {
                    throw new MongoCmdException(72, "cannot drop _id index");
                }
                throw new MongoCmdException(27, "can't find index with key: " + MongoFmt.doc(which.asDocument()));
            }
            names.add(found);
        } else {
            throw new MongoCmdException(14, "dropIndexes 'index' must be a string, array or object");
        }
        for (String n : names) {
            if (n.equals("_id_")) {
                throw new MongoCmdException(72, "cannot drop _id index");
            }
            BsonDocument hit = null;
            for (BsonDocument c : current) {
                if (c.getString("name").getValue().equals(n)) {
                    hit = c;
                }
            }
            if (hit == null) {
                throw new MongoCmdException(27, "index not found with name [" + n + "]");
            }
        }
        for (String n : names) {
            current.removeIf(c -> c.getString("name").getValue().equals(n));
            store.dropUniqueKeys(db, coll, n);
        }
        store.saveMeta(db, coll, m.options, current, m.uuid);
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    BsonDocument collMod(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "collMod");
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (!m.exists) {
            throw new MongoCmdException(26, "ns does not exist: " + db + "." + coll);
        }
        BsonDocument options = m.options.clone();
        List<BsonDocument> indexes = new ArrayList<>(m.indexes);
        BsonDocument r = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : cmd.entrySet()) {
            String k = e.getKey();
            switch (k) {
                case "collMod", "$db", "lsid", "writeConcern", "comment", "maxTimeMS", "$clusterTime", "txnNumber" -> { }
                case "validator" -> {
                    if (!e.getValue().isDocument()) {
                        throw new MongoCmdException(14, "BSON field 'collMod.validator' is the wrong type, expected type 'object'");
                    }
                    MongoMatcher.compile(e.getValue().asDocument());
                    options.put("validator", e.getValue());
                    if (!options.containsKey("validationLevel")) {
                        options.put("validationLevel", new BsonString("strict"));
                    }
                    if (!options.containsKey("validationAction")) {
                        options.put("validationAction", new BsonString("error"));
                    }
                }
                case "validationLevel" -> {
                    String v = e.getValue().isString() ? e.getValue().asString().getValue() : "";
                    if (!List.of("off", "strict", "moderate").contains(v)) {
                        throw new MongoCmdException(2, "Enumeration value '" + v + "' for field 'collMod.validationLevel' is not a valid value.");
                    }
                    options.put("validationLevel", e.getValue());
                }
                case "validationAction" -> {
                    String v = e.getValue().isString() ? e.getValue().asString().getValue() : "";
                    if (!List.of("warn", "error").contains(v)) {
                        throw new MongoCmdException(2, "Enumeration value '" + v + "' for field 'collMod.validationAction' is not a valid value.");
                    }
                    options.put("validationAction", e.getValue());
                }
                case "index" -> {
                    BsonDocument spec = e.getValue().asDocument();
                    String name = null;
                    if (spec.containsKey("name")) {
                        name = spec.getString("name").getValue();
                    } else if (spec.containsKey("keyPattern")) {
                        for (BsonDocument c : indexes) {
                            if (keyMatches(c.getDocument("key"), spec.getDocument("keyPattern"))) {
                                name = c.getString("name").getValue();
                            }
                        }
                    }
                    BsonDocument hit = null;
                    int pos = -1;
                    for (int i = 0; i < indexes.size(); i++) {
                        if (indexes.get(i).getString("name").getValue().equals(name)) {
                            hit = indexes.get(i);
                            pos = i;
                        }
                    }
                    if (hit == null) {
                        throw new MongoCmdException(27, "cannot find index " + name + " for ns " + db + "." + coll);
                    }
                    BsonDocument upd = hit.clone();
                    if (spec.containsKey("expireAfterSeconds")) {
                        if (!hit.containsKey("expireAfterSeconds")) {
                            throw new MongoCmdException(2, "no expireAfterSeconds field to update");
                        }
                        r.put("expireAfterSeconds_old", new BsonInt64(MongoNum.truncLong(hit.get("expireAfterSeconds"))));
                        r.put("expireAfterSeconds_new", new BsonInt64(MongoNum.truncLong(spec.get("expireAfterSeconds"))));
                        upd.put("expireAfterSeconds", spec.get("expireAfterSeconds"));
                    }
                    if (spec.containsKey("hidden")) {
                        r.put("hidden_old", hit.containsKey("hidden") ? hit.get("hidden") : BsonBoolean.FALSE);
                        r.put("hidden_new", spec.get("hidden"));
                        upd.put("hidden", spec.get("hidden"));
                    }
                    indexes.set(pos, upd);
                }
                case "capped", "size", "max", "recordPreImages", "changeStreamPreAndPostImages", "expireAfterSeconds", "viewOn", "pipeline",
                        "cacheEnabled", "timeseries", "dryRun", "prepareUnique", "unique", "clusteredIndex" ->
                    throw new MongoCmdException(115, "collMod option '" + k + "' is not supported by Warp's MongoDB frontend");
                default -> throw new MongoCmdException(40415, "BSON field 'collMod." + k + "' is an unknown field.");
            }
        }
        store.saveMeta(db, coll, options, indexes, m.uuid);
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    // ------------------------------------------------------------------ stats

    BsonDocument dbStats(BsonDocument cmd, String db) throws SQLException {
        long scale = cmd.containsKey("scale") ? Math.max(1, MongoNum.truncLong(cmd.get("scale"))) : 1;
        long objects = 0;
        long dataSize = 0;
        long indexes = 0;
        List<String> colls = store.listCollections(db);
        for (String c : colls) {
            long[] st = store.storageStats(db, c);
            objects += st[0];
            dataSize += st[1];
            indexes += store.meta(db, c).indexes.size() + 1;
        }
        BsonDocument r = new BsonDocument("db", new BsonString(db)).append("collections", new BsonInt64(colls.size()))
                .append("views", new BsonInt64(0)).append("objects", new BsonInt64(objects))
                .append("avgObjSize", new BsonDouble(objects == 0 ? 0 : (double) dataSize / objects))
                .append("dataSize", new BsonDouble((double) dataSize / scale))
                .append("storageSize", new BsonDouble((double) (dataSize + colls.size() * 4096L) / scale))
                .append("indexes", new BsonInt64(indexes)).append("indexSize", new BsonDouble((double) indexes * 4096 / scale))
                .append("totalSize", new BsonDouble((double) (dataSize + colls.size() * 4096L + indexes * 4096) / scale))
                .append("scaleFactor", new BsonInt64(scale)).append("fsUsedSize", new BsonDouble(0)).append("fsTotalSize", new BsonDouble(0));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    BsonDocument collStats(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "collStats");
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        long scale = cmd.containsKey("scale") ? Math.max(1, MongoNum.truncLong(cmd.get("scale"))) : 1;
        long[] st = store.storageStats(db, coll);
        BsonDocument sizes = new BsonDocument("_id_", new BsonInt32(4096));
        for (BsonDocument ix : m.indexes) {
            sizes.put(ix.getString("name").getValue(), new BsonInt32(4096));
        }
        BsonDocument r = new BsonDocument("ns", new BsonString(db + "." + coll)).append("size", new BsonInt64(st[1] / scale))
                .append("count", new BsonInt64(st[0])).append("avgObjSize", new BsonInt64(st[0] == 0 ? 0 : st[1] / st[0]))
                .append("numOrders", new BsonInt32(0)).append("storageSize", new BsonInt64((st[1] + 4096) / scale))
                .append("freeStorageSize", new BsonInt64(0)).append("capped", BsonBoolean.valueOf(m.options.containsKey("capped")
                        && m.options.getBoolean("capped").getValue()))
                .append("nindexes", new BsonInt32(m.indexes.size() + 1))
                .append("indexBuilds", new BsonArray()).append("totalIndexSize", new BsonInt64(4096L * (m.indexes.size() + 1) / scale))
                .append("totalSize", new BsonInt64((st[1] + 4096 + 4096L * (m.indexes.size() + 1)) / scale)).append("indexSizes", sizes)
                .append("scaleFactor", new BsonInt64(scale));
        r.remove("numOrders");
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    BsonDocument validate(BsonDocument cmd, String db) throws SQLException {
        String coll = MongoCrud.collName(cmd, "validate");
        PostgresDocumentStore.CollMeta m = store.meta(db, coll);
        if (!m.exists) {
            throw new MongoCmdException(26, "Collection '" + db + "." + coll + "' does not exist to validate.");
        }
        long n = store.countAll(db, coll);
        BsonDocument keys = new BsonDocument("_id_", new BsonInt64(n));
        for (BsonDocument ix : m.indexes) {
            keys.put(ix.getString("name").getValue(), new BsonInt64(n));
        }
        BsonDocument r = new BsonDocument("ns", new BsonString(db + "." + coll)).append("uuid", m.uuid).append("nInvalidDocuments", new BsonInt32(0))
                .append("nNonCompliantDocuments", new BsonInt32(0)).append("nrecords", new BsonInt64(n))
                .append("nIndexes", new BsonInt32(m.indexes.size() + 1)).append("keysPerIndex", keys)
                .append("indexDetails", new BsonDocument()).append("valid", BsonBoolean.TRUE).append("warnings", new BsonArray())
                .append("errors", new BsonArray()).append("extraIndexEntries", new BsonArray()).append("missingIndexEntries", new BsonArray());
        r.put("ok", new BsonDouble(1.0));
        return r;
    }

    // ------------------------------------------------------------------ server info

    static BsonDocument serverStatus(int connections) {
        long now = System.currentTimeMillis();
        BsonDocument r = new BsonDocument("host", new BsonString("warp")).append("version", new BsonString("7.0.0"))
                .append("process", new BsonString("mongod")).append("pid", new BsonInt64(ProcessHandle.current().pid()))
                .append("uptime", new BsonDouble((now - START) / 1000.0)).append("uptimeMillis", new BsonInt64(now - START))
                .append("uptimeEstimate", new BsonInt64((now - START) / 1000)).append("localTime", new BsonDateTime(now))
                .append("connections", new BsonDocument("current", new BsonInt32(Math.max(1, connections))).append("available", new BsonInt32(100000))
                        .append("totalCreated", new BsonInt32(Math.max(1, connections))).append("active", new BsonInt32(1)))
                .append("opcounters", new BsonDocument("insert", new BsonInt64(0)).append("query", new BsonInt64(0))
                        .append("update", new BsonInt64(0)).append("delete", new BsonInt64(0)).append("getmore", new BsonInt64(0))
                        .append("command", new BsonInt64(0)))
                .append("mem", new BsonDocument("bits", new BsonInt32(64)).append("resident", new BsonInt32(100)).append("virtual", new BsonInt32(1000)))
                .append("storageEngine", new BsonDocument("name", new BsonString("postgres")));
        r.put("ok", new BsonDouble(1.0));
        return r;
    }
}
