package com.sayonora.warp.mongowire;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/** The aggregation pipeline engine: stages run as lazy streams over documents scanned from the store. */
final class MongoAgg {

    private MongoAgg() {
    }

    interface Source {
        /** All documents of a collection (caller closes the stream). */
        Stream<BsonDocument> scan(String db, String coll);
    }

    interface Sink {
        void replaceCollection(String db, String coll, List<BsonDocument> docs);

        BsonDocument findOne(String db, String coll, BsonDocument filter);

        void insert(String db, String coll, BsonDocument doc);

        void replace(String db, String coll, BsonDocument oldDoc, BsonDocument newDoc);
    }

    static final class Ctx {
        String db;
        String coll;
        BsonCmp.Collation collation;
        long now = System.currentTimeMillis();
        Source source;
        Sink sink;
        Map<String, BsonValue> vars = Map.of();
        /** Set once a $out/$merge stage ran. */
        boolean wrote;

        Ctx child(Map<String, BsonValue> newVars) {
            Ctx c = new Ctx();
            c.db = db;
            c.coll = coll;
            c.collation = collation;
            c.now = now;
            c.source = source;
            c.sink = sink;
            c.vars = newVars;
            return c;
        }

        MongoExpr.Scope scope(BsonDocument d) {
            return new MongoExpr.Scope(d, d, vars, collation, now);
        }
    }

    static Stream<BsonDocument> run(BsonArray pipeline, Stream<BsonDocument> input, Ctx ctx) {
        Stream<BsonDocument> s = input;
        int n = 0;
        List<BsonDocument> stages = new ArrayList<>();
        for (BsonValue st : pipeline) {
            if (!st.isDocument()) {
                throw new MongoCmdException(14, "Each element of the 'pipeline' array must be an object");
            }
            if (st.asDocument().size() != 1) {
                throw new MongoCmdException(40323, "A pipeline stage specification object must contain exactly one field.");
            }
            stages.add(st.asDocument());
        }
        for (int i = 0; i < stages.size(); i++) {
            BsonDocument st = stages.get(i);
            String name = st.getFirstKey();
            if ((name.equals("$out") || name.equals("$merge")) && i != stages.size() - 1) {
                throw new MongoCmdException(40601, "$out/$merge can only be the final stage in the pipeline");
            }
            s = stage(name, st.get(name), s, ctx, i == 0);
        }
        return s;
    }

    static BsonDocument applyStages(BsonArray stages, BsonDocument doc, BsonCmp.Collation coll) {
        return applyStages(stages, doc, coll, Map.of());
    }

    static BsonDocument applyStages(BsonArray stages, BsonDocument doc, BsonCmp.Collation coll, Map<String, BsonValue> vars) {
        Ctx ctx = new Ctx();
        ctx.collation = coll;
        ctx.vars = vars;
        List<BsonDocument> out = run(stages, Stream.of(doc), ctx).collect(Collectors.toList());
        if (out.size() != 1) {
            throw new MongoCmdException(2, "pipeline update produced " + out.size() + " documents");
        }
        return out.get(0);
    }

    static boolean isNumberLike(BsonValue v) {
        return BsonCmp.isNumber(v);
    }

    private static long positiveInt(BsonValue v, int code, String what) {
        if (!BsonCmp.isNumber(v)) {
            throw new MongoCmdException(code, "Argument to " + what + " must be a number");
        }
        double d = MongoNum.toDouble(v);
        if (d != Math.floor(d) || Double.isInfinite(d)) {
            throw new MongoCmdException(code, "invalid argument to " + what + ": " + MongoFmt.value(v) + " (must be an integer)");
        }
        return (long) d;
    }

    private static Stream<BsonDocument> stage(String name, BsonValue spec, Stream<BsonDocument> in, Ctx ctx, boolean first) {
        BsonCmp.Collation coll = ctx.collation;
        switch (name) {
            case "$match": {
                if (!spec.isDocument()) {
                    throw new MongoCmdException(15959, "the match filter must be an expression in an object");
                }
                MongoMatcher.Pred p = MongoMatcher.compile(spec.asDocument(), coll, ctx.vars);
                return in.filter(p::test);
            }
            case "$project": {
                if (!spec.isDocument()) {
                    throw new MongoCmdException(15969, "$project specification must be an object");
                }
                MongoProjection pr = MongoProjection.compile(spec.asDocument(), true, null, coll);
                return in.map(d -> pr.apply(d, ctx));
            }
            case "$addFields": case "$set": {
                if (!spec.isDocument()) {
                    throw new MongoCmdException(40272, name + " specification stage must be an object, got " + MongoMatcher.typeName(spec));
                }
                List<String[]> paths = new ArrayList<>();
                List<MongoExpr.Expr> exprs = new ArrayList<>();
                flattenSpec(spec.asDocument(), "", paths, exprs);
                return in.map(d -> {
                    MongoExpr.Scope sc = ctx.scope(d);
                    BsonDocument out = d;
                    for (int i = 0; i < paths.size(); i++) {
                        BsonValue v = exprs.get(i).eval(sc);
                        out = (BsonDocument) setCow(out, paths.get(i), 0, v);
                    }
                    return out;
                });
            }
            case "$unset": {
                List<String> fields = new ArrayList<>();
                if (spec.isString()) {
                    fields.add(spec.asString().getValue());
                } else if (spec.isArray() && !spec.asArray().isEmpty()) {
                    for (BsonValue v : spec.asArray()) {
                        if (!v.isString()) {
                            throw new MongoCmdException(31120, "$unset specification must be a string or an array containing only string values");
                        }
                        fields.add(v.asString().getValue());
                    }
                } else {
                    throw new MongoCmdException(31002, "$unset specification must be a string or an array");
                }
                BsonDocument ps = new BsonDocument();
                fields.forEach(f -> ps.put(f, new BsonInt32(0)));
                MongoProjection pr = MongoProjection.compile(ps, true, null, coll);
                return in.map(d -> pr.apply(d, ctx));
            }
            case "$replaceRoot": case "$replaceWith": {
                BsonValue e;
                if (name.equals("$replaceRoot")) {
                    if (!spec.isDocument() || !spec.asDocument().containsKey("newRoot")) {
                        throw new MongoCmdException(40414, "BSON field '$replaceRoot.newRoot' is missing but a required field");
                    }
                    if (spec.asDocument().size() != 1) {
                        throw new MongoCmdException(40230, "unrecognized option to $replaceRoot stage: "
                                + spec.asDocument().keySet().stream().filter(k -> !k.equals("newRoot")).findFirst().get()
                                + ", only newRoot is supported.");
                    }
                    e = spec.asDocument().get("newRoot");
                } else {
                    e = spec;
                }
                MongoExpr.Expr ex = MongoExpr.parse(e);
                return in.map(d -> {
                    BsonValue v = ex.eval(ctx.scope(d));
                    if (v == MongoExpr.MISSING || !v.isDocument()) {
                        throw new MongoCmdException(40228, "'newRoot' expression must evaluate to an object, but resulting value was: "
                                + (v == MongoExpr.MISSING ? "MISSING" : MongoFmt.value(v)) + ". Type of resulting value: '"
                                + MongoExpr.typeOf(v) + "'. Input document: " + MongoFmt.doc(d));
                    }
                    return v.asDocument();
                });
            }
            case "$sort": {
                if (!spec.isDocument()) {
                    throw new MongoCmdException(15973, "the $sort key specification must be an object");
                }
                MongoSort.validate(spec.asDocument(), "$sort");
                Comparator<BsonDocument> c = MongoSort.comparator(spec.asDocument(), coll);
                return Stream.of((Object) null).flatMap(x -> in.sorted(c));
            }
            case "$limit": {
                long n = positiveInt(spec, 5107201, "$limit");
                if (n < 0) {
                    throw new MongoCmdException(5107201, "invalid argument to $limit stage: Expected a non-negative number in: $limit: " + MongoFmt.value(spec));
                }
                if (n == 0) {
                    throw new MongoCmdException(15958, "the limit must be positive");
                }
                return in.limit(n);
            }
            case "$skip": {
                long n = positiveInt(spec, 5107200, "$skip");
                if (n < 0) {
                    throw new MongoCmdException(5107200, "invalid argument to $skip stage: Expected a non-negative number in: $skip: " + MongoFmt.value(spec));
                }
                return in.skip(n);
            }
            case "$count": {
                if (!spec.isString() || spec.asString().getValue().isEmpty()) {
                    throw new MongoCmdException(spec.isString() ? 40157 : 40156, "the count field must be a non-empty string");
                }
                String f = spec.asString().getValue();
                if (f.startsWith("$")) {
                    throw new MongoCmdException(40158, "the count field cannot be a $-prefixed path");
                }
                if (f.contains(".")) {
                    throw new MongoCmdException(40160, "the count field cannot contain '.'");
                }
                return Stream.of((Object) null).flatMap(x -> {
                    long n = in.count();
                    return n == 0 ? Stream.<BsonDocument>empty() : Stream.of(new BsonDocument(f, MongoNum.fromLong(n)));
                });
            }
            case "$group":
                return group(spec, in, ctx);
            case "$unwind":
                return unwind(spec, in);
            case "$lookup":
                return lookup(spec, in, ctx);
            case "$facet": {
                if (!spec.isDocument()) {
                    throw new MongoCmdException(40169, "the $facet specification must be a non-empty object");
                }
                if (spec.asDocument().isEmpty()) {
                    throw new MongoCmdException(40169, "the $facet specification must be a non-empty object, but found: $facet: {}");
                }
                return Stream.of((Object) null).flatMap(x -> {
                    List<BsonDocument> all = in.collect(Collectors.toList());
                    BsonDocument out = new BsonDocument();
                    for (Map.Entry<String, BsonValue> e : spec.asDocument().entrySet()) {
                        if (!e.getValue().isArray()) {
                            throw new MongoCmdException(40170, "arguments to $facet must be arrays, " + e.getKey() + " is type "
                                    + MongoMatcher.typeName(e.getValue()));
                        }
                        for (BsonValue st : e.getValue().asArray()) {
                            if (st.isDocument() && !st.asDocument().isEmpty()) {
                                String sn = st.asDocument().getFirstKey();
                                if (List.of("$out", "$merge", "$facet").contains(sn)) {
                                    throw new MongoCmdException(40600, sn + " is not allowed to be used within a $facet stage");
                                }
                            }
                        }
                        BsonArray arr = new BsonArray(run(e.getValue().asArray(), all.stream(), ctx).collect(Collectors.toList()));
                        out.put(e.getKey(), arr);
                    }
                    return Stream.of(out);
                });
            }
            case "$bucket":
                return bucket(spec, in, ctx);
            case "$bucketAuto":
                return bucketAuto(spec, in, ctx);
            case "$sample": {
                if (!spec.isDocument() || !spec.asDocument().containsKey("size")) {
                    throw new MongoCmdException(28749, "$sample stage must specify a size");
                }
                long n = positiveInt(spec.asDocument().get("size"), 28746, "$sample size");
                if (n < 0) {
                    throw new MongoCmdException(28747, "size argument to $sample must not be negative");
                }
                return Stream.of((Object) null).flatMap(x -> {
                    List<BsonDocument> all = in.collect(Collectors.toList());
                    java.util.Collections.shuffle(all);
                    return all.stream().limit(n);
                });
            }
            case "$sortByCount": {
                if (!spec.isString() && !spec.isDocument()) {
                    throw new MongoCmdException(40149, "the sortByCount field must be specified as a string or as an object");
                }
                BsonDocument g = new BsonDocument("_id", spec).append("count", new BsonDocument("$sum", new BsonInt32(1)));
                Stream<BsonDocument> grouped = group(g, in, ctx);
                return grouped.sorted((a, b) -> BsonCmp.compare(b.get("count"), a.get("count")));
            }
            case "$unionWith": {
                String c;
                BsonArray pipe = new BsonArray();
                if (spec.isString()) {
                    c = spec.asString().getValue();
                } else if (spec.isDocument() && spec.asDocument().containsKey("coll")) {
                    c = spec.asDocument().getString("coll").getValue();
                    if (spec.asDocument().containsKey("pipeline")) {
                        pipe = spec.asDocument().getArray("pipeline");
                    }
                } else {
                    throw new MongoCmdException(9, "$unionWith must be a string or object with 'coll'");
                }
                BsonArray fp = pipe;
                return Stream.concat(in, Stream.of((Object) null).flatMap(x -> run(fp, ctx.source.scan(ctx.db, c), ctx)));
            }
            case "$redact": {
                MongoExpr.Expr e = MongoExpr.parse(spec);
                return in.map(d -> redact(d, e, ctx)).filter(d -> d != null);
            }
            case "$out": case "$merge":
                return writeStage(name, spec, in, ctx);
            case "$listLocalSessions": case "$listSessions":
                return Stream.of(new BsonDocument("_id", new BsonDocument("id", new org.bson.BsonBinary(java.util.UUID.randomUUID()))
                        .append("uid", new org.bson.BsonBinary(new byte[32]))).append("lastUse", new org.bson.BsonDateTime(System.currentTimeMillis())));
            case "$documents": {
                if (ctx.coll == null || !ctx.coll.equals("$cmd.aggregate")) {
                    throw new MongoCmdException(73, "'$documents' can only be run with {aggregate: 1}");
                }
                if (!first) {
                    throw new MongoCmdException(40602, "$documents is only valid as the first stage in a pipeline");
                }
                MongoExpr.Expr e = MongoExpr.parse(spec);
                BsonValue v = e.eval(ctx.scope(new BsonDocument()));
                List<BsonDocument> docs = new ArrayList<>();
                for (BsonValue x : v.asArray()) {
                    docs.add(x.asDocument());
                }
                return docs.stream();
            }
            case "$collStats": {
                return Stream.of((Object) null).flatMap(x -> {
                    long n = in.count();
                    BsonDocument out = new BsonDocument("ns", new BsonString(ctx.db + "." + ctx.coll))
                            .append("host", new BsonString("warp")).append("localTime", new org.bson.BsonDateTime(System.currentTimeMillis()));
                    if (spec.isDocument() && spec.asDocument().containsKey("count")) {
                        out.put("count", MongoNum.fromLong(n));
                    }
                    return Stream.of(out);
                });
            }
            case "$setWindowFields":
                return MongoWindow.setWindowFields(spec, in, ctx);
            case "$graphLookup":
                return graphLookup(spec, in, ctx);
            case "$densify": case "$fill": case "$geoNear": case "$search": case "$searchMeta": case "$indexStats":
            case "$planCacheStats": case "$currentOp": case "$changeStream":
            case "$listSearchIndexes": case "$queryStats": case "$shardedDataDistribution":
                throw new MongoCmdException(115, "Pipeline stage " + name + " is not supported by Warp");
            default:
                break;
        }
        throw new MongoCmdException(40324, "Unrecognized pipeline stage name: '" + name + "'");
    }

    // ------------------------------------------------------------------ $addFields helpers

    private static void flattenSpec(BsonDocument spec, String prefix, List<String[]> paths, List<MongoExpr.Expr> exprs) {
        for (Map.Entry<String, BsonValue> e : spec.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("$")) {
                throw new MongoCmdException(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField.");
            }
            String full = prefix.isEmpty() ? k : prefix + "." + k;
            BsonValue v = e.getValue();
            if (v.isDocument() && !v.asDocument().isEmpty() && !v.asDocument().getFirstKey().startsWith("$")) {
                flattenSpec(v.asDocument(), full, paths, exprs);
            } else {
                paths.add(full.split("\\.", -1));
                exprs.add(MongoExpr.parse(v));
            }
        }
    }

    /** Copy-on-write nested set; MISSING removes the field. */
    static BsonValue setCow(BsonValue node, String[] parts, int i, BsonValue value) {
        if (i == parts.length) {
            return value;
        }
        String p = parts[i];
        if (node != null && node.isDocument()) {
            BsonDocument copy = new BsonDocument();
            boolean found = false;
            for (Map.Entry<String, BsonValue> e : node.asDocument().entrySet()) {
                if (e.getKey().equals(p)) {
                    found = true;
                    BsonValue nv = setCow(e.getValue(), parts, i + 1, value);
                    if (nv != MongoExpr.MISSING) {
                        copy.put(p, nv);
                    }
                } else {
                    copy.put(e.getKey(), e.getValue());
                }
            }
            if (!found) {
                BsonValue nv = setCow(null, parts, i + 1, value);
                if (nv != MongoExpr.MISSING) {
                    copy.put(p, nv);
                }
            }
            return copy;
        }
        if (node != null && node.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : node.asArray()) {
                out.add(e.isDocument() || e.isArray() ? setCow(e, parts, i, value) : setCow(null, parts, i, value));
            }
            return out;
        }
        if (value == MongoExpr.MISSING) {
            return node == null ? MongoExpr.MISSING : node;
        }
        BsonValue nv = setCow(null, parts, i + 1, value);
        return nv == MongoExpr.MISSING ? new BsonDocument() : new BsonDocument(p, nv);
    }

    // ------------------------------------------------------------------ $unwind

    private static Stream<BsonDocument> unwind(BsonValue spec, Stream<BsonDocument> in) {
        String path;
        String idxField = null;
        boolean preserve = false;
        if (spec.isString()) {
            path = spec.asString().getValue();
        } else if (spec.isDocument()) {
            BsonDocument d = spec.asDocument();
            for (String k : d.keySet()) {
                if (!List.of("path", "includeArrayIndex", "preserveNullAndEmptyArrays").contains(k)) {
                    throw new MongoCmdException(28811, "unrecognized option to $unwind: " + k);
                }
            }
            if (!d.containsKey("path")) {
                throw new MongoCmdException(28812, "no path specified to $unwind stage");
            }
            if (!d.get("path").isString()) {
                throw new MongoCmdException(28808, "expected a string as the path for $unwind stage, got " + MongoMatcher.typeName(d.get("path")));
            }
            path = d.getString("path").getValue();
            if (d.containsKey("includeArrayIndex")) {
                if (!d.get("includeArrayIndex").isString()) {
                    throw new MongoCmdException(28810, "expected a non-empty string for the includeArrayIndex option to $unwind stage, got "
                            + MongoMatcher.typeName(d.get("includeArrayIndex")));
                }
                idxField = d.getString("includeArrayIndex").getValue();
                if (idxField.startsWith("$")) {
                    throw new MongoCmdException(28822, "includeArrayIndex option to $unwind stage should not be prefixed with a '$': " + idxField);
                }
            }
            if (d.containsKey("preserveNullAndEmptyArrays")) {
                if (!d.get("preserveNullAndEmptyArrays").isBoolean()) {
                    throw new MongoCmdException(28809, "expected a boolean for the preserveNullAndEmptyArrays option to $unwind stage, got "
                            + MongoMatcher.typeName(d.get("preserveNullAndEmptyArrays")));
                }
                preserve = d.getBoolean("preserveNullAndEmptyArrays").getValue();
            }
        } else {
            throw new MongoCmdException(15981, "expected either a string or an object as specification for $unwind stage, got "
                    + MongoMatcher.typeName(spec));
        }
        if (!path.startsWith("$")) {
            throw new MongoCmdException(28818, "path option to $unwind stage should be prefixed with a '$': " + path);
        }
        String[] parts = path.substring(1).split("\\.", -1);
        String idx = idxField;
        boolean keep = preserve;
        return in.flatMap(d -> {
            BsonValue v = plainPath(d, parts);
            if (v == MongoExpr.MISSING || v.isNull() || BsonCmp.isUndef(v) || v.isArray() && v.asArray().isEmpty()) {
                if (!keep) {
                    return Stream.empty();
                }
                BsonDocument out = d;
                if (v.isArray()) {
                    out = (BsonDocument) setCow(d, parts, 0, MongoExpr.MISSING);
                }
                if (idx != null) {
                    out = (BsonDocument) setCow(out, idx.split("\\.", -1), 0, BsonNull.VALUE);
                }
                return Stream.of(out);
            }
            if (!v.isArray()) {
                BsonDocument out = d;
                if (idx != null) {
                    out = (BsonDocument) setCow(out, idx.split("\\.", -1), 0, BsonNull.VALUE);
                }
                return Stream.of(out);
            }
            List<BsonDocument> outs = new ArrayList<>();
            BsonArray arr = v.asArray();
            for (int i = 0; i < arr.size(); i++) {
                BsonDocument o = (BsonDocument) setCow(d, parts, 0, arr.get(i));
                if (idx != null) {
                    o = (BsonDocument) setCow(o, idx.split("\\.", -1), 0, new BsonInt64(i));
                }
                outs.add(o);
            }
            return outs.stream();
        });
    }

    /** Field path lookup that does not traverse arrays (as $unwind requires). */
    static BsonValue plainPath(BsonValue v, String[] parts) {
        BsonValue cur = v;
        for (String p : parts) {
            if (cur == null || !cur.isDocument()) {
                return MongoExpr.MISSING;
            }
            cur = cur.asDocument().get(p);
        }
        return cur == null ? MongoExpr.MISSING : cur;
    }

    // ------------------------------------------------------------------ $lookup

    private static Stream<BsonDocument> lookup(BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        if (!spec.isDocument()) {
            throw new MongoCmdException(9, "the $lookup specification must be a non-empty object");
        }
        BsonDocument d = spec.asDocument();
        for (String k : d.keySet()) {
            if (!List.of("from", "localField", "foreignField", "as", "let", "pipeline").contains(k)) {
                throw new MongoCmdException(9, "unknown argument to $lookup: " + k);
            }
        }
        if (!d.containsKey("as")) {
            throw new MongoCmdException(9, "must specify 'as' field for a $lookup");
        }
        if (!d.get("as").isString()) {
            throw new MongoCmdException(9, "'as' field must be a string, but found " + MongoMatcher.typeName(d.get("as")));
        }
        String as = d.getString("as").getValue();
        String[] asParts = as.split("\\.", -1);
        boolean hasPipeline = d.containsKey("pipeline");
        boolean hasLocal = d.containsKey("localField");
        boolean hasForeign = d.containsKey("foreignField");
        if (!d.containsKey("from") && !hasPipeline) {
            throw new MongoCmdException(9, "must specify 'from' field for a $lookup");
        }
        if (hasLocal != hasForeign) {
            throw new MongoCmdException(9, "$lookup requires both or neither of 'localField' and 'foreignField' to be specified");
        }
        if (!hasLocal && !hasPipeline) {
            throw new MongoCmdException(9, "$lookup requires either 'pipeline' or both 'localField' and 'foreignField' to be specified");
        }
        if (d.containsKey("let") && !hasPipeline) {
            throw new MongoCmdException(9, "$lookup with 'let' requires a 'pipeline' argument");
        }
        String from = d.containsKey("from") ? d.getString("from").getValue() : null;
        String lf = hasLocal ? d.getString("localField").getValue() : null;
        String ff = hasForeign ? d.getString("foreignField").getValue() : null;
        BsonArray pipeline = hasPipeline ? d.getArray("pipeline") : null;
        BsonDocument let = d.containsKey("let") ? d.getDocument("let") : new BsonDocument();
        Map<String, MongoExpr.Expr> letExprs = new LinkedHashMap<>();
        let.forEach((k, v) -> letExprs.put(k, MongoExpr.parse(v)));
        // lazily loaded foreign collection
        List<BsonDocument>[] foreign = new List[1];
        Map<String, List<BsonDocument>>[] index = new Map[1];
        return in.map(doc -> {
            if (foreign[0] == null) {
                try (Stream<BsonDocument> fs = from == null ? Stream.<BsonDocument>empty() : ctx.source.scan(ctx.db, from)) {
                    foreign[0] = fs.collect(Collectors.toList());
                }
                if (hasLocal) {
                    Map<String, List<BsonDocument>> m = new HashMap<>();
                    for (BsonDocument f : foreign[0]) {
                        java.util.Set<String> keys = new java.util.LinkedHashSet<>();
                        for (BsonValue c : MongoMatcher.resolve(f, ff)) {
                            if (c == MongoMatcher.MISSING || c.isNull() || BsonCmp.isUndef(c)) {
                                keys.add("null");
                            } else {
                                keys.add(BsonCmp.key(c, ctx.collation));
                                if (c.isArray()) {
                                    for (BsonValue e : c.asArray()) {
                                        keys.add(e.isNull() ? "null" : BsonCmp.key(e, ctx.collation));
                                    }
                                }
                            }
                        }
                        for (String k : keys) {
                            m.computeIfAbsent(k, x -> new ArrayList<>()).add(f);
                        }
                    }
                    index[0] = m;
                }
            }
            List<BsonDocument> matches = new ArrayList<>();
            if (hasLocal) {
                java.util.Set<BsonDocument> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
                List<String> keys = new ArrayList<>();
                for (BsonValue c : MongoMatcher.resolve(doc, lf)) {
                    if (c == MongoMatcher.MISSING || c.isNull() || BsonCmp.isUndef(c)) {
                        keys.add("null");
                    } else if (c.isArray()) {
                        for (BsonValue e : c.asArray()) {
                            keys.add(e.isNull() ? "null" : BsonCmp.key(e, ctx.collation));
                        }
                    } else {
                        keys.add(BsonCmp.key(c, ctx.collation));
                    }
                }
                List<BsonDocument> cand = new ArrayList<>();
                for (String k : keys) {
                    for (BsonDocument f : index[0].getOrDefault(k, List.of())) {
                        if (seen.add(f)) {
                            cand.add(f);
                        }
                    }
                }
                // preserve foreign natural order
                Map<BsonDocument, Integer> order = new java.util.IdentityHashMap<>();
                for (int i = 0; i < foreign[0].size(); i++) {
                    order.put(foreign[0].get(i), i);
                }
                cand.sort(Comparator.comparingInt(order::get));
                matches = cand;
                if (hasPipeline) {
                    Map<String, BsonValue> vars = new HashMap<>(ctx.vars);
                    MongoExpr.Scope sc = ctx.scope(doc);
                    letExprs.forEach((k, e) -> vars.put(k, e.eval(sc) == MongoExpr.MISSING ? BsonNull.VALUE : e.eval(sc)));
                    matches = run(pipeline, matches.stream(), ctx.child(vars)).collect(Collectors.toList());
                }
            } else {
                Map<String, BsonValue> vars = new HashMap<>(ctx.vars);
                MongoExpr.Scope sc = ctx.scope(doc);
                letExprs.forEach((k, e) -> {
                    BsonValue v = e.eval(sc);
                    vars.put(k, v);
                });
                matches = run(pipeline, foreign[0].stream(), ctx.child(vars)).collect(Collectors.toList());
            }
            return (BsonDocument) setCow(doc, asParts, 0, new BsonArray(new ArrayList<BsonValue>(matches)));
        });
    }

    private static Stream<BsonDocument> graphLookup(BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        BsonDocument d = spec.asDocument();
        String from = d.getString("from").getValue();
        MongoExpr.Expr start = MongoExpr.parse(d.get("startWith"));
        String connectFrom = d.getString("connectFromField").getValue();
        String connectTo = d.getString("connectToField").getValue();
        String as = d.getString("as").getValue();
        long maxDepth = d.containsKey("maxDepth") ? MongoNum.truncLong(d.get("maxDepth")) : Long.MAX_VALUE;
        String depthField = d.containsKey("depthField") ? d.getString("depthField").getValue() : null;
        MongoMatcher.Pred restrict = d.containsKey("restrictSearchWithMatch")
                ? MongoMatcher.compile(d.getDocument("restrictSearchWithMatch"), ctx.collation) : d0 -> true;
        List<BsonDocument>[] foreign = new List[1];
        return in.map(doc -> {
            if (foreign[0] == null) {
                try (Stream<BsonDocument> fs = ctx.source.scan(ctx.db, from)) {
                    foreign[0] = fs.filter(restrict::test).collect(Collectors.toList());
                }
            }
            BsonValue sv = start.eval(ctx.scope(doc));
            List<BsonValue> frontier = new ArrayList<>();
            if (sv.isArray()) {
                frontier.addAll(sv.asArray().getValues());
            } else if (sv != MongoExpr.MISSING) {
                frontier.add(sv);
            }
            java.util.Set<BsonDocument> visited = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            List<BsonValue> results = new ArrayList<>();
            long depth = 0;
            while (!frontier.isEmpty() && depth <= maxDepth) {
                List<BsonValue> next = new ArrayList<>();
                for (BsonDocument f : foreign[0]) {
                    if (visited.contains(f)) {
                        continue;
                    }
                    boolean hit = false;
                    for (BsonValue c : MongoMatcher.resolve(f, connectTo)) {
                        for (BsonValue fv : frontier) {
                            if (c != MongoMatcher.MISSING && (BsonCmp.compare(c, fv) == 0
                                    || c.isArray() && c.asArray().getValues().stream().anyMatch(x -> BsonCmp.compare(x, fv) == 0))) {
                                hit = true;
                            }
                        }
                    }
                    if (hit) {
                        visited.add(f);
                        BsonDocument r = f;
                        if (depthField != null) {
                            r = f.clone();
                            r.put(depthField, new BsonInt64(depth));
                        }
                        results.add(r);
                        for (BsonValue c : MongoMatcher.resolve(f, connectFrom)) {
                            if (c == MongoMatcher.MISSING || c.isNull()) {
                                continue;
                            }
                            if (c.isArray()) {
                                next.addAll(c.asArray().getValues());
                            } else {
                                next.add(c);
                            }
                        }
                    }
                }
                frontier = next;
                depth++;
            }
            return (BsonDocument) setCow(doc, as.split("\\.", -1), 0, new BsonArray(results));
        });
    }

    private static BsonDocument redact(BsonDocument d, MongoExpr.Expr e, Ctx ctx) {
        BsonValue r = e.eval(new MongoExpr.Scope(d, d, ctx.vars, ctx.collation, ctx.now));
        String s = r.isString() ? r.asString().getValue() : "";
        switch (s) {
            case "$$KEEP": return d;
            case "$$PRUNE": return null;
            case "$$DESCEND": {
                BsonDocument out = new BsonDocument();
                for (Map.Entry<String, BsonValue> en : d.entrySet()) {
                    BsonValue v = en.getValue();
                    if (v.isDocument()) {
                        BsonDocument sub = redact(v.asDocument(), e, ctx);
                        if (sub != null) {
                            out.put(en.getKey(), sub);
                        }
                    } else if (v.isArray()) {
                        BsonArray a = new BsonArray();
                        for (BsonValue x : v.asArray()) {
                            if (x.isDocument()) {
                                BsonDocument sub = redact(x.asDocument(), e, ctx);
                                if (sub != null) {
                                    a.add(sub);
                                }
                            } else {
                                a.add(x);
                            }
                        }
                        out.put(en.getKey(), a);
                    } else {
                        out.put(en.getKey(), v);
                    }
                }
                return out;
            }
            default:
                throw new MongoCmdException(17053, "$redact's expression should not return anything aside from the variables $$KEEP, $$DESCEND, and $$PRUNE, but returned " + MongoFmt.value(r));
        }
    }

    // ------------------------------------------------------------------ $group and accumulators

    interface Acc {
        void add(MongoExpr.Scope sc);

        BsonValue result();
    }

    interface AccFactory {
        Acc create();
    }

    static AccFactory accumulator(String field, BsonValue spec, Ctx ctx) {
        if (!spec.isDocument() || spec.asDocument().size() != 1) {
            throw new MongoCmdException(40234, "The field '" + field + "' must be an accumulator object");
        }
        String op = spec.asDocument().getFirstKey();
        BsonValue arg = spec.asDocument().get(op);
        BsonCmp.Collation coll = ctx.collation;
        switch (op) {
            case "$sum": case "$avg": case "$stdDevPop": case "$stdDevSamp": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                return () -> new Acc() {
                    BsonValue sum = new BsonInt32(0);
                    long n = 0;
                    double dsum = 0;
                    double dsq = 0;
                    double mean = 0;
                    double m2 = 0;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        BsonValue v = e.eval(sc);
                        if (v == MongoExpr.MISSING || !BsonCmp.isNumber(v)) {
                            return;
                        }
                        n++;
                        if (op.equals("$sum") || op.equals("$avg")) {
                            sum = MongoNum.add(sum, v);
                        } else {
                            double x = MongoNum.toDouble(v);
                            double delta = x - mean;
                            mean += delta / n;
                            m2 += delta * (x - mean);
                        }
                    }

                    @Override
                    public BsonValue result() {
                        switch (op) {
                            case "$sum": return sum;
                            case "$avg": return n == 0 ? BsonNull.VALUE : MongoNum.divide(sum, new BsonInt64(n));
                            case "$stdDevPop": return n == 0 ? BsonNull.VALUE : new BsonDouble(Math.sqrt(m2 / n));
                            default: return n <= 1 ? BsonNull.VALUE : new BsonDouble(Math.sqrt(m2 / (n - 1)));
                        }
                    }
                };
            }
            case "$count": {
                if (!arg.isDocument() || !arg.asDocument().isEmpty()) {
                    throw new MongoCmdException(40237, "$count takes no arguments, i.e. $count:{}");
                }
                return () -> new Acc() {
                    long n;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        n++;
                    }

                    @Override
                    public BsonValue result() {
                        return MongoNum.fromLong(n);
                    }
                };
            }
            case "$min": case "$max": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                boolean max = op.equals("$max");
                return () -> new Acc() {
                    BsonValue best;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        BsonValue v = e.eval(sc);
                        if (MongoExpr.nullish(v)) {
                            return;
                        }
                        if (best == null || (max ? BsonCmp.compare(v, best, coll) > 0 : BsonCmp.compare(v, best, coll) < 0)) {
                            best = v;
                        }
                    }

                    @Override
                    public BsonValue result() {
                        return best == null ? BsonNull.VALUE : best;
                    }
                };
            }
            case "$first": case "$last": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                boolean last = op.equals("$last");
                return () -> new Acc() {
                    BsonValue val;
                    boolean set;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        if (last || !set) {
                            BsonValue v = e.eval(sc);
                            val = v == MongoExpr.MISSING ? BsonNull.VALUE : v;
                            set = true;
                        }
                    }

                    @Override
                    public BsonValue result() {
                        return set ? val : BsonNull.VALUE;
                    }
                };
            }
            case "$push": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                return () -> new Acc() {
                    final BsonArray out = new BsonArray();

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        BsonValue v = e.eval(sc);
                        if (v != MongoExpr.MISSING) {
                            out.add(v);
                        }
                    }

                    @Override
                    public BsonValue result() {
                        return out;
                    }
                };
            }
            case "$addToSet": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                return () -> new Acc() {
                    final LinkedHashMap<String, BsonValue> out = new LinkedHashMap<>();

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        BsonValue v = e.eval(sc);
                        if (v != MongoExpr.MISSING) {
                            out.putIfAbsent(BsonCmp.key(v, coll), v);
                        }
                    }

                    @Override
                    public BsonValue result() {
                        return new BsonArray(new ArrayList<>(out.values()));
                    }
                };
            }
            case "$mergeObjects": {
                MongoExpr.Expr e = MongoExpr.parse(arg);
                return () -> new Acc() {
                    final BsonDocument out = new BsonDocument();

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        BsonValue v = e.eval(sc);
                        if (MongoExpr.nullish(v)) {
                            return;
                        }
                        if (!v.isDocument()) {
                            throw new MongoCmdException(40400, "$mergeObjects requires object inputs, but input " + MongoFmt.value(v) + " is of type " + MongoMatcher.typeName(v));
                        }
                        v.asDocument().forEach(out::put);
                    }

                    @Override
                    public BsonValue result() {
                        return out;
                    }
                };
            }
            case "$top": case "$bottom": case "$topN": case "$bottomN": {
                BsonDocument s = arg.asDocument();
                if (!s.containsKey("sortBy") || !s.containsKey("output")) {
                    throw new MongoCmdException(5788001, op + " requires 'sortBy' and 'output'");
                }
                boolean bottom = op.startsWith("$bottom");
                boolean many = op.endsWith("N");
                MongoExpr.Expr out = MongoExpr.parse(s.get("output"));
                MongoExpr.Expr nExpr = many ? MongoExpr.parse(s.get("n")) : null;
                Comparator<BsonDocument> cmp = MongoSort.comparator(s.getDocument("sortBy"), coll);
                return () -> new Acc() {
                    final List<BsonDocument[]> items = new ArrayList<>();
                    long n = 1;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        if (nExpr != null && items.isEmpty()) {
                            n = MongoNum.truncLong(nExpr.eval(sc));
                        }
                        BsonValue o = out.eval(sc);
                        items.add(new BsonDocument[] {sc.current.asDocument(), new BsonDocument("v", o == MongoExpr.MISSING ? BsonNull.VALUE : o)});
                    }

                    @Override
                    public BsonValue result() {
                        items.sort((a, b) -> cmp.compare(a[0], b[0]));
                        List<BsonValue> vals = new ArrayList<>();
                        List<BsonDocument[]> sel = bottom ? items.subList(Math.max(0, items.size() - (int) n), items.size())
                                : items.subList(0, (int) Math.min(n, items.size()));
                        for (BsonDocument[] i : sel) {
                            vals.add(i[1].get("v"));
                        }
                        if (!many) {
                            return vals.isEmpty() ? BsonNull.VALUE : vals.get(0);
                        }
                        return new BsonArray(vals);
                    }
                };
            }
            case "$firstN": case "$lastN": case "$maxN": case "$minN": {
                BsonDocument s = arg.asDocument();
                MongoExpr.Expr in = MongoExpr.parse(s.get("input"));
                MongoExpr.Expr nExpr = MongoExpr.parse(s.get("n"));
                return () -> new Acc() {
                    final List<BsonValue> vals = new ArrayList<>();
                    long n = -1;

                    @Override
                    public void add(MongoExpr.Scope sc) {
                        if (n < 0) {
                            n = MongoNum.truncLong(nExpr.eval(sc));
                        }
                        BsonValue v = in.eval(sc);
                        vals.add(v == MongoExpr.MISSING ? BsonNull.VALUE : v);
                    }

                    @Override
                    public BsonValue result() {
                        List<BsonValue> l = new ArrayList<>(vals);
                        if (op.equals("$firstN")) {
                            return new BsonArray(new ArrayList<>(l.subList(0, (int) Math.min(n, l.size()))));
                        }
                        if (op.equals("$lastN")) {
                            return new BsonArray(new ArrayList<>(l.subList((int) Math.max(0, l.size() - n), l.size())));
                        }
                        l.removeIf(MongoExpr::nullish);
                        l.sort(op.equals("$maxN") ? (a, b) -> BsonCmp.compare(b, a) : BsonCmp::compare);
                        return new BsonArray(new ArrayList<>(l.subList(0, (int) Math.min(n, l.size()))));
                    }
                };
            }
            default:
                throw new MongoCmdException(15952, "unknown group operator '" + op + "'");
        }
    }

    private static Stream<BsonDocument> group(BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        if (!spec.isDocument()) {
            throw new MongoCmdException(15947, "a group's fields must be specified in an object");
        }
        BsonDocument d = spec.asDocument();
        if (!d.containsKey("_id")) {
            throw new MongoCmdException(15955, "a group specification must include an _id");
        }
        MongoExpr.Expr idExpr = MongoExpr.parse(d.get("_id"));
        List<String> names = new ArrayList<>();
        List<AccFactory> facs = new ArrayList<>();
        for (Map.Entry<String, BsonValue> e : d.entrySet()) {
            if (e.getKey().equals("_id")) {
                continue;
            }
            if (e.getKey().contains(".")) {
                throw new MongoCmdException(40235, "The field name '" + e.getKey() + "' cannot contain '.'");
            }
            if (e.getKey().startsWith("$")) {
                throw new MongoCmdException(40236, "The field name '" + e.getKey() + "' cannot be an operator name");
            }
            names.add(e.getKey());
            facs.add(accumulator(e.getKey(), e.getValue(), ctx));
        }
        return Stream.of((Object) null).flatMap(x -> {
            LinkedHashMap<String, Object[]> groups = new LinkedHashMap<>();
            in.forEach(doc -> {
                MongoExpr.Scope sc = ctx.scope(doc);
                BsonValue id = idExpr.eval(sc);
                if (id == MongoExpr.MISSING) {
                    id = BsonNull.VALUE;
                }
                String k = BsonCmp.key(id, ctx.collation);
                Object[] g = groups.get(k);
                if (g == null) {
                    Acc[] accs = new Acc[facs.size()];
                    for (int i = 0; i < accs.length; i++) {
                        accs[i] = facs.get(i).create();
                    }
                    g = new Object[] {id, accs};
                    groups.put(k, g);
                }
                for (Acc a : (Acc[]) g[1]) {
                    a.add(sc);
                }
            });
            List<BsonDocument> out = new ArrayList<>();
            for (Object[] g : groups.values()) {
                BsonDocument r = new BsonDocument("_id", (BsonValue) g[0]);
                Acc[] accs = (Acc[]) g[1];
                for (int i = 0; i < accs.length; i++) {
                    r.put(names.get(i), accs[i].result());
                }
                out.add(r);
            }
            return out.stream();
        });
    }

    // ------------------------------------------------------------------ $bucket / $bucketAuto

    private static Stream<BsonDocument> bucket(BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        if (!spec.isDocument()) {
            throw new MongoCmdException(40201, "Argument to $bucket stage must be an object, but found type: " + MongoMatcher.typeName(spec));
        }
        BsonDocument d = spec.asDocument();
        for (String k : d.keySet()) {
            if (!List.of("groupBy", "boundaries", "default", "output").contains(k)) {
                throw new MongoCmdException(40197, "Unrecognized option to $bucket: " + k + ".");
            }
        }
        if (!d.containsKey("groupBy")) {
            throw new MongoCmdException(40198, "$bucket requires 'groupBy' and 'boundaries' to be specified.");
        }
        if (!d.containsKey("boundaries")) {
            throw new MongoCmdException(40198, "$bucket requires 'groupBy' and 'boundaries' to be specified.");
        }
        MongoExpr.Expr groupBy = MongoExpr.parse(d.get("groupBy"));
        if (!d.get("boundaries").isArray()) {
            throw new MongoCmdException(40200, "The $bucket 'boundaries' field must be an array, but found type: " + MongoMatcher.typeName(d.get("boundaries")));
        }
        List<BsonValue> bounds = d.getArray("boundaries").getValues();
        if (bounds.size() < 2) {
            throw new MongoCmdException(40192, "The $bucket 'boundaries' field must have at least 2 values, but found " + bounds.size() + " value(s).");
        }
        for (int i = 1; i < bounds.size(); i++) {
            if (BsonCmp.typeOrder(bounds.get(i)) != BsonCmp.typeOrder(bounds.get(0))) {
                throw new MongoCmdException(40193, "All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types " + MongoMatcher.typeName(bounds.get(0)) + " and " + MongoMatcher.typeName(bounds.get(i)) + ".");
            }
            if (BsonCmp.compare(bounds.get(i - 1), bounds.get(i)) >= 0) {
                throw new MongoCmdException(40194, "The 'boundaries' option to $bucket must be sorted in ascending order, but elements " + (i - 1) + " and " + i + " are not in ascending order (" + MongoFmt.value(bounds.get(i - 1)) + " is not less than " + MongoFmt.value(bounds.get(i)) + ").");
            }
        }
        BsonValue def = d.get("default");
        if (def != null && BsonCmp.typeOrder(def) == BsonCmp.typeOrder(bounds.get(0))
                && BsonCmp.compare(def, bounds.get(0)) >= 0 && BsonCmp.compare(def, bounds.get(bounds.size() - 1)) < 0) {
            throw new MongoCmdException(40199, "The $bucket 'default' field must be less than the lowest boundary or greater than or equal to the highest boundary.");
        }
        BsonDocument output = d.containsKey("output") ? d.getDocument("output") : new BsonDocument("count", new BsonDocument("$sum", new BsonInt32(1)));
        List<String> names = new ArrayList<>(output.keySet());
        List<AccFactory> facs = new ArrayList<>();
        output.forEach((k, v) -> facs.add(accumulator(k, v, ctx)));
        return Stream.of((Object) null).flatMap(x -> {
            Acc[][] accs = new Acc[bounds.size()][];
            boolean[] used = new boolean[bounds.size()];
            Acc[] defAccs = new Acc[facs.size()];
            boolean[] defUsed = {false};
            for (int i = 0; i < defAccs.length; i++) {
                defAccs[i] = facs.get(i).create();
            }
            in.forEach(doc -> {
                MongoExpr.Scope sc = ctx.scope(doc);
                BsonValue v = groupBy.eval(sc);
                if (v == MongoExpr.MISSING) {
                    v = BsonNull.VALUE;
                }
                int idx = -1;
                if (BsonCmp.typeOrder(v) == BsonCmp.typeOrder(bounds.get(0))) {
                    for (int i = 0; i < bounds.size() - 1; i++) {
                        if (BsonCmp.compare(v, bounds.get(i)) >= 0 && BsonCmp.compare(v, bounds.get(i + 1)) < 0) {
                            idx = i;
                            break;
                        }
                    }
                }
                if (idx < 0) {
                    if (def == null) {
                        throw new MongoCmdException(40066, "$switch could not find a matching branch for an input, and no default was specified.");
                    }
                    defUsed[0] = true;
                    for (Acc a : defAccs) {
                        a.add(sc);
                    }
                    return;
                }
                if (accs[idx] == null) {
                    accs[idx] = new Acc[facs.size()];
                    for (int i = 0; i < facs.size(); i++) {
                        accs[idx][i] = facs.get(i).create();
                    }
                }
                used[idx] = true;
                for (Acc a : accs[idx]) {
                    a.add(sc);
                }
            });
            List<BsonDocument> out = new ArrayList<>();
            for (int i = 0; i < bounds.size() - 1; i++) {
                if (used[i]) {
                    BsonDocument r = new BsonDocument("_id", bounds.get(i));
                    for (int j = 0; j < names.size(); j++) {
                        r.put(names.get(j), accs[i][j].result());
                    }
                    out.add(r);
                }
            }
            if (defUsed[0]) {
                BsonDocument r = new BsonDocument("_id", def);
                for (int j = 0; j < names.size(); j++) {
                    r.put(names.get(j), defAccs[j].result());
                }
                out.add(r);
            }
            return out.stream();
        });
    }

    private static Stream<BsonDocument> bucketAuto(BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        BsonDocument d = spec.asDocument();
        for (String k : d.keySet()) {
            if (!List.of("groupBy", "buckets", "output", "granularity").contains(k)) {
                throw new MongoCmdException(40245, "Unrecognized option to $bucketAuto: " + k + ".");
            }
        }
        if (!d.containsKey("groupBy") || !d.containsKey("buckets")) {
            throw new MongoCmdException(40246, "$bucketAuto requires 'groupBy' and 'buckets' to be specified");
        }
        MongoExpr.Expr groupBy = MongoExpr.parse(d.get("groupBy"));
        long nb = MongoNum.truncLong(d.get("buckets"));
        if (nb <= 0) {
            throw new MongoCmdException(40243, "The $bucketAuto 'buckets' field must be greater than 0, but found: " + nb);
        }
        BsonDocument output = d.containsKey("output") ? d.getDocument("output") : new BsonDocument("count", new BsonDocument("$sum", new BsonInt32(1)));
        List<String> names = new ArrayList<>(output.keySet());
        List<AccFactory> facs = new ArrayList<>();
        output.forEach((k, v) -> facs.add(accumulator(k, v, ctx)));
        return Stream.of((Object) null).flatMap(x -> {
            List<Object[]> rows = new ArrayList<>();
            in.forEach(doc -> {
                BsonValue v = groupBy.eval(ctx.scope(doc));
                rows.add(new Object[] {v == MongoExpr.MISSING ? BsonNull.VALUE : v, doc});
            });
            rows.sort((a, b) -> BsonCmp.compare((BsonValue) a[0], (BsonValue) b[0], ctx.collation));
            int total = rows.size();
            List<BsonDocument> out = new ArrayList<>();
            if (total == 0) {
                return out.stream();
            }
            long buckets = Math.min(nb, total);
            int start = 0;
            long base = total / buckets;
            long extra = total % buckets;
            for (long b = 0; b < buckets; b++) {
                int size = (int) (base + (b >= buckets - extra ? 1 : 0));
                int end = start + size;
                // keep equal values in one bucket
                while (end < total && BsonCmp.compare((BsonValue) rows.get(end - 1)[0], (BsonValue) rows.get(end)[0]) == 0) {
                    end++;
                }
                if (start >= total) {
                    break;
                }
                Acc[] accs = new Acc[facs.size()];
                for (int i = 0; i < accs.length; i++) {
                    accs[i] = facs.get(i).create();
                }
                for (int i = start; i < end; i++) {
                    for (Acc a : accs) {
                        a.add(ctx.scope((BsonDocument) rows.get(i)[1]));
                    }
                }
                BsonValue min = (BsonValue) rows.get(start)[0];
                BsonValue max = end < total ? (BsonValue) rows.get(end)[0] : (BsonValue) rows.get(total - 1)[0];
                BsonDocument r = new BsonDocument("_id", new BsonDocument("min", min).append("max", max));
                for (int j = 0; j < names.size(); j++) {
                    r.put(names.get(j), accs[j].result());
                }
                out.add(r);
                start = end;
            }
            return out.stream();
        });
    }

    // ------------------------------------------------------------------ $out / $merge

    private static Stream<BsonDocument> writeStage(String name, BsonValue spec, Stream<BsonDocument> in, Ctx ctx) {
        if (ctx.sink == null) {
            throw new MongoCmdException(115, name + " is not allowed in this context");
        }
        if (name.equals("$out")) {
            String db = ctx.db;
            String coll;
            if (spec.isString()) {
                coll = spec.asString().getValue();
            } else if (spec.isDocument() && spec.asDocument().containsKey("coll")) {
                coll = spec.asDocument().getString("coll").getValue();
                if (spec.asDocument().containsKey("db")) {
                    db = spec.asDocument().getString("db").getValue();
                }
            } else {
                throw new MongoCmdException(16990, "$out only supports a string or object argument, not " + MongoMatcher.typeName(spec));
            }
            String fdb = db;
            return Stream.of((Object) null).flatMap(x -> {
                List<BsonDocument> docs = in.collect(Collectors.toList());
                ctx.sink.replaceCollection(fdb, coll, docs);
                ctx.wrote = true;
                return Stream.<BsonDocument>empty();
            });
        }
        BsonDocument d = spec.isString() ? new BsonDocument("into", spec) : spec.asDocument();
        BsonValue into = d.get("into");
        if (into == null) {
            throw new MongoCmdException(51299, "$merge requires 'into' field");
        }
        String db = ctx.db;
        String coll;
        if (into.isString()) {
            coll = into.asString().getValue();
        } else {
            coll = into.asDocument().getString("coll").getValue();
            if (into.asDocument().containsKey("db")) {
                db = into.asDocument().getString("db").getValue();
            }
        }
        List<String> on = new ArrayList<>();
        if (!d.containsKey("on")) {
            on.add("_id");
        } else if (d.get("on").isString()) {
            on.add(d.getString("on").getValue());
        } else {
            d.getArray("on").forEach(v -> on.add(v.asString().getValue()));
        }
        BsonValue whenMatched = d.containsKey("whenMatched") ? d.get("whenMatched") : new BsonString("merge");
        String whenNotMatched = d.containsKey("whenNotMatched") ? d.getString("whenNotMatched").getValue() : "insert";
        String fdb = db;
        return Stream.of((Object) null).flatMap(x -> {
            in.forEach(doc -> {
                BsonDocument filter = new BsonDocument();
                for (String f : on) {
                    BsonValue v = MongoExpr.fieldPath(doc, f.split("\\.", -1), 0);
                    filter.put(f, v == MongoExpr.MISSING ? BsonNull.VALUE : v);
                }
                BsonDocument existing = ctx.sink.findOne(fdb, coll, filter);
                if (existing == null) {
                    if (whenNotMatched.equals("insert")) {
                        BsonDocument ins = doc.containsKey("_id") ? doc : withId(doc);
                        ctx.sink.insert(fdb, coll, ins);
                    } else if (whenNotMatched.equals("fail")) {
                        throw new MongoCmdException(13113, "MergeStageNoMatchingDocument: $merge could not find a matching document in the target collection for at least one document in the source collection");
                    }
                    return;
                }
                if (whenMatched.isString()) {
                    switch (whenMatched.asString().getValue()) {
                        case "merge" -> {
                            BsonDocument merged = existing.clone();
                            doc.forEach(merged::put);
                            ctx.sink.replace(fdb, coll, existing, merged);
                        }
                        case "replace" -> {
                            BsonDocument r = doc.clone();
                            if (!r.containsKey("_id")) {
                                r = withIdValue(doc, existing.get("_id"));
                            }
                            ctx.sink.replace(fdb, coll, existing, r);
                        }
                        case "keepExisting" -> { }
                        case "fail" -> throw new MongoCmdException(11000, "$merge failed due to a matching document in the target collection");
                        default -> throw new MongoCmdException(2, "Enumeration value '" + whenMatched.asString().getValue() + "' for field '$merge.whenMatched' is not a valid value.");
                    }
                } else if (whenMatched.isArray()) {
                    Map<String, BsonValue> vars = new HashMap<>(ctx.vars);
                    vars.put("new", doc);
                    if (d.containsKey("let")) {
                        d.getDocument("let").forEach((k, v) -> vars.put(k, MongoExpr.parse(v).eval(MongoExpr.Scope.root(existing, ctx.collation, Map.of("new", doc)))));
                    }
                    Ctx c = ctx.child(vars);
                    BsonDocument updated = run(whenMatched.asArray(), Stream.of(existing), c).findFirst().orElse(existing);
                    ctx.sink.replace(fdb, coll, existing, updated);
                }
            });
            ctx.wrote = true;
            return Stream.<BsonDocument>empty();
        });
    }

    private static BsonDocument withId(BsonDocument doc) {
        BsonDocument r = new BsonDocument("_id", MongoUpdate.newObjectId());
        doc.forEach(r::put);
        return r;
    }

    private static BsonDocument withIdValue(BsonDocument doc, BsonValue id) {
        BsonDocument r = new BsonDocument("_id", id);
        doc.forEach(r::put);
        return r;
    }

}
