package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** find projections and aggregation {@code $project}. */
final class MongoProjection {

    private static final int CONTAINER = 0;
    private static final int INCLUDE = 1;
    private static final int EXCLUDE = 2;
    private static final int EXPR = 3;
    private static final int SLICE = 4;
    private static final int ELEMMATCH = 5;
    private static final int POSITIONAL = 6;

    private static final class Node {
        final Map<String, Node> kids = new LinkedHashMap<>();
        int kind = CONTAINER;
        MongoExpr.Expr expr;
        long sliceSkip;
        long sliceLimit;
        boolean sliceHasSkip;
        BsonValue elemSpec;
        MongoMatcher.Pred elemPred;
        String path;
    }

    private final Node root = new Node();
    private boolean inclusion;
    private boolean idIncluded = true;
    private boolean idExplicit;
    private boolean hasModifiers;
    private final BsonDocument filter;
    private final BsonCmp.Collation coll;
    private final boolean aggregation;
    private boolean empty;

    private MongoProjection(BsonDocument filter, BsonCmp.Collation coll, boolean aggregation) {
        this.filter = filter;
        this.coll = coll;
        this.aggregation = aggregation;
    }

    static MongoProjection compile(BsonDocument spec, boolean aggregation, BsonDocument filter, BsonCmp.Collation coll) {
        MongoProjection p = new MongoProjection(filter, coll, aggregation);
        p.build(spec);
        return p;
    }

    private static boolean isSimpleFlag(BsonValue v) {
        return v.isBoolean() || BsonCmp.isNumber(v);
    }

    private static boolean flag(BsonValue v) {
        return v.isBoolean() ? v.asBoolean().getValue() : BsonCmp.compareNumbers(v, new org.bson.BsonInt32(0)) != 0;
    }

    private void build(BsonDocument spec) {
        if (spec.isEmpty()) {
            empty = true;
            if (aggregation) {
                throw new MongoCmdException(51272, "Invalid $project :: caused by :: projection specification must have at least one field");
            }
            return;
        }
        Integer mode = null; // 1 inclusion, 2 exclusion
        String modeField = null;
        boolean modifiersOnly = true;
        for (Map.Entry<String, BsonValue> e : spec.entrySet()) {
            String path = e.getKey();
            BsonValue v = e.getValue();
            if (path.startsWith("$")) {
                throw new MongoCmdException(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField.");
            }
            if (path.endsWith(".") && !path.isEmpty()) {
                throw new MongoCmdException(40353, "FieldPath must not end with a '.'.");
            }
            if (path.isEmpty() || path.startsWith(".") || path.contains("..")) {
                throw new MongoCmdException(15998, "FieldPath field names may not be empty strings.");
            }
            boolean isId = path.equals("_id");
            int kind;
            Node leaf = new Node();
            leaf.path = path;
            boolean positional = path.endsWith(".$");
            String realPath = positional ? path.substring(0, path.length() - 2) : path;
            if (positional) {
                if (aggregation) {
                    throw new MongoCmdException(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField.");
                }
                kind = POSITIONAL;
                leaf.path = realPath;
                hasModifiers = true;
            } else if (isSimpleFlag(v)) {
                kind = flag(v) ? INCLUDE : EXCLUDE;
            } else if (v.isDocument() && !v.asDocument().isEmpty() && v.asDocument().getFirstKey().startsWith("$")
                    && (v.asDocument().getFirstKey().equals("$slice") && !aggregation
                            || v.asDocument().getFirstKey().equals("$elemMatch") && !aggregation)) {
                BsonDocument d = v.asDocument();
                hasModifiers = true;
                if (d.getFirstKey().equals("$slice")) {
                    kind = SLICE;
                    BsonValue s = d.get("$slice");
                    if (BsonCmp.isNumber(s)) {
                        leaf.sliceHasSkip = false;
                        leaf.sliceSkip = MongoNum.truncLong(s);
                    } else if (s.isArray() && s.asArray().size() == 2 && BsonCmp.isNumber(s.asArray().get(0))
                            && BsonCmp.isNumber(s.asArray().get(1))) {
                        leaf.sliceHasSkip = true;
                        leaf.sliceSkip = MongoNum.truncLong(s.asArray().get(0));
                        leaf.sliceLimit = MongoNum.truncLong(s.asArray().get(1));
                        if (leaf.sliceLimit <= 0) {
                            throw new MongoCmdException(28724, "Invalid $slice :: caused by :: Third argument to $slice must be positive: "
                                    + leaf.sliceLimit);
                        }
                    } else {
                        throw new MongoCmdException(28667, "Invalid $slice syntax. The given syntax {$slice: "
                                + s + "} did not match the find() syntax because :: Expression $slice takes at least 2 arguments, and at most 3, but 1 were passed in.");
                    }
                } else {
                    kind = ELEMMATCH;
                    if (!d.get("$elemMatch").isDocument()) {
                        throw new MongoCmdException(31274, "elemMatch: Invalid argument, object required, but got " + MongoMatcher.typeName(d.get("$elemMatch")));
                    }
                    leaf.elemSpec = d.get("$elemMatch");
                    leaf.elemPred = MongoMatcher.compile(new BsonDocument("v", new BsonDocument("$elemMatch", d.get("$elemMatch"))), coll);
                }
                modifiersOnly = modifiersOnly && true;
            } else if (v.isDocument() && !v.asDocument().isEmpty() && v.asDocument().getFirstKey().equals("$meta")) {
                throw new MongoCmdException(40218, "query requires text score metadata, but it is not available");
            } else {
                kind = EXPR;
                try {
                    leaf.expr = MongoExpr.parse(v);
                } catch (MongoCmdException ex) {
                    if (ex.code == 168) {
                        throw new MongoCmdException(31325, "Unknown expression " + ex.getMessage().replaceAll(".*'(.*)'.*", "$1"));
                    }
                    throw ex;
                }
            }
            leaf.kind = kind;
            if (isId) {
                idExplicit = true;
                if (kind == EXCLUDE) {
                    idIncluded = false;
                    continue;
                }
                if (kind == INCLUDE) {
                    idIncluded = true;
                    if (spec.size() == 1) {
                        mode = 1;
                    }
                    continue;
                }
            }
            if (kind == ELEMMATCH) {
                mode = mode == null ? Integer.valueOf(1) : mode;
                if (mode == 1) {
                    modifiersOnly = false;
                }
            }
            if (kind == INCLUDE || kind == EXPR) {
                if (mode != null && mode == 2) {
                    throw new MongoCmdException(31253, "Cannot do inclusion on field " + path + " in exclusion projection");
                }
                mode = 1;
                modeField = path;
                modifiersOnly = false;
            } else if (kind == EXCLUDE) {
                if (mode != null && mode == 1) {
                    throw new MongoCmdException(31254, "Cannot do exclusion on field " + path + " in inclusion projection");
                }
                mode = 2;
                modeField = path;
                modifiersOnly = false;
            } else if (kind == POSITIONAL) {
                modifiersOnly = false;
                if (mode != null && mode == 2) {
                    throw new MongoCmdException(31253, "Cannot do inclusion on field " + path + " in exclusion projection");
                }
                mode = 1;
            }
            place(realPath, leaf);
        }
        if (mode == null) {
            // only _id / modifiers
            inclusion = idExplicit && idIncluded && spec.size() == 1 && isSimpleFlag(spec.get("_id"));
            if (!root.kids.isEmpty()) {
                inclusion = false;
            }
        } else {
            inclusion = mode == 1;
        }
        if (aggregation && mode == null && !idExplicit) {
            throw new MongoCmdException(51272, "Invalid $project :: caused by :: projection specification must have at least one field");
        }
        if (!aggregation && modifiersOnly && !root.kids.isEmpty()) {
            inclusion = false;
        }
    }

    private void place(String path, Node leaf) {
        String[] parts = path.split("\\.", -1);
        Node cur = root;
        for (int i = 0; i < parts.length - 1; i++) {
            Node next = cur.kids.get(parts[i]);
            if (next == null) {
                next = new Node();
                cur.kids.put(parts[i], next);
            } else if (next.kind != CONTAINER) {
                throw new MongoCmdException(31249, "Path collision at " + path + " remaining portion " + String.join(".", java.util.Arrays.copyOfRange(parts, i, parts.length)));
            }
            cur = next;
        }
        String last = parts[parts.length - 1];
        Node existing = cur.kids.get(last);
        if (existing != null) {
            throw new MongoCmdException(existing.kind == CONTAINER ? 31250 : 31250,
                    "Path collision at " + path);
        }
        cur.kids.put(last, leaf);
    }

    /** True when the projection keeps every field unchanged (e.g. {@code {}}). */
    boolean isIdentity() {
        return empty;
    }

    BsonDocument apply(BsonDocument doc) {
        return apply(doc, null);
    }

    BsonDocument apply(BsonDocument doc, MongoAgg.Ctx ctx) {
        if (empty) {
            return doc;
        }
        MongoExpr.Scope sc = ctx != null ? ctx.scope(doc) : aggregation || hasExpr(root) ? MongoExpr.Scope.root(doc, coll) : null;
        if (inclusion) {
            BsonDocument out = incl(doc, root, sc, true, doc);
            return out;
        }
        return excl(doc, root, true, doc);
    }

    private boolean hasExpr(Node n) {
        for (Node k : n.kids.values()) {
            if (k.kind == EXPR || hasExpr(k)) {
                return true;
            }
        }
        return false;
    }

    private BsonDocument incl(BsonDocument doc, Node n, MongoExpr.Scope sc, boolean top, BsonDocument rootDoc) {
        BsonDocument out = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : doc.entrySet()) {
            String k = e.getKey();
            Node c = n.kids.get(k);
            if (c == null) {
                if (top && k.equals("_id") && idIncluded) {
                    out.put(k, e.getValue());
                }
                continue;
            }
            switch (c.kind) {
                case INCLUDE -> out.put(k, e.getValue());
                case EXPR -> {
                    BsonValue v = c.expr.eval(sc);
                    if (v != MongoExpr.MISSING) {
                        out.put(k, v);
                    }
                }
                case SLICE, ELEMMATCH, POSITIONAL -> {
                    BsonValue v = modify(c, e.getValue(), rootDoc);
                    if (v != null) {
                        out.put(k, v);
                    }
                }
                default -> {
                    BsonValue v = inclValue(e.getValue(), c, sc, rootDoc);
                    if (v != null) {
                        out.put(k, v);
                    }
                }
            }
        }
        // computed / not-present fields
        for (Map.Entry<String, Node> e : n.kids.entrySet()) {
            String k = e.getKey();
            Node c = e.getValue();
            if (out.containsKey(k) || doc.containsKey(k) && c.kind != CONTAINER) {
                continue;
            }
            if (c.kind == EXPR) {
                BsonValue v = c.expr.eval(sc);
                if (v != MongoExpr.MISSING) {
                    out.put(k, v);
                }
            } else if (c.kind == CONTAINER && hasExpr(c) && !doc.containsKey(k)) {
                BsonDocument sub = incl(new BsonDocument(), c, sc, false, rootDoc);
                if (!sub.isEmpty()) {
                    out.put(k, sub);
                }
            }
        }
        if (top && !idIncluded) {
            out.remove("_id");
        }
        return out;
    }

    private BsonValue inclValue(BsonValue v, Node c, MongoExpr.Scope sc, BsonDocument rootDoc) {
        if (v.isDocument()) {
            return incl(v.asDocument(), c, sc, false, rootDoc);
        }
        if (v.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : v.asArray()) {
                if (e.isDocument() || e.isArray()) {
                    BsonValue r = inclValue(e, c, sc, rootDoc);
                    if (r != null) {
                        out.add(r);
                    }
                }
            }
            return out;
        }
        return hasExpr(c) && false ? v : null;
    }

    private BsonDocument excl(BsonDocument doc, Node n, boolean top, BsonDocument rootDoc) {
        BsonDocument out = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : doc.entrySet()) {
            String k = e.getKey();
            if (top && k.equals("_id") && !idIncluded) {
                continue;
            }
            Node c = n.kids.get(k);
            if (c == null) {
                out.put(k, e.getValue());
                continue;
            }
            switch (c.kind) {
                case EXCLUDE -> { }
                case INCLUDE -> out.put(k, e.getValue());
                case SLICE, ELEMMATCH, POSITIONAL -> {
                    BsonValue v = modify(c, e.getValue(), rootDoc);
                    if (v != null) {
                        out.put(k, v);
                    }
                }
                case CONTAINER -> out.put(k, exclValue(e.getValue(), c, rootDoc));
                default -> out.put(k, e.getValue());
            }
        }
        return out;
    }

    private BsonValue exclValue(BsonValue v, Node c, BsonDocument rootDoc) {
        if (v.isDocument()) {
            return excl(v.asDocument(), c, false, rootDoc);
        }
        if (v.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : v.asArray()) {
                out.add(e.isDocument() || e.isArray() ? exclValue(e, c, rootDoc) : e);
            }
            return out;
        }
        return v;
    }

    private BsonValue modify(Node c, BsonValue v, BsonDocument rootDoc) {
        switch (c.kind) {
            case SLICE: {
                if (!v.isArray()) {
                    return v;
                }
                List<BsonValue> a = v.asArray().getValues();
                int size = a.size();
                long from;
                long to;
                if (!c.sliceHasSkip) {
                    long n = c.sliceSkip;
                    if (n >= 0) {
                        from = 0;
                        to = Math.min(n, size);
                    } else {
                        from = Math.max(0, size + n);
                        to = size;
                    }
                } else {
                    from = c.sliceSkip < 0 ? Math.max(0, size + c.sliceSkip) : Math.min(c.sliceSkip, size);
                    to = Math.min(size, from + c.sliceLimit);
                }
                return new BsonArray(new ArrayList<>(a.subList((int) from, (int) Math.max(from, to))));
            }
            case ELEMMATCH: {
                if (!v.isArray()) {
                    return null;
                }
                for (BsonValue e : v.asArray()) {
                    if (c.elemPred.test(new BsonDocument("v", new BsonArray(List.of(e))))) {
                        return new BsonArray(List.of(e));
                    }
                }
                return null;
            }
            default: {
                if (!v.isArray()) {
                    return v;
                }
                int idx = filter == null ? -1 : positionalIndex(rootDoc, filter, c.path, coll);
                if (idx < 0 || idx >= v.asArray().size()) {
                    throw new MongoCmdException(51246, "Executor error during find command :: caused by :: positional operator '.$' couldn't find a matching element in the array");
                }
                return new BsonArray(List.of(v.asArray().get(idx)));
            }
        }
    }

    /** Index of the first element of array {@code arrPath} matched by the query, or -1. */
    static int positionalIndex(BsonDocument doc, BsonDocument filter, String arrPath, BsonCmp.Collation coll) {
        for (Map.Entry<String, BsonValue> e : filter.entrySet()) {
            String k = e.getKey();
            if (k.equals("$and") && e.getValue().isArray()) {
                for (BsonValue m : e.getValue().asArray()) {
                    if (m.isDocument()) {
                        int i = positionalIndex(doc, m.asDocument(), arrPath, coll);
                        if (i >= 0) {
                            return i;
                        }
                    }
                }
                continue;
            }
            if (k.startsWith("$")) {
                continue;
            }
            if (k.equals(arrPath) || k.startsWith(arrPath + ".") || arrPath.startsWith(k + ".") && false) {
                List<BsonValue> cands = MongoMatcher.resolve(doc, arrPath);
                for (BsonValue arr : cands) {
                    if (arr == MongoMatcher.MISSING || !arr.isArray()) {
                        continue;
                    }
                    BsonArray a = arr.asArray();
                    for (int i = 0; i < a.size(); i++) {
                        BsonValue el = a.get(i);
                        MongoMatcher.Pred p;
                        BsonDocument wrapper;
                        if (k.equals(arrPath)) {
                            BsonValue cond = e.getValue();
                            if (cond.isDocument() && cond.asDocument().containsKey("$elemMatch")) {
                                p = MongoMatcher.compile(new BsonDocument("v", cond), coll);
                                wrapper = new BsonDocument("v", new BsonArray(List.of(el)));
                            } else {
                                p = MongoMatcher.compile(new BsonDocument("v", cond), coll);
                                wrapper = new BsonDocument("v", el);
                            }
                        } else {
                            if (!el.isDocument()) {
                                continue;
                            }
                            p = MongoMatcher.compile(new BsonDocument(k.substring(arrPath.length() + 1), e.getValue()), coll);
                            wrapper = el.asDocument();
                        }
                        if (p.test(wrapper)) {
                            return i;
                        }
                    }
                }
            }
        }
        return -1;
    }
}
