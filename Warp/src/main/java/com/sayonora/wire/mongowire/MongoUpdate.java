package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/** Applies update documents (operators, replacement, aggregation pipeline) to a document. */
final class MongoUpdate {

    private MongoUpdate() {
    }

    static boolean isPipeline(BsonValue update) {
        return update.isArray();
    }

    static boolean isReplacement(BsonDocument u) {
        if (u.isEmpty()) {
            return true;
        }
        boolean anyOp = false;
        boolean anyPlain = false;
        for (String k : u.keySet()) {
            if (k.startsWith("$")) {
                anyOp = true;
            } else {
                anyPlain = true;
            }
        }
        if (anyOp && anyPlain) {
            throw new MongoCmdException(9, "Unknown modifier: " + u.keySet().stream().filter(k -> !k.startsWith("$")).findFirst().get()
                    + ". Expected a valid update modifier or pipeline-style update specified as an array");
        }
        return !anyOp;
    }

    /** Validates an update spec eagerly (used before any documents are touched, so errors are raised once). */
    static void validate(BsonValue update, BsonArray arrayFilters) {
        if (update.isArray()) {
            for (BsonValue s : update.asArray()) {
                if (!s.isDocument() || s.asDocument().size() != 1) {
                    throw new MongoCmdException(40323, "A pipeline stage specification object must contain exactly one field.");
                }
                String sn = s.asDocument().getFirstKey();
                if (!List.of("$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith").contains(sn)) {
                    if (List.of("$match", "$limit", "$group", "$sort", "$skip", "$count", "$lookup", "$unwind", "$facet", "$bucket", "$sample").contains(sn)) {
                        throw new MongoCmdException(72, sn + " is not allowed to be used within an update");
                    }
                    throw new MongoCmdException(40324, "Unrecognized pipeline stage name: '" + sn + "'");
                }
            }
            return;
        }
        BsonDocument u = update.asDocument();
        if (isReplacement(u)) {
            for (String k : u.keySet()) {
                if (k.startsWith("$")) {
                    throw new MongoCmdException(52, "The dollar ($) prefixed field '" + k + "' in '" + k + "' is not valid for storage.");
                }
            }
            return;
        }
        List<String> paths = new ArrayList<>();
        for (Map.Entry<String, BsonValue> e : u.entrySet()) {
            String op = e.getKey();
            if (!KNOWN.contains(op)) {
                throw new MongoCmdException(9, "Unknown modifier: " + op
                        + ". Expected a valid update modifier or pipeline-style update specified as an array");
            }
            if (!e.getValue().isDocument()) {
                throw new MongoCmdException(9, "Modifiers operate on fields but we found type " + MongoMatcher.typeName(e.getValue())
                        + " instead. For example: {$mod: {<field>: ...}} not {" + op + ": " + MongoFmt.value(e.getValue()) + "}");
            }
            for (Map.Entry<String, BsonValue> f : e.getValue().asDocument().entrySet()) {
                String p = f.getKey();
                if (p.isEmpty() || p.startsWith(".") || p.endsWith(".") || p.contains("..")) {
                    throw new MongoCmdException(56, "The update path '" + p + "' contains an empty field name, which is not allowed.");
                }
                paths.add(p);
                if (op.equals("$rename")) {
                    if (f.getValue().isString() && (f.getValue().asString().getValue().isEmpty() || f.getValue().asString().getValue().contains(".."))) {
                        throw new MongoCmdException(56, "An empty update path is not valid.");
                    }
                    if (!f.getValue().isString()) {
                        throw new MongoCmdException(2, "The 'to' field for $rename must be a string: " + p + ": " + MongoFmt.value(f.getValue()));
                    }
                    if (f.getValue().asString().getValue().equals(p)) {
                        throw new MongoCmdException(2, "The source and target field for $rename must differ: " + p + ": \"" + p + "\"");
                    }
                    paths.add(f.getValue().asString().getValue());
                }
            }
        }
        for (int i = 0; i < paths.size(); i++) {
            for (int j = i + 1; j < paths.size(); j++) {
                String a = paths.get(i);
                String b = paths.get(j);
                if (a.equals(b)) {
                    throw new MongoCmdException(40, "Updating the path '" + b + "' would create a conflict at '" + b + "'");
                }
                if (b.startsWith(a + ".")) {
                    throw new MongoCmdException(40, "Updating the path '" + b + "' would create a conflict at '" + a + "'");
                }
                if (a.startsWith(b + ".")) {
                    throw new MongoCmdException(40, "Updating the path '" + a + "' would create a conflict at '" + b + "'");
                }
            }
        }
        // array filters used / unused
        Set<String> usedIds = new HashSet<>();
        for (String p : paths) {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\$\\[([^\\]]*)\\]").matcher(p);
            while (m.find()) {
                if (!m.group(1).isEmpty()) {
                    usedIds.add(m.group(1));
                }
            }
        }
        Map<String, BsonDocument> filters = parseArrayFilters(arrayFilters);
        for (String id : usedIds) {
            if (!filters.containsKey(id)) {
                throw new MongoCmdException(2, "No array filter found for identifier '" + id + "' in path '"
                        + paths.stream().filter(p -> p.contains("$[" + id + "]")).findFirst().orElse("") + "'");
            }
        }
        for (String id : filters.keySet()) {
            if (!usedIds.contains(id)) {
                throw new MongoCmdException(9, "The array filter for identifier '" + id + "' was not used in the update " + MongoFmt.doc(u));
            }
        }
    }

    private static final Set<String> KNOWN = Set.of("$set", "$unset", "$inc", "$mul", "$min", "$max", "$rename", "$currentDate",
            "$setOnInsert", "$push", "$pull", "$pullAll", "$addToSet", "$pop", "$bit");

    static Map<String, BsonDocument> parseArrayFilters(BsonArray filters) {
        Map<String, BsonDocument> out = new HashMap<>();
        if (filters == null) {
            return out;
        }
        for (BsonValue f : filters) {
            if (!f.isDocument()) {
                throw new MongoCmdException(14, "arrayFilters entries must be objects");
            }
            String id = null;
            for (String k : f.asDocument().keySet()) {
                String top = k.contains(".") ? k.substring(0, k.indexOf('.')) : k;
                if (k.startsWith("$")) {
                    // $and/$or at top: find identifier inside
                    continue;
                }
                if (!top.matches("[a-z][a-zA-Z0-9]*")) {
                    throw new MongoCmdException(2, "Error parsing array filter :: caused by :: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found '" + top + "'");
                }
                if (id != null && !id.equals(top)) {
                    throw new MongoCmdException(9, "Error parsing array filter :: caused by :: Expected a single top-level field name, found '" + id + "' and '" + top + "'");
                }
                id = top;
            }
            if (id == null) {
                id = findIdentifier(f.asDocument());
            }
            if (id == null) {
                throw new MongoCmdException(9, "Cannot use an expression without a top-level field name in arrayFilters");
            }
            if (out.containsKey(id)) {
                throw new MongoCmdException(9, "Found multiple array filters with the same top-level field name " + id);
            }
            out.put(id, f.asDocument());
        }
        return out;
    }

    private static String findIdentifier(BsonDocument d) {
        for (Map.Entry<String, BsonValue> e : d.entrySet()) {
            if (e.getKey().startsWith("$") && e.getValue().isArray()) {
                for (BsonValue m : e.getValue().asArray()) {
                    if (m.isDocument()) {
                        for (String k : m.asDocument().keySet()) {
                            if (!k.startsWith("$")) {
                                return k.contains(".") ? k.substring(0, k.indexOf('.')) : k;
                            }
                        }
                        String r = findIdentifier(m.asDocument());
                        if (r != null) {
                            return r;
                        }
                    }
                }
            }
        }
        return null;
    }

    /** Context for one apply call. */
    static final class Ctx {
        boolean insert;
        BsonDocument filter;
        Map<String, BsonDocument> arrayFilters = new HashMap<>();
        BsonCmp.Collation coll;
        Map<String, MongoMatcher.Pred> filterPreds = new HashMap<>();
        java.util.function.Function<String, Integer> positionalResolver;
        Map<String, BsonValue> vars = Map.of();
    }

    /** Returns the updated copy of {@code original}. */
    static BsonDocument apply(BsonDocument original, BsonValue update, Ctx ctx) {
        BsonDocument out;
        if (update.isArray()) {
            out = MongoAgg.applyStages(update.asArray(), original, ctx.coll, ctx.vars);
            if (!out.containsKey("_id") && original.containsKey("_id")) {
                BsonDocument withId = new BsonDocument("_id", original.get("_id"));
                out.forEach(withId::put);
                out = withId;
            }
            checkId(original, out, ctx.insert);
            return out;
        }
        BsonDocument u = update.asDocument();
        if (isReplacement(u)) {
            out = new BsonDocument();
            BsonValue id = original.get("_id");
            if (u.containsKey("_id") && id != null && !sameExact(u.get("_id"), id)) {
                throw new MongoCmdException(66, "After applying the update, the (immutable) field '_id' was found to have been altered to _id: "
                        + MongoFmt.value(u.get("_id")));
            }
            if (id != null) {
                out.put("_id", id);
            }
            for (Map.Entry<String, BsonValue> e : u.entrySet()) {
                out.put(e.getKey(), e.getValue());
            }
            if (!out.containsKey("_id") && u.containsKey("_id")) {
                out.put("_id", u.get("_id"));
            }
            return out;
        }
        // deep copy so nested mutation doesn't touch the original
        out = deepCopy(original);
        if (ctx.filter != null) {
            ctx.positionalResolver = p -> MongoProjection.positionalIndex(original, ctx.filter, p, ctx.coll);
        }
        List<Map.Entry<String, BsonValue>> ordered = new ArrayList<>(u.entrySet());
        ordered.sort(java.util.Map.Entry.comparingByKey());
        for (Map.Entry<String, BsonValue> e : ordered) {
            String op = e.getKey();
            if (op.equals("$setOnInsert") && !ctx.insert) {
                continue;
            }
            for (Map.Entry<String, BsonValue> f : e.getValue().asDocument().entrySet()) {
                for (List<String> path : expand(out, f.getKey(), ctx)) {
                    applyOne(out, op, path, f.getValue(), f.getKey(), ctx);
                }
            }
        }
        checkId(original, out, ctx.insert);
        return out;
    }

    private static boolean sameExact(BsonValue a, BsonValue b) {
        return a.getBsonType() == b.getBsonType() && BsonCmp.compare(a, b) == 0;
    }

    private static void checkId(BsonDocument original, BsonDocument out, boolean insert) {
        BsonValue a = original.get("_id");
        BsonValue b = out.get("_id");
        if (a == null || insert && false) {
            return;
        }
        if (b == null) {
            throw new MongoCmdException(66, "After applying the update, the (immutable) field '_id' was found to have been altered to _id: missing");
        }
        if (!sameExact(a, b)) {
            throw new MongoCmdException(66, "Performing an update on the path '_id' would modify the immutable field '_id'");
        }
    }

    static BsonDocument deepCopy(BsonDocument d) {
        BsonDocument out = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : d.entrySet()) {
            out.put(e.getKey(), deepCopy(e.getValue()));
        }
        return out;
    }

    static BsonValue deepCopy(BsonValue v) {
        if (v.isDocument()) {
            return deepCopy(v.asDocument());
        }
        if (v.isArray()) {
            BsonArray a = new BsonArray();
            for (BsonValue x : v.asArray()) {
                a.add(deepCopy(x));
            }
            return a;
        }
        return v;
    }

    // ------------------------------------------------------------------ positional expansion

    private static List<List<String>> expand(BsonDocument doc, String path, Ctx ctx) {
        String[] parts = path.split("\\.", -1);
        boolean positional = false;
        for (String p : parts) {
            if (p.equals("$") || p.startsWith("$[")) {
                positional = true;
            }
        }
        List<List<String>> out = new ArrayList<>();
        if (!positional) {
            out.add(List.of(parts));
            return out;
        }
        expand(doc, parts, 0, new ArrayList<>(), out, ctx, path);
        return out;
    }

    private static void expand(BsonValue node, String[] parts, int i, List<String> acc, List<List<String>> out, Ctx ctx, String full) {
        if (i == parts.length) {
            out.add(new ArrayList<>(acc));
            return;
        }
        String part = parts[i];
        if (part.equals("$") || part.startsWith("$[")) {
            if (node == null) {
                throw new MongoCmdException(2, "The path '" + String.join(".", java.util.Arrays.copyOfRange(parts, 0, i)) + "' must exist in the document in order to apply array updates.");
            }
            if (!node.isArray()) {
                throw new MongoCmdException(2, "Cannot apply array updates to non-array element " + MongoFmt.elem(i > 0 ? parts[i - 1] : "", node));
            }
            BsonArray arr = node.asArray();
            if (part.equals("$")) {
                String arrPath = String.join(".", java.util.Arrays.copyOfRange(parts, 0, i)).replace("$", "");
                int idx = ctx.filter == null ? -1 : positionalIndexForUpdate(acc, ctx);
                if (idx < 0) {
                    throw new MongoCmdException(2, "The positional operator did not find the match needed from the query.");
                }
                acc.add(Integer.toString(idx));
                expand(idx < arr.size() ? arr.get(idx) : null, parts, i + 1, acc, out, ctx, full);
                acc.remove(acc.size() - 1);
                return;
            }
            String id = part.substring(2, part.length() - 1);
            MongoMatcher.Pred pred = null;
            if (!id.isEmpty()) {
                BsonDocument af = ctx.arrayFilters.get(id);
                pred = ctx.filterPreds.computeIfAbsent(id, k -> MongoMatcher.compile(af, ctx.coll));
            }
            for (int k = 0; k < arr.size(); k++) {
                BsonValue el = arr.get(k);
                if (pred != null && !pred.test(new BsonDocument(id, el))) {
                    continue;
                }
                acc.add(Integer.toString(k));
                expand(el, parts, i + 1, acc, out, ctx, full);
                acc.remove(acc.size() - 1);
            }
            return;
        }
        acc.add(part);
        BsonValue child = null;
        if (node != null && node.isDocument()) {
            child = node.asDocument().get(part);
        } else if (node != null && node.isArray() && MongoMatcher.isIndex(part)) {
            int idx = Integer.parseInt(part);
            child = idx < node.asArray().size() ? node.asArray().get(idx) : null;
        }
        expand(child, parts, i + 1, acc, out, ctx, full);
        acc.remove(acc.size() - 1);
    }

    private static int positionalIndexForUpdate(List<String> acc, Ctx ctx) {
        return ctx.positionalResolver == null ? -1 : ctx.positionalResolver.apply(String.join(".", acc));
    }

    // ------------------------------------------------------------------ path navigation

    private static BsonValue get(BsonValue root, List<String> parts) {
        BsonValue cur = root;
        for (String p : parts) {
            if (cur == null) {
                return null;
            }
            if (cur.isDocument()) {
                cur = cur.asDocument().get(p);
            } else if (cur.isArray() && MongoMatcher.isIndex(p)) {
                int i = Integer.parseInt(p);
                cur = i < cur.asArray().size() ? cur.asArray().get(i) : null;
            } else {
                return null;
            }
        }
        return cur;
    }

    /** Sets a value, creating intermediate documents; PathNotViable (28) when blocked by a scalar or bad array index. */
    private static void set(BsonDocument root, List<String> parts, BsonValue value) {
        BsonValue cur = root;
        for (int i = 0; i < parts.size(); i++) {
            String p = parts.get(i);
            boolean last = i == parts.size() - 1;
            if (cur.isDocument()) {
                BsonDocument d = cur.asDocument();
                if (last) {
                    d.put(p, value);
                    return;
                }
                BsonValue next = d.get(p);
                if (next == null) {
                    next = new BsonDocument();
                    d.put(p, next);
                } else if (!next.isDocument() && !next.isArray()) {
                    throw new MongoCmdException(28, "Cannot create field '" + parts.get(i + 1) + "' in element " + MongoFmt.elem(p, next));
                }
                cur = next;
            } else {
                BsonArray a = cur.asArray();
                if (!MongoMatcher.isIndex(p)) {
                    throw new MongoCmdException(28, "Cannot create field '" + p + "' in element " + MongoFmt.elem(i > 0 ? parts.get(i - 1) : "", a));
                }
                int idx = Integer.parseInt(p);
                while (a.size() <= idx) {
                    a.add(BsonNull.VALUE);
                }
                if (last) {
                    a.set(idx, value);
                    return;
                }
                BsonValue next = a.get(idx);
                if (next.isNull()) {
                    next = new BsonDocument();
                    a.set(idx, next);
                } else if (!next.isDocument() && !next.isArray()) {
                    throw new MongoCmdException(28, "Cannot create field '" + parts.get(i + 1) + "' in element " + MongoFmt.elem(p, next));
                }
                cur = next;
            }
        }
    }

    private static BsonValue getStrict(BsonValue root, List<String> parts) {
        BsonValue cur = root;
        for (int i = 0; i < parts.size(); i++) {
            String p = parts.get(i);
            if (cur == null) {
                return null;
            }
            if (cur.isDocument()) {
                cur = cur.asDocument().get(p);
            } else if (cur.isArray()) {
                if (!MongoMatcher.isIndex(p)) {
                    throw new MongoCmdException(28, "Cannot use the part (" + p + ") of (" + String.join(".", parts) + ") to traverse the element ("
                            + MongoFmt.elem(i > 0 ? parts.get(i - 1) : "", cur) + ")");
                }
                int k = Integer.parseInt(p);
                cur = k < cur.asArray().size() ? cur.asArray().get(k) : null;
            } else {
                return null;
            }
        }
        return cur;
    }

    private static void checkRenamePath(BsonDocument doc, List<String> parts, String what) {
        BsonValue cur = doc;
        for (int i = 0; i < parts.size(); i++) {
            if (cur == null) {
                return;
            }
            if (cur.isArray()) {
                throw new MongoCmdException(MongoMatcher.isIndex(parts.get(i)) ? 2 : 28, "The " + what + " field cannot be an array element, '"
                        + String.join(".", parts) + "' in doc with _id: " + (doc.containsKey("_id") ? MongoFmt.value(doc.get("_id")) : "")
                        + " has an array field called '" + (i > 0 ? parts.get(i - 1) : "") + "'");
            }
            cur = cur.isDocument() ? cur.asDocument().get(parts.get(i)) : null;
        }
    }

    private static void unset(BsonDocument root, List<String> parts) {
        BsonValue parent = parts.size() == 1 ? root : get(root, parts.subList(0, parts.size() - 1));
        String last = parts.get(parts.size() - 1);
        if (parent == null) {
            return;
        }
        if (parent.isDocument()) {
            parent.asDocument().remove(last);
        } else if (parent.isArray() && MongoMatcher.isIndex(last)) {
            int i = Integer.parseInt(last);
            if (i < parent.asArray().size()) {
                parent.asArray().set(i, BsonNull.VALUE);
            }
        }
    }

    private static String fieldName(List<String> parts) {
        return String.join(".", parts);
    }

    private static void applyOne(BsonDocument doc, String op, List<String> path, BsonValue arg, String rawPath, Ctx ctx) {
        if (!ctx.insert) {
            for (String part : path) {
                if (part.startsWith("$") && !MongoMatcher.isIndex(part)) {
                    throw new MongoCmdException(52, "The dollar ($) prefixed field '" + part + "' in '" + rawPath + "' is not allowed in the context of an update's replacement document. Consider using an aggregation pipeline with $replaceWith.");
                }
            }
        }
        BsonValue cur = op.equals("$unset") || op.equals("$rename") ? get(doc, path) : getStrict(doc, path);
        if (op.equals("$rename")) {
            checkRenamePath(doc, path, "source");
            checkRenamePath(doc, List.of(arg.asString().getValue().split("\\.", -1)), "destination");
            if (arg.asString().getValue().equals("_id") || rawPath.equals("_id")) {
                throw new MongoCmdException(66, "Performing an update on the path '_id' would modify the immutable field '_id'");
            }
        }
        switch (op) {
            case "$set", "$setOnInsert" -> set(doc, path, deepCopy(arg));
            case "$unset" -> unset(doc, path);
            case "$inc", "$mul" -> {
                boolean inc = op.equals("$inc");
                if (!BsonCmp.isNumber(arg)) {
                    throw new MongoCmdException(14, "Cannot " + (inc ? "increment" : "multiply") + " with non-numeric argument: "
                            + MongoFmt.elem(rawPath, arg));
                }
                if (cur == null) {
                    set(doc, path, inc ? arg : zeroLike(arg));
                } else {
                    if (!BsonCmp.isNumber(cur)) {
                        throw new MongoCmdException(14, "Cannot apply " + op + " to a value of non-numeric type. {_id: "
                                + (doc.containsKey("_id") ? MongoFmt.value(doc.get("_id")) : "") + "} has the field '"
                                + path.get(path.size() - 1) + "' of non-numeric type " + MongoMatcher.typeName(cur));
                    }
                    set(doc, path, inc ? MongoNum.add(cur, arg) : MongoNum.multiply(cur, arg));
                }
            }
            case "$min", "$max" -> {
                if (cur == null) {
                    set(doc, path, deepCopy(arg));
                } else {
                    int c = BsonCmp.compare(arg, cur, ctx.coll);
                    if (op.equals("$min") ? c < 0 : c > 0) {
                        set(doc, path, deepCopy(arg));
                    }
                }
            }
            case "$rename" -> {
                String to = arg.asString().getValue();
                if (cur != null) {
                    unset(doc, path);
                    set(doc, List.of(to.split("\\.", -1)), cur);
                }
            }
            case "$currentDate" -> {
                boolean ts = false;
                if (arg.isDocument()) {
                    BsonValue t = arg.asDocument().get("$type");
                    if (t == null || !t.isString() || !(t.asString().getValue().equals("date") || t.asString().getValue().equals("timestamp"))) {
                        throw new MongoCmdException(2, "The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {"
                                + rawPath + ": " + MongoFmt.value(arg) + "}}");
                    }
                    ts = t.asString().getValue().equals("timestamp");
                } else if (!arg.isBoolean()) {
                    throw new MongoCmdException(2, "'" + MongoMatcher.typeName(arg) + "' is the wrong type for $currentDate. Must be a boolean or a $type expression");
                }
                long now = System.currentTimeMillis();
                set(doc, path, ts ? new BsonTimestamp((int) (now / 1000), 1) : new BsonDateTime(now));
            }
            case "$push" -> push(doc, path, cur, arg);
            case "$addToSet" -> {
                BsonArray arr = arrayTarget(doc, path, cur, "$addToSet");
                if (arg.isDocument() && arg.asDocument().containsKey("$each") && arg.asDocument().size() > 1) {
                    throw new MongoCmdException(2, "Found unexpected fields after $each in $addToSet: " + MongoFmt.value(arg));
                }
                List<BsonValue> vals = arg.isDocument() && arg.asDocument().containsKey("$each") ? eachList(arg.asDocument(), "$addToSet")
                        : List.of(arg);
                for (BsonValue v : vals) {
                    boolean found = false;
                    for (BsonValue e : arr) {
                        if (BsonCmp.compare(e, v, ctx.coll) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        arr.add(deepCopy(v));
                    }
                }
                if (cur == null) {
                    set(doc, path, arr);
                }
            }
            case "$pull" -> {
                if (cur == null) {
                    return;
                }
                if (!cur.isArray()) {
                    throw new MongoCmdException(2, "Cannot apply $pull to a non-array value");
                }
                BsonArray arr = cur.asArray();
                MongoMatcher.Pred p = null;
                MongoMatcher.Pred docPred = null;
                String fk = arg.isDocument() && !arg.asDocument().isEmpty() ? arg.asDocument().getFirstKey() : "";
                boolean topLogical = List.of("$and", "$or", "$nor", "$expr", "$jsonSchema", "$comment", "$alwaysTrue", "$alwaysFalse").contains(fk);
                if (arg.isDocument() && fk.equals("$not")) {
                    throw new MongoCmdException(2, "unknown top level operator: $not. If you are trying to negate an entire expression, use $nor.");
                }
                if (arg.isDocument() && fk.startsWith("$") && !topLogical) {
                    p = MongoMatcher.compile(new BsonDocument("v", arg), ctx.coll);
                } else if (arg.isDocument()) {
                    docPred = MongoMatcher.compile(arg.asDocument(), ctx.coll);
                } else if (arg.isRegularExpression()) {
                    p = MongoMatcher.compile(new BsonDocument("v", arg), ctx.coll);
                }
                List<BsonValue> keep = new ArrayList<>();
                for (BsonValue e : arr) {
                    boolean remove;
                    if (p != null) {
                        remove = p.test(new BsonDocument("v", e));
                    } else if (docPred != null) {
                        remove = e.isDocument() && docPred.test(e.asDocument());
                    } else {
                        remove = BsonCmp.compare(e, arg, ctx.coll) == 0;
                    }
                    if (!remove) {
                        keep.add(e);
                    }
                }
                arr.clear();
                arr.addAll(keep);
            }
            case "$pullAll" -> {
                if (!arg.isArray()) {
                    throw new MongoCmdException(2, "$pullAll requires an array argument but was given a " + MongoMatcher.typeName(arg));
                }
                if (cur == null) {
                    return;
                }
                if (!cur.isArray()) {
                    throw new MongoCmdException(2, "Cannot apply $pull to a non-array value");
                }
                BsonArray arr = cur.asArray();
                List<BsonValue> keep = new ArrayList<>();
                for (BsonValue e : arr) {
                    boolean remove = false;
                    for (BsonValue r : arg.asArray()) {
                        if (BsonCmp.compare(e, r, ctx.coll) == 0) {
                            remove = true;
                            break;
                        }
                    }
                    if (!remove) {
                        keep.add(e);
                    }
                }
                arr.clear();
                arr.addAll(keep);
            }
            case "$pop" -> {
                if (!BsonCmp.isNumber(arg)) {
                    throw new MongoCmdException(9, "Expected a number in: " + rawPath + ": " + MongoFmt.value(arg));
                }
                long n = MongoNum.truncLong(arg);
                if (n != 1 && n != -1) {
                    throw new MongoCmdException(9, "$pop expects 1 or -1, found: " + MongoFmt.value(arg));
                }
                if (cur == null) {
                    return;
                }
                if (!cur.isArray()) {
                    throw new MongoCmdException(14, "Path '" + fieldName(path) + "' contains an element of non-array type '"
                            + MongoMatcher.typeName(cur) + "'");
                }
                if (!cur.asArray().isEmpty()) {
                    cur.asArray().remove(n == 1 ? cur.asArray().size() - 1 : 0);
                }
            }
            case "$bit" -> {
                if (!arg.isDocument()) {
                    throw new MongoCmdException(2, "The $bit modifier is not compatible with a " + MongoMatcher.typeName(arg)
                            + ". You must pass in an embedded document: {$bit: {field: {and/or/xor: #}}}");
                }
                BsonValue res = cur == null ? new BsonInt32(0) : cur;
                if (!(res.isInt32() || res.isInt64())) {
                    throw new MongoCmdException(2, "Cannot apply $bit to a value of non-integral type.{_id: "
                            + (doc.containsKey("_id") ? MongoFmt.value(doc.get("_id")) : "") + "} has the field '"
                            + path.get(path.size() - 1) + "' of non-integer type " + MongoMatcher.typeName(res));
                }
                for (Map.Entry<String, BsonValue> b : arg.asDocument().entrySet()) {
                    if (!List.of("and", "or", "xor").contains(b.getKey())) {
                        throw new MongoCmdException(2, "The $bit modifier only supports 'and', 'or', and 'xor', not '" + b.getKey()
                                + "' which is an unknown operator: {\"" + b.getKey() + "\" : " + MongoFmt.value(b.getValue()) + "}");
                    }
                    if (!(b.getValue().isInt32() || b.getValue().isInt64())) {
                        throw new MongoCmdException(2, "The $bit modifier field must be an Integer(32/64 bit); a '"
                                + MongoMatcher.typeName(b.getValue()) + "' is not supported here: {" + b.getKey() + ": " + MongoFmt.value(b.getValue()) + "}");
                    }
                    long x = MongoNum.truncLong(res);
                    long y = MongoNum.truncLong(b.getValue());
                    long r = switch (b.getKey()) {
                        case "and" -> x & y;
                        case "or" -> x | y;
                        default -> x ^ y;
                    };
                    res = res.isInt64() || b.getValue().isInt64() ? new BsonInt64(r) : new BsonInt32((int) r);
                }
                set(doc, path, res);
            }
            default -> throw new MongoCmdException(9, "Unknown modifier: " + op);
        }
    }

    private static BsonValue zeroLike(BsonValue v) {
        if (v.isInt32()) {
            return new BsonInt32(0);
        }
        if (v.isInt64()) {
            return new BsonInt64(0);
        }
        if (v.isDouble()) {
            return new org.bson.BsonDouble(0);
        }
        return new org.bson.BsonDecimal128(org.bson.types.Decimal128.parse("0"));
    }

    private static BsonArray arrayTarget(BsonDocument doc, List<String> path, BsonValue cur, String op) {
        if (cur == null) {
            return new BsonArray();
        }
        if (!cur.isArray()) {
            throw new MongoCmdException(2, op.equals("$addToSet")
                    ? "Cannot apply $addToSet to non-array field. Field named '" + path.get(path.size() - 1) + "' has non-array type "
                            + MongoMatcher.typeName(cur)
                    : "The field '" + fieldName(path) + "' must be an array but is of type " + MongoMatcher.typeName(cur)
                            + " in document {_id: " + (doc.containsKey("_id") ? MongoFmt.value(doc.get("_id")) : "") + "}");
        }
        return cur.asArray();
    }

    private static List<BsonValue> eachList(BsonDocument spec, String op) {
        BsonValue each = spec.get("$each");
        if (!each.isArray()) {
            throw new MongoCmdException(op.equals("$push") ? 2 : 14, "The argument to $each in " + op + " must be an array but it was of type: " + MongoMatcher.typeName(each));
        }
        return each.asArray().getValues();
    }

    private static void push(BsonDocument doc, List<String> path, BsonValue cur, BsonValue arg) {
        BsonArray arr = arrayTarget(doc, path, cur, "$push");
        List<BsonValue> vals;
        Long slice = null;
        int position = Integer.MAX_VALUE;
        boolean hasPosition = false;
        BsonValue sort = null;
        if (arg.isDocument() && arg.asDocument().containsKey("$each")) {
            BsonDocument spec = arg.asDocument();
            for (String k : spec.keySet()) {
                if (!List.of("$each", "$slice", "$position", "$sort").contains(k)) {
                    throw new MongoCmdException(2, "Unrecognized clause in $push: " + k);
                }
            }
            vals = eachList(spec, "$push");
            if (spec.containsKey("$slice")) {
                BsonValue s = spec.get("$slice");
                if (!BsonCmp.isNumber(s) || MongoNum.toDouble(s) != Math.floor(MongoNum.toDouble(s))) {
                    throw new MongoCmdException(2, "The value for $slice must be an integer value but was given type: " + MongoMatcher.typeName(s));
                }
                slice = MongoNum.truncLong(s);
            }
            if (spec.containsKey("$position")) {
                BsonValue s = spec.get("$position");
                if (!BsonCmp.isNumber(s) || MongoNum.toDouble(s) != Math.floor(MongoNum.toDouble(s))) {
                    throw new MongoCmdException(2, "The value for $position must be an integer value, not of type: " + MongoMatcher.typeName(s));
                }
                long p = MongoNum.truncLong(s);
                position = (int) (p < 0 ? Math.max(0, arr.size() + p) : Math.min(p, arr.size()));
                hasPosition = true;
            }
            if (spec.containsKey("$sort")) {
                sort = spec.get("$sort");
                if (sort.isDocument()) {
                    MongoSort.validate(sort.asDocument(), "$push");
                } else if (!BsonCmp.isNumber(sort) || Math.abs(MongoNum.toDouble(sort)) != 1) {
                    throw new MongoCmdException(2, "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields");
                }
            }
        } else {
            vals = List.of(arg);
        }
        List<BsonValue> list = new ArrayList<>(arr.getValues());
        List<BsonValue> copies = new ArrayList<>();
        for (BsonValue v : vals) {
            copies.add(deepCopy(v));
        }
        if (hasPosition) {
            list.addAll(position, copies);
        } else {
            list.addAll(copies);
        }
        if (sort != null) {
            if (sort.isDocument()) {
                java.util.Comparator<BsonDocument> c = MongoSort.comparator(sort.asDocument(), null);
                list.sort((x, y) -> c.compare(x.isDocument() ? x.asDocument() : new BsonDocument(), y.isDocument() ? y.asDocument() : new BsonDocument()));
            } else {
                int dir = MongoNum.toDouble(sort) < 0 ? -1 : 1;
                list.sort((x, y) -> dir * BsonCmp.compare(x, y));
            }
        }
        if (slice != null) {
            int size = list.size();
            if (slice == 0) {
                list = new ArrayList<>();
            } else if (slice > 0) {
                list = new ArrayList<>(list.subList(0, (int) Math.min(slice, size)));
            } else {
                list = new ArrayList<>(list.subList((int) Math.max(0, size + slice), size));
            }
        }
        BsonArray result = new BsonArray(list);
        set(doc, path, result);
    }

    // ------------------------------------------------------------------ upsert seeding

    /** Builds the seed document for an upsert from the equality conditions of the filter. */
    static BsonDocument upsertSeed(BsonDocument filter) {
        BsonDocument seed = new BsonDocument();
        seedFrom(filter, seed);
        return seed;
    }

    private static void seedFrom(BsonDocument filter, BsonDocument seed) {
        for (Map.Entry<String, BsonValue> e : filter.entrySet()) {
            String k = e.getKey();
            BsonValue v = e.getValue();
            if (k.equals("$and") && v.isArray()) {
                for (BsonValue m : v.asArray()) {
                    if (m.isDocument()) {
                        seedFrom(m.asDocument(), seed);
                    }
                }
                continue;
            }
            if (k.startsWith("$")) {
                continue;
            }
            BsonValue eq = null;
            if (v.isDocument() && !v.asDocument().isEmpty() && v.asDocument().getFirstKey().startsWith("$")) {
                BsonDocument ops = v.asDocument();
                if (ops.containsKey("$eq")) {
                    eq = ops.get("$eq");
                } else if (ops.size() == 1 && ops.containsKey("$in") && ops.get("$in").isArray() && ops.getArray("$in").size() == 1) {
                    eq = ops.getArray("$in").get(0);
                }
            } else if (!v.isRegularExpression()) {
                eq = v;
            }
            if (eq == null) {
                continue;
            }
            List<String> parts = List.of(k.split("\\.", -1));
            try {
                set(seed, parts, deepCopy(eq));
            } catch (MongoCmdException ex) {
                throw new MongoCmdException(2, "cannot infer query fields to set, path '" + k + "' is matched twice");
            }
        }
    }

    static BsonValue newObjectId() {
        return new BsonObjectId(new ObjectId());
    }
}
