package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/** MongoDB query filter evaluation ({@code find} filters, {@code $match}, update/delete/count selectors). */
final class MongoMatcher {

    interface Pred {
        boolean test(BsonDocument doc);
    }

    /** Marker for "path does not exist" among resolved candidates. */
    static final BsonValue MISSING = new BsonString("<missing>");

    private final BsonCmp.Collation coll;
    private final Map<String, BsonValue> vars;
    private boolean flat;

    private MongoMatcher(BsonCmp.Collation coll, Map<String, BsonValue> vars) {
        this.coll = coll;
        this.vars = vars;
    }

    static Pred compile(BsonDocument filter) {
        return compile(filter, null);
    }

    static Pred compile(BsonDocument filter, BsonCmp.Collation coll) {
        return compile(filter, coll, Map.of());
    }

    static Pred compile(BsonDocument filter, BsonCmp.Collation coll, Map<String, BsonValue> vars) {
        if (filter == null || filter.isEmpty()) {
            return d -> true;
        }
        return new MongoMatcher(coll, vars).top(filter);
    }

    // ------------------------------------------------------------------ path resolution

    static boolean isIndex(String part) {
        if (part.isEmpty() || part.length() > 9) {
            return false;
        }
        for (int i = 0; i < part.length(); i++) {
            if (part.charAt(i) < '0' || part.charAt(i) > '9') {
                return false;
            }
        }
        return true;
    }

    record Cand(BsonValue v, boolean expand) {
    }

    /** Like {@link #resolve} but marks values reached through a numeric array index (not expanded further) and
     *  never invents MISSING for scalar-only arrays. */
    static List<Cand> resolveX(BsonValue base, String path) {
        List<Cand> out = new ArrayList<>(2);
        String[] parts = path.split("\\.", -1);
        boolean[] sawArray = {false};
        resolveX(base, parts, 0, out, sawArray, true);
        if (out.isEmpty() && !sawArray[0]) {
            out.add(new Cand(MISSING, true));
        }
        return out;
    }

    private static void resolveX(BsonValue v, String[] parts, int i, List<Cand> out, boolean[] sawArray, boolean emitMissing) {
        if (i == parts.length) {
            out.add(new Cand(v, true));
            return;
        }
        String part = parts[i];
        if (v.isDocument()) {
            BsonValue child = v.asDocument().get(part);
            if (child == null) {
                if (emitMissing) {
                    out.add(new Cand(MISSING, true));
                }
            } else {
                resolveX(child, parts, i + 1, out, sawArray, true);
            }
        } else if (v.isArray()) {
            sawArray[0] = true;
            BsonArray arr = v.asArray();
            boolean idx = isIndex(part);
            if (idx) {
                int k = Integer.parseInt(part);
                if (k < arr.size()) {
                    if (i == parts.length - 1) {
                        out.add(new Cand(arr.get(k), false));
                    } else {
                        resolveX(arr.get(k), parts, i + 1, out, sawArray, true);
                    }
                }
            }
            for (BsonValue e : arr) {
                if (e.isDocument()) {
                    resolveX(e, parts, i, out, sawArray, !idx);
                }
            }
        }
    }

    /** Values reachable at {@code path} with MongoDB array traversal; {@link #MISSING} for absent branches. */
    static List<BsonValue> resolve(BsonValue base, String path) {
        List<BsonValue> out = new ArrayList<>(2);
        resolve(base, path.split("\\.", -1), 0, out);
        if (out.isEmpty()) {
            out.add(MISSING);
        }
        return out;
    }

    private static void resolve(BsonValue v, String[] parts, int i, List<BsonValue> out) {
        if (i == parts.length) {
            out.add(v);
            return;
        }
        String part = parts[i];
        if (v.isDocument()) {
            BsonValue child = v.asDocument().get(part);
            if (child == null) {
                out.add(MISSING);
            } else {
                resolve(child, parts, i + 1, out);
            }
        } else if (v.isArray()) {
            BsonArray arr = v.asArray();
            if (isIndex(part)) {
                int idx = Integer.parseInt(part);
                if (idx < arr.size()) {
                    resolve(arr.get(idx), parts, i + 1, out);
                }
            }
            for (BsonValue e : arr) {
                if (e.isDocument()) {
                    resolve(e, parts, i, out);
                }
            }
        }
    }

    // ------------------------------------------------------------------ compile

    private Pred top(BsonDocument filter) {
        List<Pred> preds = new ArrayList<>();
        for (Map.Entry<String, BsonValue> e : filter.entrySet()) {
            preds.add(topClause(e.getKey(), e.getValue()));
        }
        if (preds.size() == 1) {
            return preds.get(0);
        }
        return d -> {
            for (Pred p : preds) {
                if (!p.test(d)) {
                    return false;
                }
            }
            return true;
        };
    }

    private List<Pred> subFilters(String op, BsonValue v) {
        if (!v.isArray()) {
            throw MongoCmdException.badValue(op + " must be an array");
        }
        if (v.asArray().isEmpty()) {
            throw MongoCmdException.badValue("$and/$or/$nor must be a nonempty array");
        }
        List<Pred> out = new ArrayList<>();
        for (BsonValue m : v.asArray()) {
            if (!m.isDocument()) {
                throw MongoCmdException.badValue("$or/$and/$nor entries need to be full objects");
            }
            out.add(top(m.asDocument()));
        }
        return out;
    }

    private Pred topClause(String key, BsonValue value) {
        if (key.startsWith("$")) {
            switch (key) {
                case "$and": {
                    List<Pred> ps = subFilters(key, value);
                    return d -> {
                        for (Pred p : ps) {
                            if (!p.test(d)) {
                                return false;
                            }
                        }
                        return true;
                    };
                }
                case "$or": {
                    List<Pred> ps = subFilters(key, value);
                    return d -> {
                        for (Pred p : ps) {
                            if (p.test(d)) {
                                return true;
                            }
                        }
                        return false;
                    };
                }
                case "$nor": {
                    List<Pred> ps = subFilters(key, value);
                    return d -> {
                        for (Pred p : ps) {
                            if (p.test(d)) {
                                return false;
                            }
                        }
                        return true;
                    };
                }
                case "$expr": {
                    MongoExpr.Expr e = MongoExpr.parse(value);
                    return d -> MongoExpr.truthy(e.eval(MongoExpr.Scope.root(d, coll, vars)));
                }
                case "$comment":
                    return d -> true;
                case "$jsonSchema": {
                    Predicate<BsonValue> s = MongoJsonSchema.compile(value);
                    return s::test;
                }
                case "$where":
                    throw new MongoCmdException(2, "$where is not supported by Warp (no JavaScript engine)");
                case "$text":
                    throw new MongoCmdException(27, "text index required for $text query");
                case "$alwaysTrue":
                    return d -> true;
                case "$alwaysFalse":
                    return d -> false;
                default:
                    throw MongoCmdException.badValue("unknown top level operator: " + key
                            + ". If you have a field name that starts with a '$' symbol, consider using $getField or $setField.");
            }
        }
        return fieldClause(key, value);
    }

    private static boolean hasOperators(BsonDocument d) {
        return !d.isEmpty() && d.getFirstKey().startsWith("$");
    }

    private Pred fieldClause(String path, BsonValue cond) {
        if (cond.isRegularExpression()) {
            return leaf(path, regexMatcher(cond.asRegularExpression().getPattern(), cond.asRegularExpression().getOptions()));
        }
        if (cond.isDocument() && hasOperators(cond.asDocument())) {
            BsonDocument opdoc = cond.asDocument();
            List<Pred> preds = new ArrayList<>();
            BsonValue regexPattern = opdoc.get("$regex");
            String regexOptions = opdoc.containsKey("$options") ? optString(opdoc.get("$options")) : null;
            for (Map.Entry<String, BsonValue> e : opdoc.entrySet()) {
                String op = e.getKey();
                if (op.equals("$options")) {
                    if (regexPattern == null) {
                        throw MongoCmdException.badValue("$options needs a $regex");
                    }
                    continue;
                }
                if (op.equals("$regex")) {
                    preds.add(regexOperator(path, e.getValue(), regexOptions));
                    continue;
                }
                preds.add(operator(path, op, e.getValue(), opdoc));
            }
            if (preds.size() == 1) {
                return preds.get(0);
            }
            return d -> {
                for (Pred p : preds) {
                    if (!p.test(d)) {
                        return false;
                    }
                }
                return true;
            };
        }
        return operator(path, "$eq", cond, null);
    }

    private static String optString(BsonValue v) {
        if (!v.isString()) {
            throw MongoCmdException.badValue("$options has to be a string");
        }
        return v.asString().getValue();
    }

    private Pred regexOperator(String path, BsonValue pattern, String options) {
        if (pattern.isRegularExpression()) {
            if (options != null && !pattern.asRegularExpression().getOptions().isEmpty()) {
                throw new MongoCmdException(51075, "options set in both $regex and $options");
            }
            String opts = pattern.asRegularExpression().getOptions() + (options == null ? "" : options);
            return leaf(path, regexMatcher(pattern.asRegularExpression().getPattern(), opts));
        }
        if (!pattern.isString()) {
            throw MongoCmdException.badValue("$regex has to be a string");
        }
        return leaf(path, regexMatcher(pattern.asString().getValue(), options == null ? "" : options));
    }

    static Predicate<BsonValue> regexMatcher(String pattern, String options) {
        Pattern p = compileRegex(pattern, options);
        return v -> (v.isString() || v.isSymbol()) && p.matcher(BsonCmp.str(v)).find()
                || v.isRegularExpression() && v.asRegularExpression().getPattern().equals(pattern)
                        && v.asRegularExpression().getOptions().equals(options == null ? "" : options);
    }

    static Pattern compileRegex(String pattern, String options) {
        int flags = 0;
        for (char c : (options == null ? "" : options).toCharArray()) {
            switch (c) {
                case 'i' -> flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
                case 'm' -> flags |= Pattern.MULTILINE;
                case 's' -> flags |= Pattern.DOTALL;
                case 'x' -> flags |= Pattern.COMMENTS;
                case 'u' -> { }
                default -> throw new MongoCmdException(51108, "invalid flag in regex options: " + c);
            }
        }
        try {
            return Pattern.compile(pattern, flags);
        } catch (java.util.regex.PatternSyntaxException e) {
            throw new MongoCmdException(51091, "Regular expression is invalid: " + e.getDescription());
        }
    }

    // ------------------------------------------------------------------ leaf helpers

    /** Doc matches if any resolved candidate (or element of an array candidate) satisfies the test. */
    private Pred leaf(String path, Predicate<BsonValue> test) {
        final boolean flatNow = flat;
        return d -> {
            for (Cand cd : resolveX(d, path)) {
                BsonValue c = cd.v;
                if (c == MISSING) {
                    if (test.test(MISSING)) {
                        return true;
                    }
                    continue;
                }
                if (test.test(c)) {
                    return true;
                }
                if (c.isArray() && cd.expand && !flatNow) {
                    for (BsonValue e : c.asArray()) {
                        if (test.test(e)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        };
    }

    /** Candidate-level (no array element expansion): matches when any candidate satisfies. */
    private static Pred whole(String path, Predicate<BsonValue> test) {
        return d -> {
            for (BsonValue c : resolve(d, path)) {
                if (test.test(c)) {
                    return true;
                }
            }
            return false;
        };
    }

    private Predicate<BsonValue> eqTest(BsonValue operand) {
        if (operand.isNull() || BsonCmp.isUndef(operand)) {
            return v -> v == MISSING || v.isNull() || BsonCmp.isUndef(v);
        }
        if (operand.isRegularExpression()) {
            return regexMatcher(operand.asRegularExpression().getPattern(), operand.asRegularExpression().getOptions());
        }
        return v -> v != MISSING && BsonCmp.compare(v, operand, coll) == 0;
    }

    private static boolean sameBracket(BsonValue a, BsonValue b) {
        if (a == MISSING) {
            return false;
        }
        int y = BsonCmp.typeOrder(b);
        return BsonCmp.typeOrder(a) == y || y == -1 || y == 127;
    }

    private Predicate<BsonValue> relTest(String op, BsonValue operand) {
        if (operand.isNull() || BsonCmp.isUndef(operand)) {
            boolean orEqual = op.equals("$gte") || op.equals("$lte");
            return v -> orEqual && (v == MISSING || v.isNull() || BsonCmp.isUndef(v));
        }
        return v -> {
            if (!sameBracket(v, operand)) {
                return false;
            }
            boolean vn = BsonCmp.isNumber(v) && isNan(v);
            boolean on = BsonCmp.isNumber(operand) && isNan(operand);
            if (vn || on) {
                return vn && on && (op.equals("$gte") || op.equals("$lte"));
            }
            int c = BsonCmp.compare(v, operand, coll);
            return switch (op) {
                case "$gt" -> c > 0;
                case "$gte" -> c >= 0;
                case "$lt" -> c < 0;
                default -> c <= 0;
            };
        };
    }

    private static boolean isNan(BsonValue v) {
        return v.isDouble() && Double.isNaN(v.asDouble().getValue())
                || v.isDecimal128() && v.asDecimal128().getValue().isNaN();
    }

    private Pred operator(String path, String op, BsonValue operand, BsonDocument opdoc) {
        switch (op) {
            case "$eq": {
                if (operand.isDocument() && hasOperators(operand.asDocument()) && opdoc == null) {
                    throw MongoCmdException.badValue("unknown operator: " + operand.asDocument().getFirstKey());
                }
                return leaf(path, eqTest(operand));
            }
            case "$ne": {
                if (operand.isRegularExpression()) {
                    throw MongoCmdException.badValue("Can't have regex as arg to $ne.");
                }
                Pred eq = leaf(path, eqTest(operand));
                return d -> !eq.test(d);
            }
            case "$gt": case "$gte": case "$lt": case "$lte":
                return leaf(path, relTest(op, operand));
            case "$in": case "$nin": {
                if (!operand.isArray()) {
                    throw MongoCmdException.badValue(op + " needs an array");
                }
                List<Predicate<BsonValue>> alts = new ArrayList<>();
                for (BsonValue v : operand.asArray()) {
                    if (v.isDocument() && hasOperators(v.asDocument())) {
                        throw MongoCmdException.badValue("cannot nest $ under " + op);
                    }
                    alts.add(eqTest(v));
                }
                Pred in = leaf(path, v -> {
                    for (Predicate<BsonValue> a : alts) {
                        if (a.test(v)) {
                            return true;
                        }
                    }
                    return false;
                });
                return op.equals("$in") ? in : d -> !in.test(d);
            }
            case "$not": {
                Pred inner;
                if (operand.isRegularExpression()) {
                    inner = leaf(path, regexMatcher(operand.asRegularExpression().getPattern(),
                            operand.asRegularExpression().getOptions()));
                } else if (operand.isDocument() && hasOperators(operand.asDocument())) {
                    inner = fieldClause(path, operand);
                } else {
                    throw MongoCmdException.badValue("$not needs a regex or a document");
                }
                return d -> !inner.test(d);
            }
            case "$exists": {
                boolean want = truthy(operand);
                Pred exists = whole(path, v -> v != MISSING);
                return want ? exists : d -> !exists.test(d);
            }
            case "$type": {
                List<Predicate<BsonValue>> types = typeTests(operand);
                return d -> {
                    for (BsonValue c : resolve(d, path)) {
                        if (c == MISSING) {
                            continue;
                        }
                        if (anyType(types, c)) {
                            return true;
                        }
                        if (c.isArray()) {
                            for (BsonValue e : c.asArray()) {
                                if (anyType(types, e)) {
                                    return true;
                                }
                            }
                        }
                    }
                    return false;
                };
            }
            case "$size": {
                if (!BsonCmp.isNumber(operand)) {
                    throw MongoCmdException.badValue("$size needs a number");
                }
                double dv = operand.isDouble() ? operand.asDouble().getValue()
                        : BsonCmp.toBigDecimal(operand).doubleValue();
                if (dv != Math.floor(dv) || Double.isInfinite(dv)) {
                    throw MongoCmdException.badValue("$size must be a whole number");
                }
                if (dv < 0) {
                    throw MongoCmdException.badValue("$size may not be negative");
                }
                long n = (long) dv;
                return whole(path, v -> v.isArray() && v.asArray().size() == n);
            }
            case "$mod": {
                if (!operand.isArray()) {
                    throw MongoCmdException.badValue("malformed mod, needs to be an array");
                }
                BsonArray a = operand.asArray();
                if (a.size() < 2) {
                    throw MongoCmdException.badValue("malformed mod, not enough elements");
                }
                if (a.size() > 2) {
                    throw MongoCmdException.badValue("malformed mod, too many elements");
                }
                if (!BsonCmp.isNumber(a.get(0)) || !BsonCmp.isNumber(a.get(1))) {
                    throw MongoCmdException.badValue(BsonCmp.isNumber(a.get(0))
                            ? "malformed mod, remainder not a number" : "malformed mod, divisor not a number");
                }
                long div = MongoNum.truncLong(a.get(0));
                long rem = MongoNum.truncLong(a.get(1));
                if (div == 0) {
                    throw MongoCmdException.badValue("divisor cannot be 0");
                }
                return leaf(path, v -> {
                    if (!BsonCmp.isNumber(v) || isNan(v) || MongoNum.isInfinite(v)) {
                        return false;
                    }
                    return MongoNum.truncLong(v) % div == rem;
                });
            }
            case "$all":
                return allOperator(path, operand);
            case "$elemMatch":
                return elemMatch(path, operand);
            case "$regex":
                return regexOperator(path, operand, opdoc != null && opdoc.containsKey("$options")
                        ? optString(opdoc.get("$options")) : null);
            case "$bitsAllSet": case "$bitsAnySet": case "$bitsAllClear": case "$bitsAnyClear":
                return bits(path, op, operand);
            case "$near": case "$nearSphere": case "$geoWithin": case "$geoIntersects": case "$box": case "$center":
            case "$centerSphere": case "$geometry": case "$maxDistance": case "$minDistance": case "$polygon":
                throw new MongoCmdException(2, "geospatial operator " + op + " is not supported by Warp");
            case "$jsonSchema":
                throw MongoCmdException.badValue("$jsonSchema is only allowed at the top-level of a filter");
            default:
                throw MongoCmdException.badValue("unknown operator: " + op);
        }
    }

    private static boolean truthy(BsonValue v) {
        if (v.isBoolean()) {
            return v.asBoolean().getValue();
        }
        if (v.isNull() || BsonCmp.isUndef(v)) {
            return false;
        }
        if (BsonCmp.isNumber(v)) {
            return BsonCmp.compareNumbers(v, new BsonInt32(0)) != 0;
        }
        return true;
    }

    private static boolean anyType(List<Predicate<BsonValue>> tests, BsonValue v) {
        for (Predicate<BsonValue> t : tests) {
            if (t.test(v)) {
                return true;
            }
        }
        return false;
    }

    static int typeNumber(String alias) {
        return switch (alias) {
            case "double" -> 1;
            case "string" -> 2;
            case "object" -> 3;
            case "array" -> 4;
            case "binData" -> 5;
            case "undefined" -> 6;
            case "objectId" -> 7;
            case "bool" -> 8;
            case "date" -> 9;
            case "null" -> 10;
            case "regex" -> 11;
            case "dbPointer" -> 12;
            case "javascript" -> 13;
            case "symbol" -> 14;
            case "javascriptWithScope" -> 15;
            case "int" -> 16;
            case "timestamp" -> 17;
            case "long" -> 18;
            case "decimal" -> 19;
            case "minKey" -> -1;
            case "maxKey" -> 127;
            case "number" -> 0;
            default -> Integer.MIN_VALUE;
        };
    }

    static int typeNumberOf(BsonValue v) {
        return switch (v.getBsonType()) {
            case DOUBLE -> 1;
            case STRING -> 2;
            case DOCUMENT -> 3;
            case ARRAY -> 4;
            case BINARY -> 5;
            case UNDEFINED -> 6;
            case OBJECT_ID -> 7;
            case BOOLEAN -> 8;
            case DATE_TIME -> 9;
            case NULL -> 10;
            case REGULAR_EXPRESSION -> 11;
            case DB_POINTER -> 12;
            case JAVASCRIPT -> 13;
            case SYMBOL -> 14;
            case JAVASCRIPT_WITH_SCOPE -> 15;
            case INT32 -> 16;
            case TIMESTAMP -> 17;
            case INT64 -> 18;
            case DECIMAL128 -> 19;
            case MIN_KEY -> -1;
            case MAX_KEY -> 127;
            default -> 0;
        };
    }

    static String typeName(BsonValue v) {
        return switch (v.getBsonType()) {
            case DOUBLE -> "double";
            case STRING -> "string";
            case DOCUMENT -> "object";
            case ARRAY -> "array";
            case BINARY -> "binData";
            case UNDEFINED -> "undefined";
            case OBJECT_ID -> "objectId";
            case BOOLEAN -> "bool";
            case DATE_TIME -> "date";
            case NULL -> "null";
            case REGULAR_EXPRESSION -> "regex";
            case DB_POINTER -> "dbPointer";
            case JAVASCRIPT -> "javascript";
            case SYMBOL -> "symbol";
            case JAVASCRIPT_WITH_SCOPE -> "javascriptWithScope";
            case INT32 -> "int";
            case TIMESTAMP -> "timestamp";
            case INT64 -> "long";
            case DECIMAL128 -> "decimal";
            case MIN_KEY -> "minKey";
            case MAX_KEY -> "maxKey";
            default -> "unknown";
        };
    }

    private List<Predicate<BsonValue>> typeTests(BsonValue operand) {
        List<BsonValue> specs = operand.isArray() ? operand.asArray().getValues() : List.of(operand);
        List<Predicate<BsonValue>> out = new ArrayList<>();
        for (BsonValue s : specs) {
            int n;
            if (s.isString()) {
                n = typeNumber(s.asString().getValue());
                if (n == Integer.MIN_VALUE) {
                    throw MongoCmdException.badValue("Unknown type name alias: " + s.asString().getValue());
                }
            } else if (BsonCmp.isNumber(s)) {
                double dv = BsonCmp.toBigDecimal(s).doubleValue();
                n = (int) dv;
                if (dv != n || !(n == -1 || n == 127 || n >= 1 && n <= 19 && n != 0)) {
                    throw MongoCmdException.badValue("Invalid numerical type code: " + BsonCmp.toBigDecimal(s).toPlainString());
                }
            } else {
                throw MongoCmdException.typeMismatch("type must be represented as a number or a string");
            }
            final int t = n;
            out.add(v -> v != MISSING && (t == 0 ? BsonCmp.isNumber(v) : typeNumberOf(v) == t));
        }
        return out;
    }

    private Pred allOperator(String path, BsonValue operand) {
        if (!operand.isArray()) {
            throw MongoCmdException.badValue("$all needs an array");
        }
        BsonArray a = operand.asArray();
        if (a.isEmpty()) {
            return d -> false;
        }
        List<Pred> each = new ArrayList<>();
        for (BsonValue m : a) {
            if (m.isDocument() && m.asDocument().size() == 1 && m.asDocument().getFirstKey().equals("$elemMatch")) {
                each.add(elemMatch(path, m.asDocument().get("$elemMatch")));
            } else if (m.isDocument() && hasOperators(m.asDocument())) {
                throw MongoCmdException.badValue("no $ expressions in $all");
            } else {
                each.add(leaf(path, eqTest(m)));
            }
        }
        // $all with a single array operand equal to the field also matches ({a:{$all:[[1,2]]}} on a:[1,2])
        return d -> {
            for (Pred p : each) {
                if (!p.test(d)) {
                    return false;
                }
            }
            return true;
        };
    }

    private Pred elemMatch(String path, BsonValue operand) {
        if (!operand.isDocument()) {
            throw MongoCmdException.badValue("$elemMatch needs an Object");
        }
        BsonDocument cond = operand.asDocument();
        if (cond.isEmpty()) {
            return whole(path, v -> {
                if (!v.isArray()) {
                    return false;
                }
                for (BsonValue e : v.asArray()) {
                    if (e.isDocument() || e.isArray()) {
                        return true;
                    }
                }
                return false;
            });
        }
        boolean valueOps = hasOperators(cond);
        if (valueOps) {
            for (String k : cond.keySet()) {
                if (k.equals("$and") || k.equals("$or") || k.equals("$nor") || k.equals("$expr") || k.equals("$jsonSchema")
                        || k.equals("$where") || k.equals("$comment")) {
                    valueOps = false;
                    break;
                }
            }
        }
        Predicate<BsonValue> elemTest;
        if (valueOps) {
            MongoMatcher fm = new MongoMatcher(coll, vars);
            fm.flat = true;
            Pred p = fm.fieldClause("v", cond);
            elemTest = e -> p.test(new BsonDocument("v", e));
        } else {
            Pred p = top(cond);
            elemTest = e -> e.isDocument() && p.test(e.asDocument());
        }
        Predicate<BsonValue> t = elemTest;
        return whole(path, v -> {
            if (!v.isArray()) {
                return false;
            }
            for (BsonValue e : v.asArray()) {
                if (t.test(e)) {
                    return true;
                }
            }
            return false;
        });
    }

    private Pred bits(String path, String op, BsonValue operand) {
        long mask;
        if (operand.isArray()) {
            mask = 0;
            for (BsonValue p : operand.asArray()) {
                if (!BsonCmp.isNumber(p)) {
                    throw MongoCmdException.badValue(op + " bit positions must be numeric");
                }
                if (MongoNum.truncLong(p) < 0) {
                    throw MongoCmdException.badValue("Failed to parse bit position. Expected a non-negative number in: 0: " + MongoNum.truncLong(p));
                }
                mask |= 1L << MongoNum.truncLong(p);
            }
        } else if (BsonCmp.isNumber(operand)) {
            mask = MongoNum.truncLong(operand);
        } else if (operand.isBinary()) {
            mask = 0;
            byte[] data = operand.asBinary().getData();
            for (int i = 0; i < data.length && i < 8; i++) {
                mask |= (long) (data[i] & 0xFF) << (8 * i);
            }
        } else {
            throw MongoCmdException.badValue(op + " takes an Array, a number, or a BinData but received: " + operand);
        }
        long m = mask;
        return leaf(path, v -> {
            long x;
            if (BsonCmp.isNumber(v)) {
                if (isNan(v) || MongoNum.isInfinite(v)) {
                    return false;
                }
                if (v.isDouble() && v.asDouble().getValue() != Math.floor(v.asDouble().getValue())
                        || v.isDecimal128() && BsonCmp.toBigDecimal(v).stripTrailingZeros().scale() > 0) {
                    return false;
                }
                x = MongoNum.truncLong(v);
            } else if (v.isBinary()) {
                x = 0;
                byte[] data = v.asBinary().getData();
                for (int i = 0; i < data.length && i < 8; i++) {
                    x |= (long) (data[i] & 0xFF) << (8 * i);
                }
            } else {
                return false;
            }
            return switch (op) {
                case "$bitsAllSet" -> (x & m) == m;
                case "$bitsAnySet" -> (x & m) != 0;
                case "$bitsAllClear" -> (x & m) == 0;
                default -> (~x & m) != 0;
            };
        });
    }

    /** Exact scalar equality _id filter helper used by the store to route/prefilter: returns the equality value or null. */
    static BsonValue idEquality(BsonDocument filter) {
        BsonValue v = filter.get("_id");
        if (v == null) {
            return null;
        }
        if (v.isDocument() && hasOperators(v.asDocument())) {
            BsonDocument ops = v.asDocument();
            if (ops.size() == 1 && ops.containsKey("$eq")) {
                v = ops.get("$eq");
            } else {
                return null;
            }
        }
        if (v.isRegularExpression() || v.isNull() || BsonCmp.isUndef(v) || v.isArray()) {
            return null;
        }
        return v;
    }

    /** {@code _id: {$in: [...]}} values when that is the _id condition, else null. */
    static List<BsonValue> idIn(BsonDocument filter) {
        BsonValue v = filter.get("_id");
        if (v != null && v.isDocument() && v.asDocument().size() == 1 && v.asDocument().containsKey("$in")
                && v.asDocument().get("$in").isArray()) {
            for (BsonValue x : v.asDocument().getArray("$in")) {
                if (x.isRegularExpression() || x.isNull() || BsonCmp.isUndef(x) || x.isArray()
                        || x.isDocument() && hasOperators(x.asDocument())) {
                    return null;
                }
            }
            return v.asDocument().getArray("$in").getValues();
        }
        return null;
    }
}
