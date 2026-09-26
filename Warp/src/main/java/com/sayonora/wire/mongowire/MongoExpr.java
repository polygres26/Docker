package com.sayonora.wire.mongowire;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/** Aggregation expression language: parse once into {@link Expr} trees, evaluate against a {@link Scope}. */
final class MongoExpr {

    static final BsonValue MISSING = MongoMatcher.MISSING;

    private MongoExpr() {
    }

    interface Expr {
        BsonValue eval(Scope s);
    }

    /** Evaluation environment: $$CURRENT, $$ROOT, user variables and the pipeline-wide clock. */
    static final class Scope {
        final BsonValue current;
        final BsonValue root;
        final Map<String, BsonValue> vars;
        final BsonCmp.Collation coll;
        final long now;

        Scope(BsonValue current, BsonValue root, Map<String, BsonValue> vars, BsonCmp.Collation coll, long now) {
            this.current = current;
            this.root = root;
            this.vars = vars;
            this.coll = coll;
            this.now = now;
        }

        static Scope root(BsonDocument d, BsonCmp.Collation coll) {
            return new Scope(d, d, Map.of(), coll, System.currentTimeMillis());
        }

        static Scope root(BsonDocument d, BsonCmp.Collation coll, Map<String, BsonValue> vars) {
            return new Scope(d, d, vars, coll, System.currentTimeMillis());
        }

        Scope withCurrent(BsonValue c) {
            return new Scope(c, root, vars, coll, now);
        }

        Scope with(String name, BsonValue v) {
            Map<String, BsonValue> m = new HashMap<>(vars);
            m.put(name, v);
            return new Scope(current, root, m, coll, now);
        }
    }

    private static BsonValue domainCheck(BsonValue in, String n, BsonValue out) {
        if (out.isDouble() && Double.isNaN(out.asDouble().getValue()) && BsonCmp.isNumber(in) && !MongoNum.isNaN(in)) {
            String range = switch (n) {
                case "asin", "acos" -> "[-1, 1]";
                case "acosh" -> "[1, inf]";
                case "atanh" -> "[-1, 1]";
                default -> null;
            };
            if (range != null) {
                throw new MongoCmdException(50989, "cannot apply $" + n + " to " + toStringValue(in) + ", value must be in " + range);
            }
        }
        return out;
    }

    static void checkVarName(String n) {
        if (n.isEmpty()) {
            throw new MongoCmdException(9, "empty variable names are not allowed");
        }
        if (!Character.isLowerCase(n.charAt(0)) && n.charAt(0) != '_' || n.charAt(0) > 127 && false) {
            throw new MongoCmdException(16867, "'" + n + "' starts with an invalid character for a user variable name");
        }
    }

    static boolean nullish(BsonValue v) {
        return v == MISSING || v.isNull() || BsonCmp.isUndef(v);
    }

    static boolean truthy(BsonValue v) {
        if (v == MISSING || v.isNull() || BsonCmp.isUndef(v)) {
            return false;
        }
        if (v.isBoolean()) {
            return v.asBoolean().getValue();
        }
        if (BsonCmp.isNumber(v)) {
            return BsonCmp.compareNumbers(v, new BsonInt32(0)) != 0;
        }
        return true;
    }

    // ---------------------------------------------------------------- parsing

    interface Fn {
        BsonValue apply(BsonValue[] a);
    }

    private record Op(int min, int max, Fn fn) {
    }

    private static final Map<String, Op> OPS = new HashMap<>();
    private static final Map<String, BiFunction<BsonValue, String, Expr>> SPECIAL = new HashMap<>();

    static Expr parse(BsonValue spec) {
        if (spec.isString()) {
            String s = spec.asString().getValue();
            if (s.startsWith("$$")) {
                return varExpr(s.substring(2));
            }
            if (s.startsWith("$")) {
                if (s.length() == 1) {
                    throw new MongoCmdException(16872, "'$' by itself is not a valid FieldPath");
                }
                String[] parts = s.substring(1).split("\\.", -1);
                return sc -> fieldPath(sc.current, parts, 0);
            }
            return sc -> spec;
        }
        if (spec.isArray()) {
            List<Expr> items = new ArrayList<>();
            for (BsonValue v : spec.asArray()) {
                items.add(parse(v));
            }
            return sc -> {
                BsonArray out = new BsonArray();
                for (Expr e : items) {
                    BsonValue v = e.eval(sc);
                    out.add(v == MISSING ? BsonNull.VALUE : v);
                }
                return out;
            };
        }
        if (spec.isDocument()) {
            BsonDocument d = spec.asDocument();
            if (!d.isEmpty() && d.getFirstKey().startsWith("$")) {
                if (d.size() != 1) {
                    throw new MongoCmdException(15983, "An object representing an expression must have exactly one field: "
                            + d.toJson());
                }
                return operator(d.getFirstKey(), d.get(d.getFirstKey()));
            }
            LinkedHashMap<String, Expr> fields = new LinkedHashMap<>();
            for (Map.Entry<String, BsonValue> e : d.entrySet()) {
                if (e.getKey().startsWith("$")) {
                    throw new MongoCmdException(15983, "this object is already an operator expression, and can't be used as a document expression (at '"
                            + e.getKey() + "')");
                }
                fields.put(e.getKey(), parse(e.getValue()));
            }
            return sc -> {
                BsonDocument out = new BsonDocument();
                for (Map.Entry<String, Expr> e : fields.entrySet()) {
                    BsonValue v = e.getValue().eval(sc);
                    if (v != MISSING) {
                        out.put(e.getKey(), v);
                    }
                }
                return out;
            };
        }
        return sc -> spec;
    }

    private static Expr varExpr(String name) {
        int dot = name.indexOf('.');
        String var = dot < 0 ? name : name.substring(0, dot);
        String[] rest = dot < 0 ? null : name.substring(dot + 1).split("\\.", -1);
        if (var.isEmpty()) {
            throw new MongoCmdException(9, "empty variable names are not allowed");
        }
        if (!(Character.isLowerCase(var.charAt(0)) || Character.isUpperCase(var.charAt(0)))) {
            throw new MongoCmdException(16867, "'" + name + "' starts with an invalid character for a user variable name");
        }
        return sc -> {
            BsonValue base;
            switch (var) {
                case "ROOT" -> base = sc.root;
                case "CURRENT" -> base = sc.current;
                case "NOW" -> base = new BsonDateTime(sc.now);
                case "REMOVE" -> base = MISSING;
                case "CLUSTER_TIME" -> base = new BsonTimestamp((int) (sc.now / 1000), 1);
                case "DESCEND", "PRUNE", "KEEP" -> base = new BsonString("$$" + var);
                default -> {
                    base = sc.vars.get(var);
                    if (base == null) {
                        throw new MongoCmdException(17276, "Use of undefined variable: " + var);
                    }
                }
            }
            return rest == null ? base : fieldPath(base, rest, 0);
        };
    }

    static BsonValue fieldPath(BsonValue v, String[] parts, int i) {
        if (i == parts.length) {
            return v;
        }
        if (v == MISSING) {
            return MISSING;
        }
        if (v.isDocument()) {
            BsonValue c = v.asDocument().get(parts[i]);
            return c == null ? MISSING : fieldPath(c, parts, i + 1);
        }
        if (v.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : v.asArray()) {
                BsonValue r = e.isDocument() || e.isArray() ? fieldPath(e, parts, i) : MISSING;
                if (r != MISSING) {
                    out.add(r);
                }
            }
            return out;
        }
        return MISSING;
    }

    private static List<Expr> argExprs(BsonValue args) {
        List<Expr> out = new ArrayList<>();
        if (args.isArray()) {
            for (BsonValue v : args.asArray()) {
                out.add(parse(v));
            }
        } else {
            out.add(parse(args));
        }
        return out;
    }

    private static Expr operator(String name, BsonValue args) {
        BiFunction<BsonValue, String, Expr> special = SPECIAL.get(name);
        if (special != null) {
            return special.apply(args, name);
        }
        Op op = OPS.get(name);
        if (op == null) {
            throw new MongoCmdException(168, "Unrecognized expression '" + name + "'");
        }
        List<Expr> exprs = argExprs(args);
        int n = exprs.size();
        if (n < op.min || n > op.max) {
            String want = op.min == op.max ? "exactly " + op.min + " argument" + (op.min == 1 ? "" : "s")
                    : op.max == Integer.MAX_VALUE ? "at least " + op.min + " argument" + (op.min == 1 ? "" : "s")
                            : "at least " + op.min + " and at most " + op.max + " arguments";
            throw new MongoCmdException(op.min == op.max ? 16020 : 28667, "Expression " + name + " takes " + want + ". " + n
                    + " were passed in.");
        }
        Expr[] es = exprs.toArray(new Expr[0]);
        Fn fn = op.fn;
        return sc -> {
            BsonValue[] vals = new BsonValue[es.length];
            for (int i = 0; i < es.length; i++) {
                vals[i] = es[i].eval(sc);
            }
            return fn.apply(vals);
        };
    }

    private static void reg(String name, int min, int max, Fn fn) {
        OPS.put(name, new Op(min, max, fn));
    }

    // ---------------------------------------------------------------- helpers

    private static BsonValue nul() {
        return BsonNull.VALUE;
    }

    private static boolean anyNullish(BsonValue[] a) {
        for (BsonValue v : a) {
            if (nullish(v)) {
                return true;
            }
        }
        return false;
    }

    private static BsonValue requireNumber(BsonValue v, String op, int code) {
        if (!BsonCmp.isNumber(v)) {
            throw new MongoCmdException(code, "$" + op + " only supports numeric types, not " + MongoMatcher.typeName(v));
        }
        return v;
    }

    private static BsonValue cmpVal(BsonValue v) {
        return v == MISSING ? new org.bson.BsonUndefined() : v;
    }

    private static BsonValue bool(boolean b) {
        return BsonBoolean.valueOf(b);
    }

    private static BsonArray arr(BsonValue v, String op) {
        if (!v.isArray()) {
            throw new MongoCmdException(28651, "$" + op + " requires an array argument but found: " + MongoMatcher.typeName(v));
        }
        return v.asArray();
    }

    static String typeOf(BsonValue v) {
        return v == MISSING ? "missing" : MongoMatcher.typeName(v);
    }

    static BsonDocument doc(Object... kv) {
        BsonDocument d = new BsonDocument();
        for (int i = 0; i < kv.length; i += 2) {
            d.put((String) kv[i], (BsonValue) kv[i + 1]);
        }
        return d;
    }

    private static BsonValue roundTo(BsonValue v, int place, boolean trunc) {
        if (v.isInt32() || v.isInt64()) {
            if (place >= 0) {
                return v;
            }
            BigDecimal b = BsonCmp.toBigDecimal(v).setScale(place, trunc ? RoundingMode.DOWN : RoundingMode.HALF_EVEN);
            return v.isInt32() ? new BsonInt32(b.intValue()) : new BsonInt64(b.longValue());
        }
        if (v.isDouble()) {
            double d = v.asDouble().getValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return v;
            }
            BigDecimal b = new BigDecimal(Double.toString(d)).setScale(place, trunc ? RoundingMode.DOWN : RoundingMode.HALF_EVEN);
            return new BsonDouble(b.doubleValue());
        }
        Decimal128 dd = v.asDecimal128().getValue();
        if (dd.isNaN() || dd.isInfinite()) {
            return v;
        }
        return new BsonDecimal128(new Decimal128(BsonCmp.toBigDecimal(v).setScale(place,
                trunc ? RoundingMode.DOWN : RoundingMode.HALF_EVEN)));
    }

    private static BsonValue math1(BsonValue v, String op, java.util.function.DoubleUnaryOperator f) {
        if (nullish(v)) {
            return nul();
        }
        requireNumber(v, op, 28765);
        if (v.isDecimal128()) {
            double r = f.applyAsDouble(MongoNum.toDouble(v));
            return new BsonDecimal128(MongoNum.toDecimal(new BsonDouble(r)));
        }
        return new BsonDouble(f.applyAsDouble(MongoNum.toDouble(v)));
    }

    static String doubleToString(double d) {
        if (Double.isNaN(d)) {
            return "NaN";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "Infinity" : "-Infinity";
        }
        if (d == 0) {
            return 1 / d < 0 ? "-0" : "0";
        }
        String s = Double.toString(d);
        if (s.contains("E")) {
            String mant = s.substring(0, s.indexOf('E'));
            int exp = Integer.parseInt(s.substring(s.indexOf('E') + 1));
            if (mant.endsWith(".0")) {
                mant = mant.substring(0, mant.length() - 2);
            }
            if (exp >= -5 && exp < 15) {
                s = new BigDecimal(s).toPlainString();
                return s.contains(".") ? s.replaceAll("0+$", "").replaceAll("\\.$", "") : s;
            }
            return mant + "e" + (exp < 0 ? "-" : "+") + (Math.abs(exp) < 10 ? "0" : "") + Math.abs(exp);
        }
        if (s.endsWith(".0")) {
            s = s.substring(0, s.length() - 2);
        }
        return s;
    }

    static String toStringValue(BsonValue v) {
        switch (v.getBsonType()) {
            case STRING: return v.asString().getValue();
            case INT32: return Integer.toString(v.asInt32().getValue());
            case INT64: return Long.toString(v.asInt64().getValue());
            case DOUBLE: return doubleToString(v.asDouble().getValue());
            case DECIMAL128: return v.asDecimal128().getValue().toString();
            case BOOLEAN: return v.asBoolean().getValue() ? "true" : "false";
            case OBJECT_ID: return v.asObjectId().getValue().toHexString();
            case DATE_TIME: return MongoDates.isoString(v.asDateTime().getValue(), java.time.ZoneOffset.UTC);
            case SYMBOL: return v.asSymbol().getSymbol();
            case JAVASCRIPT: return v.asJavaScript().getCode();
            case BINARY:
                if (v.asBinary().getType() == 4 && v.asBinary().getData().length == 16) {
                    return v.asBinary().asUuid().toString();
                }
                throw new MongoCmdException(241, "Unsupported conversion from binData to string in $convert with no onError value");
            default:
                throw new MongoCmdException(241, "Unsupported conversion from " + MongoMatcher.typeName(v)
                        + " to string in $convert with no onError value");
        }
    }

    private static BsonValue parseNumber(String s, String target) {
        String t = s;
        try {
            switch (target) {
                case "int": {
                    if (t.startsWith("0x") || t.startsWith("0X")) {
                        throw new NumberFormatException();
                    }
                    long l = Long.parseLong(t);
                    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                        throw new MongoCmdException(241, "Overflow while parsing string to int");
                    }
                    return new BsonInt32((int) l);
                }
                case "long":
                    return new BsonInt64(Long.parseLong(t));
                case "double": {
                    switch (t) {
                        case "NaN": case "-NaN": return new BsonDouble(Double.NaN);
                        case "Infinity": case "inf": case "+inf": case "+Infinity": return new BsonDouble(Double.POSITIVE_INFINITY);
                        case "-Infinity": case "-inf": return new BsonDouble(Double.NEGATIVE_INFINITY);
                        default: break;
                    }
                    if (t.isEmpty() || Character.isWhitespace(t.charAt(0)) || Character.isWhitespace(t.charAt(t.length() - 1))
                            || t.endsWith("d") || t.endsWith("D") || t.endsWith("f") || t.endsWith("F")
                            || t.startsWith("0x") || t.startsWith("0X")) {
                        throw new NumberFormatException();
                    }
                    return new BsonDouble(Double.parseDouble(t));
                }
                default:
                    return new BsonDecimal128(Decimal128.parse(t));
            }
        } catch (NumberFormatException e) {
            throw new MongoCmdException(241, "Failed to parse number '" + s + "' in $convert with no onError value: Did not consume the whole string.");
        }
    }

    static BsonValue convert(BsonValue v, String to, BsonValue subtype) {
        if (nullish(v)) {
            return nul();
        }
        switch (to) {
            case "string": return new BsonString(toStringValue(v));
            case "bool":
                return bool(truthy(v) || v.isString() || v.isDocument() || v.isArray());
            case "int": case "long": case "double": case "decimal": {
                if (v.isString()) {
                    return parseNumber(v.asString().getValue(), to);
                }
                if (v.isBoolean()) {
                    boolean b = v.asBoolean().getValue();
                    return to.equals("int") ? new BsonInt32(b ? 1 : 0) : to.equals("long") ? new BsonInt64(b ? 1 : 0)
                            : to.equals("double") ? new BsonDouble(b ? 1 : 0) : new BsonDecimal128(new Decimal128(b ? 1 : 0));
                }
                if (v.isDateTime() && !to.equals("int")) {
                    long ms = v.asDateTime().getValue();
                    return to.equals("long") ? new BsonInt64(ms) : to.equals("double") ? new BsonDouble(ms)
                            : new BsonDecimal128(new Decimal128(ms));
                }
                if (BsonCmp.isNumber(v)) {
                    return numConvert(v, to);
                }
                throw new MongoCmdException(241, "Unsupported conversion from " + MongoMatcher.typeName(v) + " to " + to
                        + " in $convert with no onError value");
            }
            case "date": {
                if (v.isDateTime()) {
                    return v;
                }
                if (v.isTimestamp()) {
                    return new BsonDateTime(v.asTimestamp().getTime() * 1000L);
                }
                if (v.isObjectId()) {
                    return new BsonDateTime(v.asObjectId().getValue().getDate().getTime());
                }
                if (BsonCmp.isNumber(v)) {
                    if (v.isInt32()) {
                        throw new MongoCmdException(241, "Unsupported conversion from int to date in $convert with no onError value");
                    }
                    if (MongoNum.isNaN(v) || MongoNum.isInfinite(v)) {
                        throw new MongoCmdException(241, "Attempt to convert NaN/Infinity value to integral type");
                    }
                    return new BsonDateTime(v.isDouble() ? (long) Math.floor(v.asDouble().getValue())
                            : BsonCmp.toBigDecimal(v).setScale(0, RoundingMode.FLOOR).longValue());
                }
                if (v.isString()) {
                    return new BsonDateTime(MongoDates.parseDate(v.asString().getValue(), null, null));
                }
                throw new MongoCmdException(241, "Unsupported conversion from " + MongoMatcher.typeName(v) + " to date in $convert with no onError value");
            }
            case "objectId": {
                if (v.isObjectId()) {
                    return v;
                }
                if (v.isString()) {
                    String s = v.asString().getValue();
                    if (!ObjectId.isValid(s)) {
                        throw new MongoCmdException(241, "Failed to parse objectId '" + s + "' in $convert with no onError value: Invalid string length for parsing to OID, expected 24 but found " + s.length());
                    }
                    return new BsonObjectId(new ObjectId(s));
                }
                throw new MongoCmdException(241, "Unsupported conversion from " + MongoMatcher.typeName(v) + " to objectId in $convert with no onError value");
            }
            case "binData": {
                if (v.isBinary()) {
                    return v;
                }
                if (v.isString()) {
                    return new BsonBinary(subtype != null && !nullish(subtype) ? (byte) MongoNum.truncLong(subtype) : 0,
                            v.asString().getValue().getBytes(StandardCharsets.UTF_8));
                }
                throw new MongoCmdException(241, "Unsupported conversion from " + MongoMatcher.typeName(v) + " to binData in $convert with no onError value");
            }
            default:
                throw new MongoCmdException(2, "Unknown type name: " + to);
        }
    }

    private static BsonValue numConvert(BsonValue v, String to) {
        switch (to) {
            case "double": return new BsonDouble(MongoNum.toDouble(v));
            case "decimal": return new BsonDecimal128(MongoNum.toDecimal(v));
            default: {
                if (MongoNum.isNaN(v) || MongoNum.isInfinite(v)) {
                    throw new MongoCmdException(241, "Attempt to convert " + (MongoNum.isNaN(v) ? "NaN" : "infinity")
                            + " value to integral type");
                }
                BigDecimal b = BsonCmp.toBigDecimal(v).setScale(0, RoundingMode.DOWN);
                if (to.equals("int")) {
                    if (b.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0 || b.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                        throw new MongoCmdException(241, "Conversion would overflow target type in $convert with no onError value: "
                                + toStringValue(v));
                    }
                    return new BsonInt32(b.intValue());
                }
                if (b.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0 || b.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                    throw new MongoCmdException(241, "Conversion would overflow target type in $convert with no onError value: "
                            + toStringValue(v));
                }
                return new BsonInt64(b.longValue());
            }
        }
    }

    private static String typeAlias(BsonValue t) {
        if (t.isString()) {
            String s = t.asString().getValue();
            if (MongoMatcher.typeNumber(s) == Integer.MIN_VALUE) {
                throw new MongoCmdException(2, "Unknown type name: " + s);
            }
            return s;
        }
        if (BsonCmp.isNumber(t)) {
            int n = (int) MongoNum.truncLong(t);
            for (String a : List.of("double", "string", "object", "array", "binData", "undefined", "objectId", "bool", "date",
                    "null", "regex", "dbPointer", "javascript", "symbol", "javascriptWithScope", "int", "timestamp", "long",
                    "decimal", "minKey", "maxKey")) {
                if (MongoMatcher.typeNumber(a) == n) {
                    return a;
                }
            }
            throw new MongoCmdException(2, "In $convert, numeric value for 'to' does not correspond to a BSON type: " + n);
        }
        throw new MongoCmdException(4, "$convert's 'to' argument must be a string or number, but is " + MongoMatcher.typeName(t));
    }

    // ---------------------------------------------------------------- operators

    static {
        // ---- arithmetic
        reg("$add", 0, Integer.MAX_VALUE, a -> {
            BsonValue acc = new BsonInt32(0);
            boolean date = false;
            boolean anyNull = false;
            long dateMs = 0;
            for (BsonValue v : a) {
                if (nullish(v)) {
                    anyNull = true;
                    continue;
                }
                if (v.isDateTime()) {
                    if (date) {
                        throw new MongoCmdException(16612, "only one date allowed in an $add expression");
                    }
                    date = true;
                    dateMs = v.asDateTime().getValue();
                    continue;
                }
                if (!BsonCmp.isNumber(v)) {
                    throw new MongoCmdException(16554, "$add only supports numeric or date types, not " + MongoMatcher.typeName(v));
                }
                acc = MongoNum.add(acc, v);
            }
            if (anyNull) {
                return nul();
            }
            if (date) {
                return new BsonDateTime(dateMs + (long) Math.floor(MongoNum.toDouble(acc)));
            }
            return acc;
        });
        reg("$subtract", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            if (a[0].isDateTime()) {
                if (a[1].isDateTime()) {
                    return new BsonInt64(a[0].asDateTime().getValue() - a[1].asDateTime().getValue());
                }
                if (BsonCmp.isNumber(a[1])) {
                    return new BsonDateTime(a[0].asDateTime().getValue() - MongoNum.truncLong(a[1]));
                }
            }
            if (!BsonCmp.isNumber(a[0]) || !BsonCmp.isNumber(a[1])) {
                throw new MongoCmdException(16556, "can't $subtract " + (a[1].isDateTime() ? "date from " + MongoMatcher.typeName(a[0])
                        : MongoMatcher.typeName(a[1]) + " from " + MongoMatcher.typeName(a[0])));
            }
            return MongoNum.subtract(a[0], a[1]);
        });
        reg("$multiply", 0, Integer.MAX_VALUE, a -> {
            BsonValue acc = new BsonInt32(1);
            boolean anyNull = false;
            for (BsonValue v : a) {
                if (nullish(v)) {
                    anyNull = true;
                    continue;
                }
                if (!BsonCmp.isNumber(v)) {
                    throw new MongoCmdException(16555, "$multiply only supports numeric types, not " + MongoMatcher.typeName(v));
                }
                acc = MongoNum.multiply(acc, v);
            }
            return anyNull ? nul() : acc;
        });
        reg("$divide", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            if (!BsonCmp.isNumber(a[0]) || !BsonCmp.isNumber(a[1])) {
                throw new MongoCmdException(16609, "$divide only supports numeric types, not " + MongoMatcher.typeName(a[0])
                        + " and " + MongoMatcher.typeName(a[1]));
            }
            return MongoNum.divide(a[0], a[1]);
        });
        reg("$mod", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            if (!BsonCmp.isNumber(a[0]) || !BsonCmp.isNumber(a[1])) {
                throw new MongoCmdException(16611, "$mod only supports numeric types, not " + MongoMatcher.typeName(a[0])
                        + " and " + MongoMatcher.typeName(a[1]));
            }
            return MongoNum.mod(a[0], a[1]);
        });
        reg("$abs", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            requireNumber(a[0], "abs", 28765);
            BsonValue v = a[0];
            if (v.isInt32()) {
                int x = v.asInt32().getValue();
                return x == Integer.MIN_VALUE ? new BsonInt64(-(long) x) : new BsonInt32(Math.abs(x));
            }
            if (v.isInt64()) {
                return new BsonInt64(Math.abs(v.asInt64().getValue()));
            }
            if (v.isDouble()) {
                return new BsonDouble(Math.abs(v.asDouble().getValue()));
            }
            Decimal128 d = v.asDecimal128().getValue();
            return d.isNegative() ? new BsonDecimal128(MongoNum.toDecimal(MongoNum.negate(v))) : v;
        });
        reg("$ceil", 1, 1, a -> nullish(a[0]) ? nul() : roundInt(requireNumber(a[0], "ceil", 28765), RoundingMode.CEILING));
        reg("$floor", 1, 1, a -> nullish(a[0]) ? nul() : roundInt(requireNumber(a[0], "floor", 28765), RoundingMode.FLOOR));
        reg("$round", 1, 2, a -> roundOrTrunc(a, "round", false));
        reg("$trunc", 1, 2, a -> roundOrTrunc(a, "trunc", true));
        reg("$sqrt", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            requireNumber(a[0], "sqrt", 28765);
            if (BsonCmp.compareNumbers(a[0], new BsonInt32(0)) < 0 && !MongoNum.isNaN(a[0])) {
                throw new MongoCmdException(28714, "$sqrt's argument must be greater than or equal to 0");
            }
            if (a[0].isDecimal128()) {
                return new BsonDecimal128(new Decimal128(BsonCmp.toBigDecimal(a[0]).sqrt(java.math.MathContext.DECIMAL128)));
            }
            return math1(a[0], "sqrt", Math::sqrt);
        });
        reg("$exp", 1, 1, a -> math1(a[0], "exp", Math::exp));
        reg("$ln", 1, 1, a -> {
            if (!nullish(a[0]) && BsonCmp.isNumber(a[0]) && BsonCmp.compareNumbers(a[0], new BsonInt32(0)) <= 0
                    && !MongoNum.isNaN(a[0])) {
                throw new MongoCmdException(28766, "$ln's argument must be a positive number, but is " + toStringValue(a[0]));
            }
            return math1(a[0], "ln", Math::log);
        });
        reg("$log10", 1, 1, a -> {
            if (!nullish(a[0]) && BsonCmp.isNumber(a[0]) && BsonCmp.compareNumbers(a[0], new BsonInt32(0)) <= 0
                    && !MongoNum.isNaN(a[0])) {
                throw new MongoCmdException(28761, "$log10's argument must be a positive number, but is " + toStringValue(a[0]));
            }
            return math1(a[0], "log10", Math::log10);
        });
        reg("$log", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            requireNumber(a[0], "log", 28756);
            requireNumber(a[1], "log", 28757);
            if (BsonCmp.compareNumbers(a[0], new BsonInt32(0)) <= 0) {
                throw new MongoCmdException(28758, "$log's argument must be a positive number, but is " + toStringValue(a[0]));
            }
            if (BsonCmp.compareNumbers(a[1], new BsonInt32(0)) <= 0 || BsonCmp.compareNumbers(a[1], new BsonInt32(1)) == 0) {
                throw new MongoCmdException(28759, "$log's base must be a positive number not equal to 1, but is " + toStringValue(a[1]));
            }
            return new BsonDouble(Math.log(MongoNum.toDouble(a[0])) / Math.log(MongoNum.toDouble(a[1])));
        });
        reg("$pow", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            requireNumber(a[0], "pow", 28762);
            requireNumber(a[1], "pow", 28763);
            if (a[0].isDecimal128() || a[1].isDecimal128()) {
                return new BsonDecimal128(MongoNum.toDecimal(new BsonDouble(Math.pow(MongoNum.toDouble(a[0]), MongoNum.toDouble(a[1])))));
            }
            if ((a[0].isInt32() || a[0].isInt64()) && (a[1].isInt32() || a[1].isInt64()) && MongoNum.truncLong(a[1]) >= 0) {
                BigDecimal r = BsonCmp.toBigDecimal(a[0]).pow((int) Math.min(MongoNum.truncLong(a[1]), 2000));
                if (r.abs().compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0) {
                    long l = r.longValue();
                    return a[0].isInt32() && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE && a[1].isInt32() ? new BsonInt32((int) l)
                            : a[0].isInt32() && a[1].isInt32() || a[0].isInt64() ? new BsonInt64(l) : MongoNum.fromLong(l);
                }
                return new BsonDouble(r.doubleValue());
            }
            if (MongoNum.toDouble(a[0]) == 0 && MongoNum.toDouble(a[1]) < 0) {
                throw new MongoCmdException(28764, "$pow cannot take a base of 0 and a negative exponent");
            }
            return new BsonDouble(Math.pow(MongoNum.toDouble(a[0]), MongoNum.toDouble(a[1])));
        });
        for (String[] t : new String[][] {{"sin"}, {"cos"}, {"tan"}, {"asin"}, {"acos"}, {"atan"}, {"sinh"}, {"cosh"}, {"tanh"},
                {"asinh"}, {"acosh"}, {"atanh"}, {"degreesToRadians"}, {"radiansToDegrees"}}) {
            String n = t[0];
            reg("$" + n, 1, 1, a -> domainCheck(a[0], n, math1(a[0], n, switch (n) {
                case "sin" -> Math::sin;
                case "cos" -> Math::cos;
                case "tan" -> Math::tan;
                case "asin" -> Math::asin;
                case "acos" -> Math::acos;
                case "atan" -> Math::atan;
                case "sinh" -> Math::sinh;
                case "cosh" -> Math::cosh;
                case "tanh" -> Math::tanh;
                case "asinh" -> x -> Math.log(x + Math.sqrt(x * x + 1));
                case "acosh" -> x -> Math.log(x + Math.sqrt(x * x - 1));
                case "atanh" -> x -> 0.5 * Math.log((1 + x) / (1 - x));
                case "degreesToRadians" -> Math::toRadians;
                default -> Math::toDegrees;
            })));
        }
        reg("$atan2", 2, 2, a -> anyNullish(a) ? nul() : new BsonDouble(Math.atan2(MongoNum.toDouble(requireNumber(a[0], "atan2", 28765)),
                MongoNum.toDouble(requireNumber(a[1], "atan2", 28765)))));
        SPECIAL.put("$rand", (args, n) -> {
            if (!(args.isDocument() && args.asDocument().isEmpty()) && !(args.isArray() && args.asArray().isEmpty())) {
                throw new MongoCmdException(3040500, "$rand not allowed inside an argument");
            }
            return sc -> new BsonDouble(Math.random());
        });

        // ---- comparison
        reg("$eq", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) == 0));
        reg("$ne", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) != 0));
        reg("$gt", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) > 0));
        reg("$gte", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) >= 0));
        reg("$lt", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) < 0));
        reg("$lte", 2, 2, a -> bool(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])) <= 0));
        reg("$cmp", 2, 2, a -> new BsonInt32(Integer.signum(BsonCmp.compare(cmpVal(a[0]), cmpVal(a[1])))));

        // ---- boolean / conditional (lazy where needed)
        SPECIAL.put("$and", (args, n) -> {
            List<Expr> es = argExprs(args);
            return sc -> {
                for (Expr e : es) {
                    if (!truthy(e.eval(sc))) {
                        return BsonBoolean.FALSE;
                    }
                }
                return BsonBoolean.TRUE;
            };
        });
        SPECIAL.put("$or", (args, n) -> {
            List<Expr> es = argExprs(args);
            return sc -> {
                for (Expr e : es) {
                    if (truthy(e.eval(sc))) {
                        return BsonBoolean.TRUE;
                    }
                }
                return BsonBoolean.FALSE;
            };
        });
        reg("$not", 1, 1, a -> bool(!truthy(a[0])));
        SPECIAL.put("$cond", (args, n) -> {
            Expr i;
            Expr t;
            Expr e;
            if (args.isArray()) {
                if (args.asArray().size() != 3) {
                    throw new MongoCmdException(16020, "Expression $cond takes exactly 3 arguments. " + args.asArray().size() + " were passed in.");
                }
                i = parse(args.asArray().get(0));
                t = parse(args.asArray().get(1));
                e = parse(args.asArray().get(2));
            } else if (args.isDocument()) {
                BsonDocument d = args.asDocument();
                for (String k : d.keySet()) {
                    if (!List.of("if", "then", "else").contains(k)) {
                        throw new MongoCmdException(17083, "Unrecognized parameter to $cond: " + k);
                    }
                }
                for (String k : List.of("if", "then", "else")) {
                    if (!d.containsKey(k)) {
                        throw new MongoCmdException(17080, "Missing '" + k + "' parameter to $cond");
                    }
                }
                i = parse(d.get("if"));
                t = parse(d.get("then"));
                e = parse(d.get("else"));
            } else {
                throw new MongoCmdException(16020, "Expression $cond takes exactly 3 arguments. 1 were passed in.");
            }
            return sc -> truthy(i.eval(sc)) ? t.eval(sc) : e.eval(sc);
        });
        SPECIAL.put("$ifNull", (args, n) -> {
            List<Expr> es = argExprs(args);
            if (es.size() < 2) {
                throw new MongoCmdException(1257300, "Expression $ifNull needs at least two arguments, had: " + es.size());
            }
            return sc -> {
                for (int i = 0; i < es.size() - 1; i++) {
                    BsonValue v = es.get(i).eval(sc);
                    if (!nullish(v)) {
                        return v;
                    }
                }
                return es.get(es.size() - 1).eval(sc);
            };
        });
        SPECIAL.put("$switch", (args, n) -> {
            if (!args.isDocument()) {
                throw new MongoCmdException(40060, "$switch requires an object as an argument, found: " + MongoMatcher.typeName(args));
            }
            BsonDocument d = args.asDocument();
            for (String k : d.keySet()) {
                if (!k.equals("branches") && !k.equals("default")) {
                    throw new MongoCmdException(40067, "$switch found an unknown argument: " + k);
                }
            }
            if (!d.containsKey("branches")) {
                throw new MongoCmdException(40068, "$switch requires at least one branch.");
            }
            if (!d.get("branches").isArray()) {
                throw new MongoCmdException(40061, "$switch expected an array for 'branches', found: " + MongoMatcher.typeName(d.get("branches")));
            }
            List<Expr[]> branches = new ArrayList<>();
            for (BsonValue b : d.getArray("branches")) {
                if (!b.isDocument()) {
                    throw new MongoCmdException(40062, "$switch expected each branch to be an object, found: " + MongoMatcher.typeName(b));
                }
                if (!b.asDocument().containsKey("case")) {
                    throw new MongoCmdException(40064, "$switch requires each branch have a 'case' expression");
                }
                if (!b.asDocument().containsKey("then")) {
                    throw new MongoCmdException(40065, "$switch requires each branch have a 'then' expression.");
                }
                branches.add(new Expr[] {parse(b.asDocument().get("case")), parse(b.asDocument().get("then"))});
            }
            if (branches.isEmpty()) {
                throw new MongoCmdException(40068, "$switch requires at least one branch.");
            }
            Expr def = d.containsKey("default") ? parse(d.get("default")) : null;
            return sc -> {
                for (Expr[] b : branches) {
                    if (truthy(b[0].eval(sc))) {
                        return b[1].eval(sc);
                    }
                }
                if (def == null) {
                    throw new MongoCmdException(40066, "$switch could not find a matching branch for an input, and no default was specified.");
                }
                return def.eval(sc);
            };
        });
        SPECIAL.put("$literal", (args, n) -> sc -> args);
        SPECIAL.put("$let", (args, n) -> {
            if (!args.isDocument() || !args.asDocument().containsKey("vars") || !args.asDocument().containsKey("in")) {
                throw new MongoCmdException(args.isDocument() ? 16876 : 16874, "Missing 'vars' or 'in' parameter to $let");
            }
            LinkedHashMap<String, Expr> vars = new LinkedHashMap<>();
            for (Map.Entry<String, BsonValue> e : args.asDocument().getDocument("vars").entrySet()) {
                checkVarName(e.getKey());
                vars.put(e.getKey(), parse(e.getValue()));
            }
            Expr in = parse(args.asDocument().get("in"));
            return sc -> {
                Scope s2 = sc;
                for (Map.Entry<String, Expr> e : vars.entrySet()) {
                    BsonValue v = e.getValue().eval(sc);
                    s2 = s2.with(e.getKey(), v);
                }
                return in.eval(s2);
            };
        });

        // ---- type
        reg("$type", 1, 1, a -> new BsonString(typeOf(a[0])));
        reg("$isNumber", 1, 1, a -> bool(a[0] != MISSING && BsonCmp.isNumber(a[0])));
        for (String t : new String[] {"int", "long", "double", "decimal", "string", "bool", "date", "objectId"}) {
            String target = t;
            reg("$to" + Character.toUpperCase(t.charAt(0)) + t.substring(1), 1, 1, a -> convert(a[0], target, null));
        }
        SPECIAL.put("$convert", (args, n) -> {
            if (!args.isDocument()) {
                throw new MongoCmdException(4, "$convert expects an object of named arguments but found: " + MongoMatcher.typeName(args));
            }
            BsonDocument d = args.asDocument();
            for (String k : d.keySet()) {
                if (!List.of("input", "to", "onError", "onNull", "format", "base").contains(k)) {
                    throw new MongoCmdException(4, "$convert found an unknown argument: " + k);
                }
            }
            if (!d.containsKey("input")) {
                throw new MongoCmdException(4, "Missing 'input' parameter to $convert");
            }
            if (!d.containsKey("to")) {
                throw new MongoCmdException(4, "Missing 'to' parameter to $convert");
            }
            Expr input = parse(d.get("input"));
            Expr to = parse(d.get("to"));
            Expr onErr = d.containsKey("onError") ? parse(d.get("onError")) : null;
            Expr onNull = d.containsKey("onNull") ? parse(d.get("onNull")) : null;
            return sc -> {
                BsonValue in = input.eval(sc);
                BsonValue t = to.eval(sc);
                if (nullish(t)) {
                    return nul();
                }
                String target;
                BsonValue sub = null;
                if (t.isDocument()) {
                    if (!t.asDocument().containsKey("type") || !(t.asDocument().get("type").isString()
                            && t.asDocument().getString("type").getValue().equals("binData"))) {
                        throw new MongoCmdException(9, "$convert: 'to' as an object is only supported for binData");
                    }
                    target = typeAlias(t.asDocument().get("type"));
                    sub = t.asDocument().get("subtype");
                } else {
                    target = typeAlias(t);
                }
                if (nullish(in)) {
                    return onNull != null ? onNull.eval(sc) : nul();
                }
                try {
                    return convert(in, target, sub);
                } catch (MongoCmdException e) {
                    if (onErr != null && e.code == 241) {
                        return onErr.eval(sc);
                    }
                    throw e;
                }
            };
        });
        reg("$bsonSize", 1, 1, a -> nullish(a[0]) ? nul() : new BsonInt32(MongoBson.encode(a[0].asDocument()).length));
        reg("$binarySize", 1, 1, a -> nullish(a[0]) ? nul()
                : new BsonInt32(a[0].isString() ? a[0].asString().getValue().getBytes(StandardCharsets.UTF_8).length
                        : a[0].asBinary().getData().length));

        // ---- strings
        reg("$concat", 0, Integer.MAX_VALUE, a -> {
            StringBuilder sb = new StringBuilder();
            boolean anyNull = false;
            for (BsonValue v : a) {
                if (nullish(v)) {
                    anyNull = true;
                } else if (!v.isString()) {
                    throw new MongoCmdException(16702, "$concat only supports strings, not " + MongoMatcher.typeName(v));
                } else {
                    sb.append(v.asString().getValue());
                }
            }
            return anyNull ? nul() : new BsonString(sb.toString());
        });
        reg("$toLower", 1, 1, a -> new BsonString(nullish(a[0]) ? "" : asciiCase(strOf(a[0]), false)));
        reg("$toUpper", 1, 1, a -> new BsonString(nullish(a[0]) ? "" : asciiCase(strOf(a[0]), true)));
        reg("$strLenBytes", 1, 1, a -> {
            if (!a[0].isString()) {
                throw new MongoCmdException(34473, "$strLenBytes requires a string argument, found: " + typeOf(a[0]));
            }
            return new BsonInt32(a[0].asString().getValue().getBytes(StandardCharsets.UTF_8).length);
        });
        reg("$strLenCP", 1, 1, a -> {
            if (!a[0].isString()) {
                throw new MongoCmdException(34471, "$strLenCP requires a string argument, found: " + typeOf(a[0]));
            }
            String s = a[0].asString().getValue();
            return new BsonInt32(s.codePointCount(0, s.length()));
        });
        reg("$substrBytes", 3, 3, a -> substr(a, true));
        reg("$substr", 3, 3, a -> substr(a, true));
        reg("$substrCP", 3, 3, a -> substr(a, false));
        reg("$strcasecmp", 2, 2, a -> {
            String x = nullish(a[0]) ? "" : strOf(a[0]).toUpperCase();
            String y = nullish(a[1]) ? "" : strOf(a[1]).toUpperCase();
            return new BsonInt32(Integer.signum(BsonCmp.compareUtf8(x, y)));
        });
        reg("$split", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            if (!a[0].isString()) {
                throw new MongoCmdException(40085, "$split requires an expression that evaluates to a string as a first argument, found: " + typeOf(a[0]));
            }
            if (!a[1].isString()) {
                throw new MongoCmdException(40086, "$split requires an expression that evaluates to a string as a second argument, found: " + typeOf(a[1]));
            }
            String s = a[0].asString().getValue();
            String sep = a[1].asString().getValue();
            if (sep.isEmpty()) {
                throw new MongoCmdException(40087, "$split requires a non-empty separator");
            }
            BsonArray out = new BsonArray();
            int from = 0;
            while (true) {
                int i = s.indexOf(sep, from);
                if (i < 0) {
                    out.add(new BsonString(s.substring(from)));
                    break;
                }
                out.add(new BsonString(s.substring(from, i)));
                from = i + sep.length();
            }
            return out;
        });
        reg("$indexOfBytes", 2, 4, a -> indexOf(a, true));
        reg("$indexOfCP", 2, 4, a -> indexOf(a, false));
        SPECIAL.put("$trim", (args, n) -> trimExpr(args, "$trim", true, true));
        SPECIAL.put("$ltrim", (args, n) -> trimExpr(args, "$ltrim", true, false));
        SPECIAL.put("$rtrim", (args, n) -> trimExpr(args, "$rtrim", false, true));
        SPECIAL.put("$replaceOne", (args, n) -> replaceExpr(args, "$replaceOne", false));
        SPECIAL.put("$replaceAll", (args, n) -> replaceExpr(args, "$replaceAll", true));
        SPECIAL.put("$regexMatch", (args, n) -> regexExpr(args, "$regexMatch", 0));
        SPECIAL.put("$regexFind", (args, n) -> regexExpr(args, "$regexFind", 1));
        SPECIAL.put("$regexFindAll", (args, n) -> regexExpr(args, "$regexFindAll", 2));

        // ---- arrays
        reg("$size", 1, 1, a -> {
            if (!a[0].isArray()) {
                throw new MongoCmdException(17124, "The argument to $size must be an array, but was of type: " + typeOf(a[0]));
            }
            return new BsonInt32(a[0].asArray().size());
        });
        reg("$isArray", 1, 1, a -> bool(a[0].isArray()));
        reg("$arrayElemAt", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            BsonArray arr = arr(a[0], "arrayElemAt");
            if (!BsonCmp.isNumber(a[1]) || MongoNum.toDouble(a[1]) != Math.floor(MongoNum.toDouble(a[1]))) {
                throw new MongoCmdException(28689, "$arrayElemAt's second argument must be a numeric value, but is " + typeOf(a[1]));
            }
            long i = MongoNum.truncLong(a[1]);
            if (i < 0) {
                i += arr.size();
            }
            return i >= 0 && i < arr.size() ? arr.get((int) i) : MISSING;
        });
        reg("$first", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            BsonArray arr = arr(a[0], "first");
            return arr.isEmpty() ? MISSING : arr.get(0);
        });
        reg("$last", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            BsonArray arr = arr(a[0], "last");
            return arr.isEmpty() ? MISSING : arr.get(arr.size() - 1);
        });
        reg("$in", 2, 2, a -> {
            if (!a[1].isArray()) {
                throw new MongoCmdException(40081, "$in requires an array as a second argument, found: " + typeOf(a[1]));
            }
            for (BsonValue v : a[1].asArray()) {
                if (BsonCmp.compare(cmpVal(v), cmpVal(a[0])) == 0) {
                    return BsonBoolean.TRUE;
                }
            }
            return BsonBoolean.FALSE;
        });
        reg("$concatArrays", 0, Integer.MAX_VALUE, a -> {
            BsonArray out = new BsonArray();
            for (BsonValue v : a) {
                if (nullish(v)) {
                    return nul();
                }
                if (!v.isArray()) {
                    throw new MongoCmdException(28664, "$concatArrays only supports arrays, not " + MongoMatcher.typeName(v));
                }
                out.addAll(v.asArray());
            }
            return out;
        });
        reg("$reverseArray", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            BsonArray in = arr(a[0], "reverseArray");
            BsonArray out = new BsonArray();
            for (int i = in.size() - 1; i >= 0; i--) {
                out.add(in.get(i));
            }
            return out;
        });
        reg("$slice", 2, 3, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            BsonArray in = arr(a[0], "slice");
            if (!BsonCmp.isNumber(a[1])) {
                throw new MongoCmdException(28724, "Second argument to $slice must be numeric, but is of type: " + typeOf(a[1]));
            }
            if (MongoNum.toDouble(a[1]) != Math.floor(MongoNum.toDouble(a[1]))) {
                throw new MongoCmdException(28726, "Second argument to $slice can't be represented as a 32-bit integer: " + toStringValue(a[1]));
            }
            long n1 = MongoNum.truncLong(a[1]);
            int size = in.size();
            int from;
            int to;
            if (a.length == 2) {
                if (n1 >= 0) {
                    from = 0;
                    to = (int) Math.min(n1, size);
                } else {
                    from = (int) Math.max(0, size + n1);
                    to = size;
                }
            } else {
                if (!BsonCmp.isNumber(a[2])) {
                    throw new MongoCmdException(28726, "Third argument to $slice must be numeric, but is of type: " + typeOf(a[2]));
                }
                long len = MongoNum.truncLong(a[2]);
                if (len <= 0) {
                    throw new MongoCmdException(28729, "Third argument to $slice must be positive: " + len);
                }
                long start = n1 < 0 ? Math.max(0, size + n1) : Math.min(n1, size);
                from = (int) start;
                to = (int) Math.min(size, start + len);
            }
            return new BsonArray(new ArrayList<>(in.getValues().subList(from, Math.max(from, to))));
        });
        reg("$range", 2, 3, a -> {
            if (!BsonCmp.isNumber(a[0]) || MongoNum.toDouble(a[0]) != Math.floor(MongoNum.toDouble(a[0]))) {
                throw new MongoCmdException(34443, "$range requires a numeric starting value, found value of type: " + typeOf(a[0]));
            }
            if (!BsonCmp.isNumber(a[1]) || MongoNum.toDouble(a[1]) != Math.floor(MongoNum.toDouble(a[1]))) {
                throw new MongoCmdException(34445, "$range requires a numeric ending value, found value of type: " + typeOf(a[1]));
            }
            long step = 1;
            if (a.length == 3) {
                if (!BsonCmp.isNumber(a[2])) {
                    throw new MongoCmdException(34447, "$range requires a numeric step value, found value of type: " + typeOf(a[2]));
                }
                step = MongoNum.truncLong(a[2]);
                if (step == 0) {
                    throw new MongoCmdException(34449, "$range requires a non-zero step value");
                }
            }
            long s = MongoNum.truncLong(a[0]);
            long e = MongoNum.truncLong(a[1]);
            BsonArray out = new BsonArray();
            for (long i = s; step > 0 ? i < e : i > e; i += step) {
                out.add(MongoNum.fromLong(i));
                if (out.size() > 10_000_000) {
                    throw new MongoCmdException(146, "$range would exceed the memory limit");
                }
            }
            return out;
        });
        reg("$indexOfArray", 2, 4, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            BsonArray in = arr(a[0], "indexOfArray");
            int from = a.length > 2 ? (int) MongoNum.truncLong(a[2]) : 0;
            int to = a.length > 3 ? (int) Math.min(MongoNum.truncLong(a[3]), in.size()) : in.size();
            for (int i = Math.max(0, from); i < to; i++) {
                if (BsonCmp.compare(in.get(i), cmpVal(a[1])) == 0) {
                    return new BsonInt32(i);
                }
            }
            return new BsonInt32(-1);
        });
        reg("$arrayToObject", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            BsonArray in = arr(a[0], "arrayToObject");
            BsonDocument out = new BsonDocument();
            for (BsonValue e : in) {
                if (e.isArray()) {
                    if (e.asArray().size() != 2) {
                        throw new MongoCmdException(40397, "$arrayToObject requires an array of size 2 arrays,found array of size: " + e.asArray().size());
                    }
                    BsonValue k = e.asArray().get(0);
                    if (!k.isString()) {
                        throw new MongoCmdException(40395, "$arrayToObject requires an array of key-value pairs, where the key must be of type string. Found key type: " + typeOf(k));
                    }
                    out.put(k.asString().getValue(), e.asArray().get(1));
                } else if (e.isDocument()) {
                    BsonDocument d = e.asDocument();
                    if (d.size() != 2 || !d.containsKey("k") || !d.containsKey("v")) {
                        throw new MongoCmdException(40392, "$arrayToObject requires an object keys of 'k' and 'v'. Found incorrect number of keys:" + d.size());
                    }
                    if (!d.get("k").isString()) {
                        throw new MongoCmdException(40394, "$arrayToObject requires an object with keys 'k' and 'v', where the value of 'k' must be of type string. Found type: " + typeOf(d.get("k")));
                    }
                    out.put(d.getString("k").getValue(), d.get("v"));
                } else {
                    throw new MongoCmdException(40398, "Unrecognised input type format for $arrayToObject: " + typeOf(e));
                }
            }
            return out;
        });
        reg("$objectToArray", 1, 1, a -> {
            if (nullish(a[0])) {
                return nul();
            }
            if (!a[0].isDocument()) {
                throw new MongoCmdException(5153010, "$objectToArray requires a document input, found: " + typeOf(a[0]));
            }
            BsonArray out = new BsonArray();
            for (Map.Entry<String, BsonValue> e : a[0].asDocument().entrySet()) {
                out.add(doc("k", new BsonString(e.getKey()), "v", e.getValue()));
            }
            return out;
        });
        reg("$mergeObjects", 0, Integer.MAX_VALUE, a -> {
            BsonDocument out = new BsonDocument();
            for (BsonValue v : a) {
                if (nullish(v)) {
                    continue;
                }
                if (v.isArray()) {
                    for (BsonValue x : v.asArray()) {
                        mergeInto(out, x);
                    }
                } else {
                    mergeInto(out, v);
                }
            }
            return out;
        });
        reg("$setUnion", 0, Integer.MAX_VALUE, a -> {
            LinkedHashMap<String, BsonValue> m = new LinkedHashMap<>();
            for (BsonValue v : a) {
                if (nullish(v)) {
                    return nul();
                }
                for (BsonValue e : setArg(v, "setUnion")) {
                    m.putIfAbsent(BsonCmp.key(e), e);
                }
            }
            return sortedSet(m);
        });
        reg("$setIntersection", 0, Integer.MAX_VALUE, a -> {
            Map<String, BsonValue> acc = null;
            for (BsonValue v : a) {
                if (nullish(v)) {
                    return nul();
                }
                LinkedHashMap<String, BsonValue> cur = new LinkedHashMap<>();
                for (BsonValue e : setArg(v, "setIntersection")) {
                    cur.putIfAbsent(BsonCmp.key(e), e);
                }
                if (acc == null) {
                    acc = cur;
                } else {
                    acc.keySet().retainAll(cur.keySet());
                }
            }
            return sortedSet(acc == null ? new LinkedHashMap<>() : acc);
        });
        reg("$setDifference", 2, 2, a -> {
            if (anyNullish(a)) {
                return nul();
            }
            LinkedHashMap<String, BsonValue> m = new LinkedHashMap<>();
            for (BsonValue e : setArg(a[0], "setDifference")) {
                m.putIfAbsent(BsonCmp.key(e), e);
            }
            for (BsonValue e : setArg(a[1], "setDifference")) {
                m.remove(BsonCmp.key(e));
            }
            return sortedSet(m);
        });
        reg("$setEquals", 2, Integer.MAX_VALUE, a -> {
            java.util.Set<String> first = null;
            for (BsonValue v : a) {
                java.util.Set<String> s = new java.util.HashSet<>();
                for (BsonValue e : setArg(v, "setEquals")) {
                    s.add(BsonCmp.key(e));
                }
                if (first == null) {
                    first = s;
                } else if (!first.equals(s)) {
                    return BsonBoolean.FALSE;
                }
            }
            return BsonBoolean.TRUE;
        });
        reg("$setIsSubset", 2, 2, a -> {
            java.util.Set<String> sup = new java.util.HashSet<>();
            for (BsonValue e : setArg(a[1], "setIsSubset")) {
                sup.add(BsonCmp.key(e));
            }
            for (BsonValue e : setArg(a[0], "setIsSubset")) {
                if (!sup.contains(BsonCmp.key(e))) {
                    return BsonBoolean.FALSE;
                }
            }
            return BsonBoolean.TRUE;
        });
        reg("$anyElementTrue", 1, 1, a -> {
            for (BsonValue e : setArg(a[0], "anyElementTrue")) {
                if (truthy(e)) {
                    return BsonBoolean.TRUE;
                }
            }
            return BsonBoolean.FALSE;
        });
        reg("$allElementsTrue", 1, 1, a -> {
            for (BsonValue e : setArg(a[0], "allElementsTrue")) {
                if (!truthy(e)) {
                    return BsonBoolean.FALSE;
                }
            }
            return BsonBoolean.TRUE;
        });
        // accumulator-style expression forms over arrays / argument lists
        reg("$sum", 0, Integer.MAX_VALUE, a -> {
            BsonValue acc = new BsonInt32(0);
            for (BsonValue v : flatten(a)) {
                if (v != MISSING && BsonCmp.isNumber(v)) {
                    acc = MongoNum.add(acc, v);
                }
            }
            return acc;
        });
        reg("$avg", 0, Integer.MAX_VALUE, a -> {
            BsonValue acc = new BsonInt32(0);
            long n = 0;
            for (BsonValue v : flatten(a)) {
                if (v != MISSING && BsonCmp.isNumber(v)) {
                    acc = MongoNum.add(acc, v);
                    n++;
                }
            }
            return n == 0 ? nul() : MongoNum.divide(acc, new BsonInt64(n));
        });
        reg("$max", 0, Integer.MAX_VALUE, a -> extreme(flatten(a), true));
        reg("$min", 0, Integer.MAX_VALUE, a -> extreme(flatten(a), false));
        reg("$stdDevPop", 1, 1, a -> stdDev(a[0], false));
        reg("$stdDevSamp", 1, 1, a -> stdDev(a[0], true));
        reg("$median", 1, 1, a -> nul());
        SPECIAL.remove("$median");
        OPS.remove("$median");
        SPECIAL.put("$map", (args, n) -> {
            BsonDocument d = args.isDocument() ? args.asDocument() : null;
            if (d == null) {
                throw new MongoCmdException(16878, "$map only supports an object as its argument");
            }
            for (String k : d.keySet()) {
                if (!List.of("input", "as", "in", "arrayIndexAs").contains(k)) {
                    throw new MongoCmdException(16879, "Unrecognized parameter to $map: " + k);
                }
            }
            if (!d.containsKey("input")) {
                throw new MongoCmdException(16880, "Missing 'input' parameter to $map");
            }
            if (!d.containsKey("in")) {
                throw new MongoCmdException(16882, "Missing 'in' parameter to $map");
            }
            Expr input = parse(d.get("input"));
            String as = d.containsKey("as") ? d.getString("as").getValue() : "this";
            checkVarName(as);
            Expr in = parse(d.get("in"));
            return sc -> {
                BsonValue arr = input.eval(sc);
                if (nullish(arr)) {
                    return nul();
                }
                if (!arr.isArray()) {
                    throw new MongoCmdException(16883, "input to $map must be an array not " + typeOf(arr));
                }
                BsonArray out = new BsonArray();
                for (BsonValue e : arr.asArray()) {
                    BsonValue v = in.eval(sc.with(as, e));
                    out.add(v == MISSING ? nul() : v);
                }
                return out;
            };
        });
        SPECIAL.put("$filter", (args, n) -> {
            if (!args.isDocument()) {
                throw new MongoCmdException(28646, "$filter only supports an object as its argument");
            }
            BsonDocument d = args.asDocument();
            for (String k : d.keySet()) {
                if (!List.of("input", "as", "cond", "limit").contains(k)) {
                    throw new MongoCmdException(28647, "Unrecognized parameter to $filter: " + k);
                }
            }
            if (!d.containsKey("input")) {
                throw new MongoCmdException(28648, "Missing 'input' parameter to $filter");
            }
            if (!d.containsKey("cond")) {
                throw new MongoCmdException(28650, "Missing 'cond' parameter to $filter");
            }
            Expr input = parse(d.get("input"));
            String as = d.containsKey("as") ? d.getString("as").getValue() : "this";
            Expr cond = parse(d.get("cond"));
            Expr limit = d.containsKey("limit") ? parse(d.get("limit")) : null;
            return sc -> {
                BsonValue arr = input.eval(sc);
                if (nullish(arr)) {
                    return nul();
                }
                if (!arr.isArray()) {
                    throw new MongoCmdException(28651, "input to $filter must be an array not " + typeOf(arr));
                }
                long lim = Long.MAX_VALUE;
                if (limit != null) {
                    BsonValue l = limit.eval(sc);
                    if (!nullish(l)) {
                        if (!BsonCmp.isNumber(l) || MongoNum.toDouble(l) != Math.floor(MongoNum.toDouble(l))) {
                            throw new MongoCmdException(327391, "$filter: limit must be a positive integer");
                        }
                        lim = MongoNum.truncLong(l);
                        if (lim < 1) {
                            throw new MongoCmdException(327392, "$filter: limit must be greater than 0");
                        }
                    }
                }
                BsonArray out = new BsonArray();
                for (BsonValue e : arr.asArray()) {
                    if (out.size() >= lim) {
                        break;
                    }
                    if (truthy(cond.eval(sc.with(as, e)))) {
                        out.add(e);
                    }
                }
                return out;
            };
        });
        SPECIAL.put("$reduce", (args, n) -> {
            if (!args.isDocument()) {
                throw new MongoCmdException(40075, "$reduce only supports an object as its argument");
            }
            BsonDocument d = args.asDocument();
            for (String k : List.of("input", "initialValue", "in")) {
                if (!d.containsKey(k)) {
                    throw new MongoCmdException(40077, "Missing '" + k + "' parameter to $reduce");
                }
            }
            Expr input = parse(d.get("input"));
            Expr init = parse(d.get("initialValue"));
            Expr in = parse(d.get("in"));
            return sc -> {
                BsonValue arr = input.eval(sc);
                if (nullish(arr)) {
                    return nul();
                }
                if (!arr.isArray()) {
                    throw new MongoCmdException(40080, "$reduce requires that 'input' be an array, found: " + typeOf(arr));
                }
                BsonValue acc = init.eval(sc);
                for (BsonValue e : arr.asArray()) {
                    acc = in.eval(sc.with("value", acc).with("this", e));
                    if (acc == MISSING) {
                        acc = nul();
                    }
                }
                return acc;
            };
        });
        SPECIAL.put("$zip", (args, n) -> {
            BsonDocument d = args.asDocument();
            List<Expr> inputs = new ArrayList<>();
            if (!d.containsKey("inputs") || d.getArray("inputs").isEmpty()) {
                throw new MongoCmdException(34465, "$zip requires at least one input array");
            }
            for (BsonValue v : d.getArray("inputs")) {
                inputs.add(parse(v));
            }
            boolean longest = d.containsKey("useLongestLength") && d.getBoolean("useLongestLength").getValue();
            Expr defaults = d.containsKey("defaults") ? parse(d.get("defaults")) : null;
            return sc -> {
                List<BsonArray> arrs = new ArrayList<>();
                for (Expr e : inputs) {
                    BsonValue v = e.eval(sc);
                    if (nullish(v)) {
                        return nul();
                    }
                    arrs.add(arr(v, "zip"));
                }
                int len = longest ? 0 : Integer.MAX_VALUE;
                for (BsonArray x : arrs) {
                    len = longest ? Math.max(len, x.size()) : Math.min(len, x.size());
                }
                if (arrs.isEmpty()) {
                    len = 0;
                }
                BsonArray defs = defaults == null ? null : defaults.eval(sc).asArray();
                BsonArray out = new BsonArray();
                for (int i = 0; i < len; i++) {
                    BsonArray row = new BsonArray();
                    for (int j = 0; j < arrs.size(); j++) {
                        row.add(i < arrs.get(j).size() ? arrs.get(j).get(i) : defs != null ? defs.get(j) : nul());
                    }
                    out.add(row);
                }
                return out;
            };
        });
        SPECIAL.put("$sortArray", (args, n) -> {
            BsonDocument d = args.asDocument();
            Expr input = parse(d.get("input"));
            BsonValue sortBy = d.get("sortBy");
            return sc -> {
                BsonValue v = input.eval(sc);
                if (nullish(v)) {
                    return nul();
                }
                if (!v.isArray()) {
                    throw new MongoCmdException(2942500, "The input argument to $sortArray must be an array, but was of type: " + typeOf(v));
                }
                List<BsonValue> copy = new ArrayList<>(v.asArray().getValues());
                Comparator<BsonValue> cmp;
                if (sortBy.isDocument()) {
                    Comparator<BsonDocument> dc = MongoSort.comparator(sortBy.asDocument(), sc.coll);
                    cmp = (x, y) -> dc.compare(x.isDocument() ? x.asDocument() : new BsonDocument(),
                            y.isDocument() ? y.asDocument() : new BsonDocument());
                } else {
                    int dir = MongoNum.truncLong(sortBy) < 0 ? -1 : 1;
                    cmp = (x, y) -> dir * BsonCmp.compare(x, y, sc.coll);
                }
                copy.sort(cmp);
                return new BsonArray(copy);
            };
        });
        for (String nm : new String[] {"$firstN", "$lastN", "$maxN", "$minN"}) {
            SPECIAL.put(nm, (args, n) -> {
                if (!args.isDocument()) {
                    throw new MongoCmdException(5787801, "specification must be an object; found " + typeOf(args));
                }
                Expr input = parse(args.asDocument().get("input"));
                Expr cnt = parse(args.asDocument().get("n"));
                return sc -> {
                    BsonValue v = input.eval(sc);
                    BsonValue c = cnt.eval(sc);
                    if (nullish(v)) {
                        throw new MongoCmdException(5788200, "Input must be an array");
                    }
                    arr(v, nm.substring(1));
                    long k = MongoNum.truncLong(c);
                    if (k <= 0) {
                        throw new MongoCmdException(5787902, "'n' must be greater than 0, found " + k);
                    }
                    List<BsonValue> list = new ArrayList<>(v.asArray().getValues());
                    if (nm.equals("$firstN")) {
                        return new BsonArray(new ArrayList<>(list.subList(0, (int) Math.min(k, list.size()))));
                    }
                    if (nm.equals("$lastN")) {
                        return new BsonArray(new ArrayList<>(list.subList((int) Math.max(0, list.size() - k), list.size())));
                    }
                    list.removeIf(x -> nullish(x));
                    list.sort(nm.equals("$maxN") ? (x, y) -> BsonCmp.compare(y, x) : BsonCmp::compare);
                    return new BsonArray(new ArrayList<>(list.subList(0, (int) Math.min(k, list.size()))));
                };
            });
        }
        SPECIAL.put("$getField", (args, n) -> {
            Expr field;
            Expr input;
            if (args.isDocument() && args.asDocument().containsKey("field") && args.asDocument().size() <= 2
                    && (args.asDocument().size() == 1 || args.asDocument().containsKey("input"))) {
                field = parse(args.asDocument().get("field"));
                input = args.asDocument().containsKey("input") ? parse(args.asDocument().get("input")) : parse(new BsonString("$$CURRENT"));
            } else {
                field = parse(args);
                input = parse(new BsonString("$$CURRENT"));
            }
            return sc -> {
                BsonValue f = field.eval(sc);
                BsonValue in = input.eval(sc);
                if (nullish(in)) {
                    return nul();
                }
                if (!f.isString()) {
                    throw new MongoCmdException(3041704, "$getField requires 'field' to evaluate to type String, but got " + typeOf(f));
                }
                if (!in.isDocument()) {
                    return MISSING;
                }
                BsonValue r = in.asDocument().get(f.asString().getValue());
                return r == null ? MISSING : r;
            };
        });
        SPECIAL.put("$setField", (args, n) -> {
            BsonDocument d = args.asDocument();
            Expr field = parse(d.get("field"));
            Expr input = parse(d.get("input"));
            Expr value = parse(d.get("value"));
            return sc -> {
                BsonValue in = input.eval(sc);
                if (nullish(in)) {
                    return nul();
                }
                if (!in.isDocument()) {
                    throw new MongoCmdException(4161109, "$setField requires 'input' to evaluate to type Object");
                }
                BsonValue f = field.eval(sc);
                BsonDocument out = in.asDocument().clone();
                BsonValue v = value.eval(sc);
                if (v == MISSING) {
                    out.remove(f.asString().getValue());
                } else {
                    out.put(f.asString().getValue(), v);
                }
                return out;
            };
        });
        SPECIAL.put("$unsetField", (args, n) -> {
            BsonDocument d = args.asDocument();
            Expr field = parse(d.get("field"));
            Expr input = parse(d.get("input"));
            return sc -> {
                BsonValue in = input.eval(sc);
                if (nullish(in)) {
                    return nul();
                }
                BsonDocument out = in.asDocument().clone();
                out.remove(field.eval(sc).asString().getValue());
                return out;
            };
        });
        MongoDates.register(MongoExpr::reg, SPECIAL);
    }

    interface Reg {
        void reg(String name, int min, int max, Fn fn);
    }

    private static List<BsonValue> flatten(BsonValue[] a) {
        List<BsonValue> out = new ArrayList<>();
        for (BsonValue v : a) {
            if (v != MISSING && v.isArray()) {
                out.addAll(v.asArray().getValues());
            } else {
                out.add(v);
            }
        }
        return out;
    }

    private static BsonValue extreme(List<BsonValue> vals, boolean max) {
        BsonValue best = null;
        for (BsonValue v : vals) {
            if (nullish(v)) {
                continue;
            }
            if (best == null || (max ? BsonCmp.compare(v, best) > 0 : BsonCmp.compare(v, best) < 0)) {
                best = v;
            }
        }
        return best == null ? nul() : best;
    }

    private static BsonValue stdDev(BsonValue arg, boolean sample) {
        List<BsonValue> vals = arg.isArray() ? arg.asArray().getValues() : List.of(arg);
        double sum = 0;
        double sumsq = 0;
        long n = 0;
        for (BsonValue v : vals) {
            if (v != MISSING && BsonCmp.isNumber(v)) {
                double d = MongoNum.toDouble(v);
                sum += d;
                n++;
            }
        }
        if (n == 0 || sample && n == 1) {
            return nul();
        }
        double mean = sum / n;
        for (BsonValue v : vals) {
            if (v != MISSING && BsonCmp.isNumber(v)) {
                double d = MongoNum.toDouble(v) - mean;
                sumsq += d * d;
            }
        }
        return new BsonDouble(Math.sqrt(sumsq / (sample ? n - 1 : n)));
    }

    private static void mergeInto(BsonDocument out, BsonValue v) {
        if (!v.isDocument()) {
            throw new MongoCmdException(40400, "$mergeObjects requires object inputs, but input " + v + " is of type " + MongoMatcher.typeName(v));
        }
        for (Map.Entry<String, BsonValue> e : v.asDocument().entrySet()) {
            out.put(e.getKey(), e.getValue());
        }
    }

    private static BsonArray setArg(BsonValue v, String op) {
        if (!v.isArray()) {
            throw new MongoCmdException(17041, "All operands of $" + op + " must be arrays. One argument is of type: " + typeOf(v));
        }
        return v.asArray();
    }

    private static BsonArray sortedSet(Map<String, BsonValue> m) {
        List<BsonValue> l = new ArrayList<>(m.values());
        l.sort(BsonCmp::compare);
        return new BsonArray(l);
    }

    static String asciiCase(String s, boolean upper) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            sb.append(upper ? (c >= 'a' && c <= 'z' ? (char) (c - 32) : c) : (c >= 'A' && c <= 'Z' ? (char) (c + 32) : c));
        }
        return sb.toString();
    }

    private static String strOf(BsonValue v) {
        if (v.isString()) {
            return v.asString().getValue();
        }
        try {
            return toStringValue(v);
        } catch (MongoCmdException e) {
            throw new MongoCmdException(16007, "can't convert from BSON type " + MongoMatcher.typeName(v) + " to String");
        }
    }

    private static BsonValue roundInt(BsonValue v, RoundingMode mode) {
        if (v.isInt32() || v.isInt64()) {
            return v;
        }
        if (v.isDouble()) {
            double d = v.asDouble().getValue();
            return new BsonDouble(mode == RoundingMode.CEILING ? Math.ceil(d) : Math.floor(d));
        }
        Decimal128 dd = v.asDecimal128().getValue();
        if (dd.isNaN() || dd.isInfinite()) {
            return v;
        }
        return new BsonDecimal128(new Decimal128(BsonCmp.toBigDecimal(v).setScale(0, mode)));
    }

    private static BsonValue roundOrTrunc(BsonValue[] a, String op, boolean trunc) {
        if (anyNullish(a)) {
            return nul();
        }
        requireNumber(a[0], op, 51081);
        int place = 0;
        if (a.length == 2) {
            if (!BsonCmp.isNumber(a[1]) || MongoNum.toDouble(a[1]) != Math.floor(MongoNum.toDouble(a[1]))) {
                throw new MongoCmdException(51082, "precision argument to $" + op + " must be a integral value");
            }
            place = (int) MongoNum.truncLong(a[1]);
            if (place < -20 || place > 100) {
                throw new MongoCmdException(51083, "cannot apply $" + op + " with precision value " + place + " value must be in [-20, 100]");
            }
        }
        return roundTo(a[0], place, trunc);
    }

    private static BsonValue substr(BsonValue[] a, boolean bytes) {
        String s = nullish(a[0]) ? "" : strOf(a[0]);
        if (!BsonCmp.isNumber(a[1])) {
            throw new MongoCmdException(16034, "$substr: starting index must be a numeric type (is BSON type " + typeOf(a[1]) + ")");
        }
        if (!BsonCmp.isNumber(a[2])) {
            throw new MongoCmdException(16035, "$substr: length must be a numeric type (is BSON type " + typeOf(a[2]) + ")");
        }
        long start = MongoNum.truncLong(a[1]);
        long len = MongoNum.truncLong(a[2]);
        if (bytes) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            if (start < 0) {
                return new BsonString("");
            }
            if (start >= b.length) {
                return new BsonString("");
            }
            long end = len < 0 ? b.length : Math.min(b.length, start + len);
            int i = (int) start;
            if (i > 0 && (b[i] & 0xC0) == 0x80) {
                throw new MongoCmdException(28656, "Invalid range, starting index is a UTF-8 continuation byte.");
            }
            if (end < b.length && (b[(int) end] & 0xC0) == 0x80) {
                throw new MongoCmdException(28657, "Invalid range, ending index is in the middle of a UTF-8 character.");
            }
            return new BsonString(new String(b, i, (int) end - i, StandardCharsets.UTF_8));
        }
        int cps = s.codePointCount(0, s.length());
        if (start < 0) {
            throw new MongoCmdException(34455, "$substrCP: the starting index must be non-negative integer.");
        }
        if (start >= cps) {
            return new BsonString("");
        }
        long end = len < 0 ? cps : Math.min(cps, start + len);
        int from = s.offsetByCodePoints(0, (int) start);
        int to = s.offsetByCodePoints(0, (int) end);
        return new BsonString(s.substring(from, to));
    }

    private static BsonValue indexOf(BsonValue[] a, boolean bytes) {
        if (nullish(a[0])) {
            return nul();
        }
        if (!a[0].isString()) {
            throw new MongoCmdException(bytes ? 40091 : 40093, "$indexOf requires a string as the first argument, found: " + typeOf(a[0]));
        }
        if (!a[1].isString()) {
            throw new MongoCmdException(bytes ? 40092 : 40094, "$indexOf requires a string as the second argument, found: " + typeOf(a[1]));
        }
        String s = a[0].asString().getValue();
        String t = a[1].asString().getValue();
        long from = a.length > 2 ? MongoNum.truncLong(a[2]) : 0;
        long to = a.length > 3 ? MongoNum.truncLong(a[3]) : Long.MAX_VALUE;
        if (bytes) {
            byte[] sb = s.getBytes(StandardCharsets.UTF_8);
            byte[] tb = t.getBytes(StandardCharsets.UTF_8);
            from = Math.max(0, from);
            to = Math.min(to, sb.length);
            for (long i = from; i + tb.length <= to; i++) {
                if (Arrays.equals(Arrays.copyOfRange(sb, (int) i, (int) i + tb.length), tb)) {
                    return new BsonInt32((int) i);
                }
            }
            return new BsonInt32(-1);
        }
        int[] cps = s.codePoints().toArray();
        int[] tps = t.codePoints().toArray();
        from = Math.max(0, from);
        to = Math.min(to, cps.length);
        for (long i = from; i + tps.length <= to; i++) {
            boolean ok = true;
            for (int j = 0; j < tps.length; j++) {
                if (cps[(int) i + j] != tps[j]) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return new BsonInt32((int) i);
            }
        }
        return new BsonInt32(-1);
    }

    private static Expr trimExpr(BsonValue args, String name, boolean left, boolean right) {
        if (!args.isDocument() || !args.asDocument().containsKey("input")) {
            throw new MongoCmdException(50696, name + " requires an object as an argument, found: " + typeOf(args));
        }
        Expr input = parse(args.asDocument().get("input"));
        Expr chars = args.asDocument().containsKey("chars") ? parse(args.asDocument().get("chars")) : null;
        return sc -> {
            BsonValue in = input.eval(sc);
            if (nullish(in)) {
                return nul();
            }
            if (!in.isString()) {
                throw new MongoCmdException(50699, name + " requires its input to be a string, got " + toStringValueSafe(in) + " (of type " + typeOf(in) + ") instead.");
            }
            String s = in.asString().getValue();
            String set = " \t\n\u000B\f\r\u0085                 　\0";
            if (chars != null) {
                BsonValue c = chars.eval(sc);
                if (nullish(c)) {
                    return nul();
                }
                if (!c.isString()) {
                    throw new MongoCmdException(50700, name + " requires 'chars' to be a string, got " + toStringValueSafe(c) + " (of type " + typeOf(c) + ") instead.");
                }
                set = c.asString().getValue();
            }
            int[] cp = s.codePoints().toArray();
            java.util.Set<Integer> chs = new java.util.HashSet<>();
            set.codePoints().forEach(chs::add);
            int i = 0;
            int j = cp.length;
            while (left && i < j && chs.contains(cp[i])) {
                i++;
            }
            while (right && j > i && chs.contains(cp[j - 1])) {
                j--;
            }
            return new BsonString(new String(cp, i, j - i));
        };
    }

    private static String toStringValueSafe(BsonValue v) {
        return v.toString();
    }

    private static Expr replaceExpr(BsonValue args, String name, boolean all) {
        if (!args.isDocument()) {
            throw new MongoCmdException(51751, name + " requires an object as an argument, found: " + typeOf(args));
        }
        BsonDocument d = args.asDocument();
        for (String k : List.of("input", "find", "replacement")) {
            if (!d.containsKey(k)) {
                throw new MongoCmdException(51750, "Missing '" + k + "' parameter to " + name);
            }
        }
        Expr input = parse(d.get("input"));
        Expr find = parse(d.get("find"));
        Expr repl = parse(d.get("replacement"));
        return sc -> {
            BsonValue in = input.eval(sc);
            BsonValue f = find.eval(sc);
            BsonValue r = repl.eval(sc);
            if (nullish(in) || nullish(f) || nullish(r)) {
                return nul();
            }
            for (BsonValue v : new BsonValue[] {in, f, r}) {
                if (!v.isString()) {
                    throw new MongoCmdException(51746, name + " requires that '" + (v == in ? "input" : v == f ? "find" : "replacement")
                            + "' be a string, found: " + toStringValueSafe(v));
                }
            }
            String s = in.asString().getValue();
            String fs = f.asString().getValue();
            String rs = r.asString().getValue();
            if (fs.isEmpty()) {
                if (!all) {
                    return new BsonString(rs + s);
                }
                StringBuilder sb = new StringBuilder(rs);
                s.codePoints().forEach(cp -> sb.appendCodePoint(cp).append(rs));
                return new BsonString(sb.toString());
            }
            return new BsonString(all ? s.replace(fs, rs) : s.replaceFirst(Pattern.quote(fs), Matcher.quoteReplacement(rs)));
        };
    }

    private static Expr regexExpr(BsonValue args, String name, int mode) {
        if (!args.isDocument()) {
            throw new MongoCmdException(51103, name + " expects an object of named arguments but found: " + typeOf(args));
        }
        BsonDocument d = args.asDocument();
        for (String k : d.keySet()) {
            if (!List.of("input", "regex", "options").contains(k)) {
                throw new MongoCmdException(31024, name + " found an unknown argument: " + k);
            }
        }
        if (!d.containsKey("input")) {
            throw new MongoCmdException(31022, name + " requires 'input' parameter");
        }
        if (!d.containsKey("regex")) {
            throw new MongoCmdException(31023, name + " requires 'regex' parameter");
        }
        Expr input = parse(d.get("input"));
        Expr regex = parse(d.get("regex"));
        Expr options = d.containsKey("options") ? parse(d.get("options")) : null;
        return sc -> {
            BsonValue in = input.eval(sc);
            BsonValue re = regex.eval(sc);
            BsonValue op = options == null ? MISSING : options.eval(sc);
            if (!nullish(in) && !in.isString()) {
                throw new MongoCmdException(51104, name + " needs 'input' to be of type string");
            }
            String pat = null;
            String opts = "";
            if (!nullish(re)) {
                if (re.isString()) {
                    pat = re.asString().getValue();
                } else if (re.isRegularExpression()) {
                    pat = re.asRegularExpression().getPattern();
                    opts = re.asRegularExpression().getOptions();
                } else {
                    throw new MongoCmdException(51105, name + " needs 'regex' to be of type string or regex");
                }
            }
            if (!nullish(op)) {
                if (!op.isString()) {
                    throw new MongoCmdException(51106, name + " needs 'options' to be of type string");
                }
                if (!opts.isEmpty() && !op.asString().getValue().isEmpty()) {
                    throw new MongoCmdException(51107, name + ": found regex option(s) specified in both 'regex' and 'option' fields");
                }
                opts = opts + op.asString().getValue();
            }
            if (nullish(in) || pat == null) {
                return mode == 0 ? BsonBoolean.FALSE : mode == 1 ? nul() : new BsonArray();
            }
            Pattern p = MongoMatcher.compileRegex(pat, opts);
            String s = in.asString().getValue();
            Matcher m = p.matcher(s);
            if (mode == 0) {
                return bool(m.find());
            }
            BsonArray all = new BsonArray();
            int from = 0;
            while (from <= s.length() && m.find(from)) {
                BsonArray caps = new BsonArray();
                for (int g = 1; g <= m.groupCount(); g++) {
                    caps.add(m.group(g) == null ? nul() : new BsonString(m.group(g)));
                }
                BsonDocument r = doc("match", new BsonString(m.group()), "idx", new BsonInt32(s.codePointCount(0, m.start())),
                        "captures", caps);
                if (mode == 1) {
                    return r;
                }
                all.add(r);
                from = m.end() == m.start() ? m.end() + 1 : m.end();
            }
            return mode == 1 ? nul() : all;
        };
    }

    // ---- $group-style accumulator evaluation helpers shared with MongoAgg / window

    static List<BsonValue> unused() {
        return new ArrayList<>(new LinkedHashSet<>());
    }
}
