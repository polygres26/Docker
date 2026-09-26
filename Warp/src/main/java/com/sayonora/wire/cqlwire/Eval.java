package com.sayonora.wire.cqlwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sayonora.wire.cqlwire.Ast.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Term evaluation: literal coercion by expected type, bind markers, scalar functions, arithmetic and JSON conversion. */
final class Eval {

    private Eval() {
    }

    /** Values bound to a statement's markers. */
    static final class Binds {
        final byte[][] values;
        final String[] names;

        Binds(byte[][] values, String[] names) {
            this.values = values;
            this.names = names;
        }

        static final Binds NONE = new Binds(new byte[0][], null);

        byte[] get(Bind b) {
            if (names != null && b.name() != null) {
                for (int i = 0; i < names.length; i++) {
                    if (b.name().equals(names[i])) {
                        return values[i];
                    }
                }
                throw CqlError.invalid("Missing value for bind marker :" + b.name());
            }
            if (b.index() >= values.length) {
                throw CqlError.invalid("there were " + values.length + " markers(?) in CQL but " + values.length + " bound variables");
            }
            return values[b.index()];
        }
    }

    record TV(Object v, CqlType t) {
    }

    /** A per-row lookup used when evaluating selectors. */
    interface RowCtx {
        TV column(String name, Term index, String field);

        TV meta(String fn, String col);
    }

    // ------------------------------------------------------------------ time helpers

    private static final AtomicLong LAST_TS = new AtomicLong();

    /** Monotonic write timestamp in microseconds. */
    static long nowMicros() {
        long t = System.currentTimeMillis() * 1000;
        return LAST_TS.updateAndGet(prev -> Math.max(prev + 1, t));
    }

    private static final long UUID_EPOCH_OFFSET_100NS = 0x01b21dd213814000L;
    private static final AtomicLong LAST_UUID_TIME = new AtomicLong();
    private static final long CLOCK_AND_NODE = 0x8000000000000000L | (new java.util.Random().nextLong() & 0x3fffffffffffffffL);

    static UUID timeUuid(long unixMillis, long lsb) {
        return timeUuid(unixMillis, lsb, 0);
    }

    static UUID timeUuid(long unixMillis, long lsb, int extra100ns) {
        long t = unixMillis * 10000 + UUID_EPOCH_OFFSET_100NS + extra100ns;
        long msb = ((t & 0xFFFFFFFFL) << 32) | (((t >>> 32) & 0xFFFFL) << 16) | 0x1000L | ((t >>> 48) & 0x0FFFL);
        return new UUID(msb, lsb);
    }

    static UUID nowUuid() {
        long t100 = LAST_UUID_TIME.updateAndGet(p -> Math.max(p + 1, System.currentTimeMillis() * 10000 + UUID_EPOCH_OFFSET_100NS));
        long msb = ((t100 & 0xFFFFFFFFL) << 32) | (((t100 >>> 32) & 0xFFFFL) << 16) | 0x1000L | ((t100 >>> 48) & 0x0FFFL);
        return new UUID(msb, CLOCK_AND_NODE);
    }

    static long uuidMillis(UUID u) {
        if (u.version() != 1) {
            throw CqlError.invalid("Cannot extract timestamp from a version " + u.version() + " UUID");
        }
        return (u.timestamp() - UUID_EPOCH_OFFSET_100NS) / 10000;
    }

    // ------------------------------------------------------------------ coercion

    private static String kindName(Object v, boolean isString) {
        if (isString) {
            return "STRING";
        }
        if (v instanceof BigInteger) {
            return "INTEGER";
        }
        if (v instanceof BigDecimal || v instanceof Double) {
            return "FLOAT";
        }
        if (v instanceof Boolean) {
            return "BOOLEAN";
        }
        if (v instanceof byte[]) {
            return "BLOB";
        }
        if (v instanceof UUID) {
            return "UUID";
        }
        if (v instanceof long[][]) {
            return "DURATION";
        }
        return "?";
    }

    private static String litText(Object v) {
        if (v instanceof byte[] b) {
            return "0x" + CqlType.hex(b);
        }
        if (v instanceof long[][] d) {
            return CqlType.formatDuration(d[0]);
        }
        return String.valueOf(v);
    }

    private static CqlError badConst(Lit l, CqlType want, String label) {
        return CqlError.invalid("Invalid " + kindName(l.value(), l.isString()) + " constant (" + litText(l.value()) + ") for \""
                + (label == null ? "value" : label) + "\" of type " + want.cql());
    }

    /** Converts a term to a value of type {@code want}. */
    static Object coerce(Term t, CqlType want, Binds b, String label) {
        if (t instanceof Lit l) {
            return literal(l, want, label);
        }
        if (t instanceof Bind bd) {
            byte[] raw = b.get(bd);
            if (raw == Wire.UNSET) {
                return UNSET_VALUE;
            }
            try {
                return want.deserialize(raw);
            } catch (CqlError e) {
                throw e;
            } catch (RuntimeException e) {
                throw CqlError.invalid("Invalid value for " + label + ": " + e);
            }
        }
        if (t instanceof ListLit ll) {
            if (want.k == CqlType.K.LIST) {
                List<Object> out = new ArrayList<>();
                for (Term x : ll.items()) {
                    out.add(elem(x, want.args.get(0), b, label));
                }
                return out;
            }
            throw CqlError.invalid("Unexpected receiver type '" + want.cql() + "'; only list and vector are expected");
        }
        if (t instanceof BraceLit br) {
            return brace(br, want, b, label);
        }
        if (t instanceof TupleLit tl) {
            if (want.k != CqlType.K.TUPLE) {
                throw CqlError.invalid("Invalid tuple literal for " + label + ": type " + want.cql() + " is not a tuple");
            }
            if (tl.items().size() > want.args.size()) {
                throw CqlError.invalid("Invalid tuple literal for " + label + ": too many elements. Type " + want.cql() + " expects "
                        + want.args.size() + " but got " + tl.items().size());
            }
            Object[] o = new Object[want.args.size()];
            for (int i = 0; i < tl.items().size(); i++) {
                o[i] = coerce(tl.items().get(i), want.args.get(i), b, label);
            }
            return o;
        }
        if (t instanceof TypeHint th) {
            CqlType ht = CqlType.natives().get(th.type());
            return convert(evalTyped(th.t(), ht, b, null), want, label);
        }
        return convert(evalTyped(t, want, b, null), want, label);
    }

    static final Object UNSET_VALUE = new Object();

    private static Object elem(Term x, CqlType t, Binds b, String label) {
        Object v = coerce(x, t, b, label);
        if (v == null) {
            throw CqlError.invalid("null is not supported inside collections");
        }
        return v;
    }

    private static Object brace(BraceLit br, CqlType want, Binds b, String label) {
        switch (want.k) {
            case SET -> {
                if (!br.pairs().isEmpty() || !br.fields().isEmpty()) {
                    throw CqlError.invalid("Invalid set literal for " + label + ": bad value");
                }
                TreeSet<Object> s = new TreeSet<>(want.args.get(0));
                for (Term x : br.items()) {
                    s.add(elem(x, want.args.get(0), b, label));
                }
                return s;
            }
            case MAP -> {
                if (!br.items().isEmpty() || !br.fields().isEmpty()) {
                    throw CqlError.invalid("Invalid map literal for " + label + ": bad value");
                }
                TreeMap<Object, Object> m = new TreeMap<>(want.args.get(0));
                for (Term[] p : br.pairs()) {
                    Object key;
                    try {
                        key = elem(p[0], want.args.get(0), b, label);
                    } catch (CqlError ce) {
                        if (ce.code == CqlError.INVALID && ce.getMessage().startsWith("Invalid ")) {
                            throw CqlError.invalid("Invalid map literal for " + label + ": key " + Select.describe(p[0]) + " is not of type "
                                    + want.args.get(0).cql());
                        }
                        throw ce;
                    }
                    m.put(key, elem(p[1], want.args.get(1), b, label)); // equal keys in one literal: Cassandra's pick is an accident of hashing
                }
                return m;
            }
            case UDT -> {
                if (!br.items().isEmpty() || !br.pairs().isEmpty()) {
                    throw CqlError.invalid("Invalid user type literal for " + label + " of type " + want.cql());
                }
                Object[] o = new Object[want.args.size()];
                for (var e : br.fields().entrySet()) {
                    int i = want.fieldNames.indexOf(e.getKey());
                    if (i < 0) {
                        throw CqlError.invalid("Unknown field '" + e.getKey() + "' in value of user defined type " + want.udtName);
                    }
                    o[i] = coerce(e.getValue(), want.args.get(i), b, label);
                }
                return o;
            }
            default -> throw CqlError.invalid("Invalid " + (br.pairs().isEmpty() ? "set" : "map") + " literal for " + label + " of type " + want.cql());
        }
    }

    static Object literal(Lit l, CqlType want, String label) {
        Object v = l.value();
        if (v == null) {
            return null;
        }
        CqlType.K k = want.k;
        try {
            switch (k) {
                case ASCII, VARCHAR -> {
                    if (l.isString()) {
                        String s = (String) v;
                        if (k == CqlType.K.ASCII) {
                            for (int i = 0; i < s.length(); i++) {
                                if (s.charAt(i) > 127) {
                                    throw CqlError.invalid("Invalid ASCII character in string literal");
                                }
                            }
                        }
                        return s;
                    }
                }
                case BIGINT, COUNTER -> {
                    if (v instanceof BigInteger n && !l.isString()) {
                        return n.longValueExact();
                    }
                }
                case INT -> {
                    if (v instanceof BigInteger n && !l.isString()) {
                        return n.intValueExact();
                    }
                }
                case SMALLINT -> {
                    if (v instanceof BigInteger n && !l.isString()) {
                        return n.shortValueExact();
                    }
                }
                case TINYINT -> {
                    if (v instanceof BigInteger n && !l.isString()) {
                        return n.byteValueExact();
                    }
                }
                case VARINT -> {
                    if (v instanceof BigInteger n && !l.isString()) {
                        return n;
                    }
                }
                case DECIMAL -> {
                    if (!l.isString()) {
                        if (v instanceof Double dd && !dd.isNaN() && !dd.isInfinite()) {
                            return BigDecimal.ZERO;
                        }
                        if (v instanceof BigInteger n) {
                            return new BigDecimal(n);
                        }
                        if (v instanceof BigDecimal d) {
                            return d;
                        }
                    }
                }
                case DOUBLE -> {
                    if (!l.isString()) {
                        if (v instanceof BigInteger n) {
                            return n.doubleValue();
                        }
                        if (v instanceof BigDecimal d) {
                            if (Double.isInfinite(d.doubleValue())) {
                                throw CqlError.syntax("Failed parsing statement: reason: NumberFormatException Character I is neither a decimal digit "
                                        + "number, decimal point, nor \"e\" notation exponential mark.");
                            }
                            return d.doubleValue();
                        }
                        if (v instanceof Double d) {
                            return d;
                        }
                    }
                }
                case FLOAT -> {
                    if (!l.isString()) {
                        if (v instanceof BigInteger n) {
                            return n.floatValue();
                        }
                        if (v instanceof BigDecimal d) {
                            return d.floatValue();
                        }
                        if (v instanceof Double d) {
                            return d.floatValue();
                        }
                    }
                }
                case BOOLEAN -> {
                    if (v instanceof Boolean) {
                        return v;
                    }
                }
                case BLOB -> {
                    if (v instanceof byte[]) {
                        return v;
                    }
                }
                case UUID -> {
                    if (v instanceof UUID) {
                        return v;
                    }
                    if (l.isString()) {
                        return UUID.fromString((String) v);
                    }
                }
                case TIMEUUID -> {
                    UUID u = v instanceof UUID x ? x : l.isString() ? UUID.fromString((String) v) : null;
                    if (u != null) {
                        if (u.version() != 1) {
                            throw CqlError.invalid("Invalid version for TimeUUID type.");
                        }
                        return u;
                    }
                }
                case INET -> {
                    if (l.isString()) {
                        return InetAddress.getByName((String) v);
                    }
                }
                case TIMESTAMP -> {
                    if (l.isString()) {
                        return CqlType.parseTimestamp((String) v);
                    }
                    if (v instanceof BigInteger n) {
                        return n.longValueExact();
                    }
                }
                case DATE -> {
                    if (l.isString()) {
                        return CqlType.parseDate((String) v);
                    }
                    if (v instanceof BigInteger n) {
                        return (int) (n.longValueExact() - CqlType.DATE_OFFSET);
                    }
                }
                case TIME -> {
                    if (l.isString()) {
                        return CqlType.parseTime((String) v);
                    }
                    if (v instanceof BigInteger n) {
                        return n.longValueExact();
                    }
                }
                case DURATION -> {
                    if (v instanceof long[][] d) {
                        return d[0];
                    }
                }
                default -> {
                }
            }
        } catch (ArithmeticException e) {
            throw CqlError.invalid("Unable to make " + want.cql() + " from '" + litText(v) + "'");
        } catch (UnknownHostException | IllegalArgumentException e) {
            throw CqlError.invalid("Unable to make " + want.cql() + " from '" + litText(v) + "'");
        }
        throw badConst(l, want, label);
    }

    // ------------------------------------------------------------------ typed evaluation

    static TV literalTyped(Lit l) {
        Object v = l.value();
        if (v == null) {
            return new TV(null, null);
        }
        if (l.isString()) {
            return new TV(v, CqlType.TEXT);
        }
        if (v instanceof BigInteger n) {
            if (n.bitLength() < 32) {
                return new TV(n.intValue(), CqlType.INT);
            }
            if (n.bitLength() < 64) {
                return new TV(n.longValue(), CqlType.BIGINT);
            }
            return new TV(n, CqlType.VARINT);
        }
        if (v instanceof BigDecimal d) {
            return new TV(d, CqlType.DECIMAL);
        }
        if (v instanceof Double d) {
            return new TV(d, CqlType.DOUBLE);
        }
        if (v instanceof Boolean) {
            return new TV(v, CqlType.BOOLEAN);
        }
        if (v instanceof byte[]) {
            return new TV(v, CqlType.BLOB);
        }
        if (v instanceof UUID u) {
            return new TV(u, u.version() == 1 ? CqlType.TIMEUUID : CqlType.UUID_T);
        }
        if (v instanceof long[][] d) {
            return new TV(d[0], CqlType.of(CqlType.K.DURATION));
        }
        throw CqlError.invalid("Unsupported literal");
    }

    /** Evaluates a term (function calls, arithmetic, casts, columns) with a type hint for untyped markers. */
    static TV evalTyped(Term t, CqlType hint, Binds b, RowCtx row) {
        if (t instanceof Lit l) {
            if (hint != null && !l.isString() && l.value() != null && !(hint.k == CqlType.K.ASCII || hint.k == CqlType.K.VARCHAR)) {
                try {
                    return new TV(literal(l, hint, null), hint);
                } catch (CqlError e) {
                    // fall back to the natural type
                }
            }
            return literalTyped(l);
        }
        if (t instanceof Bind bd) {
            byte[] raw = b.get(bd);
            if (hint == null) {
                throw CqlError.invalid("Unable to infer the type of bind marker; add a type hint");
            }
            return new TV(hint.deserialize(raw), hint);
        }
        if (t instanceof ColRef c) {
            if (row == null) {
                throw CqlError.invalid("Unknown identifier " + c.name());
            }
            return row.column(c.name(), c.index(), c.field());
        }
        if (t instanceof TypeHint th) {
            CqlType ht = CqlType.natives().get(th.type());
            if (ht == null) {
                throw CqlError.invalid("Unknown type " + th.type());
            }
            return new TV(coerce(th.t(), ht, b, null), ht);
        }
        if (t instanceof Cast c) {
            return cast(evalTyped(c.t(), null, b, row), c.type());
        }
        if (t instanceof Arith a) {
            TV l = evalTyped(a.l(), hint, b, row), r = evalTyped(a.r(), hint == null ? l.t() : hint, b, row);
            try {
                return arith(a.op(), l, r);
            } catch (CqlError ce) {
                if (ce.getMessage() != null && ce.getMessage().startsWith("the '") && ce.getMessage().contains("operation is not supported between")) {
                    throw CqlError.invalid("the '" + a.op() + "' operation is not supported between " + Select.describe(a.l()) + " and " + Select.describe(a.r()));
                }
                throw ce;
            }
        }
        if (t instanceof Func f) {
            return func(f, hint, b, row);
        }
        if (hint != null) {
            return new TV(coerce(t, hint, b, null), hint);
        }
        throw CqlError.invalid("Cannot infer the type of this expression");
    }

    /** Converts a typed value to the type an assignment expects. */
    static Object convert(TV tv, CqlType want, String label) {
        if (tv.v() == null) {
            return null;
        }
        CqlType have = tv.t();
        if (have == null || have.sameShape(want)) {
            return tv.v();
        }
        // numeric widening as CQL allows for function results
        if (have.isNumeric() && want.isNumeric() && want.k != CqlType.K.COUNTER) {
            return numeric(tv.v(), want);
        }
        if (have.isString() && want.isString()) {
            return tv.v();
        }
        if (have.k == CqlType.K.TIMEUUID && want.k == CqlType.K.UUID) {
            return tv.v();
        }
        throw CqlError.invalid("Type error: cannot assign result of function/expression (type " + have.cql() + ") to " + label
                + " (type " + want.cql() + ")");
    }

    static Object numeric(Object v, CqlType want) {
        BigDecimal d = toBig(v);
        try {
            return switch (want.k) {
                case TINYINT -> d.byteValueExact();
                case SMALLINT -> d.shortValueExact();
                case INT -> d.intValueExact();
                case BIGINT, COUNTER -> d.longValueExact();
                case VARINT -> d.toBigIntegerExact();
                case DECIMAL -> d;
                case DOUBLE -> d.doubleValue();
                case FLOAT -> d.floatValue();
                default -> v;
            };
        } catch (ArithmeticException e) {
            throw CqlError.invalid("Value out of range for " + want.cql());
        }
    }

    static BigDecimal toBig(Object v) {
        if (v instanceof BigDecimal d) {
            return d;
        }
        if (v instanceof BigInteger n) {
            return new BigDecimal(n);
        }
        if (v instanceof Double x) {
            if (x.isNaN() || x.isInfinite()) {
                throw CqlError.invalid("Cannot convert NaN/Infinity");
            }
            return BigDecimal.valueOf(x);
        }
        if (v instanceof Float x) {
            return new BigDecimal(x.toString());
        }
        return BigDecimal.valueOf(((Number) v).longValue());
    }

    private static int rank(CqlType t) {
        return switch (t.k) {
            case TINYINT -> 1;
            case SMALLINT -> 2;
            case INT -> 3;
            case BIGINT, COUNTER -> 4;
            case VARINT -> 5;
            case FLOAT -> 6;
            case DOUBLE -> 7;
            case DECIMAL -> 8;
            default -> 0;
        };
    }

    static TV arith(char op, TV l, TV r) {
        if (l.v() == null || r.v() == null) {
            CqlType lt = l.t(), rt2 = r.t();
            if (lt != null && rt2 != null && lt.isNumeric() && rt2.isNumeric()) {
                CqlType w = rank(lt) >= rank(rt2) ? lt : rt2;
                w = w.k == CqlType.K.COUNTER ? CqlType.BIGINT : rank(w) < 4 && rank(w) > 0 ? CqlType.of(rank(w) == 3 ? CqlType.K.INT : w.k) : w;
                return new TV(null, w);
            }
            return new TV(null, lt != null ? lt : rt2);
        }
        if (l.t().k == CqlType.K.TIMESTAMP || l.t().k == CqlType.K.DATE || l.t().k == CqlType.K.TIME) {
            if (r.t().k == CqlType.K.DURATION && (op == '+' || op == '-')) {
                long[] d = (long[]) r.v();
                int sgn = op == '+' ? 1 : -1;
                if (l.t().k == CqlType.K.TIMESTAMP) {
                    java.time.ZonedDateTime z = Instant.ofEpochMilli((Long) l.v()).atZone(java.time.ZoneOffset.UTC);
                    z = z.plusMonths(sgn * d[0]).plusDays(sgn * d[1]).plusNanos(sgn * d[2]);
                    return new TV(z.toInstant().toEpochMilli(), l.t());
                }
                if (l.t().k == CqlType.K.DATE) {
                    if (d[2] != 0) {
                        throw CqlError.functionFailure("operation", "the operation 'date " + op + " duration' failed: The duration must have a day precision. Was: "
                                + CqlType.formatDuration(d));
                    }
                    LocalDate z = LocalDate.ofEpochDay((Integer) l.v()).plusMonths(sgn * d[0]).plusDays(sgn * d[1]);
                    return new TV((int) z.toEpochDay(), l.t());
                }
            }
        }
        if (!l.t().isNumeric() || !r.t().isNumeric()) {
            if (op == '+' && l.t().isString() && r.t().isString()) {
                return new TV((String) l.v() + r.v(), CqlType.TEXT);
            }
            throw CqlError.invalid("the '" + op + "' operation is not supported between " + l.t().cql() + " and " + r.t().cql());
        }
        CqlType rt = rank(l.t()) >= rank(r.t()) ? l.t() : r.t();
        if (rank(rt) < 4 && rank(rt) > 0) {
            rt = CqlType.of(rank(rt) == 3 ? CqlType.K.INT : rt.k);
        }
        if (rt.k == CqlType.K.COUNTER) {
            rt = CqlType.BIGINT;
        }
        Object out;
        try {
            if (rt.k == CqlType.K.DOUBLE || rt.k == CqlType.K.FLOAT) {
                double a = ((Number) num(l.v())).doubleValue(), c = ((Number) num(r.v())).doubleValue();
                double x = switch (op) {
                    case '+' -> a + c;
                    case '-' -> a - c;
                    case '*' -> a * c;
                    case '/' -> a / c;
                    default -> a % c;
                };
                out = rt.k == CqlType.K.DOUBLE ? (Object) x : (Object) (float) x;
            } else {
                BigDecimal a = toBig(l.v()), c = toBig(r.v());
                BigDecimal x;
                boolean integral = rt.k != CqlType.K.DECIMAL;
                switch (op) {
                    case '+' -> x = a.add(c);
                    case '-' -> x = a.subtract(c);
                    case '*' -> x = a.multiply(c);
                    case '/' -> {
                        if (c.signum() == 0) {
                            throw CqlError.functionFailure("operation", "the operation '" + l.t().cql() + " / " + r.t().cql() + "' failed: / by zero");
                        }
                        x = integral ? new BigDecimal(a.toBigInteger().divide(c.toBigInteger())) : a.divide(c, java.math.MathContext.DECIMAL128);
                    }
                    default -> {
                        if (c.signum() == 0) {
                            throw CqlError.functionFailure("operation", "the operation '" + l.t().cql() + " % " + r.t().cql() + "' failed: / by zero");
                        }
                        x = integral ? new BigDecimal(a.toBigInteger().remainder(c.toBigInteger())) : a.remainder(c);
                    }
                }
                out = integral ? wrapInt(x.toBigInteger(), rt) : x;
            }
        } catch (ArithmeticException e) {
            throw CqlError.invalid("Arithmetic error: " + e.getMessage());
        }
        return new TV(out, rt);
    }

    private static Object num(Object v) {
        return v instanceof BigDecimal || v instanceof BigInteger ? ((Number) v) : v;
    }

    private static Object wrapInt(BigInteger x, CqlType t) {
        return switch (t.k) {
            case TINYINT -> (byte) x.longValue();
            case SMALLINT -> (short) x.longValue();
            case INT -> (int) x.longValue();
            case BIGINT -> x.longValue();
            default -> x;
        };
    }

    // ------------------------------------------------------------------ functions

    static TV cast(TV v, String to) {
        CqlType target = CqlType.natives().get(to);
        if (target == null) {
            throw CqlError.invalid("Unknown type " + to);
        }
        if (v.v() == null) {
            return new TV(null, target);
        }
        CqlType from = v.t();
        if (target.k == CqlType.K.VARCHAR || target.k == CqlType.K.ASCII) {
            if (from.k == CqlType.K.DURATION || from.k == CqlType.K.BLOB || from.isCollection() || from.k == CqlType.K.TUPLE || from.k == CqlType.K.UDT) {
                throw CqlError.invalid("Invalid call to function system.cast_as_text, none of its type signatures match");
            }
            return new TV(from.k == CqlType.K.VARCHAR || from.k == CqlType.K.ASCII ? (String) v.v() : from.format(v.v()), target);
        }
        if (from.isNumeric() && target.isNumeric()) {
            if (target.k == CqlType.K.DOUBLE || target.k == CqlType.K.FLOAT) {
                double d = v.v() instanceof Number nn ? nn.doubleValue() : 0;
                return new TV(target.k == CqlType.K.DOUBLE ? (Object) d : (Object) (float) d, target);
            }
            BigDecimal bd = v.v() instanceof Double x ? BigDecimal.valueOf(x) : toBig(v.v());
            if (target.k == CqlType.K.DECIMAL) {
                return new TV(bd, target);
            }
            BigInteger bi = bd.toBigInteger();
            return new TV(wrapInt(bi, target.k == CqlType.K.COUNTER ? CqlType.BIGINT : target), target);
        }
        if (from.k == CqlType.K.TIMESTAMP && target.k == CqlType.K.DATE) {
            return new TV((int) Math.floorDiv((Long) v.v(), 86_400_000L), target);
        }
        if (from.k == CqlType.K.DATE && target.k == CqlType.K.TIMESTAMP) {
            return new TV((long) (Integer) v.v() * 86_400_000L, target);
        }
        if (from.k == CqlType.K.TIMEUUID && target.k == CqlType.K.TIMESTAMP) {
            return new TV(uuidMillis((UUID) v.v()), target);
        }
        if (from.k == CqlType.K.TIMEUUID && target.k == CqlType.K.DATE) {
            return new TV((int) Math.floorDiv(uuidMillis((UUID) v.v()), 86_400_000L), target);
        }
        if (from.k == CqlType.K.TIMESTAMP && (target.k == CqlType.K.BIGINT)) {
            return new TV(v.v(), target);
        }
        if (from.k == target.k) {
            return v;
        }
        throw CqlError.invalid("Invalid call to function system.cast_as_" + target.cql() + ", none of its type signatures match");
    }

    private static final Map<String, CqlType> NATIVES = CqlType.natives();

    private static TV arg(Func f, int i, CqlType hint, Binds b, RowCtx row) {
        return evalTyped(f.args().get(i), hint, b, row);
    }

    static TV func(Func f, CqlType hint, Binds b, RowCtx row) {
        String n = f.name().toLowerCase();
        int na = f.args().size();
        switch (n) {
            case "now" -> {
                return new TV(nowUuid(), CqlType.TIMEUUID);
            }
            case "uuid" -> {
                return new TV(UUID.randomUUID(), CqlType.UUID_T);
            }
            case "currenttimestamp" -> {
                return new TV(System.currentTimeMillis(), CqlType.TIMESTAMP);
            }
            case "currentdate" -> {
                return new TV((int) LocalDate.now(java.time.ZoneOffset.UTC).toEpochDay(), CqlType.DATE);
            }
            case "currenttime" -> {
                return new TV(java.time.LocalTime.now(java.time.ZoneOffset.UTC).toNanoOfDay(), CqlType.TIME);
            }
            case "currenttimeuuid" -> {
                return new TV(nowUuid(), CqlType.TIMEUUID);
            }
            case "totimestamp" -> {
                TV a = arg(f, 0, null, b, row);
                if (a.v() == null) {
                    return new TV(null, CqlType.TIMESTAMP);
                }
                return cast(a, "timestamp");
            }
            case "todate" -> {
                return cast(arg(f, 0, null, b, row), "date");
            }
            case "tounixtimestamp" -> {
                TV a = arg(f, 0, null, b, row);
                if (a.v() == null) {
                    return new TV(null, CqlType.BIGINT);
                }
                if (a.t().k == CqlType.K.TIMESTAMP) {
                    return new TV(a.v(), CqlType.BIGINT);
                }
                if (a.t().k == CqlType.K.DATE) {
                    return new TV((long) (Integer) a.v() * 86_400_000L, CqlType.BIGINT);
                }
                return new TV(uuidMillis((UUID) a.v()), CqlType.BIGINT);
            }
            case "mintimeuuid", "maxtimeuuid" -> {
                TV a = arg(f, 0, CqlType.TIMESTAMP, b, row);
                if (a.v() == null) {
                    return new TV(null, CqlType.TIMEUUID);
                }
                long ms = a.t().k == CqlType.K.TIMESTAMP || a.t().isNumeric() ? ((Number) a.v()).longValue() : CqlType.parseTimestamp((String) a.v());
                return new TV(n.startsWith("min") ? timeUuid(ms, 0x8080808080808080L) : timeUuid(ms, 0x7f7f7f7f7f7f7f7fL, 9999), CqlType.TIMEUUID);
            }
            case "abs" -> {
                TV a = arg(f, 0, hint, b, row);
                if (a.v() == null) {
                    return a;
                }
                return new TV(switch (a.t().k) {
                    case TINYINT -> (byte) Math.abs((Byte) a.v());
                    case SMALLINT -> (short) Math.abs((Short) a.v());
                    case INT -> Math.abs((Integer) a.v());
                    case BIGINT -> Math.abs((Long) a.v());
                    case VARINT -> ((BigInteger) a.v()).abs();
                    case DECIMAL -> ((BigDecimal) a.v()).abs();
                    case FLOAT -> Math.abs((Float) a.v());
                    default -> Math.abs((Double) a.v());
                }, a.t());
            }
            case "tojson" -> {
                TV a = arg(f, 0, null, b, row);
                return new TV(toJson(a.v(), a.t()), CqlType.TEXT);
            }
            case "fromjson" -> {
                if (hint == null) {
                    throw CqlError.invalid("fromjson() cannot be used in the selection clause of a SELECT statement");
                }
                TV a = arg(f, 0, CqlType.TEXT, b, row);
                return new TV(a.v() == null ? null : fromJson(JsonParser.parseString((String) a.v()), hint), hint);
            }
            case "token" -> {
                throw CqlError.invalid("token() is only supported on partition key columns");
            }
            case "ttl", "writetime", "maxwritetime" -> {
                if (row == null || !(f.args().size() == 1 && f.args().get(0) instanceof ColRef c)) {
                    throw CqlError.invalid("Cannot use selection function " + n + " here");
                }
                return row.meta(n, c.name());
            }
            case "typeof" -> {
                TV a = arg(f, 0, null, b, row);
                return new TV(a.t() == null ? "null" : a.t().cql(), CqlType.TEXT);
            }
            default -> {
            }
        }
        // blob conversions: <type>AsBlob / blobAs<Type>
        if (n.endsWith("asblob") && na == 1) {
            String tn = n.substring(0, n.length() - 6);
            CqlType t = NATIVES.get(tn);
            if (t != null) {
                TV a = arg(f, 0, t, b, row);
                return new TV(a.v() == null ? null : t.serialize(convert(a, t, "value")), CqlType.BLOB);
            }
        }
        if (n.startsWith("blobas") && na == 1) {
            CqlType t = NATIVES.get(n.substring(6));
            if (t != null) {
                TV a = arg(f, 0, CqlType.BLOB, b, row);
                if (a.v() == null) {
                    return new TV(null, t);
                }
                try {
                    return new TV(t.deserialize((byte[]) a.v()), t);
                } catch (RuntimeException e) {
                    throw CqlError.invalid("In call to function system." + n + ", value 0x" + CqlType.hex((byte[]) a.v())
                            + " is not a valid binary representation for type " + t.cql());
                }
            }
        }
        throw CqlError.invalid("Unknown function '" + n + "'");
    }

    // ------------------------------------------------------------------ JSON

    static String toJson(Object v, CqlType t) {
        StringBuilder b = new StringBuilder();
        json(b, v, t);
        return b.toString();
    }

    static void jsonString(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                case '\b' -> b.append("\\b");
                case '\f' -> b.append("\\f");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        b.append('"');
    }

    private static void json(StringBuilder b, Object v, CqlType t) {
        if (v == null) {
            b.append("null");
            return;
        }
        switch (t.k) {
            case ASCII, VARCHAR -> jsonString(b, (String) v);
            case BOOLEAN, INT, BIGINT, SMALLINT, TINYINT, COUNTER, VARINT -> b.append(v);
            case DECIMAL -> b.append(v);
            case DOUBLE, FLOAT -> {
                double d = ((Number) v).doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    b.append(d > 0 ? "\"Infinity\"" : d < 0 ? "\"-Infinity\"" : "\"NaN\"");
                } else {
                    b.append(v);
                }
            }
            case TIMESTAMP -> jsonString(b, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS'Z'")
                    .withZone(java.time.ZoneOffset.UTC).format(Instant.ofEpochMilli((Long) v)));
            case LIST, SET -> {
                b.append('[');
                boolean first = true;
                for (Object x : (java.util.Collection<?>) v) {
                    b.append(first ? "" : ", ");
                    first = false;
                    json(b, x, t.args.get(0));
                }
                b.append(']');
            }
            case MAP -> {
                b.append('{');
                boolean first = true;
                for (var e : ((Map<?, ?>) v).entrySet()) {
                    b.append(first ? "" : ", ");
                    first = false;
                    StringBuilder k = new StringBuilder();
                    json(k, e.getKey(), t.args.get(0));
                    String ks = k.toString();
                    jsonString(b, t.args.get(0).isString() ? (String) e.getKey() : ks.startsWith("\"") ? ks.substring(1, ks.length() - 1) : ks);
                    b.append(": ");
                    json(b, e.getValue(), t.args.get(1));
                }
                b.append('}');
            }
            case TUPLE -> {
                b.append('[');
                Object[] x = (Object[]) v;
                for (int i = 0; i < t.args.size(); i++) {
                    b.append(i > 0 ? ", " : "");
                    json(b, i < x.length ? x[i] : null, t.args.get(i));
                }
                b.append(']');
            }
            case UDT -> {
                b.append('{');
                Object[] x = (Object[]) v;
                for (int i = 0; i < t.args.size(); i++) {
                    b.append(i > 0 ? ", " : "");
                    jsonString(b, t.fieldNames.get(i));
                    b.append(": ");
                    json(b, i < x.length ? x[i] : null, t.args.get(i));
                }
                b.append('}');
            }
            default -> jsonString(b, t.format(v));
        }
    }

    static Object fromJson(JsonElement e, CqlType t) {
        if (e == null || e.isJsonNull()) {
            return null;
        }
        try {
            switch (t.k) {
                case ASCII, VARCHAR -> {
                    return e.getAsString();
                }
                case BOOLEAN -> {
                    return e.getAsBoolean();
                }
                case INT -> {
                    return jsonNum(e).intValueExact();
                }
                case BIGINT, COUNTER -> {
                    return jsonNum(e).longValueExact();
                }
                case SMALLINT -> {
                    return jsonNum(e).shortValueExact();
                }
                case TINYINT -> {
                    return jsonNum(e).byteValueExact();
                }
                case VARINT -> {
                    return jsonNum(e).toBigIntegerExact();
                }
                case DECIMAL -> {
                    return jsonNum(e);
                }
                case DOUBLE -> {
                    return e.getAsJsonPrimitive().isString() ? Double.parseDouble(e.getAsString()) : e.getAsDouble();
                }
                case FLOAT -> {
                    return e.getAsJsonPrimitive().isString() ? Float.parseFloat(e.getAsString()) : e.getAsFloat();
                }
                case BLOB -> {
                    String s = e.getAsString();
                    return CqlType.unhex(s.startsWith("0x") ? s.substring(2) : s);
                }
                case UUID, TIMEUUID -> {
                    return UUID.fromString(e.getAsString());
                }
                case INET -> {
                    return InetAddress.getByName(e.getAsString());
                }
                case TIMESTAMP -> {
                    return e.getAsJsonPrimitive().isNumber() ? e.getAsLong() : CqlType.parseTimestamp(e.getAsString());
                }
                case DATE -> {
                    return CqlType.parseDate(e.getAsString());
                }
                case TIME -> {
                    return CqlType.parseTime(e.getAsString());
                }
                case DURATION -> {
                    return CqlType.parseDuration(e.getAsString());
                }
                case LIST -> {
                    List<Object> l = new ArrayList<>();
                    for (JsonElement x : e.getAsJsonArray()) {
                        l.add(fromJson(x, t.args.get(0)));
                    }
                    return l;
                }
                case SET -> {
                    TreeSet<Object> s = new TreeSet<>(t.args.get(0));
                    for (JsonElement x : e.getAsJsonArray()) {
                        s.add(fromJson(x, t.args.get(0)));
                    }
                    return s;
                }
                case MAP -> {
                    TreeMap<Object, Object> m = new TreeMap<>(t.args.get(0));
                    for (var en : e.getAsJsonObject().entrySet()) {
                        Object k = t.args.get(0).isString() ? en.getKey() : fromJson(JsonParser.parseString(
                                t.args.get(0).k == CqlType.K.BOOLEAN || t.args.get(0).isNumeric() ? en.getKey() : "\"" + en.getKey() + "\""),
                                t.args.get(0));
                        m.put(k, fromJson(en.getValue(), t.args.get(1)));
                    }
                    return m;
                }
                case TUPLE -> {
                    JsonArray a = e.getAsJsonArray();
                    Object[] o = new Object[t.args.size()];
                    for (int i = 0; i < a.size() && i < o.length; i++) {
                        o[i] = fromJson(a.get(i), t.args.get(i));
                    }
                    return o;
                }
                default -> {
                    JsonObject j = e.getAsJsonObject();
                    Object[] o = new Object[t.args.size()];
                    for (var en : j.entrySet()) {
                        int i = t.fieldNames.indexOf(en.getKey());
                        if (i < 0) {
                            throw CqlError.invalid("Unknown field '" + en.getKey() + "' in value of user defined type " + t.udtName);
                        }
                        o[i] = fromJson(en.getValue(), t.args.get(i));
                    }
                    return o;
                }
            }
        } catch (CqlError x) {
            throw x;
        } catch (NumberFormatException | ArithmeticException x) {
            throw CqlError.invalid("Unable to make " + t.cql() + " from '" + e.getAsString() + "'");
        } catch (IllegalStateException | ClassCastException | UnknownHostException x) {
            throw CqlError.invalid("Error decoding JSON value for type " + t.cql() + ": " + x.getMessage());
        }
    }

    private static BigDecimal jsonNum(JsonElement e) {
        return e.getAsJsonPrimitive().isString() ? new BigDecimal(e.getAsString()) : e.getAsBigDecimal();
    }

}
