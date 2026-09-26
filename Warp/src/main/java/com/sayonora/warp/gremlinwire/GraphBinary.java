package com.sayonora.warp.gremlinwire;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** GraphBinary 1.0: the response writer and the request reader (op messages, bytecode, predicates, enums). */
final class GraphBinary {

    private GraphBinary() {
    }

    static final int INT = 0x01;
    static final int LONG = 0x02;
    static final int STRING = 0x03;
    static final int DATE = 0x04;
    static final int TIMESTAMP = 0x05;
    static final int CLASS = 0x06;
    static final int DOUBLE = 0x07;
    static final int FLOAT = 0x08;
    static final int LIST = 0x09;
    static final int MAP = 0x0a;
    static final int SET = 0x0b;
    static final int UUID_T = 0x0c;
    static final int EDGE = 0x0d;
    static final int PATH = 0x0e;
    static final int PROPERTY = 0x0f;
    static final int VERTEX = 0x11;
    static final int VERTEXPROPERTY = 0x12;
    static final int BARRIER = 0x13;
    static final int BINDING = 0x14;
    static final int BYTECODE = 0x15;
    static final int CARDINALITY = 0x16;
    static final int COLUMN = 0x17;
    static final int DIRECTION = 0x18;
    static final int OPERATOR = 0x19;
    static final int ORDER = 0x1a;
    static final int PICK = 0x1b;
    static final int POP = 0x1c;
    static final int LAMBDA = 0x1d;
    static final int P = 0x1e;
    static final int SCOPE = 0x1f;
    static final int T = 0x20;
    static final int TRAVERSER = 0x21;
    static final int BIGDECIMAL = 0x22;
    static final int BIGINTEGER = 0x23;
    static final int BYTE = 0x24;
    static final int BYTEBUFFER = 0x25;
    static final int SHORT = 0x26;
    static final int BOOLEAN = 0x27;
    static final int TEXTP = 0x28;
    static final int TRAVERSALSTRATEGY = 0x29;
    static final int BULKSET = 0x2a;
    static final int TREE = 0x2b;
    static final int MERGE = 0x2e;
    static final int DT = 0x2f;
    static final int CHAR = 0x80;
    static final int NULL = 0xfe;

    // ================================================================== writing

    static final class Out {
        final ByteArrayOutputStream b = new ByteArrayOutputStream();

        Out i8(int v) {
            b.write(v);
            return this;
        }

        Out i32(int v) {
            b.write(v >>> 24);
            b.write(v >>> 16);
            b.write(v >>> 8);
            b.write(v);
            return this;
        }

        Out i64(long v) {
            i32((int) (v >>> 32));
            i32((int) v);
            return this;
        }

        Out bytes(byte[] x) {
            b.write(x, 0, x.length);
            return this;
        }

        /** int length + UTF-8 bytes (a value-form String). */
        Out str(String s) {
            byte[] x = s.getBytes(StandardCharsets.UTF_8);
            i32(x.length);
            return bytes(x);
        }

        /** nullable value-form string: flag then value. */
        Out nstr(String s) {
            if (s == null) {
                return i8(1);
            }
            i8(0);
            return str(s);
        }

        Out nullFq() {
            return i8(NULL).i8(1);
        }

        byte[] toBytes() {
            return b.toByteArray();
        }
    }

    private static void bigint(Out o, BigInteger v) {
        byte[] x = v.toByteArray();
        o.i32(x.length).bytes(x);
    }

    private static void enumFq(Out o, int type, String name) {
        o.i8(type).i8(0).i8(STRING).i8(0).str(name);
    }

    /** Fully-qualified write (type code, value flag, value). */
    static void write(Out o, Object v) {
        if (v == null) {
            o.nullFq();
            return;
        }
        if (v instanceof String s) {
            o.i8(STRING).i8(0).str(s);
        } else if (v instanceof Integer i) {
            o.i8(INT).i8(0).i32(i);
        } else if (v instanceof Long l) {
            o.i8(LONG).i8(0).i64(l);
        } else if (v instanceof Double d) {
            o.i8(DOUBLE).i8(0).i64(Double.doubleToRawLongBits(d));
        } else if (v instanceof Float f) {
            o.i8(FLOAT).i8(0).i32(Float.floatToRawIntBits(f));
        } else if (v instanceof Boolean b) {
            o.i8(BOOLEAN).i8(0).i8(b ? 1 : 0);
        } else if (v instanceof Short s) {
            o.i8(SHORT).i8(0).i8(s >> 8).i8(s);
        } else if (v instanceof Byte b) {
            o.i8(BYTE).i8(0).i8(b);
        } else if (v instanceof BigDecimal d) {
            o.i8(BIGDECIMAL).i8(0).i32(d.scale());
            bigint(o, d.unscaledValue());
        } else if (v instanceof BigInteger d) {
            o.i8(BIGINTEGER).i8(0);
            bigint(o, d);
        } else if (v instanceof Character c) {
            o.i8(CHAR).i8(0).bytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
        } else if (v instanceof Date d) {
            o.i8(DATE).i8(0).i64(d.getTime());
        } else if (v instanceof java.time.Instant d) {
            o.i8(DATE).i8(0).i64(d.toEpochMilli());
        } else if (v instanceof UUID u) {
            o.i8(UUID_T).i8(0).i64(u.getMostSignificantBits()).i64(u.getLeastSignificantBits());
        } else if (v instanceof byte[] b) {
            o.i8(BYTEBUFFER).i8(0).i32(b.length).bytes(b);
        } else if (v instanceof G.Vertex x) {
            o.i8(VERTEX).i8(0);
            write(o, x.id);
            o.str(x.label);
            List<Object> props = new ArrayList<>();
            for (List<G.VProp> l : x.props.values()) {
                props.addAll(l);
            }
            if (props.isEmpty()) {
                o.nullFq();
            } else {
                write(o, props);
            }
        } else if (v instanceof G.Edge e) {
            o.i8(EDGE).i8(0);
            write(o, e.id);
            o.str(e.label);
            write(o, e.inId);
            o.str(e.inLabel);
            write(o, e.outId);
            o.str(e.outLabel);
            o.nullFq();
            if (e.props.isEmpty()) {
                o.nullFq();
            } else {
                List<Object> props = new ArrayList<>();
                for (Map.Entry<String, Object> pe : e.props.entrySet()) {
                    props.add(new G.Prop(pe.getKey(), pe.getValue(), e));
                }
                write(o, props);
            }
        } else if (v instanceof G.VProp p) {
            o.i8(VERTEXPROPERTY).i8(0);
            write(o, p.id);
            o.str(p.key);
            write(o, p.value);
            o.nullFq();
            if (p.meta == null || p.meta.isEmpty()) {
                o.nullFq();
            } else {
                List<Object> props = new ArrayList<>();
                for (Map.Entry<String, Object> pe : p.meta.entrySet()) {
                    props.add(new G.Prop(pe.getKey(), pe.getValue(), p));
                }
                write(o, props);
            }
        } else if (v instanceof G.Prop p) {
            o.i8(PROPERTY).i8(0).str(p.key);
            write(o, p.value);
            o.nullFq();
        } else if (v instanceof G.Path p) {
            o.i8(PATH).i8(0);
            List<Object> labs = new ArrayList<>();
            for (Set<String> s : p.labels) {
                labs.add(new LinkedHashSet<Object>(s));
            }
            write(o, labs);
            write(o, p.objects);
        } else if (v instanceof G.Tree t) {
            o.i8(TREE).i8(0);
            tree(o, t);
        } else if (v instanceof G.WireTraverser t) {
            o.i8(TRAVERSER).i8(0).i64(t.bulk);
            write(o, t.value);
        } else if (v instanceof G.T e) {
            enumFq(o, T, e.name());
        } else if (v instanceof G.Direction e) {
            enumFq(o, DIRECTION, e.name());
        } else if (v instanceof G.Column e) {
            enumFq(o, COLUMN, e.name());
        } else if (v instanceof G.Scope e) {
            enumFq(o, SCOPE, e.name());
        } else if (v instanceof G.Order e) {
            enumFq(o, ORDER, e.name());
        } else if (v instanceof G.Cardinality e) {
            enumFq(o, CARDINALITY, e.name());
        } else if (v instanceof G.Merge e) {
            enumFq(o, MERGE, e.name());
        } else if (v instanceof G.BulkSet bs) {
            o.i8(BULKSET).i8(0).i32(bs.size());
            for (Map.Entry<Object, Long> e : bs.entrySet()) {
                write(o, e.getKey());
                o.i64(e.getValue());
            }
        } else if (v instanceof Map<?, ?> m) {
            o.i8(MAP).i8(0).i32(m.size());
            for (Map.Entry<?, ?> e : m.entrySet()) {
                write(o, e.getKey());
                write(o, e.getValue());
            }
        } else if (v instanceof Map.Entry<?, ?> e) {
            o.i8(MAP).i8(0).i32(1);
            write(o, e.getKey());
            write(o, e.getValue());
        } else if (v instanceof Set<?> s) {
            o.i8(SET).i8(0).i32(s.size());
            for (Object x : s) {
                write(o, x);
            }
        } else if (v instanceof Collection<?> c) {
            o.i8(LIST).i8(0).i32(c.size());
            for (Object x : c) {
                write(o, x);
            }
        } else if (v instanceof Object[] arr) {
            o.i8(LIST).i8(0).i32(arr.length);
            for (Object x : arr) {
                write(o, x);
            }
        } else {
            o.i8(STRING).i8(0).str(String.valueOf(v));
        }
    }

    private static void tree(Out o, G.Tree t) {
        o.i32(t.size());
        for (Map.Entry<Object, G.Tree> e : t.entrySet()) {
            write(o, e.getKey());
            tree(o, e.getValue());
        }
    }

    /** One response message (the WebSocket binary frame payload / the HTTP body). */
    static byte[] response(UUID requestId, int code, String message, Map<String, Object> attrs, Map<String, Object> meta, Object data, boolean hasData,
            boolean asStrings) {
        Out o = new Out();
        o.i8(0x81);
        if (requestId == null) {
            o.i8(1);
        } else {
            o.i8(0).i64(requestId.getMostSignificantBits()).i64(requestId.getLeastSignificantBits());
        }
        o.i32(code);
        o.nstr(message);
        map(o, attrs);
        map(o, meta);
        if (!hasData) {
            o.nullFq();
        } else if (asStrings && data instanceof List<?> l) {
            List<Object> strs = new ArrayList<>();
            for (Object x : l) {
                strs.add(String.valueOf(x));
            }
            write(o, strs);
        } else {
            write(o, data);
        }
        return o.toBytes();
    }

    private static void map(Out o, Map<String, Object> m) {
        o.i32(m.size());
        for (Map.Entry<String, Object> e : m.entrySet()) {
            write(o, e.getKey());
            write(o, e.getValue());
        }
    }

    // ================================================================== reading

    /** A parsed request message. */
    static final class Request {
        UUID requestId;
        String op;
        String processor;
        Map<String, Object> args = new LinkedHashMap<>();
    }

    static Request readRequest(ByteBuffer b) {
        int version = b.get() & 0xff;
        if (version != 0x81) {
            throw new G.GremlinError(498, "Unsupported GraphBinary message version " + version);
        }
        Request r = new Request();
        r.requestId = new UUID(b.getLong(), b.getLong());
        r.op = valueString(b);
        r.processor = valueString(b);
        int n = b.getInt();
        for (int i = 0; i < n; i++) {
            Object k = read(b);
            Object v = read(b);
            r.args.put(String.valueOf(k), v);
        }
        return r;
    }

    private static String valueString(ByteBuffer b) {
        int len = b.getInt();
        if (len < 0) {
            return null; // a null string (the Java driver writes one for an unset processor)
        }
        byte[] x = new byte[len];
        b.get(x);
        return new String(x, StandardCharsets.UTF_8);
    }

    static Object read(ByteBuffer b) {
        int type = b.get() & 0xff;
        if (type == NULL) {
            b.get();
            return null;
        }
        int flag = b.get() & 0xff;
        if (flag == 1) {
            return null;
        }
        return value(type, b);
    }

    private static Object enumFor(int type, String s) {
        try {
            return switch (type) {
                case T -> G.T.valueOf(s);
                case DIRECTION -> G.Direction.valueOf(s.toUpperCase(java.util.Locale.ROOT));
                case ORDER -> G.Order.valueOf(s);
                case SCOPE -> G.Scope.valueOf(s);
                case CARDINALITY -> G.Cardinality.valueOf(s);
                case COLUMN -> G.Column.valueOf(s);
                case POP -> G.Pop.valueOf(s);
                case OPERATOR -> G.Operator.valueOf(s);
                case PICK -> G.Pick.valueOf(s);
                case MERGE -> G.Merge.valueOf(s);
                case BARRIER -> G.Barrier.valueOf(s);
                default -> s;
            };
        } catch (IllegalArgumentException e) {
            throw new G.GremlinError(499, "unknown enum value " + s);
        }
    }

    private static Object value(int type, ByteBuffer b) {
        switch (type) {
            case INT:
                return b.getInt();
            case LONG:
                return b.getLong();
            case STRING, CLASS:
                return valueString(b);
            case DATE, TIMESTAMP:
                return new Date(b.getLong());
            case DOUBLE:
                return b.getDouble();
            case FLOAT:
                return b.getFloat();
            case BOOLEAN:
                return b.get() != 0;
            case SHORT:
                return b.getShort();
            case BYTE:
                return b.get();
            case UUID_T:
                return new UUID(b.getLong(), b.getLong());
            case CHAR: {
                int first = b.get() & 0xff;
                int extra = first >= 0xf0 ? 3 : first >= 0xe0 ? 2 : first >= 0xc0 ? 1 : 0;
                byte[] x = new byte[1 + extra];
                x[0] = (byte) first;
                b.get(x, 1, extra);
                return new String(x, StandardCharsets.UTF_8).charAt(0);
            }
            case BIGINTEGER: {
                byte[] x = new byte[b.getInt()];
                b.get(x);
                return new BigInteger(x);
            }
            case BIGDECIMAL: {
                int scale = b.getInt();
                byte[] x = new byte[b.getInt()];
                b.get(x);
                return new BigDecimal(new BigInteger(x), scale);
            }
            case BYTEBUFFER: {
                byte[] x = new byte[b.getInt()];
                b.get(x);
                return x;
            }
            case LIST: {
                int n = b.getInt();
                List<Object> l = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    l.add(read(b));
                }
                return l;
            }
            case SET: {
                int n = b.getInt();
                Set<Object> l = new LinkedHashSet<>();
                for (int i = 0; i < n; i++) {
                    l.add(read(b));
                }
                return l;
            }
            case MAP: {
                int n = b.getInt();
                Map<Object, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    Object k = read(b);
                    m.put(k, read(b));
                }
                return m;
            }
            case BULKSET: {
                int n = b.getInt();
                List<Object> l = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    Object item = read(b);
                    long bulk = b.getLong();
                    for (long k = 0; k < bulk; k++) {
                        l.add(item);
                    }
                }
                return l;
            }
            case VERTEX: {
                Object id = read(b);
                String label = valueString(b);
                read(b);
                return new G.Vertex(id, label);
            }
            case EDGE: {
                Object id = read(b);
                String label = valueString(b);
                Object in = read(b);
                String inl = valueString(b);
                Object out = read(b);
                String outl = valueString(b);
                read(b);
                read(b);
                return new G.Edge(id, label, out, outl, in, inl);
            }
            case VERTEXPROPERTY: {
                Object id = read(b);
                String label = valueString(b);
                Object v = read(b);
                read(b);
                read(b);
                return new G.VProp(id, label, v);
            }
            case PROPERTY: {
                String key = valueString(b);
                Object v = read(b);
                read(b);
                return new G.Prop(key, v, null);
            }
            case PATH: {
                @SuppressWarnings("unchecked")
                List<Object> labs = (List<Object>) read(b);
                @SuppressWarnings("unchecked")
                List<Object> objs = (List<Object>) read(b);
                List<Set<String>> ls = new ArrayList<>();
                for (Object o : labs) {
                    Set<String> s = new LinkedHashSet<>();
                    if (o instanceof Collection<?> c) {
                        for (Object x : c) {
                            s.add(String.valueOf(x));
                        }
                    }
                    ls.add(s);
                }
                return new G.Path(objs, ls);
            }
            case BINDING: {
                valueString(b);
                return read(b);
            }
            case TRAVERSER: {
                long bulk = b.getLong();
                return new G.WireTraverser(read(b), bulk);
            }
            case LAMBDA: {
                valueString(b); // language
                String script = valueString(b);
                b.getInt();
                return Script.lambda(script);
            }
            case P, TEXTP: {
                String op = valueString(b);
                int n = b.getInt();
                List<Object> args = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    args.add(read(b));
                }
                return GraphSon.makeP(op, args, type == TEXTP);
            }
            case BYTECODE: {
                G.Bytecode bc = new G.Bytecode();
                int steps = b.getInt();
                for (int i = 0; i < steps; i++) {
                    bc.steps.add(instruction(b));
                }
                int src = b.getInt();
                for (int i = 0; i < src; i++) {
                    bc.source.add(instruction(b));
                }
                return bc;
            }
            case TRAVERSALSTRATEGY: {
                valueString(b);
                int n = b.getInt();
                for (int i = 0; i < n; i++) {
                    read(b);
                    read(b);
                }
                return null;
            }
            case CARDINALITY, COLUMN, DIRECTION, OPERATOR, ORDER, PICK, POP, SCOPE, T, MERGE, BARRIER, DT: {
                Object s = read(b);
                return enumFor(type, String.valueOf(s));
            }
            default:
                throw new G.GremlinError(499, "Unsupported GraphBinary type code 0x" + Integer.toHexString(type));
        }
    }

    private static Object[] instruction(ByteBuffer b) {
        String name = valueString(b);
        int n = b.getInt();
        Object[] out = new Object[n + 1];
        out[0] = name;
        for (int i = 0; i < n; i++) {
            out[i + 1] = read(b);
        }
        return out;
    }
}
