package com.sayonora.warp.cqlwire;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * A CQL data type: its native protocol option encoding, value (de)serialization, ordering and an order-preserving byte encoding
 * used for clustering keys. Java value model: ascii/text String, bigint/counter Long, int Integer, smallint Short, tinyint Byte,
 * blob byte[], boolean Boolean, decimal BigDecimal, double Double, float Float, varint BigInteger, timestamp Long (epoch millis),
 * date Integer (days since epoch), time Long (nanos), uuid/timeuuid UUID, inet InetAddress, duration long[]{months,days,nanos},
 * list List, set TreeSet, map TreeMap, tuple/udt Object[] (null = null component).
 */
final class CqlType implements Comparator<Object> {

    enum K {
        ASCII(1, "ascii"), BIGINT(2, "bigint"), BLOB(3, "blob"), BOOLEAN(4, "boolean"), COUNTER(5, "counter"), DECIMAL(6, "decimal"),
        DOUBLE(7, "double"), FLOAT(8, "float"), INT(9, "int"), TIMESTAMP(0xB, "timestamp"), UUID(0xC, "uuid"), VARCHAR(0xD, "text"),
        VARINT(0xE, "varint"), TIMEUUID(0xF, "timeuuid"), INET(0x10, "inet"), DATE(0x11, "date"), TIME(0x12, "time"),
        SMALLINT(0x13, "smallint"), TINYINT(0x14, "tinyint"), DURATION(0x15, "duration"), LIST(0x20, "list"), MAP(0x21, "map"),
        SET(0x22, "set"), UDT(0x30, "udt"), TUPLE(0x31, "tuple");

        final int id;
        final String cql;

        K(int id, String cql) {
            this.id = id;
            this.cql = cql;
        }
    }

    final K k;
    final List<CqlType> args;
    final boolean frozen;
    final String udtKs;
    final String udtName;
    final List<String> fieldNames;

    private CqlType(K k, List<CqlType> args, boolean frozen, String udtKs, String udtName, List<String> fieldNames) {
        this.k = k;
        this.args = args;
        this.frozen = frozen;
        this.udtKs = udtKs;
        this.udtName = udtName;
        this.fieldNames = fieldNames;
    }

    private static final Map<K, CqlType> PRIMS = new java.util.EnumMap<>(K.class);

    static {
        for (K k : K.values()) {
            if (k.id < 0x20) {
                PRIMS.put(k, new CqlType(k, List.of(), false, null, null, null));
            }
        }
    }

    static CqlType of(K k) {
        return PRIMS.get(k);
    }

    static final CqlType TEXT = of(K.VARCHAR), INT = of(K.INT), BIGINT = of(K.BIGINT), BOOLEAN = of(K.BOOLEAN), UUID_T = of(K.UUID),
            TIMESTAMP = of(K.TIMESTAMP), BLOB = of(K.BLOB), DOUBLE = of(K.DOUBLE), COUNTER = of(K.COUNTER), TIMEUUID = of(K.TIMEUUID),
            INET = of(K.INET), VARINT = of(K.VARINT), DECIMAL = of(K.DECIMAL), DATE = of(K.DATE), TIME = of(K.TIME);

    static CqlType list(CqlType e, boolean frozen) {
        return new CqlType(K.LIST, List.of(e), frozen, null, null, null);
    }

    static CqlType set(CqlType e, boolean frozen) {
        return new CqlType(K.SET, List.of(e), frozen, null, null, null);
    }

    static CqlType map(CqlType key, CqlType v, boolean frozen) {
        return new CqlType(K.MAP, List.of(key, v), frozen, null, null, null);
    }

    static CqlType tuple(List<CqlType> comps, boolean frozen) {
        return new CqlType(K.TUPLE, List.copyOf(comps), frozen, null, null, null);
    }

    static CqlType udt(String ks, String name, List<String> fields, List<CqlType> types, boolean frozen) {
        return new CqlType(K.UDT, List.copyOf(types), frozen, ks, name, List.copyOf(fields));
    }

    CqlType asFrozen() {
        return frozen || args.isEmpty() && k != K.UDT ? this : new CqlType(k, args, true, udtKs, udtName, fieldNames);
    }

    CqlType asUnfrozen() {
        return !frozen ? this : new CqlType(k, args, false, udtKs, udtName, fieldNames);
    }

    boolean isCollection() {
        return k == K.LIST || k == K.SET || k == K.MAP;
    }

    /** A non-frozen collection: stored one cell per element. */
    boolean isMultiCell() {
        return isCollection() && !frozen;
    }

    boolean isCounter() {
        return k == K.COUNTER;
    }

    boolean isNumeric() {
        return switch (k) {
            case BIGINT, INT, SMALLINT, TINYINT, VARINT, DECIMAL, DOUBLE, FLOAT, COUNTER -> true;
            default -> false;
        };
    }

    boolean isString() {
        return k == K.ASCII || k == K.VARCHAR;
    }

    // ------------------------------------------------------------------ names

    /** CQL type name as system_schema prints it, e.g. {@code frozen<map<text, int>>}. */
    String cql() {
        String s;
        switch (k) {
            case LIST, SET -> s = k.cql + "<" + args.get(0).cql() + ">";
            case MAP -> s = "map<" + args.get(0).cql() + ", " + args.get(1).cql() + ">";
            case TUPLE -> {
                StringBuilder b = new StringBuilder("tuple<");
                for (int i = 0; i < args.size(); i++) {
                    b.append(i > 0 ? ", " : "").append(args.get(i).cql());
                }
                s = b.append('>').toString();
            }
            case UDT -> s = udtName.matches("[a-z][a-z0-9_]*") ? udtName : "\"" + udtName + "\"";
            default -> {
                return k.cql;
            }
        }
        if (frozen) {
            return "frozen<" + s + ">";
        }
        return s;
    }

    /** Persistable form: like {@link #cql()} but UDTs are qualified ({@code ks.name}). */
    String qualified() {
        switch (k) {
            case LIST, SET -> {
                String s = k.cql + "<" + args.get(0).qualified() + ">";
                return frozen ? "frozen<" + s + ">" : s;
            }
            case MAP -> {
                String s = "map<" + args.get(0).qualified() + ", " + args.get(1).qualified() + ">";
                return frozen ? "frozen<" + s + ">" : s;
            }
            case TUPLE -> {
                StringBuilder b = new StringBuilder("tuple<");
                for (int i = 0; i < args.size(); i++) {
                    b.append(i > 0 ? ", " : "").append(args.get(i).qualified());
                }
                return b.append('>').toString();
            }
            case UDT -> {
                String s = "\"" + udtKs + "\".\"" + udtName + "\"";
                return frozen ? "frozen<" + s + ">" : s;
            }
            default -> {
                return k.cql;
            }
        }
    }

    @Override
    public String toString() {
        return cql();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CqlType t && t.qualified().equals(qualified());
    }

    @Override
    public int hashCode() {
        return qualified().hashCode();
    }

    /** True when a value of {@code o} can be assigned where this type is expected (same shape ignoring frozen). */
    boolean sameShape(CqlType o) {
        return asUnfrozen().qualified().equals(o.asUnfrozen().qualified());
    }

    // ------------------------------------------------------------------ native protocol type option

    void writeOption(Wire.Out o) {
        switch (k) {
            case LIST, SET -> {
                o.u16(k.id);
                args.get(0).writeOption(o);
            }
            case MAP -> {
                o.u16(k.id);
                args.get(0).writeOption(o);
                args.get(1).writeOption(o);
            }
            case TUPLE -> {
                o.u16(k.id).u16(args.size());
                args.forEach(a -> a.writeOption(o));
            }
            case UDT -> {
                o.u16(k.id).string(udtKs).string(udtName).u16(args.size());
                for (int i = 0; i < args.size(); i++) {
                    o.string(fieldNames.get(i));
                    args.get(i).writeOption(o);
                }
            }
            default -> o.u16(k.id);
        }
    }

    // ------------------------------------------------------------------ serialization

    static final byte[] EMPTY = new byte[0];

    static byte[] be(long v, int n) {
        byte[] b = new byte[n];
        for (int i = n - 1; i >= 0; i--) {
            b[i] = (byte) v;
            v >>= 8;
        }
        return b;
    }

    static long beLong(byte[] b) {
        long v = 0;
        for (byte x : b) {
            v = (v << 8) | (x & 0xFF);
        }
        return v;
    }

    private static long signedBe(byte[] b) {
        long v = b.length > 0 && b[0] < 0 ? -1 : 0;
        for (byte x : b) {
            v = (v << 8) | (x & 0xFF);
        }
        return v;
    }

    static final long DATE_OFFSET = 1L << 31;

    byte[] serialize(Object v) {
        if (v == null) {
            return null;
        }
        switch (k) {
            case ASCII, VARCHAR -> {
                return ((String) v).getBytes(StandardCharsets.UTF_8);
            }
            case BIGINT, COUNTER, TIMESTAMP, TIME -> {
                return be((Long) v, 8);
            }
            case INT -> {
                return be((Integer) v, 4);
            }
            case SMALLINT -> {
                return be((Short) v, 2);
            }
            case TINYINT -> {
                return new byte[] {(Byte) v};
            }
            case BLOB -> {
                return (byte[]) v;
            }
            case BOOLEAN -> {
                return new byte[] {(byte) ((Boolean) v ? 1 : 0)};
            }
            case DECIMAL -> {
                BigDecimal d = (BigDecimal) v;
                byte[] u = d.unscaledValue().toByteArray();
                byte[] r = new byte[4 + u.length];
                System.arraycopy(be(d.scale(), 4), 0, r, 0, 4);
                System.arraycopy(u, 0, r, 4, u.length);
                return r;
            }
            case DOUBLE -> {
                return be(Double.doubleToRawLongBits((Double) v), 8);
            }
            case FLOAT -> {
                return be(Float.floatToRawIntBits((Float) v), 4);
            }
            case VARINT -> {
                return ((BigInteger) v).toByteArray();
            }
            case UUID, TIMEUUID -> {
                UUID u = (UUID) v;
                return ByteBuffer.allocate(16).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).array();
            }
            case INET -> {
                return ((InetAddress) v).getAddress();
            }
            case DATE -> {
                return be(((Integer) v) + DATE_OFFSET, 4);
            }
            case DURATION -> {
                long[] d = (long[]) v;
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                for (long x : d) {
                    writeVInt(o, (x << 1) ^ (x >> 63));
                }
                return o.toByteArray();
            }
            case LIST, SET -> {
                java.util.Collection<?> c = (java.util.Collection<?>) v;
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                o.writeBytes(be(c.size(), 4));
                CqlType e = args.get(0);
                for (Object x : c) {
                    byte[] b = e.serialize(x);
                    o.writeBytes(be(b.length, 4));
                    o.writeBytes(b);
                }
                return o.toByteArray();
            }
            case MAP -> {
                Map<?, ?> m = (Map<?, ?>) v;
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                o.writeBytes(be(m.size(), 4));
                for (var en : m.entrySet()) {
                    byte[] kb = args.get(0).serialize(en.getKey());
                    byte[] vb = args.get(1).serialize(en.getValue());
                    o.writeBytes(be(kb.length, 4));
                    o.writeBytes(kb);
                    o.writeBytes(be(vb.length, 4));
                    o.writeBytes(vb);
                }
                return o.toByteArray();
            }
            default -> {
                Object[] t = (Object[]) v;
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                for (int i = 0; i < args.size() && i < t.length; i++) {
                    byte[] b = t[i] == null ? null : args.get(i).serialize(t[i]);
                    if (b == null) {
                        o.writeBytes(be(-1, 4));
                    } else {
                        o.writeBytes(be(b.length, 4));
                        o.writeBytes(b);
                    }
                }
                return o.toByteArray();
            }
        }
    }

    Object deserialize(byte[] b) {
        if (b == null) {
            return null;
        }
        try {
            switch (k) {
                case ASCII, VARCHAR -> {
                    return new String(b, StandardCharsets.UTF_8);
                }
                case BIGINT, COUNTER, TIMESTAMP, TIME -> {
                    need(b, 8);
                    return beLong(b);
                }
                case INT -> {
                    need(b, 4);
                    return (int) beLong(b);
                }
                case SMALLINT -> {
                    need(b, 2);
                    return (short) signedBe(b);
                }
                case TINYINT -> {
                    need(b, 1);
                    return b[0];
                }
                case BLOB -> {
                    return b;
                }
                case BOOLEAN -> {
                    need(b, 1);
                    return b[0] != 0;
                }
                case DECIMAL -> {
                    need(b, 5);
                    return new BigDecimal(new BigInteger(Arrays.copyOfRange(b, 4, b.length)), (int) signedBe(Arrays.copyOfRange(b, 0, 4)));
                }
                case DOUBLE -> {
                    need(b, 8);
                    return Double.longBitsToDouble(beLong(b));
                }
                case FLOAT -> {
                    need(b, 4);
                    return Float.intBitsToFloat((int) beLong(b));
                }
                case VARINT -> {
                    need(b, 1);
                    return new BigInteger(b);
                }
                case UUID, TIMEUUID -> {
                    need(b, 16);
                    ByteBuffer bb = ByteBuffer.wrap(b);
                    return new UUID(bb.getLong(), bb.getLong());
                }
                case INET -> {
                    return InetAddress.getByAddress(b);
                }
                case DATE -> {
                    need(b, 4);
                    return (int) (beLong(b) - DATE_OFFSET);
                }
                case DURATION -> {
                    int[] p = {0};
                    long[] d = new long[3];
                    for (int i = 0; i < 3; i++) {
                        long u = readVInt(b, p);
                        d[i] = (u >>> 1) ^ -(u & 1);
                    }
                    return d;
                }
                case LIST, SET -> {
                    ByteBuffer bb = ByteBuffer.wrap(b);
                    int n = bb.getInt();
                    CqlType e = args.get(0);
                    java.util.Collection<Object> c = k == K.LIST ? new ArrayList<>() : new TreeSet<>(e);
                    for (int i = 0; i < n; i++) {
                        c.add(e.deserialize(take(bb)));
                    }
                    return c;
                }
                case MAP -> {
                    ByteBuffer bb = ByteBuffer.wrap(b);
                    int n = bb.getInt();
                    Map<Object, Object> m = new TreeMap<>(args.get(0));
                    for (int i = 0; i < n; i++) {
                        Object key = args.get(0).deserialize(take(bb));
                        m.put(key, args.get(1).deserialize(take(bb)));
                    }
                    return m;
                }
                default -> {
                    ByteBuffer bb = ByteBuffer.wrap(b);
                    Object[] t = new Object[args.size()];
                    for (int i = 0; i < t.length && bb.hasRemaining(); i++) {
                        int n = bb.getInt();
                        if (n >= 0) {
                            byte[] x = new byte[n];
                            bb.get(x);
                            t[i] = args.get(i).deserialize(x);
                        }
                    }
                    return t;
                }
            }
        } catch (UnknownHostException | java.nio.BufferUnderflowException | NegativeArraySizeException e) {
            throw CqlError.invalid("Invalid " + cql() + " value: " + e.getMessage());
        }
    }

    private static void need(byte[] b, int n) {
        if (b.length < n) {
            throw CqlError.invalid("Expected " + n + " or more bytes, but got " + b.length);
        }
    }

    private static byte[] take(ByteBuffer bb) {
        int n = bb.getInt();
        if (n < 0) {
            return null;
        }
        byte[] x = new byte[n];
        bb.get(x);
        return x;
    }

    static void writeVInt(ByteArrayOutputStream o, long v) {
        int lz = Long.numberOfLeadingZeros(v | 1);
        int size = (639 - lz * 9) >> 6;
        if (size == 1) {
            o.write((int) v);
            return;
        }
        int extra = size - 1;
        o.write(((0xFF << (8 - extra)) & 0xFF) | (int) (v >>> (8 * extra)));
        for (int i = extra - 1; i >= 0; i--) {
            o.write((int) (v >>> (8 * i)));
        }
    }

    static long readVInt(byte[] b, int[] p) {
        int first = b[p[0]++] & 0xFF;
        int extra = Integer.numberOfLeadingZeros(~first & 0xFF) - 24;
        long v = first & (0xFF >> extra);
        for (int i = 0; i < extra; i++) {
            v = (v << 8) | (b[p[0]++] & 0xFF);
        }
        return v;
    }

    // ------------------------------------------------------------------ ordering

    @Override
    public int compare(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : -1) : 1;
        }
        switch (k) {
            case ASCII, VARCHAR -> {
                return Arrays.compareUnsigned(((String) a).getBytes(StandardCharsets.UTF_8), ((String) b).getBytes(StandardCharsets.UTF_8));
            }
            case BIGINT, COUNTER, TIMESTAMP, TIME -> {
                return Long.compare((Long) a, (Long) b);
            }
            case INT, DATE -> {
                return Integer.compare((Integer) a, (Integer) b);
            }
            case SMALLINT -> {
                return Short.compare((Short) a, (Short) b);
            }
            case TINYINT -> {
                return Byte.compare((Byte) a, (Byte) b);
            }
            case BLOB -> {
                return Arrays.compareUnsigned((byte[]) a, (byte[]) b);
            }
            case BOOLEAN -> {
                return Boolean.compare((Boolean) a, (Boolean) b);
            }
            case DECIMAL -> {
                return ((BigDecimal) a).compareTo((BigDecimal) b);
            }
            case DOUBLE -> {
                return Double.compare((Double) a, (Double) b);
            }
            case FLOAT -> {
                return Float.compare((Float) a, (Float) b);
            }
            case VARINT -> {
                return ((BigInteger) a).compareTo((BigInteger) b);
            }
            case UUID, TIMEUUID -> {
                return Arrays.compareUnsigned(uuidKey((UUID) a, k == K.TIMEUUID), uuidKey((UUID) b, k == K.TIMEUUID));
            }
            case INET -> {
                return Arrays.compareUnsigned(((InetAddress) a).getAddress(), ((InetAddress) b).getAddress());
            }
            case DURATION -> {
                return Arrays.compare((long[]) a, (long[]) b);
            }
            case LIST, SET -> {
                java.util.Iterator<?> i = ((java.util.Collection<?>) a).iterator(), j = ((java.util.Collection<?>) b).iterator();
                CqlType e = args.get(0);
                while (i.hasNext() && j.hasNext()) {
                    int c = e.compare(i.next(), j.next());
                    if (c != 0) {
                        return c;
                    }
                }
                return Boolean.compare(i.hasNext(), j.hasNext());
            }
            case MAP -> {
                java.util.Iterator<? extends Map.Entry<?, ?>> i = ((Map<?, ?>) a).entrySet().iterator(),
                        j = ((Map<?, ?>) b).entrySet().iterator();
                while (i.hasNext() && j.hasNext()) {
                    var x = i.next();
                    var y = j.next();
                    int c = args.get(0).compare(x.getKey(), y.getKey());
                    if (c == 0) {
                        c = args.get(1).compare(x.getValue(), y.getValue());
                    }
                    if (c != 0) {
                        return c;
                    }
                }
                return Boolean.compare(i.hasNext(), j.hasNext());
            }
            default -> {
                Object[] x = (Object[]) a, y = (Object[]) b;
                for (int i = 0; i < args.size(); i++) {
                    int c = args.get(i).compare(i < x.length ? x[i] : null, i < y.length ? y[i] : null);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }
        }
    }

    /** Value equality in this type. */
    boolean same(Object a, Object b) {
        return compare(a, b) == 0;
    }

    private static long uuidTimestamp(UUID u) {
        long msb = u.getMostSignificantBits();
        return ((msb & 0x0FFFL) << 48) | (((msb >> 16) & 0xFFFFL) << 32) | ((msb >>> 32) & 0xFFFFFFFFL);
    }

    /** Sortable key of a UUID as UUIDType / TimeUUIDType order them (timestamp first for version 1). */
    private static byte[] uuidKey(UUID u, boolean time) {
        int version = u.version();
        ByteBuffer bb = ByteBuffer.allocate(17);
        if (time || version == 1) {
            if (!time) {
                bb.put((byte) version);
            } else {
                bb.put((byte) 1);
            }
            bb.putLong(uuidTimestamp(u));
            long lsb = u.getLeastSignificantBits() ^ 0x8080808080808080L;
            bb.putLong(lsb);
            if (!time) {
                return Arrays.copyOf(bb.array(), 17);
            }
            return bb.array();
        }
        bb = ByteBuffer.allocate(17);
        bb.put((byte) version).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
        return bb.array();
    }

    // ------------------------------------------------------------------ order preserving key encoding

    /** Order-preserving, prefix-free encoding of a value; {@code desc} inverts every byte (reversed clustering order). */
    byte[] keyEncode(Object v, boolean desc) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        enc(o, v);
        byte[] r = o.toByteArray();
        if (desc) {
            for (int i = 0; i < r.length; i++) {
                r[i] = (byte) ~r[i];
            }
        }
        return r;
    }

    private static void escaped(ByteArrayOutputStream o, byte[] b) {
        for (byte x : b) {
            o.write(x);
            if (x == 0) {
                o.write(0xFF);
            }
        }
        o.write(0);
        o.write(1);
    }

    private void enc(ByteArrayOutputStream o, Object v) {
        switch (k) {
            case ASCII, VARCHAR -> escaped(o, ((String) v).getBytes(StandardCharsets.UTF_8));
            case BLOB -> escaped(o, (byte[]) v);
            case INET -> escaped(o, ((InetAddress) v).getAddress());
            case BIGINT, COUNTER, TIMESTAMP, TIME -> o.writeBytes(be((Long) v ^ Long.MIN_VALUE, 8));
            case INT, DATE -> o.writeBytes(be(((Integer) v) ^ Integer.MIN_VALUE, 4));
            case SMALLINT -> o.writeBytes(be(((Short) v) ^ 0x8000, 2));
            case TINYINT -> o.write((Byte) v ^ 0x80);
            case BOOLEAN -> o.write((Boolean) v ? 1 : 0);
            case DOUBLE -> {
                long bits = Double.doubleToLongBits((Double) v);
                o.writeBytes(be(bits < 0 ? ~bits : bits ^ Long.MIN_VALUE, 8));
            }
            case FLOAT -> {
                int bits = Float.floatToIntBits((Float) v);
                o.writeBytes(be(bits < 0 ? ~bits : bits ^ Integer.MIN_VALUE, 4));
            }
            case VARINT -> encInt(o, (BigInteger) v);
            case DECIMAL -> {
                BigDecimal d = (BigDecimal) v;
                if (d.signum() == 0) {
                    o.write(1);
                    return;
                }
                d = d.stripTrailingZeros();
                int exp = d.precision() - d.scale() - 1;
                byte[] digits = d.unscaledValue().abs().toString().getBytes(StandardCharsets.US_ASCII);
                boolean neg = d.signum() < 0;
                o.write(neg ? 0 : 2);
                byte[] e = be(exp ^ Integer.MIN_VALUE, 4);
                for (byte x : e) {
                    o.write(neg ? ~x : x);
                }
                for (byte x : digits) {
                    o.write(neg ? ~x : x);
                }
                o.write(neg ? 0xFF : 0);
            }
            case UUID, TIMEUUID -> o.writeBytes(uuidKey((UUID) v, k == K.TIMEUUID));
            case DURATION -> {
                for (long x : (long[]) v) {
                    o.writeBytes(be(x ^ Long.MIN_VALUE, 8));
                }
            }
            case LIST, SET -> {
                for (Object x : (java.util.Collection<?>) v) {
                    o.write(1);
                    args.get(0).enc(o, x);
                }
                o.write(0);
            }
            case MAP -> {
                for (var en : ((Map<?, ?>) v).entrySet()) {
                    o.write(1);
                    args.get(0).enc(o, en.getKey());
                    args.get(1).enc(o, en.getValue());
                }
                o.write(0);
            }
            default -> {
                Object[] t = (Object[]) v;
                for (int i = 0; i < args.size(); i++) {
                    Object x = i < t.length ? t[i] : null;
                    if (x == null) {
                        o.write(0);
                    } else {
                        o.write(1);
                        args.get(i).enc(o, x);
                    }
                }
            }
        }
    }

    private static void encInt(ByteArrayOutputStream o, BigInteger v) {
        byte[] mag = v.abs().toByteArray();
        int s = 0;
        while (s < mag.length - 1 && mag[s] == 0) {
            s++;
        }
        mag = Arrays.copyOfRange(mag, s, mag.length);
        if (v.signum() == 0) {
            mag = new byte[0];
        }
        if (v.signum() >= 0) {
            o.write(1);
            o.write(mag.length >>> 8);
            o.write(mag.length);
            o.writeBytes(mag);
        } else {
            o.write(0);
            o.write(~(mag.length >>> 8));
            o.write(~mag.length);
            for (byte x : mag) {
                o.write(~x);
            }
        }
    }

    // ------------------------------------------------------------------ text parsing / formatting

    static final DateTimeFormatter TS_OUT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxx").withZone(ZoneOffset.UTC);

    /** Parses a timestamp string ('2020-01-01 10:00:00+0000', ISO 8601 forms, 'yyyy-MM-dd') into epoch millis. */
    static long parseTimestamp(String s) {
        String t = s.trim();
        try {
            if (t.matches("-?\\d+")) {
                return Long.parseLong(t);
            }
            java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                    "(\\d{4})-(\\d{2})-(\\d{2})(?:[T ](\\d{2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?)?\\s*(Z|UTC|[+-]\\d{2}(?::?\\d{2})?)?")
                    .matcher(t);
            if (!m.matches()) {
                throw new IllegalArgumentException();
            }
            int frac = m.group(7) == null ? 0 : (int) (Long.parseLong((m.group(7) + "000000000").substring(0, 9)) / 1_000_000);
            LocalDateTime ldt = LocalDateTime.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
                    m.group(4) == null ? 0 : Integer.parseInt(m.group(4)), m.group(5) == null ? 0 : Integer.parseInt(m.group(5)),
                    m.group(6) == null ? 0 : Integer.parseInt(m.group(6)), frac * 1_000_000);
            ZoneOffset off = ZoneOffset.UTC;
            String z = m.group(8);
            if (z != null && !z.equals("Z") && !z.equals("UTC")) {
                String d = z.replace(":", "");
                off = ZoneOffset.ofHoursMinutes(Integer.parseInt(d.substring(0, 3)), d.length() > 3 ? Integer.parseInt(d.charAt(0) + d.substring(3)) : 0);
            }
            return ldt.toInstant(off).toEpochMilli();
        } catch (RuntimeException e) {
            throw CqlError.invalid("Unable to parse a date/time from '" + s + "'");
        }
    }

    static int parseDate(String s) {
        String t = s.trim();
        try {
            if (t.matches("-?\\d+")) {
                return (int) (Long.parseLong(t) - DATE_OFFSET);
            }
            return (int) LocalDate.parse(t).toEpochDay();
        } catch (RuntimeException e) {
            throw CqlError.invalid("Unable to coerce '" + s + "' to a formatted date (long)");
        }
    }

    static long parseTime(String s) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d{1,2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?").matcher(s.trim());
        if (!m.matches()) {
            if (s.trim().matches("\\d+")) {
                return Long.parseLong(s.trim());
            }
            throw CqlError.invalid("(TimeType) Unable to coerce '" + s + "' to a formatted time (long)");
        }
        long h = Long.parseLong(m.group(1)), mi = Long.parseLong(m.group(2)), se = m.group(3) == null ? 0 : Long.parseLong(m.group(3));
        long ns = m.group(4) == null ? 0 : Long.parseLong((m.group(4) + "000000000").substring(0, 9));
        if (h > 23 || mi > 59 || se > 59) {
            throw CqlError.invalid("(TimeType) Unable to coerce '" + s + "' to a formatted time (long)");
        }
        return ((h * 60 + mi) * 60 + se) * 1_000_000_000L + ns;
    }

    static String formatTime(long ns) {
        long h = ns / 3_600_000_000_000L;
        ns %= 3_600_000_000_000L;
        long m = ns / 60_000_000_000L;
        ns %= 60_000_000_000L;
        long s = ns / 1_000_000_000L;
        ns %= 1_000_000_000L;
        return String.format("%02d:%02d:%02d.%09d", h, m, s, ns);
    }

    private static final java.util.regex.Pattern DUR_STD = java.util.regex.Pattern
            .compile("(\\d+)(y|mo|ms|w|d|h|m|us|µs|ns|s)", java.util.regex.Pattern.CASE_INSENSITIVE);

    static long[] parseDuration(String s) {
        String t = s.trim();
        boolean neg = t.startsWith("-");
        if (neg) {
            t = t.substring(1);
        }
        long months = 0, days = 0, nanos = 0;
        if (t.startsWith("P") || t.startsWith("p")) {
            java.util.regex.Matcher iso = java.util.regex.Pattern
                    .compile("[Pp](?:(\\d+)[Yy])?(?:(\\d+)[Mm])?(?:(\\d+)[Ww])?(?:(\\d+)[Dd])?(?:[Tt](?:(\\d+)[Hh])?(?:(\\d+)[Mm])?(?:(\\d+)[Ss])?)?")
                    .matcher(t);
            java.util.regex.Matcher alt = java.util.regex.Pattern.compile("[Pp](\\d{4})-(\\d{2})-(\\d{2})[Tt](\\d{2}):(\\d{2}):(\\d{2})").matcher(t);
            if (alt.matches()) {
                months = Long.parseLong(alt.group(1)) * 12 + Long.parseLong(alt.group(2));
                days = Long.parseLong(alt.group(3));
                nanos = ((Long.parseLong(alt.group(4)) * 60 + Long.parseLong(alt.group(5))) * 60 + Long.parseLong(alt.group(6))) * 1_000_000_000L;
            } else if (iso.matches() && t.length() > 1) {
                months = num(iso.group(1)) * 12 + num(iso.group(2));
                days = num(iso.group(3)) * 7 + num(iso.group(4));
                nanos = (num(iso.group(5)) * 3600 + num(iso.group(6)) * 60 + num(iso.group(7))) * 1_000_000_000L;
            } else {
                throw CqlError.invalid("Unable to convert '" + s + "' to a duration");
            }
        } else {
            java.util.regex.Matcher m = DUR_STD.matcher(t);
            int pos = 0;
            while (pos < t.length()) {
                if (!m.find(pos) || m.start() != pos) {
                    throw CqlError.invalid("Unable to convert '" + s + "' to a duration");
                }
                long n = Long.parseLong(m.group(1));
                switch (m.group(2).toLowerCase()) {
                    case "y" -> months += n * 12;
                    case "mo" -> months += n;
                    case "w" -> days += n * 7;
                    case "d" -> days += n;
                    case "h" -> nanos += n * 3_600_000_000_000L;
                    case "m" -> nanos += n * 60_000_000_000L;
                    case "s" -> nanos += n * 1_000_000_000L;
                    case "ms" -> nanos += n * 1_000_000L;
                    case "us", "µs" -> nanos += n * 1000L;
                    default -> nanos += n;
                }
                pos = m.end();
            }
        }
        return neg ? new long[] {-months, -days, -nanos} : new long[] {months, days, nanos};
    }

    private static long num(String s) {
        return s == null ? 0 : Long.parseLong(s);
    }

    static String formatDuration(long[] d) {
        boolean neg = d[0] < 0 || d[1] < 0 || d[2] < 0;
        long mo = Math.abs(d[0]), da = Math.abs(d[1]), ns = Math.abs(d[2]);
        StringBuilder b = new StringBuilder(neg ? "-" : "");
        if (mo / 12 > 0) {
            b.append(mo / 12).append('y');
        }
        if (mo % 12 > 0) {
            b.append(mo % 12).append("mo");
        }
        if (da > 0) {
            b.append(da).append('d');
        }
        long[] u = {3_600_000_000_000L, 60_000_000_000L, 1_000_000_000L, 1_000_000L, 1000L, 1L};
        String[] n = {"h", "m", "s", "ms", "us", "ns"};
        for (int i = 0; i < u.length; i++) {
            if (ns / u[i] > 0) {
                b.append(ns / u[i]).append(n[i]);
                ns %= u[i];
            }
        }
        return b.length() == 0 || b.toString().equals("-") ? "0ns" : b.toString();
    }

    static String hex(byte[] b) {
        StringBuilder s = new StringBuilder(b.length * 2);
        for (byte x : b) {
            s.append(Character.forDigit((x >> 4) & 15, 16)).append(Character.forDigit(x & 15, 16));
        }
        return s.toString();
    }

    static byte[] unhex(String h) {
        if (h.length() % 2 != 0) {
            throw CqlError.invalid("Invalid hex string length");
        }
        byte[] b = new byte[h.length() / 2];
        for (int i = 0; i < b.length; i++) {
            int hi = Character.digit(h.charAt(2 * i), 16), lo = Character.digit(h.charAt(2 * i + 1), 16);
            if (hi < 0 || lo < 0) {
                throw CqlError.invalid("Invalid hex digit");
            }
            b[i] = (byte) (hi * 16 + lo);
        }
        return b;
    }

    /** Text form of a value as CQL prints it in a cast to text / literals in error messages. */
    String format(Object v) {
        if (v == null) {
            return "null";
        }
        switch (k) {
            case ASCII, VARCHAR, INT, BIGINT, SMALLINT, TINYINT, COUNTER, BOOLEAN, UUID, TIMEUUID, VARINT -> {
                return v.toString();
            }
            case DECIMAL -> {
                return ((BigDecimal) v).toString();
            }
            case DOUBLE, FLOAT -> {
                return v.toString();
            }
            case BLOB -> {
                return "0x" + hex((byte[]) v);
            }
            case TIMESTAMP -> {
                return TS_OUT.format(Instant.ofEpochMilli((Long) v));
            }
            case DATE -> {
                return LocalDate.ofEpochDay((Integer) v).toString();
            }
            case TIME -> {
                return formatTime((Long) v);
            }
            case INET -> {
                return ((InetAddress) v).getHostAddress();
            }
            case DURATION -> {
                return formatDuration((long[]) v);
            }
            case LIST -> {
                return listText((java.util.Collection<?>) v, "[", "]");
            }
            case SET -> {
                return listText((java.util.Collection<?>) v, "{", "}");
            }
            case MAP -> {
                StringBuilder b = new StringBuilder("{");
                boolean first = true;
                for (var e : ((Map<?, ?>) v).entrySet()) {
                    b.append(first ? "" : ", ").append(lit(args.get(0), e.getKey())).append(": ").append(lit(args.get(1), e.getValue()));
                    first = false;
                }
                return b.append('}').toString();
            }
            default -> {
                Object[] t = (Object[]) v;
                StringBuilder b = new StringBuilder(k == K.TUPLE ? "(" : "{");
                for (int i = 0; i < args.size(); i++) {
                    if (i > 0) {
                        b.append(", ");
                    }
                    if (k == K.UDT) {
                        b.append(fieldNames.get(i)).append(": ");
                    }
                    b.append(lit(args.get(i), i < t.length ? t[i] : null));
                }
                return b.append(k == K.TUPLE ? ")" : "}").toString();
            }
        }
    }

    private String listText(java.util.Collection<?> c, String open, String close) {
        StringBuilder b = new StringBuilder(open);
        boolean first = true;
        for (Object x : c) {
            b.append(first ? "" : ", ").append(lit(args.get(0), x));
            first = false;
        }
        return b.append(close).toString();
    }

    private static String lit(CqlType t, Object v) {
        if (v == null) {
            return "null";
        }
        String f = t.format(v);
        return switch (t.k) {
            case ASCII, VARCHAR, TIMESTAMP, DATE, TIME, INET -> "'" + f.replace("'", "''") + "'";
            default -> f;
        };
    }

    static Map<String, CqlType> natives() {
        Map<String, CqlType> m = new LinkedHashMap<>();
        for (K x : K.values()) {
            if (x.id < 0x20) {
                m.put(x.cql, of(x));
            }
        }
        m.put("varchar", TEXT);
        return m;
    }
}
