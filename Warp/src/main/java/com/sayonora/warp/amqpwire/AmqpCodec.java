package com.sayonora.warp.amqpwire;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** AMQP 0-9-1 primitive encoding: octets, shorts, longs, short/long strings, field tables (RabbitMQ's type letters). */
final class AmqpCodec {

    private AmqpCodec() {
    }

    /** A field-table timestamp (seconds since the epoch, type {@code T}). */
    record Ts(long seconds) {
    }

    static final class Reader {
        final byte[] b;
        int p;
        final int end;

        Reader(byte[] b) {
            this(b, 0, b.length);
        }

        Reader(byte[] b, int off, int end) {
            this.b = b;
            this.p = off;
            this.end = end;
        }

        int remaining() {
            return end - p;
        }

        private void need(int n) {
            if (n < 0 || end - p < n) {
                throw AmqpException.conn(501, "FRAME_ERROR - short frame payload");
            }
        }

        int u8() {
            need(1);
            return b[p++] & 0xff;
        }

        int u16() {
            need(2);
            int v = ((b[p] & 0xff) << 8) | (b[p + 1] & 0xff);
            p += 2;
            return v;
        }

        long u32() {
            need(4);
            long v = ((long) (b[p] & 0xff) << 24) | ((b[p + 1] & 0xff) << 16) | ((b[p + 2] & 0xff) << 8) | (b[p + 3] & 0xff);
            p += 4;
            return v;
        }

        int i32() {
            return (int) u32();
        }

        long i64() {
            long hi = u32();
            long lo = u32();
            return (hi << 32) | lo;
        }

        byte[] bytes(int n) {
            need(n);
            byte[] r = Arrays.copyOfRange(b, p, p + n);
            p += n;
            return r;
        }

        String shortStr() {
            int n = u8();
            need(n);
            String s = new String(b, p, n, StandardCharsets.UTF_8);
            p += n;
            return s;
        }

        String longStr() {
            long n = u32();
            if (n > remaining()) {
                throw AmqpException.conn(501, "FRAME_ERROR - long string too long");
            }
            String s = new String(b, p, (int) n, StandardCharsets.UTF_8);
            p += (int) n;
            return s;
        }

        byte[] longStrBytes() {
            long n = u32();
            if (n > remaining()) {
                throw AmqpException.conn(501, "FRAME_ERROR - long string too long");
            }
            return bytes((int) n);
        }

        Map<String, Object> table() {
            long n = u32();
            if (n > remaining()) {
                throw AmqpException.conn(501, "FRAME_ERROR - table too long");
            }
            int stop = p + (int) n;
            Map<String, Object> m = new LinkedHashMap<>();
            while (p < stop) {
                String k = shortStr();
                m.put(k, value());
            }
            if (p != stop) {
                throw AmqpException.conn(502, "SYNTAX_ERROR - malformed table");
            }
            return m;
        }

        Object value() {
            int t = u8();
            switch (t) {
                case 't':
                    return u8() != 0;
                case 'b':
                    return (byte) u8();
                case 'B':
                    return u8();
                case 'U':
                case 's':
                    return (short) u16();
                case 'u':
                    return u16();
                case 'I':
                    return i32();
                case 'i':
                    return u32();
                case 'l':
                    return i64();
                case 'L': {
                    long v = i64();
                    return v;
                }
                case 'f':
                    return Float.intBitsToFloat(i32());
                case 'd':
                    return Double.longBitsToDouble(i64());
                case 'D': {
                    int scale = u8();
                    return new BigDecimal(BigInteger.valueOf(i32()), scale);
                }
                case 'S':
                    return longStr();
                case 'x':
                    return longStrBytes();
                case 'A': {
                    long n = u32();
                    if (n > remaining()) {
                        throw AmqpException.conn(501, "FRAME_ERROR - array too long");
                    }
                    int stop = p + (int) n;
                    List<Object> l = new ArrayList<>();
                    while (p < stop) {
                        l.add(value());
                    }
                    return l;
                }
                case 'T':
                    return new Ts(i64());
                case 'F':
                    return table();
                case 'V':
                    return null;
                default:
                    throw AmqpException.conn(502, "SYNTAX_ERROR - unknown field table value type " + (char) t);
            }
        }
    }

    static final class Writer {
        byte[] buf = new byte[128];
        int n;

        private void ensure(int k) {
            if (n + k > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, n + k));
            }
        }

        Writer u8(int v) {
            ensure(1);
            buf[n++] = (byte) v;
            return this;
        }

        Writer u16(int v) {
            ensure(2);
            buf[n++] = (byte) (v >> 8);
            buf[n++] = (byte) v;
            return this;
        }

        Writer u32(long v) {
            ensure(4);
            buf[n++] = (byte) (v >> 24);
            buf[n++] = (byte) (v >> 16);
            buf[n++] = (byte) (v >> 8);
            buf[n++] = (byte) v;
            return this;
        }

        Writer i64(long v) {
            u32(v >>> 32);
            return u32(v & 0xffffffffL);
        }

        Writer raw(byte[] a) {
            return raw(a, 0, a.length);
        }

        Writer raw(byte[] a, int off, int len) {
            ensure(len);
            System.arraycopy(a, off, buf, n, len);
            n += len;
            return this;
        }

        Writer shortStr(String s) {
            byte[] a = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
            if (a.length > 255) {
                throw AmqpException.conn(541, "INTERNAL_ERROR - short string too long");
            }
            u8(a.length);
            return raw(a);
        }

        Writer longStr(String s) {
            byte[] a = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
            u32(a.length);
            return raw(a);
        }

        Writer longStr(byte[] a) {
            u32(a.length);
            return raw(a);
        }

        Writer table(Map<String, Object> m) {
            Writer t = new Writer();
            if (m != null) {
                for (Map.Entry<String, Object> e : m.entrySet()) {
                    t.shortStr(e.getKey());
                    t.value(e.getValue());
                }
            }
            u32(t.n);
            return raw(t.buf, 0, t.n);
        }

        @SuppressWarnings("unchecked")
        Writer value(Object v) {
            if (v == null) {
                return u8('V');
            } else if (v instanceof Boolean x) {
                return u8('t').u8(x ? 1 : 0);
            } else if (v instanceof Byte x) {
                return u8('b').u8(x);
            } else if (v instanceof Short x) {
                return u8('s').u16(x);
            } else if (v instanceof Integer x) {
                return u8('I').u32(x);
            } else if (v instanceof Long x) {
                return u8('l').i64(x);
            } else if (v instanceof Float x) {
                return u8('f').u32(Float.floatToIntBits(x));
            } else if (v instanceof Double x) {
                return u8('d').i64(Double.doubleToLongBits(x));
            } else if (v instanceof BigDecimal x) {
                return u8('D').u8(x.scale()).u32(x.unscaledValue().intValue());
            } else if (v instanceof String x) {
                return u8('S').longStr(x);
            } else if (v instanceof byte[] x) {
                return u8('x').longStr(x);
            } else if (v instanceof Ts x) {
                return u8('T').i64(x.seconds());
            } else if (v instanceof List<?> x) {
                Writer t = new Writer();
                for (Object o : x) {
                    t.value(o);
                }
                u8('A').u32(t.n);
                return raw(t.buf, 0, t.n);
            } else if (v instanceof Map<?, ?> x) {
                u8('F');
                return table((Map<String, Object>) x);
            }
            throw new IllegalArgumentException("unsupported field value " + v.getClass());
        }

        byte[] toBytes() {
            return Arrays.copyOf(buf, n);
        }
    }

    static byte[] encodeTable(Map<String, Object> m) {
        Writer w = new Writer();
        w.table(m);
        return w.toBytes();
    }

    static Map<String, Object> decodeTable(byte[] b) {
        if (b == null || b.length == 0) {
            return new LinkedHashMap<>();
        }
        return new Reader(b).table();
    }

    /** Order- and numeric-width-insensitive text of a table, used to compare declared arguments and to key bindings. */
    static String canonical(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Map<?, ?> m) {
            StringBuilder sb = new StringBuilder("{");
            new TreeMap<String, Object>(cast(m)).forEach((k, x) -> sb.append(k).append('=').append(canonical(x)).append(','));
            return sb.append('}').toString();
        }
        if (v instanceof List<?> l) {
            StringBuilder sb = new StringBuilder("[");
            l.forEach(x -> sb.append(canonical(x)).append(','));
            return sb.append(']').toString();
        }
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return "n" + ((Number) v).longValue();
        }
        if (v instanceof byte[] b) {
            return "x" + Arrays.toString(b);
        }
        if (v instanceof Ts t) {
            return "T" + t.seconds();
        }
        return v instanceof String ? "s" + v : v.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    /** Numeric value of a table argument (any integer type), or null. */
    static Long asLong(Object v) {
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return ((Number) v).longValue();
        }
        return null;
    }
}
