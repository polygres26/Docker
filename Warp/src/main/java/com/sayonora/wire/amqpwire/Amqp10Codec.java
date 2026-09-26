package com.sayonora.wire.amqpwire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** AMQP 1.0 type system (OASIS AMQP 1.0 part 1): decoding of every primitive, list, map, array and described type; encoding of what the broker sends. */
final class Amqp10Codec {

    private Amqp10Codec() {
    }

    /** A symbol. */
    record Sym(String value) {
        @Override
        public String toString() {
            return value;
        }
    }

    /** A timestamp in milliseconds since the epoch. */
    record Ts(long millis) {
    }

    /** An unsigned integer of any width (ubyte, ushort, uint, ulong) kept as a long; the width matters only when encoding. */
    record UInt(long value) {
    }

    /** A described value: descriptor (ulong or symbol) and the described value. */
    record Described(Object descriptor, Object value) {
        long code() {
            return descriptor instanceof Long l ? l : descriptor instanceof UInt u ? u.value() : -1;
        }
    }

    /** Typed unsigned integers for encoding (a decoded unsigned is a {@link UInt}). */
    record U8(int v) {
    }

    record U16(int v) {
    }

    record U32(long v) {
    }

    record U64(long v) {
    }

    /** A "symbol, multiple" field: encoded as an array of symbols. */
    record SymArr(List<String> values) {
    }

    /** An AMQP map keeps the order of its entries; keys may be of any type. */
    static final class AMap extends LinkedHashMap<Object, Object> {
        private static final long serialVersionUID = 1L;
    }

    static final class AmqpFormatException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        AmqpFormatException(String m) {
            super(m, null, false, false);
        }
    }

    // ------------------------------------------------------------------------------------------ reading

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

        boolean more() {
            return p < end;
        }

        private void need(int n) {
            if (n < 0 || end - p < n) {
                throw new AmqpFormatException("short data");
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

        long i64() {
            return (u32() << 32) | u32();
        }

        byte[] bytes(long n) {
            if (n < 0 || n > end - p) {
                throw new AmqpFormatException("short data");
            }
            byte[] r = Arrays.copyOfRange(b, p, p + (int) n);
            p += (int) n;
            return r;
        }

        Object read() {
            int code = u8();
            if (code == 0x00) {
                Object d = read();
                Object v = read();
                return new Described(d, v);
            }
            return readTyped(code);
        }

        private Object readTyped(int code) {
            switch (code) {
                case 0x40:
                    return null;
                case 0x41:
                    return Boolean.TRUE;
                case 0x42:
                    return Boolean.FALSE;
                case 0x56:
                    return u8() != 0;
                case 0x43:
                    return new UInt(0);
                case 0x44:
                    return new UInt(0);
                case 0x50:
                    return new UInt(u8());
                case 0x60:
                    return new UInt(u16());
                case 0x52:
                case 0x53:
                    return new UInt(u8());
                case 0x70:
                    return new UInt(u32());
                case 0x80:
                    return new UInt(i64());
                case 0x51:
                    return (long) (byte) u8();
                case 0x61:
                    return (long) (short) u16();
                case 0x54:
                case 0x55:
                    return (long) (byte) u8();
                case 0x71:
                    return (long) (int) u32();
                case 0x81:
                    return i64();
                case 0x72:
                    return Float.intBitsToFloat((int) u32());
                case 0x82:
                    return Double.longBitsToDouble(i64());
                case 0x73:
                    return String.valueOf(Character.toChars((int) u32()));
                case 0x83:
                    return new Ts(i64());
                case 0x98: {
                    long hi = i64();
                    long lo = i64();
                    return new UUID(hi, lo);
                }
                case 0xa0:
                    return bytes(u8());
                case 0xb0:
                    return bytes(u32());
                case 0xa1:
                    return new String(bytes(u8()), StandardCharsets.UTF_8);
                case 0xb1:
                    return new String(bytes(u32()), StandardCharsets.UTF_8);
                case 0xa3:
                    return new Sym(new String(bytes(u8()), StandardCharsets.US_ASCII));
                case 0xb3:
                    return new Sym(new String(bytes(u32()), StandardCharsets.US_ASCII));
                case 0x45:
                    return new ArrayList<Object>();
                case 0xc0:
                case 0xd0: {
                    long size = code == 0xc0 ? u8() : u32();
                    int stop = p + (int) size;
                    if (size < 0 || stop > end) {
                        throw new AmqpFormatException("short list");
                    }
                    long count = code == 0xc0 ? u8() : u32();
                    List<Object> l = new ArrayList<>();
                    for (long i = 0; i < count; i++) {
                        l.add(read());
                    }
                    p = stop;
                    return l;
                }
                case 0xc1:
                case 0xd1: {
                    long size = code == 0xc1 ? u8() : u32();
                    int stop = p + (int) size;
                    if (size < 0 || stop > end) {
                        throw new AmqpFormatException("short map");
                    }
                    long count = code == 0xc1 ? u8() : u32();
                    AMap m = new AMap();
                    for (long i = 0; i + 1 < count; i += 2) {
                        Object k = read();
                        m.put(k instanceof UInt u ? (Object) u.value() : k, read());
                    }
                    p = stop;
                    return m;
                }
                case 0xe0:
                case 0xf0: {
                    long size = code == 0xe0 ? u8() : u32();
                    int stop = p + (int) size;
                    if (size < 0 || stop > end) {
                        throw new AmqpFormatException("short array");
                    }
                    long count = code == 0xe0 ? u8() : u32();
                    int elem = u8();
                    List<Object> l = new ArrayList<>();
                    for (long i = 0; i < count; i++) {
                        l.add(elem == 0x00 ? new Described(read(), readTyped(u8())) : readTyped(elem));
                    }
                    p = stop;
                    return l;
                }
                default:
                    throw new AmqpFormatException("unknown format code 0x" + Integer.toHexString(code));
            }
        }
    }

    // ------------------------------------------------------------------------------------------ writing

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
            return u8(v >> 8).u8(v);
        }

        Writer u32(long v) {
            return u8((int) (v >> 24)).u8((int) (v >> 16)).u8((int) (v >> 8)).u8((int) v);
        }

        Writer i64(long v) {
            return u32(v >>> 32).u32(v & 0xffffffffL);
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

        byte[] toBytes() {
            return Arrays.copyOf(buf, n);
        }

        Writer nul() {
            return u8(0x40);
        }

        Writer bool(boolean v) {
            return u8(v ? 0x41 : 0x42);
        }

        Writer ubyte(int v) {
            return u8(0x50).u8(v);
        }

        Writer ushort(int v) {
            return u8(0x60).u16(v);
        }

        Writer uint(long v) {
            if (v == 0) {
                return u8(0x43);
            }
            return v < 256 ? u8(0x52).u8((int) v) : u8(0x70).u32(v);
        }

        Writer ulong(long v) {
            if (v == 0) {
                return u8(0x44);
            }
            return v > 0 && v < 256 ? u8(0x53).u8((int) v) : u8(0x80).i64(v);
        }

        Writer sint(long v) {
            return v >= -128 && v <= 127 ? u8(0x54).u8((int) v) : u8(0x71).u32(v);
        }

        Writer slong(long v) {
            return v >= -128 && v <= 127 ? u8(0x55).u8((int) v) : u8(0x81).i64(v);
        }

        Writer timestamp(long ms) {
            return u8(0x83).i64(ms);
        }

        Writer str(String s) {
            byte[] a = s.getBytes(StandardCharsets.UTF_8);
            return a.length < 256 ? u8(0xa1).u8(a.length).raw(a) : u8(0xb1).u32(a.length).raw(a);
        }

        Writer sym(String s) {
            byte[] a = s.getBytes(StandardCharsets.US_ASCII);
            return a.length < 256 ? u8(0xa3).u8(a.length).raw(a) : u8(0xb3).u32(a.length).raw(a);
        }

        Writer bin(byte[] a) {
            return a.length < 256 ? u8(0xa0).u8(a.length).raw(a) : u8(0xb0).u32(a.length).raw(a);
        }

        Writer uuid(UUID u) {
            return u8(0x98).i64(u.getMostSignificantBits()).i64(u.getLeastSignificantBits());
        }

        /** A list whose items were written by {@code items} (count of items given). */
        Writer list(int count, java.util.function.Consumer<Writer> items) {
            Writer body = new Writer();
            items.accept(body);
            if (count == 0) {
                return u8(0x45);
            }
            if (body.n + 1 < 256 && count < 256) {
                return u8(0xc0).u8(body.n + 1).u8(count).raw(body.buf, 0, body.n);
            }
            return u8(0xd0).u32(body.n + 4).u32(count).raw(body.buf, 0, body.n);
        }

        /** An array of symbols (the encoding of "symbol, multiple" fields). */
        Writer symArray(String... syms) {
            Writer body = new Writer();
            for (String s : syms) {
                byte[] a = s.getBytes(StandardCharsets.US_ASCII);
                body.u8(a.length).raw(a);
            }
            if (body.n + 2 < 256 && syms.length < 256) {
                return u8(0xe0).u8(body.n + 2).u8(syms.length).u8(0xa3).raw(body.buf, 0, body.n);
            }
            return u8(0xf0).u32(body.n + 5).u32(syms.length).u8(0xa3).raw(body.buf, 0, body.n);
        }

        Writer map(int pairs, java.util.function.Consumer<Writer> items) {
            Writer body = new Writer();
            items.accept(body);
            if (body.n + 1 < 256 && pairs * 2 < 256) {
                return u8(0xc1).u8(body.n + 1).u8(pairs * 2).raw(body.buf, 0, body.n);
            }
            return u8(0xd1).u32(body.n + 4).u32(pairs * 2L).raw(body.buf, 0, body.n);
        }

        Writer described(long code) {
            return u8(0x00).ulong(code);
        }

        /** Writes any decoded value back (used when converting 0-9-1 headers to application properties and re-encoding message sections). */
        @SuppressWarnings("unchecked")
        Writer value(Object v) {
            if (v == null) {
                return nul();
            } else if (v instanceof Boolean x) {
                return bool(x);
            } else if (v instanceof Long x) {
                return slong(x);
            } else if (v instanceof Integer x) {
                return sint(x);
            } else if (v instanceof Short x) {
                return sint(x);
            } else if (v instanceof Byte x) {
                return sint(x);
            } else if (v instanceof UInt x) {
                return ulong(x.value());
            } else if (v instanceof Double x) {
                return u8(0x82).i64(Double.doubleToLongBits(x));
            } else if (v instanceof Float x) {
                return u8(0x72).u32(Float.floatToIntBits(x));
            } else if (v instanceof String x) {
                return str(x);
            } else if (v instanceof Sym x) {
                return sym(x.value());
            } else if (v instanceof byte[] x) {
                return bin(x);
            } else if (v instanceof Ts x) {
                return timestamp(x.millis());
            } else if (v instanceof UUID x) {
                return uuid(x);
            } else if (v instanceof U8 x) {
                return ubyte(x.v());
            } else if (v instanceof U16 x) {
                return ushort(x.v());
            } else if (v instanceof U32 x) {
                return uint(x.v());
            } else if (v instanceof U64 x) {
                return ulong(x.v());
            } else if (v instanceof SymArr x) {
                return symArray(x.values().toArray(new String[0]));
            } else if (v instanceof Described x) {
                u8(0x00).value(x.descriptor());
                return value(x.value());
            } else if (v instanceof Map<?, ?> m) {
                return map(m.size(), w -> m.forEach((k, x) -> w.value(k).value(x)));
            } else if (v instanceof List<?> l) {
                return list(l.size(), w -> l.forEach(w::value));
            }
            throw new IllegalArgumentException("cannot encode " + v.getClass());
        }
    }

    /** Reads a whole payload as a sequence of values (message sections). */
    static List<Object> readAll(byte[] payload) {
        Reader r = new Reader(payload);
        List<Object> out = new ArrayList<>();
        while (r.more()) {
            out.add(r.read());
        }
        return out;
    }

    static long asLong(Object o, long dflt) {
        if (o instanceof Long l) {
            return l;
        }
        if (o instanceof UInt u) {
            return u.value();
        }
        return dflt;
    }

    static boolean asBool(Object o, boolean dflt) {
        return o instanceof Boolean b ? b : dflt;
    }

    static String asString(Object o) {
        return o instanceof String s ? s : o instanceof Sym y ? y.value() : null;
    }
}
