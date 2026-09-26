package com.sayonora.warp.cqlwire;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Native protocol primitives: a reader over a frame body and a growable writer. */
final class Wire {

    private Wire() {
    }

    static final class In {
        final byte[] b;
        int p;
        final int end;

        In(byte[] b) {
            this(b, 0, b.length);
        }

        In(byte[] b, int off, int len) {
            this.b = b;
            this.p = off;
            this.end = off + len;
        }

        private void need(int n) {
            if (n < 0 || p + n > end) {
                throw new CqlError(CqlError.PROTOCOL, "Truncated frame body");
            }
        }

        int u8() {
            need(1);
            return b[p++] & 0xFF;
        }

        int i8() {
            need(1);
            return b[p++];
        }

        int u16() {
            need(2);
            int v = ((b[p] & 0xFF) << 8) | (b[p + 1] & 0xFF);
            p += 2;
            return v;
        }

        int i32() {
            need(4);
            int v = ((b[p] & 0xFF) << 24) | ((b[p + 1] & 0xFF) << 16) | ((b[p + 2] & 0xFF) << 8) | (b[p + 3] & 0xFF);
            p += 4;
            return v;
        }

        long i64() {
            long hi = i32() & 0xFFFFFFFFL;
            long lo = i32() & 0xFFFFFFFFL;
            return (hi << 32) | lo;
        }

        byte[] raw(int n) {
            need(n);
            byte[] r = java.util.Arrays.copyOfRange(b, p, p + n);
            p += n;
            return r;
        }

        String string() {
            return new String(raw(u16()), StandardCharsets.UTF_8);
        }

        String longString() {
            return new String(raw(i32()), StandardCharsets.UTF_8);
        }

        /** [bytes]: null for a negative length. */
        byte[] bytes() {
            int n = i32();
            return n < 0 ? null : raw(n);
        }

        /** [short bytes]. */
        byte[] shortBytes() {
            return raw(u16());
        }

        /** A [value]: null = SQL null, {@link #UNSET} = not set. */
        byte[] value() {
            int n = i32();
            if (n == -1) {
                return null;
            }
            if (n == -2) {
                return UNSET;
            }
            if (n < 0) {
                throw new CqlError(CqlError.PROTOCOL, "Invalid value length " + n);
            }
            return raw(n);
        }

        Map<String, String> stringMap() {
            int n = u16();
            Map<String, String> m = new java.util.LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                String k = string();
                m.put(k, string());
            }
            return m;
        }

        boolean more() {
            return p < end;
        }
    }

    static final byte[] UNSET = new byte[0];

    static final class Out {
        private final ByteArrayOutputStream o = new ByteArrayOutputStream(256);

        Out u8(int v) {
            o.write(v);
            return this;
        }

        Out u16(int v) {
            o.write(v >>> 8);
            o.write(v);
            return this;
        }

        Out i32(int v) {
            o.write(v >>> 24);
            o.write(v >>> 16);
            o.write(v >>> 8);
            o.write(v);
            return this;
        }

        Out i64(long v) {
            i32((int) (v >>> 32));
            return i32((int) v);
        }

        Out raw(byte[] b) {
            o.write(b, 0, b.length);
            return this;
        }

        Out string(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            u16(b.length);
            return raw(b);
        }

        Out longString(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            i32(b.length);
            return raw(b);
        }

        Out bytes(byte[] b) {
            if (b == null) {
                return i32(-1);
            }
            i32(b.length);
            return raw(b);
        }

        Out shortBytes(byte[] b) {
            u16(b.length);
            return raw(b);
        }

        Out stringList(List<String> l) {
            u16(l.size());
            for (String s : l) {
                string(s);
            }
            return this;
        }

        Out stringMultimap(Map<String, List<String>> m) {
            u16(m.size());
            for (var e : m.entrySet()) {
                string(e.getKey());
                stringList(e.getValue());
            }
            return this;
        }

        Out inet(java.net.InetSocketAddress a) {
            byte[] ip = a.getAddress().getAddress();
            u8(ip.length);
            raw(ip);
            return i32(a.getPort());
        }

        int size() {
            return o.size();
        }

        byte[] toBytes() {
            return o.toByteArray();
        }
    }
}
