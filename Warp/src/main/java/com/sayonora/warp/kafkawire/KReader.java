package com.sayonora.warp.kafkawire;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Cursor over one Kafka request body. {@code flex} selects the flexible-version encodings (compact strings/arrays, tagged fields). */
final class KReader {

    final byte[] b;
    int p;
    final int end;
    final boolean flex;

    KReader(byte[] b, int off, int end, boolean flex) {
        this.b = b;
        this.p = off;
        this.end = end;
        this.flex = flex;
    }

    private void need(int n) {
        if (n < 0 || p + n > end) {
            throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "request truncated");
        }
    }

    int i8() {
        need(1);
        return b[p++];
    }

    boolean bool() {
        return i8() != 0;
    }

    int i16() {
        need(2);
        int v = (short) (((b[p] & 0xff) << 8) | (b[p + 1] & 0xff));
        p += 2;
        return v;
    }

    int i32() {
        need(4);
        int v = ((b[p] & 0xff) << 24) | ((b[p + 1] & 0xff) << 16) | ((b[p + 2] & 0xff) << 8) | (b[p + 3] & 0xff);
        p += 4;
        return v;
    }

    long i64() {
        long hi = i32() & 0xffffffffL;
        long lo = i32() & 0xffffffffL;
        return (hi << 32) | lo;
    }

    int uvarint() {
        int v = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            int x = i8();
            v |= (x & 0x7f) << shift;
            if ((x & 0x80) == 0) {
                return v;
            }
        }
        throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "bad varint");
    }

    UUID uuid() {
        return new UUID(i64(), i64());
    }

    /** Nullable string. */
    String str() {
        int n = flex ? uvarint() - 1 : i16();
        if (n < 0) {
            return null;
        }
        need(n);
        String s = new String(b, p, n, StandardCharsets.UTF_8);
        p += n;
        return s;
    }

    String strNN() {
        String s = str();
        return s == null ? "" : s;
    }

    /** Nullable bytes. */
    byte[] bytes() {
        int n = flex ? uvarint() - 1 : i32();
        if (n < 0) {
            return null;
        }
        need(n);
        byte[] out = new byte[n];
        System.arraycopy(b, p, out, 0, n);
        p += n;
        return out;
    }

    /** Array length, -1 for a null array. */
    int arr() {
        int n = flex ? uvarint() - 1 : i32();
        if (n > end - p + 1 && n > 0 && n > 10_000_000) {
            throw new KafkaError(KafkaError.CORRUPT_MESSAGE, "array too long");
        }
        return n;
    }

    /** Skips the tagged-field section of a flexible struct (no-op for older versions). */
    void tagged() {
        if (!flex) {
            return;
        }
        int n = uvarint();
        for (int i = 0; i < n; i++) {
            uvarint();
            int len = uvarint();
            need(len);
            p += len;
        }
    }

    int remaining() {
        return end - p;
    }

    @SuppressWarnings("unused")
    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
