package com.sayonora.wire.kafkawire;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

/** Growable Kafka response body writer; {@code flex} selects the flexible-version encodings. */
final class KWriter {

    byte[] b = new byte[256];
    int n;
    final boolean flex;

    KWriter(boolean flex) {
        this.flex = flex;
    }

    private void ensure(int more) {
        if (n + more > b.length) {
            b = Arrays.copyOf(b, Math.max(b.length * 2, n + more));
        }
    }

    KWriter i8(int v) {
        ensure(1);
        b[n++] = (byte) v;
        return this;
    }

    KWriter bool(boolean v) {
        return i8(v ? 1 : 0);
    }

    KWriter i16(int v) {
        ensure(2);
        b[n++] = (byte) (v >> 8);
        b[n++] = (byte) v;
        return this;
    }

    KWriter i32(int v) {
        ensure(4);
        b[n++] = (byte) (v >> 24);
        b[n++] = (byte) (v >> 16);
        b[n++] = (byte) (v >> 8);
        b[n++] = (byte) v;
        return this;
    }

    KWriter i64(long v) {
        i32((int) (v >> 32));
        return i32((int) v);
    }

    KWriter uvarint(int v) {
        ensure(5);
        while ((v & ~0x7f) != 0) {
            b[n++] = (byte) ((v & 0x7f) | 0x80);
            v >>>= 7;
        }
        b[n++] = (byte) v;
        return this;
    }

    KWriter uuid(UUID u) {
        i64(u.getMostSignificantBits());
        return i64(u.getLeastSignificantBits());
    }

    KWriter raw(byte[] src, int off, int len) {
        ensure(len);
        System.arraycopy(src, off, b, n, len);
        n += len;
        return this;
    }

    /** Nullable string. */
    KWriter str(String s) {
        if (s == null) {
            if (flex) {
                return uvarint(0);
            }
            return i16(-1);
        }
        byte[] u = s.getBytes(StandardCharsets.UTF_8);
        if (flex) {
            uvarint(u.length + 1);
        } else {
            i16(u.length);
        }
        return raw(u, 0, u.length);
    }

    /** Nullable bytes. */
    KWriter bytes(byte[] v) {
        if (v == null) {
            return flex ? uvarint(0) : i32(-1);
        }
        return bytes(v, 0, v.length);
    }

    KWriter bytes(byte[] v, int off, int len) {
        if (flex) {
            uvarint(len + 1);
        } else {
            i32(len);
        }
        return raw(v, off, len);
    }

    /** Array header; -1 writes a null array. */
    KWriter arr(int len) {
        if (flex) {
            return uvarint(len + 1);
        }
        return i32(len);
    }

    KWriter tagged() {
        return flex ? uvarint(0) : this;
    }

    byte[] toBytes() {
        return Arrays.copyOf(b, n);
    }
}
