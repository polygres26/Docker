package com.sayonora.warp.tls.acme;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/** Minimal DER encoder (just enough for a PKCS#10 CSR and a placeholder X.509 certificate). */
final class Der {

    private Der() {
    }

    static byte[] tlv(int tag, byte[] content) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(tag);
        int n = content.length;
        if (n < 0x80) {
            o.write(n);
        } else if (n <= 0xff) {
            o.write(0x81);
            o.write(n);
        } else if (n <= 0xffff) {
            o.write(0x82);
            o.write(n >> 8);
            o.write(n);
        } else {
            o.write(0x83);
            o.write(n >> 16);
            o.write(n >> 8);
            o.write(n);
        }
        o.writeBytes(content);
        return o.toByteArray();
    }

    static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    static byte[] seq(byte[]... parts) {
        return tlv(0x30, cat(parts));
    }

    static byte[] set(byte[]... parts) {
        return tlv(0x31, cat(parts));
    }

    static byte[] integer(long v) {
        return tlv(0x02, BigInteger.valueOf(v).toByteArray());
    }

    static byte[] integer(BigInteger v) {
        return tlv(0x02, v.toByteArray());
    }

    static byte[] octets(byte[] b) {
        return tlv(0x04, b);
    }

    static byte[] bitString(byte[] b) {
        return tlv(0x03, cat(new byte[] {0}, b));
    }

    static byte[] utf8(String s) {
        return tlv(0x0c, s.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] ia5(String s) {
        return tlv(0x16, s.getBytes(StandardCharsets.US_ASCII));
    }

    static byte[] bool(boolean v) {
        return tlv(0x01, new byte[] {(byte) (v ? 0xff : 0)});
    }

    static byte[] ctx(int n, byte[] content) {
        return tlv(0xa0 | n, content);
    }

    static byte[] ctxPrim(int n, byte[] content) {
        return tlv(0x80 | n, content);
    }

    /** UTCTime for dates before 2050, else GeneralizedTime. */
    static byte[] time(java.time.Instant t) {
        java.time.ZonedDateTime z = t.atZone(java.time.ZoneOffset.UTC);
        if (z.getYear() < 2050) {
            return tlv(0x17, z.format(java.time.format.DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'")).getBytes(StandardCharsets.US_ASCII));
        }
        return tlv(0x18, z.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss'Z'")).getBytes(StandardCharsets.US_ASCII));
    }

    static byte[] oid(String dotted) {
        String[] p = dotted.split("\\.");
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(Integer.parseInt(p[0]) * 40 + Integer.parseInt(p[1]));
        for (int i = 2; i < p.length; i++) {
            long v = Long.parseLong(p[i]);
            byte[] tmp = new byte[10];
            int n = 0;
            tmp[n++] = (byte) (v & 0x7f);
            v >>= 7;
            while (v > 0) {
                tmp[n++] = (byte) ((v & 0x7f) | 0x80);
                v >>= 7;
            }
            for (int j = n - 1; j >= 0; j--) {
                o.write(tmp[j]);
            }
        }
        return tlv(0x06, o.toByteArray());
    }

    static final byte[] NULL = {0x05, 0x00};
}
