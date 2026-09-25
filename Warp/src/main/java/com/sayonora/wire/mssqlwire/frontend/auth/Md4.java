package com.sayonora.wire.mssqlwire.frontend.auth;

/**
 * A standalone MD4 digest (RFC 1320) -- NTLMv2's NT hash (NTOWFv2, see {@link NtlmMessages}) is
 * defined as {@code MD4(UTF-16LE(password))}, and unlike MD5/SHA-*, MD4 was dropped from the JDK's
 * built-in {@code MessageDigest} providers (and this project has no BouncyCastle-style crypto
 * dependency to reach for instead), so it's implemented here directly rather than pulled in as a
 * new third-party dependency for one 64-bit-block hash function. Textbook RFC 1320 reference
 * implementation -- MD4 has no place in anything security-sensitive on its own (it's cryptanalyzed
 * to pieces), but NTLMv2 mandates it as exactly this one building block of a stronger
 * (HMAC-MD5-based) overall scheme, the same way it's used inside every real NTLMv2
 * implementation.
 */
final class Md4 {

    static byte[] digest(byte[] message) {
        int[] state = {0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476};

        int origLenBytes = message.length;
        long bitLen = (long) origLenBytes * 8;
        int paddedLen = ((origLenBytes + 8) / 64 + 1) * 64;
        byte[] padded = new byte[paddedLen];
        System.arraycopy(message, 0, padded, 0, origLenBytes);
        padded[origLenBytes] = (byte) 0x80;
        for (int i = 0; i < 8; i++) {
            padded[paddedLen - 8 + i] = (byte) ((bitLen >>> (8 * i)) & 0xFF);
        }

        int[] x = new int[16];
        for (int block = 0; block < paddedLen; block += 64) {
            for (int i = 0; i < 16; i++) {
                int off = block + i * 4;
                x[i] = (padded[off] & 0xFF) | ((padded[off + 1] & 0xFF) << 8)
                        | ((padded[off + 2] & 0xFF) << 16) | ((padded[off + 3] & 0xFF) << 24);
            }
            int a = state[0], b = state[1], c = state[2], d = state[3];

            // Round 1
            a = ff(a, b, c, d, x[0], 3); d = ff(d, a, b, c, x[1], 7); c = ff(c, d, a, b, x[2], 11); b = ff(b, c, d, a, x[3], 19);
            a = ff(a, b, c, d, x[4], 3); d = ff(d, a, b, c, x[5], 7); c = ff(c, d, a, b, x[6], 11); b = ff(b, c, d, a, x[7], 19);
            a = ff(a, b, c, d, x[8], 3); d = ff(d, a, b, c, x[9], 7); c = ff(c, d, a, b, x[10], 11); b = ff(b, c, d, a, x[11], 19);
            a = ff(a, b, c, d, x[12], 3); d = ff(d, a, b, c, x[13], 7); c = ff(c, d, a, b, x[14], 11); b = ff(b, c, d, a, x[15], 19);

            // Round 2
            a = gg(a, b, c, d, x[0], 3); d = gg(d, a, b, c, x[4], 5); c = gg(c, d, a, b, x[8], 9); b = gg(b, c, d, a, x[12], 13);
            a = gg(a, b, c, d, x[1], 3); d = gg(d, a, b, c, x[5], 5); c = gg(c, d, a, b, x[9], 9); b = gg(b, c, d, a, x[13], 13);
            a = gg(a, b, c, d, x[2], 3); d = gg(d, a, b, c, x[6], 5); c = gg(c, d, a, b, x[10], 9); b = gg(b, c, d, a, x[14], 13);
            a = gg(a, b, c, d, x[3], 3); d = gg(d, a, b, c, x[7], 5); c = gg(c, d, a, b, x[11], 9); b = gg(b, c, d, a, x[15], 13);

            // Round 3
            a = hh(a, b, c, d, x[0], 3); d = hh(d, a, b, c, x[8], 9); c = hh(c, d, a, b, x[4], 11); b = hh(b, c, d, a, x[12], 15);
            a = hh(a, b, c, d, x[2], 3); d = hh(d, a, b, c, x[10], 9); c = hh(c, d, a, b, x[6], 11); b = hh(b, c, d, a, x[14], 15);
            a = hh(a, b, c, d, x[1], 3); d = hh(d, a, b, c, x[9], 9); c = hh(c, d, a, b, x[5], 11); b = hh(b, c, d, a, x[13], 15);
            a = hh(a, b, c, d, x[3], 3); d = hh(d, a, b, c, x[11], 9); c = hh(c, d, a, b, x[7], 11); b = hh(b, c, d, a, x[15], 15);

            state[0] += a; state[1] += b; state[2] += c; state[3] += d;
        }

        byte[] out = new byte[16];
        for (int i = 0; i < 4; i++) {
            out[i * 4] = (byte) (state[i] & 0xFF);
            out[i * 4 + 1] = (byte) ((state[i] >>> 8) & 0xFF);
            out[i * 4 + 2] = (byte) ((state[i] >>> 16) & 0xFF);
            out[i * 4 + 3] = (byte) ((state[i] >>> 24) & 0xFF);
        }
        return out;
    }

    private static int rotl(int x, int n) {
        return (x << n) | (x >>> (32 - n));
    }

    private static int ff(int a, int b, int c, int d, int x, int s) {
        return rotl(a + ((b & c) | (~b & d)) + x, s);
    }

    private static int gg(int a, int b, int c, int d, int x, int s) {
        return rotl(a + ((b & c) | (b & d) | (c & d)) + x + 0x5a827999, s);
    }

    private static int hh(int a, int b, int c, int d, int x, int s) {
        return rotl(a + (b ^ c ^ d) + x + 0x6ed9eba1, s);
    }

    private Md4() {
    }
}
