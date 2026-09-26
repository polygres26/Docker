package com.sayonora.wire.cqlwire;

/** Cassandra's Murmur3Partitioner token (MurmurHash3 x64 128, first half, with Cassandra's signed-byte tail quirk). */
final class Murmur3 {

    private Murmur3() {
    }

    private static long rotl(long v, int r) {
        return (v << r) | (v >>> (64 - r));
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long block(byte[] k, int i) {
        long v = 0;
        for (int j = 7; j >= 0; j--) {
            v = (v << 8) | (k[i + j] & 0xFFL);
        }
        return v;
    }

    static long token(byte[] key) {
        long h1 = hash(key);
        return h1 == Long.MIN_VALUE ? Long.MAX_VALUE : h1;
    }

    static long hash(byte[] key) {
        final long c1 = 0x87c37b91114253d5L, c2 = 0x4cf5ad432745937fL;
        int len = key.length;
        int nblocks = len >> 4;
        long h1 = 0, h2 = 0;
        for (int i = 0; i < nblocks; i++) {
            long k1 = block(key, i * 16), k2 = block(key, i * 16 + 8);
            k1 *= c1;
            k1 = rotl(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= c2;
            k2 = rotl(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        int off = nblocks * 16;
        long k1 = 0, k2 = 0;
        switch (len & 15) {
            case 15: k2 ^= ((long) key[off + 14]) << 48;
            case 14: k2 ^= ((long) key[off + 13]) << 40;
            case 13: k2 ^= ((long) key[off + 12]) << 32;
            case 12: k2 ^= ((long) key[off + 11]) << 24;
            case 11: k2 ^= ((long) key[off + 10]) << 16;
            case 10: k2 ^= ((long) key[off + 9]) << 8;
            case 9:
                k2 ^= key[off + 8];
                k2 *= c2;
                k2 = rotl(k2, 33);
                k2 *= c1;
                h2 ^= k2;
            case 8: k1 ^= ((long) key[off + 7]) << 56;
            case 7: k1 ^= ((long) key[off + 6]) << 48;
            case 6: k1 ^= ((long) key[off + 5]) << 40;
            case 5: k1 ^= ((long) key[off + 4]) << 32;
            case 4: k1 ^= ((long) key[off + 3]) << 24;
            case 3: k1 ^= ((long) key[off + 2]) << 16;
            case 2: k1 ^= ((long) key[off + 1]) << 8;
            case 1:
                k1 ^= key[off];
                k1 *= c1;
                k1 = rotl(k1, 31);
                k1 *= c2;
                h1 ^= k1;
            default:
                break;
        }
        h1 ^= len;
        h2 ^= len;
        h1 += h2;
        h2 += h1;
        h1 = fmix(h1);
        h2 = fmix(h2);
        h1 += h2;
        return h1;
    }
}
