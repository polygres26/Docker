package com.sayonora.warp.rediswire;

import java.util.Arrays;

/**
 * HyperLogLog: the standard 16384-register sketch (Redis' 64-bit murmur hash, 14 index bits, Ertl's improved estimator,
 * ~0.81% standard error). Stored as a string value "HYLL" + one byte per register, so TYPE/GET/STRLEN behave like Redis
 * (the bytes are not Redis' packed encoding).
 */
final class HllCmds {

    static final int P = 14;
    static final int M = 1 << P;
    static final int Q = 64 - P;
    private static final byte[] MAGIC = {'H', 'Y', 'L', 'L'};
    private static final String INVALID = "WRONGTYPE Key is not a valid HyperLogLog string value.";

    private HllCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("pfadd", -2, "write denyoom fast", 1, 1, 1, "@write @hyperloglog @fast", HllCmds::pfadd);
        r.add("pfcount", -2, "readonly may_replicate", 1, -1, 1, "@read @hyperloglog @slow", HllCmds::pfcount);
        r.add("pfmerge", -2, "write denyoom", 1, -1, 1, "@write @hyperloglog @slow", HllCmds::pfmerge);
    }

    static long murmur64a(byte[] key, long seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        int len = key.length;
        long h = seed ^ (len * m);
        int nblocks = len / 8;
        for (int i = 0; i < nblocks; i++) {
            long k = 0;
            for (int j = 7; j >= 0; j--) {
                k = (k << 8) | (key[i * 8 + j] & 0xffL);
            }
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
        }
        int tail = nblocks * 8;
        switch (len & 7) {
            case 7: h ^= (key[tail + 6] & 0xffL) << 48;
            case 6: h ^= (key[tail + 5] & 0xffL) << 40;
            case 5: h ^= (key[tail + 4] & 0xffL) << 32;
            case 4: h ^= (key[tail + 3] & 0xffL) << 24;
            case 3: h ^= (key[tail + 2] & 0xffL) << 16;
            case 2: h ^= (key[tail + 1] & 0xffL) << 8;
            case 1: h ^= (key[tail] & 0xffL);
                h *= m;
            default:
                break;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }

    /** Adds an element to the registers; true when a register changed. */
    static boolean add(byte[] regs, byte[] element) {
        long hash = murmur64a(element, 0xadc83b19L);
        int index = (int) (hash & (M - 1));
        hash >>>= P;
        hash |= 1L << Q;
        int count = 1;
        long bit = 1;
        while ((hash & bit) == 0) {
            count++;
            bit <<= 1;
        }
        if (regs[4 + index] < count) {
            regs[4 + index] = (byte) count;
            return true;
        }
        return false;
    }

    static byte[] fresh() {
        byte[] b = new byte[4 + M];
        System.arraycopy(MAGIC, 0, b, 0, 4);
        return b;
    }

    static byte[] validate(byte[] v) {
        if (v.length != 4 + M || v[0] != 'H' || v[1] != 'Y' || v[2] != 'L' || v[3] != 'L') {
            throw new RedisError(INVALID);
        }
        return v;
    }

    private static double sigma(double x) {
        if (x == 1.0) {
            return Double.POSITIVE_INFINITY;
        }
        double zPrime;
        double y = 1;
        double z = x;
        do {
            x *= x;
            zPrime = z;
            z += x * y;
            y += y;
        } while (zPrime != z);
        return z;
    }

    private static double tau(double x) {
        if (x == 0.0 || x == 1.0) {
            return 0.0;
        }
        double zPrime;
        double y = 1.0;
        double z = 1 - x;
        do {
            x = Math.sqrt(x);
            zPrime = z;
            y *= 0.5;
            z -= Math.pow(1 - x, 2) * y;
        } while (zPrime != z);
        return z / 3;
    }

    static long count(byte[] regs) {
        int[] histo = new int[64];
        for (int i = 0; i < M; i++) {
            histo[regs[4 + i]]++;
        }
        double z = M * tau((M - histo[Q + 1]) / (double) M);
        for (int j = Q; j >= 1; --j) {
            z += histo[j];
            z *= 0.5;
        }
        z += M * sigma(histo[0] / (double) M);
        double alphaInf = 0.721347520444481703680;
        return Math.round(alphaInf * M * (double) M / z);
    }

    private static byte[] load(Ctx x, byte[] key) throws Exception {
        byte[] v = StringCmds.getString(x, key);
        return v == null ? null : validate(v);
    }

    private static Object pfadd(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            byte[] regs = m.created() || m.sv() == null ? fresh() : validate(m.sv());
            boolean changed = m.created();
            for (int i = 2; i < a.length; i++) {
                changed |= add(regs, a[i]);
            }
            if (changed) {
                x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", regs, x.db, a[1]);
            }
            return changed ? 1L : 0L;
        });
    }

    private static Object pfcount(Ctx x, byte[][] a) throws Exception {
        byte[] merged = null;
        for (int i = 1; i < a.length; i++) {
            byte[] v = load(x, a[i]);
            if (v == null) {
                continue;
            }
            if (a.length == 2) {
                return count(v);
            }
            if (merged == null) {
                merged = v.clone();
            } else {
                for (int j = 4; j < v.length; j++) {
                    if (v[j] > merged[j]) {
                        merged[j] = v[j];
                    }
                }
            }
        }
        return merged == null ? 0L : count(merged);
    }

    private static Object pfmerge(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            byte[] merged = fresh();
            Ctx.Meta dm = x.lockExisting(a[1], Ctx.T_STRING);
            if (dm != null) {
                merged = validate(dm.sv() == null ? new byte[0] : dm.sv()).clone();
            }
            for (int i = 2; i < a.length; i++) {
                byte[] v = load(x, a[i]);
                if (v != null) {
                    for (int j = 4; j < v.length; j++) {
                        if (v[j] > merged[j]) {
                            merged[j] = v[j];
                        }
                    }
                }
            }
            StringCmds.putString(x, a[1], merged, dm == null ? null : dm.exp());
            return Resp.OK;
        });
    }
}
