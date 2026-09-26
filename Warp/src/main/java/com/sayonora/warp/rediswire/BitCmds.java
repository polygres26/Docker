package com.sayonora.warp.rediswire;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Bitmap commands on string values (bit 0 is the most significant bit of byte 0, like Redis). */
final class BitCmds {

    private static final long MAX_BIT = (1L << 32) - 1;

    private BitCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("setbit", 4, "write denyoom", 1, 1, 1, "@write @bitmap @slow", BitCmds::setbit);
        r.add("getbit", 3, "readonly fast", 1, 1, 1, "@read @bitmap @fast", BitCmds::getbit);
        r.add("bitcount", -2, "readonly", 1, 1, 1, "@read @bitmap @slow", BitCmds::bitcount);
        r.add("bitpos", -3, "readonly", 1, 1, 1, "@read @bitmap @slow", BitCmds::bitpos);
        r.add("bitop", -4, "write denyoom", 2, -1, 1, "@write @bitmap @slow", BitCmds::bitop);
        r.add("bitfield", -2, "write denyoom", 1, 1, 1, "@write @bitmap @slow", (x, a) -> bitfield(x, a, false));
        r.add("bitfield_ro", -2, "readonly fast", 1, 1, 1, "@read @bitmap @fast", (x, a) -> bitfield(x, a, true));
    }

    private static long bitOffset(byte[] b) {
        Long o = Num.parseLong(b);
        if (o == null || o < 0 || o > MAX_BIT) {
            throw RedisError.err("bit offset is not an integer or out of range");
        }
        return o;
    }

    private static Object setbit(Ctx x, byte[][] a) throws Exception {
        long off = bitOffset(a[2]);
        Long v = Num.parseLong(a[3]);
        if (v == null || (v != 0 && v != 1)) {
            throw RedisError.err("bit is not an integer or out of range");
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            byte[] cur = m.sv() == null ? new byte[0] : m.sv();
            int byteIdx = (int) (off >> 3);
            int mask = 0x80 >> (int) (off & 7);
            byte[] nv = cur.length > byteIdx ? cur : Arrays.copyOf(cur, byteIdx + 1);
            int old = (nv[byteIdx] & mask) != 0 ? 1 : 0;
            if (v == 1) {
                nv[byteIdx] |= (byte) mask;
            } else {
                nv[byteIdx] &= (byte) ~mask;
            }
            x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", nv, x.db, a[1]);
            return (long) old;
        });
    }

    private static Object getbit(Ctx x, byte[][] a) throws Exception {
        long off = bitOffset(a[2]);
        byte[] v = StringCmds.getString(x, a[1]);
        int byteIdx = (int) (off >> 3);
        if (v == null || byteIdx >= v.length) {
            return 0L;
        }
        return (v[byteIdx] & (0x80 >> (int) (off & 7))) != 0 ? 1L : 0L;
    }

    /** Resolves start/end (possibly negative) to an inclusive [from, to] range in bits or bytes; null when empty. */
    private static long[] range(long start, long end, long len) {
        if (start < 0) {
            start = Math.max(0, len + start);
        }
        if (end < 0) {
            end = len + end;
        }
        if (end < 0) {
            return null;
        }
        if (end >= len) {
            end = len - 1;
        }
        if (start > end) {
            return null;
        }
        return new long[] {start, end};
    }

    private static Object bitcount(Ctx x, byte[][] a) throws Exception {
        boolean bitMode = false;
        Long start = null;
        Long end = null;
        if (a.length == 3 || a.length > 5) {
            throw RedisError.syntax();
        }
        if (a.length >= 4) {
            start = Num.parseLong(a[2]);
            end = Num.parseLong(a[3]);
            if (start == null || end == null) {
                throw RedisError.notInt();
            }
            if (a.length == 5) {
                if (Num.eq(a[4], "BIT")) {
                    bitMode = true;
                } else if (!Num.eq(a[4], "BYTE")) {
                    throw RedisError.syntax();
                }
            }
        }
        byte[] v = StringCmds.getString(x, a[1]);
        if (v == null) {
            return 0L;
        }
        if (start == null) {
            long n = 0;
            for (byte b : v) {
                n += Integer.bitCount(b & 0xff);
            }
            return n;
        }
        long len = bitMode ? (long) v.length * 8 : v.length;
        long[] r = range(start, end, len);
        if (r == null) {
            return 0L;
        }
        long n = 0;
        if (!bitMode) {
            for (long i = r[0]; i <= r[1]; i++) {
                n += Integer.bitCount(v[(int) i] & 0xff);
            }
        } else {
            for (long i = r[0]; i <= r[1]; i++) {
                if ((v[(int) (i >> 3)] & (0x80 >> (int) (i & 7))) != 0) {
                    n++;
                }
            }
        }
        return n;
    }

    private static Object bitpos(Ctx x, byte[][] a) throws Exception {
        Long bit = Num.parseLong(a[2]);
        if (bit == null) {
            throw RedisError.notInt();
        }
        if (bit != 0 && bit != 1) {
            throw RedisError.err("The bit argument must be 1 or 0.");
        }
        boolean bitMode = false;
        Long start = null;
        Long end = null;
        if (a.length > 6) {
            throw RedisError.syntax();
        }
        if (a.length >= 4) {
            start = Num.parseLong(a[3]);
            if (start == null) {
                throw RedisError.notInt();
            }
        }
        if (a.length >= 5) {
            end = Num.parseLong(a[4]);
            if (end == null) {
                throw RedisError.notInt();
            }
        }
        if (a.length == 6) {
            if (Num.eq(a[5], "BIT")) {
                bitMode = true;
            } else if (!Num.eq(a[5], "BYTE")) {
                throw RedisError.syntax();
            }
        }
        byte[] v = StringCmds.getString(x, a[1]);
        if (v == null) {
            return bit == 1 ? -1L : 0L;
        }
        boolean endGiven = end != null;
        long totalBits = (long) v.length * 8;
        long len = bitMode ? totalBits : v.length;
        long s = start == null ? 0 : start;
        long e = end == null ? len - 1 : end;
        long[] r = range(s, e, len);
        if (r == null) {
            return -1L;
        }
        long fromBit = bitMode ? r[0] : r[0] * 8;
        long toBit = bitMode ? r[1] : r[1] * 8 + 7;
        for (long i = fromBit; i <= toBit; i++) {
            int b = (v[(int) (i >> 3)] & (0x80 >> (int) (i & 7))) != 0 ? 1 : 0;
            if (b == bit) {
                return i;
            }
        }
        if (bit == 0 && !endGiven) {
            return totalBits;
        }
        return -1L;
    }

    private static Object bitop(Ctx x, byte[][] a) throws Exception {
        String op = Num.upper(a[1]);
        if (!op.equals("AND") && !op.equals("OR") && !op.equals("XOR") && !op.equals("NOT")) {
            throw RedisError.syntax();
        }
        if (op.equals("NOT") && a.length != 4) {
            throw RedisError.err("BITOP NOT must be called with a single source key.");
        }
        byte[] dst = a[2];
        return x.atomic(() -> {
            List<byte[]> srcs = new ArrayList<>();
            int max = 0;
            for (int i = 3; i < a.length; i++) {
                byte[] v = StringCmds.getString(x, a[i]);
                srcs.add(v == null ? new byte[0] : v);
                max = Math.max(max, srcs.get(srcs.size() - 1).length);
            }
            byte[] res = new byte[max];
            for (int i = 0; i < max; i++) {
                int acc = 0;
                for (int s = 0; s < srcs.size(); s++) {
                    byte[] v = srcs.get(s);
                    int b = i < v.length ? v[i] & 0xff : 0;
                    if (s == 0) {
                        acc = b;
                    } else if (op.equals("AND")) {
                        acc &= b;
                    } else if (op.equals("OR")) {
                        acc |= b;
                    } else {
                        acc ^= b;
                    }
                }
                res[i] = (byte) (op.equals("NOT") ? ~acc : acc);
            }
            if (max == 0) {
                x.delete(dst);
            } else {
                StringCmds.putString(x, dst, res, null);
            }
            return (long) max;
        });
    }

    // ------------------------------------------------------------------------------------------
    // BITFIELD
    // ------------------------------------------------------------------------------------------

    private record FieldType(boolean signed, int bits) {
    }

    private static FieldType parseType(byte[] b) {
        String s = Num.str(b);
        if (s.length() < 2 || (s.charAt(0) != 'i' && s.charAt(0) != 'I' && s.charAt(0) != 'u' && s.charAt(0) != 'U')) {
            throw RedisError.err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
        }
        boolean signed = s.charAt(0) == 'i' || s.charAt(0) == 'I';
        Long bits = Num.parseLong(s.substring(1));
        if (bits == null || bits < 1 || (signed && bits > 64) || (!signed && bits > 63)) {
            throw RedisError.err("Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
        }
        return new FieldType(signed, bits.intValue());
    }

    private static long parseBfOffset(byte[] b, FieldType t) {
        String s = Num.str(b);
        boolean mult = s.startsWith("#");
        Long o = Num.parseLong(mult ? s.substring(1) : s);
        if (o == null || o < 0 || (mult && o > MAX_BIT / t.bits()) || (!mult && o > MAX_BIT)) {
            throw RedisError.err("bit offset is not an integer or out of range");
        }
        return mult ? o * t.bits() : o;
    }

    private static BigInteger readField(byte[] v, long off, FieldType t) {
        BigInteger val = BigInteger.ZERO;
        for (int i = 0; i < t.bits(); i++) {
            long bit = off + i;
            int b = (int) (bit >> 3) < v.length && (v[(int) (bit >> 3)] & (0x80 >> (int) (bit & 7))) != 0 ? 1 : 0;
            val = val.shiftLeft(1).or(BigInteger.valueOf(b));
        }
        if (t.signed() && val.testBit(t.bits() - 1)) {
            val = val.subtract(BigInteger.ONE.shiftLeft(t.bits()));
        }
        return val;
    }

    private static byte[] writeField(byte[] v, long off, FieldType t, BigInteger value) {
        long lastBit = off + t.bits() - 1;
        byte[] out = (int) (lastBit >> 3) < v.length ? v : Arrays.copyOf(v, (int) (lastBit >> 3) + 1);
        BigInteger unsigned = value.mod(BigInteger.ONE.shiftLeft(t.bits()));
        for (int i = 0; i < t.bits(); i++) {
            long bit = off + i;
            boolean set = unsigned.testBit(t.bits() - 1 - i);
            int mask = 0x80 >> (int) (bit & 7);
            if (set) {
                out[(int) (bit >> 3)] |= (byte) mask;
            } else {
                out[(int) (bit >> 3)] &= (byte) ~mask;
            }
        }
        return out;
    }

    private static BigInteger min(FieldType t) {
        return t.signed() ? BigInteger.ONE.shiftLeft(t.bits() - 1).negate() : BigInteger.ZERO;
    }

    private static BigInteger max(FieldType t) {
        return t.signed() ? BigInteger.ONE.shiftLeft(t.bits() - 1).subtract(BigInteger.ONE) : BigInteger.ONE.shiftLeft(t.bits()).subtract(BigInteger.ONE);
    }

    /** Applies overflow policy; returns null for FAIL overflow. */
    private static BigInteger fit(BigInteger v, FieldType t, String overflow) {
        if (v.compareTo(min(t)) >= 0 && v.compareTo(max(t)) <= 0) {
            return v;
        }
        switch (overflow) {
            case "FAIL":
                return null;
            case "SAT":
                return v.compareTo(min(t)) < 0 ? min(t) : max(t);
            default: {
                BigInteger mod = BigInteger.ONE.shiftLeft(t.bits());
                BigInteger w = v.mod(mod);
                if (t.signed() && w.testBit(t.bits() - 1)) {
                    w = w.subtract(mod);
                }
                return w;
            }
        }
    }

    private static Object bitfield(Ctx x, byte[][] a, boolean ro) throws Exception {
        // parse into operations first (errors must precede any change)
        List<Object[]> ops = new ArrayList<>();
        String overflow = "WRAP";
        boolean write = false;
        for (int i = 2; i < a.length;) {
            byte[] o = a[i];
            if (Num.eq(o, "GET") && i + 2 < a.length) {
                FieldType t = parseType(a[i + 1]);
                ops.add(new Object[] {"GET", t, parseBfOffset(a[i + 2], t), null, null});
                i += 3;
            } else if (Num.eq(o, "SET") && i + 3 < a.length && !ro) {
                FieldType t = parseType(a[i + 1]);
                long off = parseBfOffset(a[i + 2], t);
                Long v = Num.parseLong(a[i + 3]);
                if (v == null) {
                    throw RedisError.notInt();
                }
                ops.add(new Object[] {"SET", t, off, BigInteger.valueOf(v), overflow});
                write = true;
                i += 4;
            } else if (Num.eq(o, "INCRBY") && i + 3 < a.length && !ro) {
                FieldType t = parseType(a[i + 1]);
                long off = parseBfOffset(a[i + 2], t);
                Long v = Num.parseLong(a[i + 3]);
                if (v == null) {
                    throw RedisError.notInt();
                }
                ops.add(new Object[] {"INCRBY", t, off, BigInteger.valueOf(v), overflow});
                write = true;
                i += 4;
            } else if (Num.eq(o, "OVERFLOW") && i + 1 < a.length && !ro) {
                String m = Num.upper(a[i + 1]);
                if (!m.equals("WRAP") && !m.equals("SAT") && !m.equals("FAIL")) {
                    throw RedisError.err("Invalid OVERFLOW type specified");
                }
                overflow = m;
                i += 2;
            } else {
                throw ro && (Num.eq(o, "SET") || Num.eq(o, "INCRBY") || Num.eq(o, "OVERFLOW"))
                        ? RedisError.err("BITFIELD_RO only supports the GET subcommand") : RedisError.syntax();
            }
        }
        if (!write) {
            byte[] v = StringCmds.getString(x, a[1]);
            byte[] cur = v == null ? new byte[0] : v;
            List<Object> out = new ArrayList<>();
            for (Object[] op : ops) {
                out.add(readField(cur, (Long) op[2], (FieldType) op[1]).longValue());
            }
            return out;
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            byte[] cur = m.sv() == null ? new byte[0] : m.sv();
            List<Object> out = new ArrayList<>();
            boolean changed = false;
            for (Object[] op : ops) {
                FieldType t = (FieldType) op[1];
                long off = (Long) op[2];
                BigInteger old = readField(cur, off, t);
                switch ((String) op[0]) {
                    case "GET" -> out.add(old.longValue());
                    case "SET" -> {
                        BigInteger nv = fit((BigInteger) op[3], t, (String) op[4]);
                        if (nv == null) {
                            out.add(null);
                        } else {
                            cur = writeField(cur, off, t, nv);
                            changed = true;
                            out.add(old.longValue());
                        }
                    }
                    default -> {
                        BigInteger nv = fit(old.add((BigInteger) op[3]), t, (String) op[4]);
                        if (nv == null) {
                            out.add(null);
                        } else {
                            cur = writeField(cur, off, t, nv);
                            changed = true;
                            out.add(nv.longValue());
                        }
                    }
                }
            }
            if (changed || m.created()) {
                x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", cur, x.db, a[1]);
            }
            if (m.created() && !changed) {
                x.delete(a[1]);
            }
            return out;
        });
    }
}
