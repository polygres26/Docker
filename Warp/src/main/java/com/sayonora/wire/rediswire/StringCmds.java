package com.sayonora.wire.rediswire;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** String commands. */
final class StringCmds {

    static final long MAX_STRING = 512L * 1024 * 1024;

    private StringCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("get", 2, "readonly fast", 1, 1, 1, "@read @string @fast", StringCmds::get);
        r.add("set", -3, "write denyoom", 1, 1, 1, "@write @string @slow", StringCmds::set);
        r.add("setnx", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", StringCmds::setnx);
        r.add("setex", 4, "write denyoom", 1, 1, 1, "@write @string @slow", (x, a) -> setWithTtl(x, a, "setex", 1000));
        r.add("psetex", 4, "write denyoom", 1, 1, 1, "@write @string @slow", (x, a) -> setWithTtl(x, a, "psetex", 1));
        r.add("getset", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", StringCmds::getset);
        r.add("getdel", 2, "write fast", 1, 1, 1, "@write @string @fast", StringCmds::getdel);
        r.add("getex", -2, "write fast", 1, 1, 1, "@write @string @fast", StringCmds::getex);
        r.add("mget", -2, "readonly fast", 1, -1, 1, "@read @string @fast", StringCmds::mget);
        r.add("mset", -3, "write denyoom", 1, -1, 2, "@write @string @slow", StringCmds::mset);
        r.add("msetnx", -3, "write denyoom", 1, -1, 2, "@write @string @slow", StringCmds::msetnx);
        r.add("incr", 2, "write denyoom fast", 1, 1, 1, "@write @string @fast", (x, a) -> incrBy(x, a[1], 1, "incr"));
        r.add("decr", 2, "write denyoom fast", 1, 1, 1, "@write @string @fast", (x, a) -> incrBy(x, a[1], -1, "decr"));
        r.add("incrby", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", (x, a) -> {
            Long d = Num.parseLong(a[2]);
            if (d == null) {
                throw RedisError.notInt();
            }
            return incrBy(x, a[1], d, "incrby");
        });
        r.add("decrby", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", (x, a) -> {
            Long d = Num.parseLong(a[2]);
            if (d == null) {
                throw RedisError.notInt();
            }
            if (d == Long.MIN_VALUE) {
                throw new RedisError("ERR decrement would overflow");
            }
            return incrBy(x, a[1], -d, "decrby");
        });
        r.add("incrbyfloat", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", StringCmds::incrByFloat);
        r.add("append", 3, "write denyoom fast", 1, 1, 1, "@write @string @fast", StringCmds::append);
        r.add("strlen", 2, "readonly fast", 1, 1, 1, "@read @string @fast", (x, a) -> {
            byte[] v = getString(x, a[1]);
            return v == null ? 0L : (long) v.length;
        });
        r.add("getrange", 4, "readonly", 1, 1, 1, "@read @string @slow", StringCmds::getrange);
        r.add("substr", 4, "readonly", 1, 1, 1, "@read @string @slow", StringCmds::getrange);
        r.add("setrange", 4, "write denyoom", 1, 1, 1, "@write @string @slow", StringCmds::setrange);
        r.add("lcs", -3, "readonly", 1, 2, 1, "@read @string @slow", StringCmds::lcs);
    }

    // ------------------------------------------------------------------------------------------
    // helpers shared by the other string-typed families (bits, hyperloglog)
    // ------------------------------------------------------------------------------------------

    /** The string value of a live key, {@code null} when missing, WRONGTYPE for another type. */
    static byte[] getString(Ctx x, byte[] k) throws Exception {
        Object[] row = x.one("SELECT type, sv FROM warp_redis_keys WHERE db = ? AND k = ? AND (exp IS NULL OR exp > ?)",
                rs -> new Object[] {rs.getInt(1), rs.getBytes(2)}, x.db, k, x.now);
        if (row == null) {
            return null;
        }
        if ((Integer) row[0] != Ctx.T_STRING) {
            throw RedisError.wrongType();
        }
        byte[] v = (byte[]) row[1];
        return v == null ? new byte[0] : v;
    }

    /** Overwrites {@code k} with a string value ({@code exp} epoch ms or null), replacing any type. */
    static void putString(Ctx x, byte[] k, byte[] v, Long exp) throws Exception {
        x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot) VALUES (?, ?, 0, ?, ?, ?) "
                + "ON CONFLICT (db, k) DO UPDATE SET type = 0, exp = EXCLUDED.exp, sv = EXCLUDED.sv, "
                + "ver = nextval('warp_redis_ver')", x.db, k, exp == null ? Ctx.NULL_BIGINT : exp, v, Slot.of(k));
    }

    /** Expiry (absolute ms) from an EX/PX/EXAT/PXAT option; errors like Redis. */
    static long expiry(Ctx x, String opt, byte[] value, String cmd) {
        Long v = Num.parseLong(value);
        if (v == null) {
            throw RedisError.notInt();
        }
        String bad = "ERR invalid expire time in '" + cmd + "' command";
        if (v <= 0) {
            throw new RedisError(bad);
        }
        long abs;
        switch (opt) {
            case "EX", "EXAT" -> {
                if (v > Long.MAX_VALUE / 1000) {
                    throw new RedisError(bad);
                }
                abs = v * 1000;
                if (opt.equals("EX")) {
                    abs += x.now;
                    if (abs < 0) {
                        throw new RedisError(bad);
                    }
                }
            }
            default -> {
                abs = v;
                if (opt.equals("PX")) {
                    abs += x.now;
                    if (abs < 0) {
                        throw new RedisError(bad);
                    }
                }
            }
        }
        return abs;
    }

    // ------------------------------------------------------------------------------------------

    private static Object get(Ctx x, byte[][] a) throws Exception {
        return getString(x, a[1]);
    }

    private static Object set(Ctx x, byte[][] a) throws Exception {
        byte[] k = a[1];
        byte[] v = a[2];
        boolean nx = false;
        boolean xx = false;
        boolean get = false;
        boolean keepTtl = false;
        String expOpt = null;
        byte[] expVal = null;
        for (int i = 3; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "NX") && !xx) {
                nx = true;
            } else if (Num.eq(o, "XX") && !nx) {
                xx = true;
            } else if (Num.eq(o, "GET")) {
                get = true;
            } else if (Num.eq(o, "KEEPTTL") && expOpt == null) {
                keepTtl = true;
            } else if ((Num.eq(o, "EX") || Num.eq(o, "PX") || Num.eq(o, "EXAT") || Num.eq(o, "PXAT"))
                    && !keepTtl && expOpt == null && i + 1 < a.length) {
                expOpt = Num.upper(o);
                expVal = a[++i];
            } else {
                throw RedisError.syntax();
            }
        }
        Long exp = expOpt == null ? null : StringCmds.expiry(x, expOpt, expVal, "set");
        boolean pastExp = exp != null && exp <= x.now;
        if (!get && !keepTtl && !xx && !pastExp) {
            // fast paths: one statement
            if (nx) {
                int n = x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot) VALUES (?, ?, 0, ?, ?, ?) "
                        + "ON CONFLICT (db, k) DO UPDATE SET type = 0, exp = EXCLUDED.exp, sv = EXCLUDED.sv, "
                        + "ver = nextval('warp_redis_ver') WHERE warp_redis_keys.exp <= ?", x.db, k,
                        exp == null ? Ctx.NULL_BIGINT : exp, v, Slot.of(k), x.now);
                return n > 0 ? Resp.OK : null;
            }
            putString(x, k, v, exp);
            return Resp.OK;
        }
        final boolean fnx = nx;
        final boolean fxx = xx;
        final boolean fget = get;
        final boolean fkeep = keepTtl;
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(k);
            if (fget && m != null && m.type() != Ctx.T_STRING) {
                throw RedisError.wrongType();
            }
            byte[] old = fget && m != null ? (m.sv() == null ? new byte[0] : m.sv()) : null;
            if ((fnx && m != null) || (fxx && m == null)) {
                return fget ? old : null;
            }
            if (pastExp) {
                x.delete(k);
            } else {
                Long e = fkeep ? (m == null ? null : m.exp()) : exp;
                putString(x, k, v, e);
            }
            return fget ? old : Resp.OK;
        });
    }

    private static Object setnx(Ctx x, byte[][] a) throws Exception {
        int n = x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot) VALUES (?, ?, 0, NULL, ?, ?) "
                + "ON CONFLICT (db, k) DO UPDATE SET type = 0, exp = NULL, sv = EXCLUDED.sv, ver = nextval('warp_redis_ver') "
                + "WHERE warp_redis_keys.exp <= ?", x.db, a[1], a[2], Slot.of(a[1]), x.now);
        return n > 0 ? 1L : 0L;
    }

    private static Object setWithTtl(Ctx x, byte[][] a, String cmd, long unit) throws Exception {
        Long t = Num.parseLong(a[2]);
        if (t == null) {
            throw RedisError.notInt();
        }
        if (t <= 0 || (unit != 1 && t > Long.MAX_VALUE / unit)) {
            throw new RedisError("ERR invalid expire time in '" + cmd + "' command");
        }
        long exp = x.now + t * unit;
        if (exp < 0) {
            throw new RedisError("ERR invalid expire time in '" + cmd + "' command");
        }
        putString(x, a[1], a[3], exp);
        return Resp.OK;
    }

    private static Object getset(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(a[1]);
            if (m != null && m.type() != Ctx.T_STRING) {
                throw RedisError.wrongType();
            }
            putString(x, a[1], a[2], null);
            return m == null ? null : (m.sv() == null ? new byte[0] : m.sv());
        });
    }

    private static Object getdel(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(a[1]);
            if (m == null) {
                return null;
            }
            if (m.type() != Ctx.T_STRING) {
                throw RedisError.wrongType();
            }
            x.delete(a[1]);
            return m.sv() == null ? new byte[0] : m.sv();
        });
    }

    private static Object getex(Ctx x, byte[][] a) throws Exception {
        String opt = null;
        byte[] val = null;
        boolean persist = false;
        for (int i = 2; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "PERSIST") && opt == null && !persist) {
                persist = true;
            } else if ((Num.eq(o, "EX") || Num.eq(o, "PX") || Num.eq(o, "EXAT") || Num.eq(o, "PXAT")) && opt == null
                    && !persist && i + 1 < a.length) {
                opt = Num.upper(o);
                val = a[++i];
            } else {
                throw RedisError.syntax();
            }
        }
        Long exp = opt == null ? null : expiry(x, opt, val, "getex");
        final boolean fpersist = persist;
        if (opt == null && !persist) {
            return getString(x, a[1]);
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(a[1]);
            if (m == null) {
                return null;
            }
            if (m.type() != Ctx.T_STRING) {
                throw RedisError.wrongType();
            }
            if (fpersist) {
                x.update("UPDATE warp_redis_keys SET exp = NULL WHERE db = ? AND k = ?", x.db, a[1]);
            } else if (exp <= x.now) {
                x.delete(a[1]);
            } else {
                x.update("UPDATE warp_redis_keys SET exp = ? WHERE db = ? AND k = ?", exp, x.db, a[1]);
            }
            return m.sv() == null ? new byte[0] : m.sv();
        });
    }

    private static Object mget(Ctx x, byte[][] a) throws Exception {
        byte[][] keys = Arrays.copyOfRange(a, 1, a.length);
        Map<Hub.BK, byte[]> found = new java.util.HashMap<>();
        for (Object[] row : x.list("SELECT k, type, sv FROM warp_redis_keys WHERE db = ? AND k = ANY(?) AND (exp IS NULL OR exp > ?)",
                rs -> new Object[] {rs.getBytes(1), rs.getInt(2), rs.getBytes(3)}, x.db, keys, x.now)) {
            if ((Integer) row[1] == Ctx.T_STRING) {
                byte[] v = (byte[]) row[2];
                found.put(new Hub.BK((byte[]) row[0]), v == null ? new byte[0] : v);
            }
        }
        List<Object> out = new ArrayList<>(keys.length);
        for (byte[] k : keys) {
            out.add(found.get(new Hub.BK(k)));
        }
        return out;
    }

    private static Map<Hub.BK, byte[]> pairs(byte[][] a) {
        Map<Hub.BK, byte[]> m = new LinkedHashMap<>();
        for (int i = 1; i + 1 < a.length; i += 2) {
            Hub.BK bk = new Hub.BK(a[i]);
            m.remove(bk); // last value wins, at its own position (irrelevant to the result)
            m.put(bk, a[i + 1]);
        }
        return m;
    }

    private static void insertPairs(Ctx x, Map<Hub.BK, byte[]> m, boolean ifAbsent) throws Exception {
        List<Map.Entry<Hub.BK, byte[]>> es = new ArrayList<>(m.entrySet());
        es.sort((p, q) -> Arrays.compareUnsigned(p.getKey().b, q.getKey().b));
        byte[][] ks = new byte[es.size()][];
        byte[][] vs = new byte[es.size()][];
        long[] slots = new long[es.size()];
        for (int i = 0; i < ks.length; i++) {
            ks[i] = es.get(i).getKey().b;
            vs[i] = es.get(i).getValue();
            slots[i] = Slot.of(ks[i]);
        }
        String conflict = ifAbsent ? "ON CONFLICT (db, k) DO NOTHING"
                : "ON CONFLICT (db, k) DO UPDATE SET type = 0, exp = NULL, sv = EXCLUDED.sv, ver = nextval('warp_redis_ver')";
        int n = x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot) SELECT ?, u.k, 0, NULL, u.v, u.s::int "
                + "FROM unnest(?::bytea[], ?::bytea[], ?::int8[]) AS u(k, v, s) " + conflict, x.db, ks, vs, slots);
        if (ifAbsent && n != ks.length) {
            x.rollbackOnly = true;
        }
    }

    private static Object mset(Ctx x, byte[][] a) throws Exception {
        if (a.length % 2 == 0) {
            throw RedisError.arity("mset");
        }
        return x.atomic(() -> {
            insertPairs(x, pairs(a), false);
            return Resp.OK;
        });
    }

    private static Object msetnx(Ctx x, byte[][] a) throws Exception {
        if (a.length % 2 == 0) {
            throw RedisError.arity("msetnx");
        }
        Map<Hub.BK, byte[]> m = pairs(a);
        byte[][] ks = new byte[m.size()][];
        int i = 0;
        for (Hub.BK k : m.keySet()) {
            ks[i++] = k.b;
        }
        return x.atomic(() -> {
            x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ANY(?) AND exp <= ?", x.db, ks, x.now);
            if (x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ANY(?)", x.db, ks) > 0) {
                return 0L;
            }
            insertPairs(x, m, true);
            return x.rollbackOnly ? 0L : 1L;
        });
    }

    static Object incrBy(Ctx x, byte[] k, long delta, String cmd) throws Exception {
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(k, Ctx.T_STRING);
            long cur = 0;
            if (!m.created()) {
                Long v = m.sv() == null ? Long.valueOf(0) : Num.parseLong(m.sv());
                if (v == null) {
                    throw RedisError.notInt();
                }
                cur = v;
            }
            long res;
            try {
                res = Math.addExact(cur, delta);
            } catch (ArithmeticException e) {
                throw new RedisError(RedisError.OVERFLOW);
            }
            x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", Num.bytes(res), x.db, k);
            return res;
        });
    }

    private static Object incrByFloat(Ctx x, byte[][] a) throws Exception {
        BigDecimal delta = Num.parseDecimal(a[2]);
        if (delta == null) {
            Double dv = Num.parseDouble(a[2]);
            if (dv != null && dv.isInfinite()) {
                throw RedisError.err("increment would produce NaN or Infinity");
            }
            throw RedisError.notFloat();
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            BigDecimal cur = BigDecimal.ZERO;
            if (!m.created() && m.sv() != null) {
                cur = Num.parseDecimal(m.sv());
                if (cur == null) {
                    throw RedisError.notFloat();
                }
            }
            byte[] res = Num.bytes(Num.fmtLongDouble(cur.add(delta)));
            x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", res, x.db, a[1]);
            return res;
        });
    }

    private static Object append(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            long len = x.scalar("UPDATE warp_redis_keys SET sv = COALESCE(sv, ''::bytea) || ? WHERE db = ? AND k = ? "
                    + "RETURNING length(sv)", a[2], x.db, a[1]);
            if ((m.sv() == null ? 0 : m.sv().length) + (long) a[2].length > MAX_STRING) {
                throw RedisError.err("string exceeds maximum allowed size (proto-max-bulk-len)");
            }
            return len;
        });
    }

    private static Object getrange(Ctx x, byte[][] a) throws Exception {
        Long s = Num.parseLong(a[2]);
        Long e = Num.parseLong(a[3]);
        if (s == null || e == null) {
            throw RedisError.notInt();
        }
        byte[] v = getString(x, a[1]);
        if (v == null) {
            return new byte[0];
        }
        long len = v.length;
        long start = s;
        long end = e;
        if (start < 0) {
            start = Math.max(0, len + start);
        }
        if (end < 0) {
            end = len + end;
            if (end < 0) {
                return new byte[0];
            }
        }
        if (start > end || len == 0 || start >= len) {
            return new byte[0];
        }
        end = Math.min(end, len - 1);
        return Arrays.copyOfRange(v, (int) start, (int) end + 1);
    }

    private static Object setrange(Ctx x, byte[][] a) throws Exception {
        Long off = Num.parseLong(a[2]);
        if (off == null) {
            throw RedisError.notInt();
        }
        if (off < 0) {
            throw RedisError.err("offset is out of range");
        }
        byte[] val = a[3];
        if (off + (long) val.length > MAX_STRING) {
            throw RedisError.err("string exceeds maximum allowed size (proto-max-bulk-len)");
        }
        if (val.length == 0) {
            byte[] cur = getString(x, a[1]);
            return cur == null ? 0L : (long) cur.length;
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockOrCreate(a[1], Ctx.T_STRING);
            byte[] cur = m.sv() == null ? new byte[0] : m.sv();
            byte[] res = new byte[(int) Math.max(cur.length, off + val.length)];
            System.arraycopy(cur, 0, res, 0, cur.length);
            System.arraycopy(val, 0, res, off.intValue(), val.length);
            x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", res, x.db, a[1]);
            return (long) res.length;
        });
    }

    private static Object lcs(Ctx x, byte[][] a) throws Exception {
        boolean len = false;
        boolean idx = false;
        boolean withLen = false;
        long minLen = 0;
        for (int i = 3; i < a.length; i++) {
            if (Num.eq(a[i], "IDX")) {
                idx = true;
            } else if (Num.eq(a[i], "LEN")) {
                len = true;
            } else if (Num.eq(a[i], "WITHMATCHLEN")) {
                withLen = true;
            } else if (Num.eq(a[i], "MINMATCHLEN") && i + 1 < a.length) {
                Long v = Num.parseLong(a[++i]);
                if (v == null) {
                    throw RedisError.notInt();
                }
                minLen = Math.max(0, v);
            } else {
                throw RedisError.syntax();
            }
        }
        if (len && idx) {
            throw RedisError.err("If you want both the length and indexes, please just use IDX.");
        }
        byte[] s1;
        byte[] s2;
        try {
            s1 = getString(x, a[1]);
            s2 = getString(x, a[2]);
        } catch (RedisError e) {
            throw RedisError.err("The specified keys must contain string values");
        }
        if (s1 == null) {
            s1 = new byte[0];
        }
        if (s2 == null) {
            s2 = new byte[0];
        }
        int n1 = s1.length;
        int n2 = s2.length;
        int[][] dp = new int[n1 + 1][n2 + 1];
        for (int i = 1; i <= n1; i++) {
            for (int j = 1; j <= n2; j++) {
                dp[i][j] = s1[i - 1] == s2[j - 1] ? dp[i - 1][j - 1] + 1 : Math.max(dp[i - 1][j], dp[i][j - 1]);
            }
        }
        int total = dp[n1][n2];
        if (len) {
            return (long) total;
        }
        byte[] res = new byte[total];
        List<Object> matches = new ArrayList<>();
        int i = n1;
        int j = n2;
        int ridx = total;
        int aEnd = -1;
        int bEnd = -1;
        int runLen = 0;
        while (i > 0 && j > 0) {
            if (s1[i - 1] == s2[j - 1]) {
                res[--ridx] = s1[i - 1];
                if (runLen == 0) {
                    aEnd = i - 1;
                    bEnd = j - 1;
                }
                runLen++;
                i--;
                j--;
                if (i == 0 || j == 0 || s1[i - 1] != s2[j - 1]) {
                    addMatch(matches, aEnd, bEnd, runLen, minLen, withLen);
                    runLen = 0;
                }
            } else if (dp[i - 1][j] >= dp[i][j - 1]) {
                if (runLen > 0) {
                    addMatch(matches, aEnd, bEnd, runLen, minLen, withLen);
                    runLen = 0;
                }
                i--;
            } else {
                if (runLen > 0) {
                    addMatch(matches, aEnd, bEnd, runLen, minLen, withLen);
                    runLen = 0;
                }
                j--;
            }
        }
        if (!idx) {
            return res;
        }
        List<Object> f = new ArrayList<>();
        f.add("matches");
        f.add(matches);
        f.add("len");
        f.add((long) total);
        return new Resp.RMap(f);
    }

    private static void addMatch(List<Object> matches, int aEnd, int bEnd, int runLen, long minLen, boolean withLen) {
        if (runLen < minLen || runLen == 0) {
            return;
        }
        List<Object> m = new ArrayList<>();
        m.add(List.of((long) (aEnd - runLen + 1), (long) aEnd));
        m.add(List.of((long) (bEnd - runLen + 1), (long) bEnd));
        if (withLen) {
            m.add((long) runLen);
        }
        matches.add(m);
    }
}
