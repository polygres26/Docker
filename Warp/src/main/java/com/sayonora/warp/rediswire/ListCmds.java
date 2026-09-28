package com.sayonora.warp.rediswire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * List commands. A list is rows of (position, value) with sparse bigint positions: pushing at the head or tail takes
 * min - STEP / max + STEP (O(1) with the primary-key index), inserting in the middle takes the midpoint of two
 * neighbours and renumbers the whole list (rarely) when no gap is left.
 */
final class ListCmds {

    static final long STEP = 1L << 32;

    private ListCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("lpush", -3, "write denyoom fast", 1, 1, 1, "@write @list @fast", (x, a) -> push(x, a, true, false));
        r.add("rpush", -3, "write denyoom fast", 1, 1, 1, "@write @list @fast", (x, a) -> push(x, a, false, false));
        r.add("lpushx", -3, "write denyoom fast", 1, 1, 1, "@write @list @fast", (x, a) -> push(x, a, true, true));
        r.add("rpushx", -3, "write denyoom fast", 1, 1, 1, "@write @list @fast", (x, a) -> push(x, a, false, true));
        r.add("lpop", -2, "write fast", 1, 1, 1, "@write @list @fast", (x, a) -> pop(x, a, true));
        r.add("rpop", -2, "write fast", 1, 1, 1, "@write @list @fast", (x, a) -> pop(x, a, false));
        r.add("llen", 2, "readonly fast", 1, 1, 1, "@read @list @fast", (x, a) -> {
            long n = x.count(a[1], Ctx.T_LIST);
            return n < 0 ? 0L : n;
        });
        r.add("lrange", 4, "readonly", 1, 1, 1, "@read @list @slow", ListCmds::lrange);
        r.add("lindex", 3, "readonly", 1, 1, 1, "@read @list @slow", ListCmds::lindex);
        r.add("lset", 4, "write denyoom", 1, 1, 1, "@write @list @slow", ListCmds::lset);
        r.add("linsert", 5, "write denyoom", 1, 1, 1, "@write @list @slow", ListCmds::linsert);
        r.add("lrem", 4, "write", 1, 1, 1, "@write @list @slow", ListCmds::lrem);
        r.add("ltrim", 4, "write", 1, 1, 1, "@write @list @slow", ListCmds::ltrim);
        r.add("lmove", 5, "write denyoom", 1, 2, 1, "@write @list @slow", (x, a) -> lmove(x, a[1], a[2], a[3], a[4]));
        r.add("rpoplpush", 3, "write denyoom", 1, 2, 1, "@write @list @slow",
                (x, a) -> lmove(x, a[1], a[2], Num.bytes("RIGHT"), Num.bytes("LEFT")));
        r.add("lpos", -3, "readonly", 1, 1, 1, "@read @list @slow", ListCmds::lpos);
        r.add("lmpop", -4, "write", 0, 0, 0, "@write @list @slow", (x, a) -> lmpop(x, a, 1)).keys(a -> numKeys(a, 1));
        r.add("blpop", -3, "write blocking", 1, -2, 1, "@write @list @slow @blocking", (x, a) -> blockPop(x, a, true))
                .blocking(a -> timeoutOf(a[a.length - 1], false), Resp.NIL_ARRAY);
        r.add("brpop", -3, "write blocking", 1, -2, 1, "@write @list @slow @blocking", (x, a) -> blockPop(x, a, false))
                .blocking(a -> timeoutOf(a[a.length - 1], false), Resp.NIL_ARRAY);
        r.add("blmove", 6, "write denyoom blocking", 1, 2, 1, "@write @list @slow @blocking",
                (x, a) -> notReadyIfNull(lmove(x, a[1], a[2], a[3], a[4]))).blocking(a -> timeoutOf(a[5], false), null);
        r.add("brpoplpush", 4, "write denyoom blocking", 1, 2, 1, "@write @list @slow @blocking",
                (x, a) -> notReadyIfNull(lmove(x, a[1], a[2], Num.bytes("RIGHT"), Num.bytes("LEFT"))))
                .blocking(a -> timeoutOf(a[3], false), null);
        r.add("blmpop", -5, "write blocking", 0, 0, 0, "@write @list @slow @blocking", (x, a) -> notReadyIfNil(lmpop(x, a, 2)))
                .keys(a -> numKeys(a, 2)).blocking(a -> timeoutOf(a[1], false), Resp.NIL_ARRAY);
    }

    static Object notReadyIfNull(Object o) {
        return o == null ? Cmd.NOT_READY : o;
    }

    static Object notReadyIfNil(Object o) {
        return o == Resp.NIL_ARRAY || o == null ? Cmd.NOT_READY : o;
    }

    /** Key positions of "numkeys key [key ...]" starting at argument {@code numAt}. */
    static int[] numKeys(byte[][] a, int numAt) {
        Long n = numAt < a.length ? Num.parseLong(a[numAt]) : null;
        if (n == null || n <= 0) {
            return new int[0];
        }
        int cnt = (int) Math.min(n, Math.max(0, a.length - numAt - 1));
        int[] r = new int[cnt];
        for (int i = 0; i < cnt; i++) {
            r[i] = numAt + 1 + i;
        }
        return r;
    }

    /** Blocking timeout argument: seconds (float, Redis 6+) or, for XREAD-style, ms; 0 = forever. */
    static long timeoutOf(byte[] arg, boolean millis) {
        if (millis) {
            Long v = Num.parseLong(arg);
            if (v == null) {
                throw RedisError.err("timeout is not an integer or out of range");
            }
            if (v < 0) {
                throw RedisError.err("timeout is negative");
            }
            return v;
        }
        Double d = Num.parseDouble(arg);
        if (d == null || d.isNaN() || d.isInfinite()) {
            throw RedisError.err("timeout is not a float or out of range");
        }
        if (d < 0) {
            throw RedisError.err("timeout is negative");
        }
        double ms = Math.ceil(d * 1000);
        if (ms > Long.MAX_VALUE / 4) {
            throw RedisError.err("timeout is out of range");
        }
        return (long) ms;
    }

    // ------------------------------------------------------------------------------------------
    // primitives (the key row is already locked by the caller)
    // ------------------------------------------------------------------------------------------

    private static long[] minMax(Ctx x, byte[] k) throws Exception {
        Object[] r = x.one("SELECT (SELECT min(pos) FROM warp_redis_lists WHERE db = ? AND k = ?), "
                + "(SELECT max(pos) FROM warp_redis_lists WHERE db = ? AND k = ?)",
                rs -> new Object[] {rs.getObject(1), rs.getObject(2)}, x.db, k, x.db, k);
        return r[0] == null ? null : new long[] {(Long) r[0], (Long) r[1]};
    }

    /** Pushes {@code values} at the head or tail (LPUSH order: each value becomes the new head); returns the new length. */
    static long pushValues(Ctx x, byte[] k, boolean left, List<byte[]> values) throws Exception {
        long[] mm = minMax(x, k);
        int n = values.size();
        long[] pos = new long[n];
        byte[][] vs = new byte[n][];
        long p = mm == null ? (left ? STEP : -STEP) : (left ? mm[0] : mm[1]);
        for (int i = 0; i < n; i++) {
            p += left ? -STEP : STEP;
            pos[i] = p;
            vs[i] = values.get(i);
        }
        x.update("INSERT INTO warp_redis_lists (db, k, pos, v) SELECT ?, ?, u.p, u.v FROM unnest(?::int8[], ?::bytea[]) AS u(p, v)",
                x.db, k, pos, vs);
        long len = x.adjust(k, n);
        x.wake(k);
        return len;
    }

    /** Removes and returns up to {@code count} elements from one end; deletes the key when it becomes empty. */
    static List<byte[]> popValues(Ctx x, byte[] k, boolean left, long count) throws Exception {
        String dir = left ? "ASC" : "DESC";
        List<byte[]> out = x.list("WITH d AS (DELETE FROM warp_redis_lists WHERE db = ? AND k = ? AND pos IN (SELECT pos FROM warp_redis_lists "
                + "WHERE db = ? AND k = ? ORDER BY pos " + dir + " LIMIT ?) RETURNING pos, v) SELECT v FROM d ORDER BY pos " + dir,
                rs -> rs.getBytes(1), x.db, k, x.db, k, count);
        if (!out.isEmpty()) {
            x.adjust(k, -out.size());
            x.deleteIfEmpty(k);
        }
        return out;
    }

    private static Object push(Ctx x, byte[][] a, boolean left, boolean onlyIfExists) throws Exception {
        List<byte[]> vals = new ArrayList<>(Arrays.asList(a).subList(2, a.length));
        return x.atomic(() -> {
            if (onlyIfExists) {
                if (x.lockExisting(a[1], Ctx.T_LIST) == null) {
                    return 0L;
                }
            } else {
                x.lockOrCreate(a[1], Ctx.T_LIST);
            }
            return pushValues(x, a[1], left, vals);
        });
    }

    private static Object pop(Ctx x, byte[][] a, boolean left) throws Exception {
        if (a.length > 3) {
            throw RedisError.syntax();
        }
        Long count = null;
        if (a.length == 3) {
            count = Num.parseLong(a[2]);
            if (count == null || count < 0) {
                throw RedisError.err("value is out of range, must be positive");
            }
        }
        final Long fcount = count;
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_LIST);
            if (m == null) {
                return fcount == null ? null : Resp.NIL_ARRAY;
            }
            if (fcount != null && fcount == 0) {
                return new ArrayList<>();
            }
            List<byte[]> got = popValues(x, a[1], left, fcount == null ? 1 : fcount);
            if (fcount == null) {
                return got.isEmpty() ? null : got.get(0);
            }
            return new ArrayList<Object>(got);
        });
    }

    // ------------------------------------------------------------------------------------------

    private static Object lrange(Ctx x, byte[][] a) throws Exception {
        Long s = Num.parseLong(a[2]);
        Long e = Num.parseLong(a[3]);
        if (s == null || e == null) {
            throw RedisError.notInt();
        }
        long n = x.count(a[1], Ctx.T_LIST);
        if (n <= 0) {
            return new ArrayList<>();
        }
        long start = s < 0 ? Math.max(0, s + n) : s;
        long end = e < 0 ? e + n : Math.min(e, n - 1);
        if (start > end || start >= n) {
            return new ArrayList<>();
        }
        return new ArrayList<Object>(x.list("SELECT v FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos OFFSET ? LIMIT ?",
                rs -> rs.getBytes(1), x.db, a[1], start, end - start + 1));
    }

    /** Position (row pos) of the element at {@code idx}, or null when out of range. */
    private static Long posAt(Ctx x, byte[] k, long idx, long n) throws Exception {
        long i = idx < 0 ? idx + n : idx;
        if (i < 0 || i >= n) {
            return null;
        }
        if (i <= n / 2) {
            return x.one("SELECT pos FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos OFFSET ? LIMIT 1",
                    rs -> rs.getLong(1), x.db, k, i);
        }
        return x.one("SELECT pos FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos DESC OFFSET ? LIMIT 1",
                rs -> rs.getLong(1), x.db, k, n - 1 - i);
    }

    private static Object lindex(Ctx x, byte[][] a) throws Exception {
        Long idx = Num.parseLong(a[2]);
        if (idx == null) {
            throw RedisError.notInt();
        }
        long n = x.count(a[1], Ctx.T_LIST);
        if (n <= 0) {
            return null;
        }
        Long pos = posAt(x, a[1], idx, n);
        return pos == null ? null : x.one("SELECT v FROM warp_redis_lists WHERE db = ? AND k = ? AND pos = ?", rs -> rs.getBytes(1), x.db, a[1], pos);
    }

    private static Object lset(Ctx x, byte[][] a) throws Exception {
        Long idx = Num.parseLong(a[2]);
        if (idx == null) {
            throw RedisError.notInt();
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_LIST);
            if (m == null) {
                throw new RedisError(RedisError.NO_KEY);
            }
            Long pos = posAt(x, a[1], idx, m.n());
            if (pos == null) {
                throw RedisError.err("index out of range");
            }
            x.update("UPDATE warp_redis_lists SET v = ? WHERE db = ? AND k = ? AND pos = ?", a[3], x.db, a[1], pos);
            return Resp.OK;
        });
    }

    private static void rebalance(Ctx x, byte[] k) throws Exception {
        long[] mm = minMax(x, k);
        if (mm == null) {
            return;
        }
        long n = x.scalar("SELECT count(*) FROM warp_redis_lists WHERE db = ? AND k = ?", x.db, k);
        long shift = (mm[1] - mm[0]) + (n + 1) * STEP + 1;
        x.update("UPDATE warp_redis_lists SET pos = pos + ? WHERE db = ? AND k = ?", shift, x.db, k);
        x.update("UPDATE warp_redis_lists l SET pos = ? + r.rn * ? FROM (SELECT pos, row_number() OVER (ORDER BY pos) - 1 AS rn "
                + "FROM warp_redis_lists WHERE db = ? AND k = ?) r WHERE l.db = ? AND l.k = ? AND l.pos = r.pos",
                mm[0], STEP, x.db, k, x.db, k);
    }

    private static Object linsert(Ctx x, byte[][] a) throws Exception {
        boolean before;
        if (Num.eq(a[2], "BEFORE")) {
            before = true;
        } else if (Num.eq(a[2], "AFTER")) {
            before = false;
        } else {
            throw RedisError.syntax();
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_LIST);
            if (m == null) {
                return 0L;
            }
            for (int attempt = 0; attempt < 2; attempt++) {
                Long p = x.one("SELECT pos FROM warp_redis_lists WHERE db = ? AND k = ? AND v = ? ORDER BY pos LIMIT 1",
                        rs -> rs.getLong(1), x.db, a[1], a[3]);
                if (p == null) {
                    return -1L;
                }
                Long neighbour = before
                        ? x.one("SELECT max(pos) FROM warp_redis_lists WHERE db = ? AND k = ? AND pos < ?", rs -> (Long) rs.getObject(1), x.db, a[1], p)
                        : x.one("SELECT min(pos) FROM warp_redis_lists WHERE db = ? AND k = ? AND pos > ?", rs -> (Long) rs.getObject(1), x.db, a[1], p);
                long np;
                if (neighbour == null) {
                    np = before ? p - STEP : p + STEP;
                } else {
                    long lo = Math.min(p, neighbour);
                    long hi = Math.max(p, neighbour);
                    if (hi - lo < 2) {
                        rebalance(x, a[1]);
                        continue;
                    }
                    np = lo + (hi - lo) / 2;
                }
                x.update("INSERT INTO warp_redis_lists (db, k, pos, v) VALUES (?, ?, ?, ?)", x.db, a[1], np, a[4]);
                return x.adjust(a[1], 1);
            }
            throw RedisError.err("could not insert into list");
        });
    }

    private static Object lrem(Ctx x, byte[][] a) throws Exception {
        Long count = Num.parseLong(a[2]);
        if (count == null) {
            throw RedisError.notInt();
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_LIST);
            if (m == null) {
                return 0L;
            }
            long removed;
            if (count == 0) {
                removed = x.one("WITH d AS (DELETE FROM warp_redis_lists WHERE db = ? AND k = ? AND v = ? RETURNING 1) SELECT count(*) FROM d",
                        rs -> rs.getLong(1), x.db, a[1], a[3]);
            } else {
                String dir = count > 0 ? "ASC" : "DESC";
                removed = x.one("WITH d AS (DELETE FROM warp_redis_lists WHERE db = ? AND k = ? AND pos IN (SELECT pos FROM warp_redis_lists "
                        + "WHERE db = ? AND k = ? AND v = ? ORDER BY pos " + dir + " LIMIT ?) RETURNING 1) SELECT count(*) FROM d",
                        rs -> rs.getLong(1), x.db, a[1], x.db, a[1], a[3], Math.abs(count));
            }
            if (removed > 0) {
                x.adjust(a[1], -removed);
                x.deleteIfEmpty(a[1]);
            }
            return removed;
        });
    }

    private static Object ltrim(Ctx x, byte[][] a) throws Exception {
        Long s = Num.parseLong(a[2]);
        Long e = Num.parseLong(a[3]);
        if (s == null || e == null) {
            throw RedisError.notInt();
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_LIST);
            if (m == null) {
                return Resp.OK;
            }
            long n = m.n();
            long start = s < 0 ? Math.max(0, s + n) : s;
            long end = e < 0 ? e + n : Math.min(e, n - 1);
            if (start > end || start >= n) {
                x.delete(a[1]);
                return Resp.OK;
            }
            long removed = 0;
            if (start > 0) {
                Long first = posAt(x, a[1], start, n);
                removed += x.update("DELETE FROM warp_redis_lists WHERE db = ? AND k = ? AND pos < ?", x.db, a[1], first);
            }
            if (end < n - 1) {
                Long last = posAt(x, a[1], end, n);
                removed += x.update("DELETE FROM warp_redis_lists WHERE db = ? AND k = ? AND pos > ?", x.db, a[1], last);
            }
            if (removed > 0) {
                x.adjust(a[1], -removed);
            }
            return Resp.OK;
        });
    }

    static Object lmove(Ctx x, byte[] src, byte[] dst, byte[] from, byte[] to) throws Exception {
        boolean fromLeft;
        boolean toLeft;
        if (Num.eq(from, "LEFT")) {
            fromLeft = true;
        } else if (Num.eq(from, "RIGHT")) {
            fromLeft = false;
        } else {
            throw RedisError.syntax();
        }
        if (Num.eq(to, "LEFT")) {
            toLeft = true;
        } else if (Num.eq(to, "RIGHT")) {
            toLeft = false;
        } else {
            throw RedisError.syntax();
        }
        return x.atomic(() -> {
            boolean same = Arrays.equals(src, dst);
            // lock in a stable order so concurrent LMOVEs in opposite directions cannot deadlock
            Ctx.Meta ms;
            if (same || Arrays.compareUnsigned(src, dst) < 0) {
                ms = x.lockExisting(src, Ctx.T_LIST);
                if (!same) {
                    x.lock(dst);
                }
            } else {
                x.lock(dst);
                ms = x.lockExisting(src, Ctx.T_LIST);
            }
            if (ms == null) {
                return null;
            }
            Ctx.Meta md = same ? ms : x.get(dst);
            if (md != null && md.type() != Ctx.T_LIST) {
                throw RedisError.wrongType();
            }
            List<byte[]> got = popValues(x, src, fromLeft, 1);
            if (got.isEmpty()) {
                return null;
            }
            x.lockOrCreate(dst, Ctx.T_LIST);
            pushValues(x, dst, toLeft, List.of(got.get(0)));
            return got.get(0);
        });
    }

    private static Object lpos(Ctx x, byte[][] a) throws Exception {
        long rank = 1;
        Long count = null;
        long maxLen = 0;
        for (int i = 3; i < a.length; i += 2) {
            if (i + 1 >= a.length) {
                throw RedisError.syntax();
            }
            Long v = Num.parseLong(a[i + 1]);
            if (Num.eq(a[i], "RANK")) {
                if (v == null) {
                    throw RedisError.notInt();
                }
                if (v == 0) {
                    throw RedisError.err("RANK can't be zero: use 1 to start from the first match, 2 from the second ... "
                            + "or use negative to start from the end of the list");
                }
                if (v == Long.MIN_VALUE) {
                    throw RedisError.err("value is out of range");
                }
                rank = v;
            } else if (Num.eq(a[i], "COUNT")) {
                if (v == null) {
                    throw RedisError.notInt();
                }
                if (v < 0) {
                    throw RedisError.err("COUNT can't be negative");
                }
                count = v;
            } else if (Num.eq(a[i], "MAXLEN")) {
                if (v == null) {
                    throw RedisError.notInt();
                }
                if (v < 0) {
                    throw RedisError.err("MAXLEN can't be negative");
                }
                maxLen = v;
            } else {
                throw RedisError.syntax();
            }
        }
        long n = x.count(a[1], Ctx.T_LIST);
        if (n < 0) {
            return count == null ? null : new ArrayList<>();
        }
        boolean fwd = rank > 0;
        long skip = Math.abs(rank) - 1;
        long want = count == null ? 1 : count == 0 ? Long.MAX_VALUE : count;
        long limit = maxLen == 0 ? n : Math.min(maxLen, n);
        // scan `limit` elements from the chosen end, keeping only the matching ones' indexes
        List<Object> found = new ArrayList<>();
        long[] idx = {0};
        long[] seen = {0};
        final long fskip = skip;
        final long fwant = want;
        try (java.sql.PreparedStatement ps = x.c().prepareStatement(
                "SELECT v FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos " + (fwd ? "ASC" : "DESC") + " LIMIT ?")) {
            ps.setInt(1, x.db);
            ps.setBytes(2, a[1]);
            ps.setLong(3, limit);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next() && found.size() < fwant) {
                    long i = fwd ? idx[0] : n - 1 - idx[0];
                    idx[0]++;
                    if (Arrays.equals(rs.getBytes(1), a[2])) {
                        if (seen[0]++ >= fskip) {
                            found.add(i);
                        }
                    }
                }
            }
        }
        if (count == null) {
            return found.isEmpty() ? null : found.get(0);
        }
        return found;
    }

    private static Object lmpop(Ctx x, byte[][] a, int numAt) throws Exception {
        Long num = Num.parseLong(a[numAt]);
        if (num == null || num <= 0) {
            throw RedisError.err("numkeys should be greater than 0");
        }
        int dirAt = numAt + 1 + num.intValue();
        if (dirAt >= a.length) {
            throw RedisError.syntax();
        }
        boolean left;
        if (Num.eq(a[dirAt], "LEFT")) {
            left = true;
        } else if (Num.eq(a[dirAt], "RIGHT")) {
            left = false;
        } else {
            throw RedisError.syntax();
        }
        long count = 1;
        boolean sawCount = false;
        for (int i = dirAt + 1; i < a.length; i++) {
            if (Num.eq(a[i], "COUNT") && !sawCount && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null || c <= 0) {
                    throw RedisError.err("count should be greater than 0");
                }
                count = c;
                sawCount = true;
            } else {
                throw RedisError.syntax();
            }
        }
        final long fcount = count;
        final int fnum = num.intValue();
        return x.atomic(() -> {
            for (int i = 0; i < fnum; i++) {
                byte[] k = a[numAt + 1 + i];
                Ctx.Meta m = x.lockExisting(k, Ctx.T_LIST);
                if (m == null) {
                    continue;
                }
                List<byte[]> got = popValues(x, k, left, fcount);
                if (!got.isEmpty()) {
                    return new ArrayList<Object>(List.of(k, new ArrayList<Object>(got)));
                }
            }
            return Resp.NIL_ARRAY;
        });
    }

    private static Object blockPop(Ctx x, byte[][] a, boolean left) throws Exception {
        return x.atomic(() -> {
            for (int i = 1; i < a.length - 1; i++) {
                Ctx.Meta m = x.lockExisting(a[i], Ctx.T_LIST);
                if (m == null) {
                    continue;
                }
                List<byte[]> got = popValues(x, a[i], left, 1);
                if (!got.isEmpty()) {
                    return new ArrayList<Object>(List.of(a[i], got.get(0)));
                }
            }
            return Cmd.NOT_READY;
        });
    }
}
