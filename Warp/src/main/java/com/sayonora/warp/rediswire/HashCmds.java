package com.sayonora.warp.rediswire;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Hash commands. */
final class HashCmds {

    private HashCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("hset", -4, "write denyoom fast", 1, 1, 1, "@write @hash @fast", (x, a) -> hset(x, a, false));
        r.add("hmset", -4, "write denyoom fast", 1, 1, 1, "@write @hash @fast", (x, a) -> hset(x, a, true));
        r.add("hsetnx", 4, "write denyoom fast", 1, 1, 1, "@write @hash @fast", HashCmds::hsetnx);
        r.add("hget", 3, "readonly fast", 1, 1, 1, "@read @hash @fast", HashCmds::hget);
        r.add("hmget", -3, "readonly fast", 1, 1, 1, "@read @hash @fast", HashCmds::hmget);
        r.add("hgetall", 2, "readonly", 1, 1, 1, "@read @hash @slow", HashCmds::hgetall);
        r.add("hdel", -3, "write fast", 1, 1, 1, "@write @hash @fast", HashCmds::hdel);
        r.add("hexists", 3, "readonly fast", 1, 1, 1, "@read @hash @fast", HashCmds::hexists);
        r.add("hlen", 2, "readonly fast", 1, 1, 1, "@read @hash @fast", HashCmds::hlen);
        r.add("hkeys", 2, "readonly", 1, 1, 1, "@read @hash @slow", (x, a) -> hlist(x, a, true, false));
        r.add("hvals", 2, "readonly", 1, 1, 1, "@read @hash @slow", (x, a) -> hlist(x, a, false, true));
        r.add("hstrlen", 3, "readonly fast", 1, 1, 1, "@read @hash @fast", HashCmds::hstrlen);
        r.add("hincrby", 4, "write denyoom fast", 1, 1, 1, "@write @hash @fast", HashCmds::hincrby);
        r.add("hincrbyfloat", 4, "write denyoom fast", 1, 1, 1, "@write @hash @fast", HashCmds::hincrbyfloat);
        r.add("hrandfield", -2, "readonly", 1, 1, 1, "@read @hash @slow", HashCmds::hrandfield);
        r.add("hscan", -3, "readonly", 1, 1, 1, "@read @hash @slow", HashCmds::hscan);
    }

    private static Object hset(Ctx x, byte[][] a, boolean legacy) throws Exception {
        if (a.length % 2 != 0) {
            throw RedisError.arity(legacy ? "hmset" : "hset");
        }
        Map<Hub.BK, byte[]> m = new LinkedHashMap<>();
        for (int i = 2; i + 1 < a.length; i += 2) {
            m.put(new Hub.BK(a[i]), a[i + 1]);
        }
        byte[][] fs = new byte[m.size()][];
        byte[][] vs = new byte[m.size()][];
        int i = 0;
        for (Map.Entry<Hub.BK, byte[]> e : m.entrySet()) {
            fs[i] = e.getKey().b;
            vs[i++] = e.getValue();
        }
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_HASH);
            long added = x.one("WITH i AS (INSERT INTO warp_redis_hashes (db, k, f, v) SELECT ?, ?, u.f, u.v "
                    + "FROM unnest(?::bytea[], ?::bytea[]) AS u(f, v) ON CONFLICT (db, k, f) DO UPDATE SET v = EXCLUDED.v "
                    + "RETURNING (xmax = 0) AS ins) SELECT count(*) FILTER (WHERE ins) FROM i",
                    rs -> rs.getLong(1), x.db, a[1], fs, vs);
            x.adjust(a[1], added);
            return legacy ? Resp.OK : (Object) added;
        });
    }

    private static Object hsetnx(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_HASH);
            int n = x.update("INSERT INTO warp_redis_hashes (db, k, f, v) VALUES (?, ?, ?, ?) ON CONFLICT (db, k, f) DO NOTHING",
                    x.db, a[1], a[2], a[3]);
            x.adjust(a[1], n);
            return (long) n;
        });
    }

    private static Object hget(Ctx x, byte[][] a) throws Exception {
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_HASH, "SELECT v FROM warp_redis_hashes WHERE db = ? AND k = ? AND f = ?",
                rs -> rs.getBytes(2), x.db, a[1], a[2]);
        t.expect(Ctx.T_HASH);
        return t.rows().isEmpty() ? null : t.rows().get(0);
    }

    private static Object hmget(Ctx x, byte[][] a) throws Exception {
        byte[][] fs = Arrays.copyOfRange(a, 2, a.length);
        Ctx.TypedRead<byte[][]> t = x.typedRead(a[1], Ctx.T_HASH,
                "SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? AND f = ANY(?)",
                rs -> new byte[][] {rs.getBytes(2), rs.getBytes(3)}, x.db, a[1], fs);
        t.expect(Ctx.T_HASH);
        Map<Hub.BK, byte[]> found = new java.util.HashMap<>();
        for (byte[][] r : t.rows()) {
            found.put(new Hub.BK(r[0]), r[1]);
        }
        List<Object> out = new ArrayList<>();
        for (byte[] f : fs) {
            out.add(found.get(new Hub.BK(f)));
        }
        return out;
    }

    private static Object hgetall(Ctx x, byte[][] a) throws Exception {
        Ctx.TypedRead<byte[][]> t = x.typedRead(a[1], Ctx.T_HASH,
                "SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? ORDER BY f",
                rs -> new byte[][] {rs.getBytes(2), rs.getBytes(3)}, x.db, a[1]);
        t.expect(Ctx.T_HASH);
        List<Object> flat = new ArrayList<>();
        for (byte[][] r : t.rows()) {
            flat.add(r[0]);
            flat.add(r[1]);
        }
        return new Resp.RMap(flat);
    }

    private static Object hlist(Ctx x, byte[][] a, boolean keys, boolean vals) throws Exception {
        Ctx.TypedRead<byte[][]> t = x.typedRead(a[1], Ctx.T_HASH,
                "SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? ORDER BY f",
                rs -> new byte[][] {rs.getBytes(2), rs.getBytes(3)}, x.db, a[1]);
        t.expect(Ctx.T_HASH);
        List<Object> out = new ArrayList<>();
        for (byte[][] r : t.rows()) {
            out.add(keys ? r[0] : r[1]);
        }
        return out;
    }

    private static Object hdel(Ctx x, byte[][] a) throws Exception {
        byte[][] fs = Arrays.copyOfRange(a, 2, a.length);
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_HASH);
            if (m == null) {
                return 0L;
            }
            long n = x.one("WITH d AS (DELETE FROM warp_redis_hashes WHERE db = ? AND k = ? AND f = ANY(?) RETURNING 1) SELECT count(*) FROM d",
                    rs -> rs.getLong(1), x.db, a[1], fs);
            x.adjust(a[1], -n);
            x.deleteIfEmpty(a[1]);
            return n;
        });
    }

    private static Object hexists(Ctx x, byte[][] a) throws Exception {
        return hget(x, a) != null ? 1L : 0L;
    }

    private static Object hlen(Ctx x, byte[][] a) throws Exception {
        long n = x.count(a[1], Ctx.T_HASH);
        return n < 0 ? 0L : n;
    }

    private static Object hstrlen(Ctx x, byte[][] a) throws Exception {
        byte[] v = (byte[]) hget(x, new byte[][] {a[0], a[1], a[2]});
        return v == null ? 0L : (long) v.length;
    }

    private static Object hincrby(Ctx x, byte[][] a) throws Exception {
        Long delta = Num.parseLong(a[3]);
        if (delta == null) {
            throw RedisError.notInt();
        }
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_HASH);
            byte[] cur = x.one("SELECT v FROM warp_redis_hashes WHERE db = ? AND k = ? AND f = ? FOR UPDATE", rs -> rs.getBytes(1),
                    x.db, a[1], a[2]);
            long v = 0;
            if (cur != null) {
                Long p = Num.parseLong(cur);
                if (p == null) {
                    throw RedisError.err("hash value is not an integer");
                }
                v = p;
            }
            long res;
            try {
                res = Math.addExact(v, delta);
            } catch (ArithmeticException e) {
                throw new RedisError(RedisError.OVERFLOW);
            }
            x.update("INSERT INTO warp_redis_hashes (db, k, f, v) VALUES (?, ?, ?, ?) ON CONFLICT (db, k, f) DO UPDATE SET v = EXCLUDED.v",
                    x.db, a[1], a[2], Num.bytes(res));
            if (cur == null) {
                x.adjust(a[1], 1);
            }
            return res;
        });
    }

    private static Object hincrbyfloat(Ctx x, byte[][] a) throws Exception {
        BigDecimal delta = Num.parseDecimal(a[3]);
        if (delta == null) {
            Double dv = Num.parseDouble(a[3]);
            if (dv != null && dv.isInfinite()) {
                throw RedisError.err("value is NaN or Infinity");
            }
            throw RedisError.notFloat();
        }
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_HASH);
            byte[] cur = x.one("SELECT v FROM warp_redis_hashes WHERE db = ? AND k = ? AND f = ? FOR UPDATE", rs -> rs.getBytes(1),
                    x.db, a[1], a[2]);
            BigDecimal v = BigDecimal.ZERO;
            if (cur != null) {
                v = Num.parseDecimal(cur);
                if (v == null) {
                    throw RedisError.err("hash value is not a float");
                }
            }
            byte[] res = Num.bytes(Num.fmtLongDouble(v.add(delta)));
            x.update("INSERT INTO warp_redis_hashes (db, k, f, v) VALUES (?, ?, ?, ?) ON CONFLICT (db, k, f) DO UPDATE SET v = EXCLUDED.v",
                    x.db, a[1], a[2], res);
            if (cur == null) {
                x.adjust(a[1], 1);
            }
            return res;
        });
    }

    private static Object hrandfield(Ctx x, byte[][] a) throws Exception {
        boolean withValues = false;
        Long count = null;
        if (a.length >= 3) {
            count = Num.parseLong(a[2]);
            if (count == null) {
                throw RedisError.notInt();
            }
            if (a.length == 4 && Num.eq(a[3], "WITHVALUES")) {
                withValues = true;
            } else if (a.length > 3) {
                throw RedisError.syntax();
            }
        }
        if (count != null && count < -(Long.MAX_VALUE / 2)) {
            throw RedisError.err("value is out of range");
        }
        Ctx.TypedRead<byte[][]> t = x.typedRead(a[1], Ctx.T_HASH,
                "SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? ORDER BY random() LIMIT ?",
                rs -> new byte[][] {rs.getBytes(2), rs.getBytes(3)}, x.db, a[1],
                count == null ? 1L : count > 0 ? count : (long) 1_000_000_000);
        t.expect(Ctx.T_HASH);
        List<byte[][]> rows = t.rows();
        if (count == null) {
            return rows.isEmpty() ? null : rows.get(0)[0];
        }
        if (!t.exists() || count == 0) {
            return new ArrayList<>();
        }
        List<byte[][]> picked = new ArrayList<>();
        if (count > 0) {
            picked.addAll(rows);
        } else {
            long n = -count;
            if (n > 100_000_000L) {
                throw RedisError.err("value is out of range");
            }
            for (long i = 0; i < n; i++) {
                picked.add(rows.get(ThreadLocalRandom.current().nextInt(rows.size())));
            }
        }
        List<Object> out = new ArrayList<>();
        for (byte[][] r : picked) {
            if (withValues && x.resp3) {
                out.add(new ArrayList<>(List.of(r[0], r[1])));
            } else {
                out.add(r[0]);
                if (withValues) {
                    out.add(r[1]);
                }
            }
        }
        return out;
    }

    private static Object hscan(Ctx x, byte[][] a) throws Exception {
        long cursor = KeyCmds.parseCursor(a[2]);
        byte[] match = null;
        long count = 10;
        boolean noValues = false;
        for (int i = 3; i < a.length; i++) {
            if (Num.eq(a[i], "MATCH") && i + 1 < a.length) {
                match = a[++i];
            } else if (Num.eq(a[i], "COUNT") && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null) {
                    throw RedisError.notInt();
                }
                if (c < 1) {
                    throw RedisError.syntax();
                }
                count = c;
            } else if (Num.eq(a[i], "NOVALUES")) {
                noValues = true;
            } else {
                throw RedisError.syntax();
            }
        }
        long[] page = ScanPage.plan(cursor, count, Math.max(0, x.count(a[1], Ctx.T_HASH)));
        Ctx.TypedRead<byte[][]> t = x.typedRead(a[1], Ctx.T_HASH,
                "SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? ORDER BY f OFFSET ? LIMIT ?",
                rs -> new byte[][] {rs.getBytes(2), rs.getBytes(3)}, x.db, a[1], page[0], page[1]);
        t.expect(Ctx.T_HASH);
        List<Object> items = new ArrayList<>();
        for (byte[][] r : t.rows()) {
            if (match == null || Glob.matches(match, r[0])) {
                items.add(r[0]);
                if (!noValues) {
                    items.add(r[1]);
                }
            }
        }
        return List.of(Long.toString(page[2]), items);
    }
}
