package com.sayonora.wire.rediswire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Set commands. */
final class SetCmds {

    private SetCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("sadd", -3, "write denyoom fast", 1, 1, 1, "@write @set @fast", SetCmds::sadd);
        r.add("srem", -3, "write fast", 1, 1, 1, "@write @set @fast", SetCmds::srem);
        r.add("sismember", 3, "readonly fast", 1, 1, 1, "@read @set @fast", SetCmds::sismember);
        r.add("smismember", -3, "readonly fast", 1, 1, 1, "@read @set @fast", SetCmds::smismember);
        r.add("smembers", 2, "readonly", 1, 1, 1, "@read @set @slow", SetCmds::smembers);
        r.add("scard", 2, "readonly fast", 1, 1, 1, "@read @set @fast", (x, a) -> {
            long n = x.count(a[1], Ctx.T_SET);
            return n < 0 ? 0L : n;
        });
        r.add("spop", -2, "write fast", 1, 1, 1, "@write @set @fast", SetCmds::spop);
        r.add("srandmember", -2, "readonly", 1, 1, 1, "@read @set @slow", SetCmds::srandmember);
        r.add("smove", 4, "write fast", 1, 2, 1, "@write @set @fast", SetCmds::smove);
        r.add("sinter", -2, "readonly", 1, -1, 1, "@read @set @slow", (x, a) -> algebra(x, a, 1, 'i', null));
        r.add("sunion", -2, "readonly", 1, -1, 1, "@read @set @slow", (x, a) -> algebra(x, a, 1, 'u', null));
        r.add("sdiff", -2, "readonly", 1, -1, 1, "@read @set @slow", (x, a) -> algebra(x, a, 1, 'd', null));
        r.add("sinterstore", -3, "write denyoom", 1, -1, 1, "@write @set @slow", (x, a) -> algebra(x, a, 2, 'i', a[1]));
        r.add("sunionstore", -3, "write denyoom", 1, -1, 1, "@write @set @slow", (x, a) -> algebra(x, a, 2, 'u', a[1]));
        r.add("sdiffstore", -3, "write denyoom", 1, -1, 1, "@write @set @slow", (x, a) -> algebra(x, a, 2, 'd', a[1]));
        r.add("sintercard", -3, "readonly movablekeys", 0, 0, 0, "@read @set @slow", SetCmds::sintercard).keys(a -> ListCmds.numKeys(a, 1));
        r.add("sscan", -3, "readonly", 1, 1, 1, "@read @set @slow", SetCmds::sscan);
    }

    private static Object sadd(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = new LinkedHashSet<Hub.BK>(java.util.stream.Stream.of(a).skip(2).map(Hub.BK::new).toList())
                .stream().map(k -> k.b).toArray(byte[][]::new);
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_SET);
            long added = x.update("INSERT INTO warp_redis_sets (db, k, m) SELECT ?, ?, u.m FROM unnest(?::bytea[]) AS u(m) ON CONFLICT DO NOTHING",
                    x.db, a[1], ms);
            x.adjust(a[1], added);
            return added;
        });
    }

    private static Object srem(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        return x.atomic(() -> {
            if (x.lockExisting(a[1], Ctx.T_SET) == null) {
                return 0L;
            }
            long n = x.update("DELETE FROM warp_redis_sets WHERE db = ? AND k = ? AND m = ANY(?)", x.db, a[1], ms);
            x.adjust(a[1], -n);
            x.deleteIfEmpty(a[1]);
            return n;
        });
    }

    private static Object sismember(Ctx x, byte[][] a) throws Exception {
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_SET, "SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? AND m = ?",
                rs -> rs.getBytes(2), x.db, a[1], a[2]);
        t.expect(Ctx.T_SET);
        return t.rows().isEmpty() ? 0L : 1L;
    }

    private static Object smismember(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_SET, "SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? AND m = ANY(?)",
                rs -> rs.getBytes(2), x.db, a[1], ms);
        t.expect(Ctx.T_SET);
        Set<Hub.BK> found = new java.util.HashSet<>();
        for (byte[] m : t.rows()) {
            found.add(new Hub.BK(m));
        }
        List<Object> out = new ArrayList<>();
        for (byte[] m : ms) {
            out.add(found.contains(new Hub.BK(m)) ? 1L : 0L);
        }
        return out;
    }

    private static Object smembers(Ctx x, byte[][] a) throws Exception {
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_SET, "SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? ORDER BY m",
                rs -> rs.getBytes(2), x.db, a[1]);
        t.expect(Ctx.T_SET);
        return new Resp.RSet(new ArrayList<Object>(t.rows()));
    }

    private static Object spop(Ctx x, byte[][] a) throws Exception {
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
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_SET);
            if (m == null) {
                return fcount == null ? null : new Resp.RSet(new ArrayList<>());
            }
            if (fcount != null && fcount == 0) {
                return new Resp.RSet(new ArrayList<>());
            }
            List<byte[]> got = x.list("DELETE FROM warp_redis_sets WHERE db = ? AND k = ? AND m IN (SELECT m FROM warp_redis_sets "
                    + "WHERE db = ? AND k = ? ORDER BY random() LIMIT ?) RETURNING m", rs -> rs.getBytes(1), x.db, a[1], x.db, a[1],
                    fcount == null ? 1L : fcount);
            x.adjust(a[1], -got.size());
            x.deleteIfEmpty(a[1]);
            if (fcount == null) {
                return got.isEmpty() ? null : got.get(0);
            }
            return new Resp.RSet(new ArrayList<Object>(got));
        });
    }

    private static Object srandmember(Ctx x, byte[][] a) throws Exception {
        if (a.length > 3) {
            throw RedisError.syntax();
        }
        Long count = null;
        if (a.length == 3) {
            count = Num.parseLong(a[2]);
            if (count == null) {
                throw RedisError.notInt();
            }
            if (count < -(Long.MAX_VALUE / 2)) {
                throw RedisError.err("value is out of range");
            }
        }
        long want = count == null ? 1 : count > 0 ? count : 1_000_000_000L;
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_SET,
                "SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? ORDER BY random() LIMIT ?", rs -> rs.getBytes(2), x.db, a[1], want);
        t.expect(Ctx.T_SET);
        List<byte[]> rows = t.rows();
        if (count == null) {
            return rows.isEmpty() ? null : rows.get(0);
        }
        List<Object> out = new ArrayList<>();
        if (rows.isEmpty() || count == 0) {
            return out;
        }
        if (count > 0) {
            out.addAll(rows);
        } else {
            for (long i = 0; i < -count; i++) {
                out.add(rows.get(java.util.concurrent.ThreadLocalRandom.current().nextInt(rows.size())));
            }
        }
        return out;
    }

    private static Object smove(Ctx x, byte[][] a) throws Exception {
        return x.atomic(() -> {
            byte[] src = a[1];
            byte[] dst = a[2];
            boolean same = Arrays.equals(src, dst);
            Ctx.Meta ms;
            if (same || Arrays.compareUnsigned(src, dst) < 0) {
                ms = x.lockExisting(src, Ctx.T_SET);
                if (!same) {
                    x.lock(dst);
                }
            } else {
                x.lock(dst);
                ms = x.lockExisting(src, Ctx.T_SET);
            }
            if (ms == null) {
                return 0L;
            }
            Ctx.Meta md = same ? ms : x.get(dst);
            if (md != null && md.type() != Ctx.T_SET) {
                throw RedisError.wrongType();
            }
            if (same) {
                return x.scalar("SELECT count(*) FROM warp_redis_sets WHERE db = ? AND k = ? AND m = ?", x.db, src, a[3]) > 0 ? 1L : 0L;
            }
            int removed = x.update("DELETE FROM warp_redis_sets WHERE db = ? AND k = ? AND m = ?", x.db, src, a[3]);
            if (removed == 0) {
                return 0L;
            }
            x.adjust(src, -1);
            x.deleteIfEmpty(src);
            x.lockOrCreate(dst, Ctx.T_SET);
            int added = x.update("INSERT INTO warp_redis_sets (db, k, m) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", x.db, dst, a[3]);
            x.adjust(dst, added);
            return 1L;
        });
    }

    /** Distinct live keys of {@code keys} in argument order, validated to be sets; missing = null entry. */
    private static List<byte[]> liveSetKeys(Ctx x, byte[][] keys) throws Exception {
        Map<Hub.BK, Integer> live = x.liveTypes(keys);
        for (Integer t : live.values()) {
            if (t != Ctx.T_SET) {
                throw RedisError.wrongType();
            }
        }
        List<byte[]> out = new ArrayList<>();
        for (byte[] k : keys) {
            out.add(live.containsKey(new Hub.BK(k)) ? k : null);
        }
        return out;
    }

    /** Computes SINTER / SUNION / SDIFF over {@code a[firstKey..]}; stores into {@code dst} when non-null. */
    private static Object algebra(Ctx x, byte[][] a, int firstKey, char op, byte[] dst) throws Exception {
        byte[][] keys = Arrays.copyOfRange(a, firstKey, a.length);
        return x.atomic(() -> {
            List<byte[]> members = compute(x, keys, op);
            if (dst == null) {
                return new Resp.RSet(new ArrayList<Object>(members));
            }
            x.delete(dst);
            if (!members.isEmpty()) {
                x.lockOrCreate(dst, Ctx.T_SET);
                x.update("INSERT INTO warp_redis_sets (db, k, m) SELECT ?, ?, u.m FROM unnest(?::bytea[]) AS u(m)", x.db, dst,
                        members.toArray(new byte[0][]));
                x.adjust(dst, members.size());
            }
            return (long) members.size();
        });
    }

    private static List<byte[]> compute(Ctx x, byte[][] keys, char op) throws Exception {
        List<byte[]> live = liveSetKeys(x, keys);
        switch (op) {
            case 'i': {
                if (live.contains(null)) {
                    return new ArrayList<>();
                }
                byte[][] distinct = live.stream().map(Hub.BK::new).distinct().map(k -> k.b).toArray(byte[][]::new);
                return x.list("SELECT m FROM warp_redis_sets WHERE db = ? AND k = ANY(?) GROUP BY m HAVING count(*) = ? ORDER BY m",
                        rs -> rs.getBytes(1), x.db, distinct, (long) distinct.length);
            }
            case 'u': {
                byte[][] present = live.stream().filter(k -> k != null).toArray(byte[][]::new);
                if (present.length == 0) {
                    return new ArrayList<>();
                }
                return x.list("SELECT DISTINCT m FROM warp_redis_sets WHERE db = ? AND k = ANY(?) ORDER BY m", rs -> rs.getBytes(1), x.db, present);
            }
            default: {
                if (live.get(0) == null) {
                    return new ArrayList<>();
                }
                byte[][] others = live.subList(1, live.size()).stream().filter(k -> k != null).toArray(byte[][]::new);
                return x.list("SELECT m FROM warp_redis_sets s WHERE db = ? AND k = ? AND NOT EXISTS (SELECT 1 FROM warp_redis_sets o "
                        + "WHERE o.db = s.db AND o.k = ANY(?) AND o.m = s.m) ORDER BY m", rs -> rs.getBytes(1), x.db, live.get(0), others);
            }
        }
    }

    private static Object sintercard(Ctx x, byte[][] a) throws Exception {
        Long num = Num.parseLong(a[1]);
        if (num == null || num <= 0) {
            throw RedisError.err("numkeys should be greater than 0");
        }
        if (num > a.length - 2) {
            throw RedisError.err("Number of keys can't be greater than number of args");
        }
        long limit = 0;
        for (int i = 2 + num.intValue(); i < a.length; i++) {
            if (Num.eq(a[i], "LIMIT") && i + 1 < a.length) {
                Long l = Num.parseLong(a[++i]);
                if (l == null || l < 0) {
                    throw RedisError.err("LIMIT can't be negative");
                }
                limit = l;
            } else {
                throw RedisError.syntax();
            }
        }
        byte[][] keys = Arrays.copyOfRange(a, 2, 2 + num.intValue());
        long size = compute(x, keys, 'i').size();
        return limit > 0 ? Math.min(size, limit) : size;
    }

    private static Object sscan(Ctx x, byte[][] a) throws Exception {
        long cursor = KeyCmds.parseCursor(a[2]);
        byte[] match = null;
        long count = 10;
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
            } else {
                throw RedisError.syntax();
            }
        }
        long[] page = ScanPage.plan(cursor, count, Math.max(0, x.count(a[1], Ctx.T_SET)));
        Ctx.TypedRead<byte[]> t = x.typedRead(a[1], Ctx.T_SET,
                "SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? ORDER BY m OFFSET ? LIMIT ?", rs -> rs.getBytes(2), x.db, a[1], page[0], page[1]);
        t.expect(Ctx.T_SET);
        List<Object> items = new ArrayList<>();
        for (byte[] m : t.rows()) {
            if (match == null || Glob.matches(match, m)) {
                items.add(m);
            }
        }
        return List.of(Long.toString(page[2]), items);
    }
}
