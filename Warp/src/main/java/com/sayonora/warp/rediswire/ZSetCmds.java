package com.sayonora.warp.rediswire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Sorted-set commands. Rows are (member, score); a covering index on (key, score, member) serves every range query. */
final class ZSetCmds {

    private ZSetCmds() {
    }

    /** One (member, score) row. */
    record ZE(byte[] m, double score) {
    }

    record ScoreBound(double v, boolean excl) {
    }

    /** Lex bound: {@code kind} -1 = "-", +1 = "+", 0 = a value. */
    record LexBound(int kind, byte[] v, boolean excl) {
    }

    static void register(Cmd.Registry r) {
        r.add("zadd", -4, "write denyoom fast", 1, 1, 1, "@write @sortedset @fast", ZSetCmds::zadd);
        r.add("zincrby", 4, "write denyoom fast", 1, 1, 1, "@write @sortedset @fast", ZSetCmds::zincrby);
        r.add("zrem", -3, "write fast", 1, 1, 1, "@write @sortedset @fast", ZSetCmds::zrem);
        r.add("zscore", 3, "readonly fast", 1, 1, 1, "@read @sortedset @fast", ZSetCmds::zscore);
        r.add("zmscore", -3, "readonly fast", 1, 1, 1, "@read @sortedset @fast", ZSetCmds::zmscore);
        r.add("zcard", 2, "readonly fast", 1, 1, 1, "@read @sortedset @fast", (x, a) -> {
            long n = x.count(a[1], Ctx.T_ZSET);
            return n < 0 ? 0L : n;
        });
        r.add("zcount", 4, "readonly fast", 1, 1, 1, "@read @sortedset @fast", ZSetCmds::zcount);
        r.add("zlexcount", 4, "readonly fast", 1, 1, 1, "@read @sortedset @fast", ZSetCmds::zlexcount);
        r.add("zrank", -3, "readonly fast", 1, 1, 1, "@read @sortedset @fast", (x, a) -> zrank(x, a, false));
        r.add("zrevrank", -3, "readonly fast", 1, 1, 1, "@read @sortedset @fast", (x, a) -> zrank(x, a, true));
        r.add("zrange", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, false, false, false));
        r.add("zrangestore", -5, "write denyoom", 1, 2, 1, "@write @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 2, 3, 4, 5, a[1], false, false, false));
        r.add("zrevrange", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, true, false, false));
        r.add("zrangebyscore", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, false, true, false));
        r.add("zrevrangebyscore", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, true, true, false));
        r.add("zrangebylex", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, false, false, true));
        r.add("zrevrangebylex", -4, "readonly", 1, 1, 1, "@read @sortedset @slow", (x, a) -> zrangeGeneric(x, a, 1, 2, 3, 4, null, true, false, true));
        r.add("zremrangebyrank", 4, "write", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zremrange(x, a, 'r'));
        r.add("zremrangebyscore", 4, "write", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zremrange(x, a, 's'));
        r.add("zremrangebylex", 4, "write", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zremrange(x, a, 'l'));
        r.add("zpopmin", -2, "write fast", 1, 1, 1, "@write @sortedset @fast", (x, a) -> zpop(x, a, false));
        r.add("zpopmax", -2, "write fast", 1, 1, 1, "@write @sortedset @fast", (x, a) -> zpop(x, a, true));
        r.add("bzpopmin", -3, "write noscript fast blocking", 1, -2, 1, "@write @sortedset @fast @blocking", (x, a) -> bzpop(x, a, false))
                .blocking(a -> ListCmds.timeoutOf(a[a.length - 1], false), Resp.NIL_ARRAY);
        r.add("bzpopmax", -3, "write noscript fast blocking", 1, -2, 1, "@write @sortedset @fast @blocking", (x, a) -> bzpop(x, a, true))
                .blocking(a -> ListCmds.timeoutOf(a[a.length - 1], false), Resp.NIL_ARRAY);
        r.add("zmpop", -4, "write movablekeys", 0, 0, 0, "@write @sortedset @slow", (x, a) -> zmpop(x, a, 1)).keys(a -> ListCmds.numKeys(a, 1));
        r.add("bzmpop", -5, "write movablekeys blocking", 0, 0, 0, "@write @sortedset @slow @blocking", (x, a) -> ListCmds.notReadyIfNil(zmpop(x, a, 2)))
                .keys(a -> ListCmds.numKeys(a, 2)).blocking(a -> ListCmds.timeoutOf(a[1], false), Resp.NIL_ARRAY);
        r.add("zunion", -3, "readonly movablekeys", 0, 0, 0, "@read @sortedset @slow", (x, a) -> zalgebra(x, a, 'u', null, 1)).keys(a -> ListCmds.numKeys(a, 1));
        r.add("zinter", -3, "readonly movablekeys", 0, 0, 0, "@read @sortedset @slow", (x, a) -> zalgebra(x, a, 'i', null, 1)).keys(a -> ListCmds.numKeys(a, 1));
        r.add("zdiff", -3, "readonly movablekeys", 0, 0, 0, "@read @sortedset @slow", (x, a) -> zalgebra(x, a, 'd', null, 1)).keys(a -> ListCmds.numKeys(a, 1));
        r.add("zunionstore", -4, "write denyoom movablekeys", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zalgebra(x, a, 'u', a[1], 2)).keys(a -> storeKeys(a));
        r.add("zinterstore", -4, "write denyoom movablekeys", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zalgebra(x, a, 'i', a[1], 2)).keys(a -> storeKeys(a));
        r.add("zdiffstore", -4, "write denyoom movablekeys", 1, 1, 1, "@write @sortedset @slow", (x, a) -> zalgebra(x, a, 'd', a[1], 2)).keys(a -> storeKeys(a));
        r.add("zintercard", -3, "readonly movablekeys", 0, 0, 0, "@read @sortedset @slow", ZSetCmds::zintercard).keys(a -> ListCmds.numKeys(a, 1));
        r.add("zrandmember", -2, "readonly", 1, 1, 1, "@read @sortedset @slow", ZSetCmds::zrandmember);
        r.add("zscan", -3, "readonly", 1, 1, 1, "@read @sortedset @slow", ZSetCmds::zscan);
    }

    private static int[] storeKeys(byte[][] a) {
        int[] nk = ListCmds.numKeys(a, 2);
        int[] r = new int[nk.length + 1];
        r[0] = 1;
        System.arraycopy(nk, 0, r, 1, nk.length);
        return r;
    }

    // ------------------------------------------------------------------------------------------
    // parsing helpers
    // ------------------------------------------------------------------------------------------

    static ScoreBound scoreBound(byte[] b) {
        boolean excl = b.length > 0 && b[0] == '(';
        Double d = Num.parseDouble(excl ? Arrays.copyOfRange(b, 1, b.length) : b);
        if (d == null) {
            throw RedisError.err("min or max is not a float");
        }
        return new ScoreBound(d, excl);
    }

    static LexBound lexBound(byte[] b) {
        if (b.length == 1 && b[0] == '-') {
            return new LexBound(-1, null, false);
        }
        if (b.length == 1 && b[0] == '+') {
            return new LexBound(1, null, false);
        }
        if (b.length >= 1 && (b[0] == '[' || b[0] == '(')) {
            return new LexBound(0, Arrays.copyOfRange(b, 1, b.length), b[0] == '(');
        }
        throw RedisError.err("min or max not valid string range item");
    }

    private static Double parseScore(byte[] b) {
        Double d = Num.parseDouble(b);
        if (d == null) {
            throw RedisError.notFloat();
        }
        return d;
    }

    private static List<ZE> readRows(Ctx x, String sql, Object... args) throws Exception {
        return x.list(sql, rs -> new ZE(rs.getBytes(1), rs.getDouble(2)), args);
    }

    /** Reply for member/score rows. RESP2 flat (score as bulk string); RESP3 array of [member, double] pairs. */
    private static Object rowsReply(Ctx x, List<ZE> rows, boolean withScores) {
        List<Object> out = new ArrayList<>();
        for (ZE e : rows) {
            if (!withScores) {
                out.add(e.m());
            } else if (x.resp3) {
                out.add(new ArrayList<Object>(List.of(e.m(), e.score())));
            } else {
                out.add(e.m());
                out.add(e.score());
            }
        }
        return out;
    }

    private static String scoreCond(ScoreBound lo, ScoreBound hi) {
        return " AND score " + (lo.excl() ? ">" : ">=") + " ? AND score " + (hi.excl() ? "<" : "<=") + " ? ";
    }

    private static String lexCond(LexBound lo, LexBound hi, List<Object> args) {
        StringBuilder sb = new StringBuilder();
        if (lo.kind() == 0) {
            sb.append(" AND m ").append(lo.excl() ? ">" : ">=").append(" ? ");
            args.add(lo.v());
        } else if (lo.kind() == 1) {
            sb.append(" AND false ");
        }
        if (hi.kind() == 0) {
            sb.append(" AND m ").append(hi.excl() ? "<" : "<=").append(" ? ");
            args.add(hi.v());
        } else if (hi.kind() == -1) {
            sb.append(" AND false ");
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------------
    // ZADD / ZINCRBY / ZREM
    // ------------------------------------------------------------------------------------------

    static Object zadd(Ctx x, byte[][] a) throws Exception {
        boolean nx = false;
        boolean xx = false;
        boolean gt = false;
        boolean lt = false;
        boolean ch = false;
        boolean incr = false;
        int i = 2;
        for (; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "NX")) {
                nx = true;
            } else if (Num.eq(o, "XX")) {
                xx = true;
            } else if (Num.eq(o, "GT")) {
                gt = true;
            } else if (Num.eq(o, "LT")) {
                lt = true;
            } else if (Num.eq(o, "CH")) {
                ch = true;
            } else if (Num.eq(o, "INCR")) {
                incr = true;
            } else {
                break;
            }
        }
        int rest = a.length - i;
        if (rest == 0 || rest % 2 != 0) {
            throw RedisError.syntax();
        }
        if (nx && xx) {
            throw RedisError.err("XX and NX options at the same time are not compatible");
        }
        if ((gt && nx) || (lt && nx) || (gt && lt)) {
            throw RedisError.err("GT, LT, and/or NX options at the same time are not compatible");
        }
        if (incr && rest > 2) {
            throw RedisError.err("INCR option supports a single increment-element pair");
        }
        int pairs = rest / 2;
        double[] scores = new double[pairs];
        byte[][] members = new byte[pairs][];
        for (int p = 0; p < pairs; p++) {
            scores[p] = parseScore(a[i + 2 * p]);
            members[p] = a[i + 2 * p + 1];
        }
        final boolean fnx = nx;
        final boolean fxx = xx;
        final boolean fgt = gt;
        final boolean flt = lt;
        final boolean fch = ch;
        final boolean fincr = incr;
        return x.atomic(() -> {
            if (fxx) {
                if (x.lockExisting(a[1], Ctx.T_ZSET) == null) {
                    return fincr ? null : (Object) 0L;
                }
            } else {
                x.lockOrCreate(a[1], Ctx.T_ZSET);
            }
            Map<Hub.BK, Double> cur = new HashMap<>();
            byte[][] distinct = Arrays.stream(members).map(Hub.BK::new).distinct().map(k -> k.b).toArray(byte[][]::new);
            for (ZE e : readRows(x, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ANY(?)", x.db, a[1], distinct)) {
                cur.put(new Hub.BK(e.m()), e.score());
            }
            Map<Hub.BK, Double> writes = new LinkedHashMap<>();
            long added = 0;
            long updated = 0;
            Double incrResult = null;
            for (int p = 0; p < pairs; p++) {
                Hub.BK mk = new Hub.BK(members[p]);
                Double old = cur.get(mk);
                double ns = scores[p];
                if (old != null) {
                    if (fnx) {
                        continue;
                    }
                    if (fincr) {
                        ns = old + ns;
                        if (Double.isNaN(ns)) {
                            throw RedisError.err("resulting score is not a number (NaN)");
                        }
                    }
                    if ((fgt && ns <= old) || (flt && ns >= old)) {
                        continue;
                    }
                    if (ns != old) {
                        updated++;
                    }
                    cur.put(mk, ns);
                    writes.put(mk, ns);
                    incrResult = ns;
                } else {
                    if (fxx) {
                        continue;
                    }
                    if (fincr && Double.isNaN(ns)) {
                        throw RedisError.err("resulting score is not a number (NaN)");
                    }
                    added++;
                    cur.put(mk, ns);
                    writes.put(mk, ns);
                    incrResult = ns;
                }
            }
            if (!writes.isEmpty()) {
                byte[][] wm = new byte[writes.size()][];
                double[] ws = new double[writes.size()];
                int n = 0;
                for (Map.Entry<Hub.BK, Double> e : writes.entrySet()) {
                    wm[n] = e.getKey().b;
                    ws[n++] = e.getValue();
                }
                x.update("INSERT INTO warp_redis_zsets (db, k, m, score) SELECT ?, ?, u.m, u.s FROM unnest(?::bytea[], ?::float8[]) AS u(m, s) "
                        + "ON CONFLICT (db, k, m) DO UPDATE SET score = EXCLUDED.score", x.db, a[1], wm, ws);
                x.adjust(a[1], added);
                x.wake(a[1]);
            } else {
                x.deleteIfEmpty(a[1]);
            }
            if (fincr) {
                return writes.isEmpty() ? null : (Object) incrResult;
            }
            return fch ? added + updated : added;
        });
    }

    private static Object zincrby(Ctx x, byte[][] a) throws Exception {
        double d = parseScore(a[2]);
        return x.atomic(() -> {
            x.lockOrCreate(a[1], Ctx.T_ZSET);
            Double old = x.one("SELECT score FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ?", rs -> rs.getDouble(1), x.db, a[1], a[3]);
            double ns = (old == null ? 0 : old) + d;
            if (Double.isNaN(ns)) {
                throw RedisError.err("resulting score is not a number (NaN)");
            }
            x.update("INSERT INTO warp_redis_zsets (db, k, m, score) VALUES (?, ?, ?, ?) ON CONFLICT (db, k, m) DO UPDATE SET score = EXCLUDED.score",
                    x.db, a[1], a[3], ns);
            if (old == null) {
                x.adjust(a[1], 1);
            }
            x.wake(a[1]);
            return ns;
        });
    }

    private static Object zrem(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        return x.atomic(() -> {
            if (x.lockExisting(a[1], Ctx.T_ZSET) == null) {
                return 0L;
            }
            long n = x.update("DELETE FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ANY(?)", x.db, a[1], ms);
            x.adjust(a[1], -n);
            x.deleteIfEmpty(a[1]);
            return n;
        });
    }

    private static Object zscore(Ctx x, byte[][] a) throws Exception {
        Ctx.TypedRead<Double> t = x.typedRead(a[1], Ctx.T_ZSET, "SELECT score FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ?",
                rs -> rs.getDouble(2), x.db, a[1], a[2]);
        t.expect(Ctx.T_ZSET);
        return t.rows().isEmpty() ? null : t.rows().get(0);
    }

    private static Object zmscore(Ctx x, byte[][] a) throws Exception {
        byte[][] ms = Arrays.copyOfRange(a, 2, a.length);
        Ctx.TypedRead<ZE> t = x.typedRead(a[1], Ctx.T_ZSET, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? AND m = ANY(?)",
                rs -> new ZE(rs.getBytes(2), rs.getDouble(3)), x.db, a[1], ms);
        t.expect(Ctx.T_ZSET);
        Map<Hub.BK, Double> m = new HashMap<>();
        for (ZE e : t.rows()) {
            m.put(new Hub.BK(e.m()), e.score());
        }
        List<Object> out = new ArrayList<>();
        for (byte[] mm : ms) {
            out.add(m.get(new Hub.BK(mm)));
        }
        return out;
    }

    private static Object zcount(Ctx x, byte[][] a) throws Exception {
        ScoreBound lo = scoreBound(a[2]);
        ScoreBound hi = scoreBound(a[3]);
        Ctx.TypedRead<Long> t = x.typedRead(a[1], Ctx.T_ZSET, "SELECT count(*) FROM warp_redis_zsets WHERE db = ? AND k = ? " + scoreCond(lo, hi),
                rs -> rs.getLong(2), x.db, a[1], lo.v(), hi.v());
        t.expect(Ctx.T_ZSET);
        return t.rows().isEmpty() ? 0L : t.rows().get(0);
    }

    private static Object zlexcount(Ctx x, byte[][] a) throws Exception {
        LexBound lo = lexBound(a[2]);
        LexBound hi = lexBound(a[3]);
        List<Object> args = new ArrayList<>();
        args.add(x.db);
        args.add(a[1]);
        String cond = lexCond(lo, hi, args);
        Ctx.TypedRead<Long> t = x.typedRead(a[1], Ctx.T_ZSET, "SELECT count(*) FROM warp_redis_zsets WHERE db = ? AND k = ? " + cond,
                rs -> rs.getLong(2), args.toArray());
        t.expect(Ctx.T_ZSET);
        return t.rows().isEmpty() ? 0L : t.rows().get(0);
    }

    private static Object zrank(Ctx x, byte[][] a, boolean rev) throws Exception {
        boolean withScore = false;
        if (a.length == 4 && Num.eq(a[3], "WITHSCORE")) {
            withScore = true;
        } else if (a.length != 3) {
            throw RedisError.syntax();
        }
        String cmp = rev ? ">" : "<";
        Ctx.TypedRead<Object[]> t = x.typedRead(a[1], Ctx.T_ZSET,
                "SELECT z.score, (SELECT count(*) FROM warp_redis_zsets y WHERE y.db = z.db AND y.k = z.k AND (y.score, y.m) " + cmp
                        + " (z.score, z.m)) AS rank FROM warp_redis_zsets z WHERE z.db = ? AND z.k = ? AND z.m = ?",
                rs -> new Object[] {rs.getDouble(2), rs.getLong(3)}, x.db, a[1], a[2]);
        t.expect(Ctx.T_ZSET);
        if (t.rows().isEmpty()) {
            return withScore ? Resp.NIL_ARRAY : null;
        }
        Object[] r = t.rows().get(0);
        if (withScore) {
            return new ArrayList<Object>(List.of((Long) r[1], (Double) r[0]));
        }
        return r[1];
    }

    // ------------------------------------------------------------------------------------------
    // ranges
    // ------------------------------------------------------------------------------------------

    /**
     * ZRANGE and its legacy forms. Argument indexes: {@code kAt} key, {@code minAt}/{@code maxAt} the two range args,
     * {@code optAt} the first option; {@code dst} non-null for ZRANGESTORE. {@code legacyRev}: ZREVRANGE-style commands
     * (reverse order, first bound is the max). {@code byScore}/{@code byLex} force the mode for the legacy commands.
     */
    private static Object zrangeGeneric(Ctx x, byte[][] a, int kAt, int minAt, int maxAt, int optAt, byte[] dst, boolean legacyRev,
            boolean legacyScore, boolean legacyLex) throws Exception {
        boolean byScore = legacyScore;
        boolean byLex = legacyLex;
        boolean rev = legacyRev;
        boolean withScores = false;
        boolean limit = false;
        long off = 0;
        long cnt = -1;
        boolean modern = Num.eq(a[0], "ZRANGE") || Num.eq(a[0], "ZRANGESTORE");
        for (int i = optAt; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "WITHSCORES") && dst == null) {
                withScores = true;
            } else if (modern && Num.eq(o, "BYSCORE")) {
                byScore = true;
            } else if (modern && Num.eq(o, "BYLEX")) {
                byLex = true;
            } else if (modern && Num.eq(o, "REV")) {
                rev = true;
            } else if (Num.eq(o, "LIMIT") && i + 2 < a.length && !(legacyRev && !legacyScore && !legacyLex)) {
                Long lo = Num.parseLong(a[i + 1]);
                Long lc = Num.parseLong(a[i + 2]);
                if (lo == null || lc == null) {
                    throw RedisError.notInt();
                }
                limit = true;
                off = lo;
                cnt = lc;
                i += 2;
            } else {
                throw RedisError.syntax();
            }
        }
        boolean legacy = !modern;
        if (byScore && byLex) {
            throw RedisError.syntax();
        }
        if (limit && !byScore && !byLex) {
            throw RedisError.err("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        }
        if (withScores && byLex) {
            throw RedisError.err("syntax error, WITHSCORES not supported in combination with BYLEX");
        }
        final boolean frev = rev;
        final boolean fws = withScores;
        final long foff = off;
        final long fcnt = cnt;
        byte[] key = a[kAt];
        List<ZE> rows;
        if (byScore || byLex) {
            // for reversed queries the first bound is the max
            byte[] lowArg = legacy ? (legacyRev ? a[maxAt] : a[minAt]) : (frev ? a[maxAt] : a[minAt]);
            byte[] highArg = legacy ? (legacyRev ? a[minAt] : a[maxAt]) : (frev ? a[minAt] : a[maxAt]);
            if (foff < 0) {
                rows = new ArrayList<>();
                x.count(key, Ctx.T_ZSET);
            } else {
                String order = frev ? "DESC" : "ASC";
                String lim = fcnt < 0 ? "" : " LIMIT " + fcnt;
                if (byScore) {
                    ScoreBound lo = scoreBound(lowArg);
                    ScoreBound hi = scoreBound(highArg);
                    Ctx.TypedRead<ZE> t = x.typedRead(key, Ctx.T_ZSET,
                            "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? " + scoreCond(lo, hi) + " ORDER BY score " + order + ", m " + order
                                    + " OFFSET ?" + lim, rs -> new ZE(rs.getBytes(2), rs.getDouble(3)), x.db, key, lo.v(), hi.v(), foff);
                    t.expect(Ctx.T_ZSET);
                    rows = t.rows();
                } else {
                    LexBound lo = lexBound(lowArg);
                    LexBound hi = lexBound(highArg);
                    List<Object> args = new ArrayList<>();
                    args.add(x.db);
                    args.add(key);
                    String cond = lexCond(lo, hi, args);
                    args.add(foff);
                    Ctx.TypedRead<ZE> t = x.typedRead(key, Ctx.T_ZSET,
                            "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? " + cond + " ORDER BY m " + order + " OFFSET ?" + lim,
                            rs -> new ZE(rs.getBytes(2), rs.getDouble(3)), args.toArray());
                    t.expect(Ctx.T_ZSET);
                    rows = t.rows();
                }
            }
        } else {
            Long s = Num.parseLong(a[minAt]);
            Long e = Num.parseLong(a[maxAt]);
            if (s == null || e == null) {
                throw RedisError.notInt();
            }
            long n = x.count(key, Ctx.T_ZSET);
            if (n <= 0) {
                rows = new ArrayList<>();
            } else {
                long start = s < 0 ? Math.max(0, s + n) : s;
                long end = e < 0 ? e + n : Math.min(e, n - 1);
                if (start > end || start >= n) {
                    rows = new ArrayList<>();
                } else {
                    String order = frev ? "DESC" : "ASC";
                    rows = readRows(x, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY score " + order + ", m " + order
                            + " OFFSET ? LIMIT ?", x.db, key, start, end - start + 1);
                }
            }
        }
        if (dst == null) {
            return rowsReply(x, rows, fws);
        }
        final List<ZE> res = rows;
        return x.atomic(() -> {
            x.delete(dst);
            if (!res.isEmpty()) {
                storeRows(x, dst, res);
            }
            return (long) res.size();
        });
    }

    /** Replaces {@code dst} (already deleted) with {@code rows}. */
    private static void storeRows(Ctx x, byte[] dst, List<ZE> rows) throws Exception {
        x.lockOrCreate(dst, Ctx.T_ZSET);
        byte[][] ms = new byte[rows.size()][];
        double[] ss = new double[rows.size()];
        for (int i = 0; i < ss.length; i++) {
            ms[i] = rows.get(i).m();
            ss[i] = rows.get(i).score();
        }
        x.update("INSERT INTO warp_redis_zsets (db, k, m, score) SELECT ?, ?, u.m, u.s FROM unnest(?::bytea[], ?::float8[]) AS u(m, s)", x.db, dst, ms, ss);
        x.adjust(dst, rows.size());
        x.wake(dst);
    }

    private static Object zremrange(Ctx x, byte[][] a, char kind) throws Exception {
        Object[] bounds = null;
        Long s = null;
        Long e = null;
        ScoreBound slo = null;
        ScoreBound shi = null;
        LexBound llo = null;
        LexBound lhi = null;
        if (kind == 'r') {
            s = Num.parseLong(a[2]);
            e = Num.parseLong(a[3]);
            if (s == null || e == null) {
                throw RedisError.notInt();
            }
        } else if (kind == 's') {
            slo = scoreBound(a[2]);
            shi = scoreBound(a[3]);
        } else {
            llo = lexBound(a[2]);
            lhi = lexBound(a[3]);
        }
        final Long fs = s;
        final Long fe = e;
        final ScoreBound fslo = slo;
        final ScoreBound fshi = shi;
        final LexBound fllo = llo;
        final LexBound flhi = lhi;
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_ZSET);
            if (m == null) {
                return 0L;
            }
            long removed;
            if (kind == 'r') {
                long n = m.n();
                long start = fs < 0 ? Math.max(0, fs + n) : fs;
                long end = fe < 0 ? fe + n : Math.min(fe, n - 1);
                if (start > end || start >= n) {
                    return 0L;
                }
                removed = x.one("WITH d AS (DELETE FROM warp_redis_zsets z USING (SELECT m FROM warp_redis_zsets WHERE db = ? AND k = ? "
                        + "ORDER BY score, m OFFSET ? LIMIT ?) r WHERE z.db = ? AND z.k = ? AND z.m = r.m RETURNING 1) SELECT count(*) FROM d",
                        rs -> rs.getLong(1), x.db, a[1], start, end - start + 1, x.db, a[1]);
            } else if (kind == 's') {
                removed = x.update("DELETE FROM warp_redis_zsets WHERE db = ? AND k = ? " + scoreCond(fslo, fshi), x.db, a[1], fslo.v(), fshi.v());
            } else {
                List<Object> args = new ArrayList<>();
                args.add(x.db);
                args.add(a[1]);
                String cond = lexCond(fllo, flhi, args);
                removed = x.update("DELETE FROM warp_redis_zsets WHERE db = ? AND k = ? " + cond, args.toArray());
            }
            x.adjust(a[1], -removed);
            x.deleteIfEmpty(a[1]);
            return removed;
        });
    }

    // ------------------------------------------------------------------------------------------
    // pops
    // ------------------------------------------------------------------------------------------

    private static List<ZE> popRows(Ctx x, byte[] k, boolean max, long count) throws Exception {
        String order = max ? "DESC" : "ASC";
        List<ZE> rows = readRows(x, "DELETE FROM warp_redis_zsets WHERE db = ? AND k = ? AND m IN (SELECT m FROM warp_redis_zsets WHERE db = ? AND k = ? "
                + "ORDER BY score " + order + ", m " + order + " LIMIT ?) RETURNING m, score", x.db, k, x.db, k, count);
        Comparator<ZE> cmp = Comparator.comparingDouble(ZE::score).thenComparing(ZE::m, Arrays::compareUnsigned);
        rows.sort(max ? cmp.reversed() : cmp);
        if (!rows.isEmpty()) {
            x.adjust(k, -rows.size());
            x.deleteIfEmpty(k);
        }
        return rows;
    }

    private static Object zpop(Ctx x, byte[][] a, boolean max) throws Exception {
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
        final Long fc = count;
        return x.atomic(() -> {
            Ctx.Meta m = x.lockExisting(a[1], Ctx.T_ZSET);
            if (m == null || (fc != null && fc == 0)) {
                return new ArrayList<>();
            }
            List<ZE> rows = popRows(x, a[1], max, fc == null ? 1 : fc);
            if (fc == null) {
                List<Object> flat = new ArrayList<>();
                for (ZE e : rows) {
                    flat.add(e.m());
                    flat.add(e.score());
                }
                return flat;
            }
            return rowsReply(x, rows, true);
        });
    }

    private static Object bzpop(Ctx x, byte[][] a, boolean max) throws Exception {
        return x.atomic(() -> {
            for (int i = 1; i < a.length - 1; i++) {
                Ctx.Meta m = x.lockExisting(a[i], Ctx.T_ZSET);
                if (m == null) {
                    continue;
                }
                List<ZE> rows = popRows(x, a[i], max, 1);
                if (!rows.isEmpty()) {
                    return new ArrayList<Object>(List.of(a[i], rows.get(0).m(), rows.get(0).score()));
                }
            }
            return Cmd.NOT_READY;
        });
    }

    private static Object zmpop(Ctx x, byte[][] a, int numAt) throws Exception {
        Long num = Num.parseLong(a[numAt]);
        if (num == null || num <= 0) {
            throw RedisError.err("numkeys should be greater than 0");
        }
        int dirAt = numAt + 1 + num.intValue();
        if (dirAt >= a.length) {
            throw RedisError.syntax();
        }
        boolean max;
        if (Num.eq(a[dirAt], "MIN")) {
            max = false;
        } else if (Num.eq(a[dirAt], "MAX")) {
            max = true;
        } else {
            throw RedisError.syntax();
        }
        long count = 1;
        boolean saw = false;
        for (int i = dirAt + 1; i < a.length; i++) {
            if (Num.eq(a[i], "COUNT") && !saw && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null || c <= 0) {
                    throw RedisError.err("count should be greater than 0");
                }
                count = c;
                saw = true;
            } else {
                throw RedisError.syntax();
            }
        }
        final long fc = count;
        final int fnum = num.intValue();
        return x.atomic(() -> {
            for (int i = 0; i < fnum; i++) {
                byte[] k = a[numAt + 1 + i];
                if (x.lockExisting(k, Ctx.T_ZSET) == null) {
                    continue;
                }
                List<ZE> rows = popRows(x, k, max, fc);
                if (!rows.isEmpty()) {
                    List<Object> pairs = new ArrayList<>();
                    for (ZE e : rows) {
                        pairs.add(new ArrayList<Object>(List.of(e.m(), e.score())));
                    }
                    return new ArrayList<Object>(List.of(k, pairs));
                }
            }
            return Resp.NIL_ARRAY;
        });
    }

    // ------------------------------------------------------------------------------------------
    // ZUNION / ZINTER / ZDIFF
    // ------------------------------------------------------------------------------------------

    private static Map<Hub.BK, Double> loadInput(Ctx x, byte[] k, Map<Hub.BK, Integer> live) throws Exception {
        Integer t = live.get(new Hub.BK(k));
        Map<Hub.BK, Double> m = new LinkedHashMap<>();
        if (t == null) {
            return m;
        }
        if (t == Ctx.T_ZSET) {
            for (ZE e : readRows(x, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ?", x.db, k)) {
                m.put(new Hub.BK(e.m()), e.score());
            }
        } else if (t == Ctx.T_SET) {
            for (byte[] mm : x.list("SELECT m FROM warp_redis_sets WHERE db = ? AND k = ?", rs -> rs.getBytes(1), x.db, k)) {
                m.put(new Hub.BK(mm), 1.0);
            }
        } else {
            throw RedisError.wrongType();
        }
        return m;
    }

    private static double aggregate(double cur, double v, String agg) {
        double r = switch (agg) {
            case "MIN" -> Math.min(cur, v);
            case "MAX" -> Math.max(cur, v);
            default -> cur + v;
        };
        return Double.isNaN(r) ? 0.0 : r;
    }

    private static double weighted(double score, double w) {
        double r = score * w;
        return Double.isNaN(r) ? 0.0 : r;
    }

    private static Object zalgebra(Ctx x, byte[][] a, char op, byte[] dst, int numAt) throws Exception {
        String cmdName = Num.str(a[0]).toLowerCase();
        Long num = Num.parseLong(a[numAt]);
        if (num == null) {
            throw RedisError.notInt();
        }
        if (num < 1) {
            throw RedisError.err("at least 1 input key is needed for '" + cmdName + "' command");
        }
        if (num > a.length - numAt - 1) {
            throw RedisError.syntax();
        }
        int nk = num.intValue();
        double[] weights = new double[nk];
        Arrays.fill(weights, 1.0);
        String agg = "SUM";
        boolean withScores = false;
        for (int i = numAt + 1 + nk; i < a.length; i++) {
            if (Num.eq(a[i], "WEIGHTS") && op != 'd') {
                if (i + nk >= a.length) {
                    throw RedisError.syntax();
                }
                for (int j = 0; j < nk; j++) {
                    Double w = Num.parseDouble(a[i + 1 + j]);
                    if (w == null) {
                        throw RedisError.err("weight value is not a float");
                    }
                    weights[j] = w;
                }
                i += nk;
            } else if (Num.eq(a[i], "AGGREGATE") && op != 'd' && i + 1 < a.length) {
                agg = Num.upper(a[++i]);
                if (!agg.equals("SUM") && !agg.equals("MIN") && !agg.equals("MAX")) {
                    throw RedisError.syntax();
                }
            } else if (Num.eq(a[i], "WITHSCORES") && dst == null) {
                withScores = true;
            } else {
                throw RedisError.syntax();
            }
        }
        byte[][] keys = Arrays.copyOfRange(a, numAt + 1, numAt + 1 + nk);
        final String fagg = agg;
        final boolean fws = withScores;
        return x.atomic(() -> {
            Map<Hub.BK, Integer> live = x.liveTypes(keys);
            List<Map<Hub.BK, Double>> inputs = new ArrayList<>();
            for (byte[] k : keys) {
                inputs.add(loadInput(x, k, live));
            }
            Map<Hub.BK, Double> result = new LinkedHashMap<>();
            if (op == 'u') {
                for (int i = 0; i < nk; i++) {
                    for (Map.Entry<Hub.BK, Double> e : inputs.get(i).entrySet()) {
                        double v = weighted(e.getValue(), weights[i]);
                        Double cur = result.get(e.getKey());
                        result.put(e.getKey(), cur == null ? v : aggregate(cur, v, fagg));
                    }
                }
            } else if (op == 'i') {
                for (Map.Entry<Hub.BK, Double> e : inputs.get(0).entrySet()) {
                    double acc = weighted(e.getValue(), weights[0]);
                    boolean all = true;
                    for (int i = 1; i < nk; i++) {
                        Double o = inputs.get(i).get(e.getKey());
                        if (o == null) {
                            all = false;
                            break;
                        }
                        acc = aggregate(acc, weighted(o, weights[i]), fagg);
                    }
                    if (all) {
                        result.put(e.getKey(), acc);
                    }
                }
            } else {
                for (Map.Entry<Hub.BK, Double> e : inputs.get(0).entrySet()) {
                    boolean found = false;
                    for (int i = 1; i < nk; i++) {
                        if (inputs.get(i).containsKey(e.getKey())) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
            List<ZE> rows = new ArrayList<>();
            for (Map.Entry<Hub.BK, Double> e : result.entrySet()) {
                rows.add(new ZE(e.getKey().b, e.getValue()));
            }
            rows.sort(Comparator.comparingDouble(ZE::score).thenComparing(ZE::m, Arrays::compareUnsigned));
            if (dst == null) {
                return rowsReply(x, rows, fws);
            }
            x.delete(dst);
            if (!rows.isEmpty()) {
                storeRows(x, dst, rows);
            }
            return (long) rows.size();
        });
    }

    private static Object zintercard(Ctx x, byte[][] a) throws Exception {
        Long num = Num.parseLong(a[1]);
        if (num == null) {
            throw RedisError.notInt();
        }
        if (num < 1) {
            throw RedisError.err("at least 1 input key is needed for 'zintercard' command");
        }
        if (num > a.length - 2) {
            throw RedisError.syntax();
        }
        long limit = 0;
        for (int i = 2 + num.intValue(); i < a.length; i++) {
            if (Num.eq(a[i], "LIMIT") && i + 1 < a.length) {
                Long l = Num.parseLong(a[++i]);
                if (l == null) {
                    throw RedisError.notInt();
                }
                if (l < 0) {
                    throw RedisError.err("LIMIT can't be negative");
                }
                limit = l;
            } else {
                throw RedisError.syntax();
            }
        }
        byte[][] keys = Arrays.copyOfRange(a, 2, 2 + num.intValue());
        Map<Hub.BK, Integer> live = x.liveTypes(keys);
        Map<Hub.BK, Double> acc = null;
        for (byte[] k : keys) {
            Map<Hub.BK, Double> in = loadInput(x, k, live);
            if (acc == null) {
                acc = in;
            } else {
                acc.keySet().retainAll(in.keySet());
            }
        }
        long n = acc == null ? 0 : acc.size();
        return limit > 0 ? Math.min(n, limit) : n;
    }

    // ------------------------------------------------------------------------------------------

    private static Object zrandmember(Ctx x, byte[][] a) throws Exception {
        boolean withScores = false;
        Long count = null;
        if (a.length >= 3) {
            count = Num.parseLong(a[2]);
            if (count == null) {
                throw RedisError.notInt();
            }
            if (a.length == 4 && Num.eq(a[3], "WITHSCORES")) {
                withScores = true;
            } else if (a.length > 3) {
                throw RedisError.syntax();
            }
        }
        if (count != null && count < -(Long.MAX_VALUE / 2)) {
            throw RedisError.err("value is out of range");
        }
        long lim = count == null ? 1 : count > 0 ? count : 1_000_000_000L;
        Ctx.TypedRead<ZE> t = x.typedRead(a[1], Ctx.T_ZSET, "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY random() LIMIT ?",
                rs -> new ZE(rs.getBytes(2), rs.getDouble(3)), x.db, a[1], lim);
        t.expect(Ctx.T_ZSET);
        List<ZE> rows = t.rows();
        if (count == null) {
            return rows.isEmpty() ? null : rows.get(0).m();
        }
        if (rows.isEmpty() || count == 0) {
            return new ArrayList<>();
        }
        List<ZE> picked = new ArrayList<>();
        if (count > 0) {
            picked.addAll(rows);
        } else {
            for (long i = 0; i < -count; i++) {
                picked.add(rows.get(ThreadLocalRandom.current().nextInt(rows.size())));
            }
        }
        return rowsReply(x, picked, withScores);
    }

    private static Object zscan(Ctx x, byte[][] a) throws Exception {
        long cursor = KeyCmds.parseCursor(a[2]);
        byte[] match = null;
        long count = 10;
        boolean noScores = false;
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
            } else if (Num.eq(a[i], "NOSCORES")) {
                noScores = true;
            } else {
                throw RedisError.syntax();
            }
        }
        long[] page = ScanPage.plan(cursor, count, Math.max(0, x.count(a[1], Ctx.T_ZSET)));
        Ctx.TypedRead<ZE> t = x.typedRead(a[1], Ctx.T_ZSET,
                "SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY m OFFSET ? LIMIT ?",
                rs -> new ZE(rs.getBytes(2), rs.getDouble(3)), x.db, a[1], page[0], page[1]);
        t.expect(Ctx.T_ZSET);
        List<Object> items = new ArrayList<>();
        for (ZE e : t.rows()) {
            if (match == null || Glob.matches(match, e.m())) {
                items.add(e.m());
                if (!noScores) {
                    items.add(Num.bytes(Num.fmtDouble(e.score())));
                }
            }
        }
        return List.of(Long.toString(page[2]), items);
    }
}
