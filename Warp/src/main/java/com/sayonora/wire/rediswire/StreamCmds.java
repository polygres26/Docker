package com.sayonora.wire.rediswire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Streams: entries are rows (ms, seq, packed fields); the stream's counters live in {@code keys.sv}; consumer groups,
 * consumers and the pending-entries list are their own tables. Ids are pairs of signed 64-bit numbers (a documented
 * narrowing of Redis' unsigned 64 bits).
 */
final class StreamCmds {

    private StreamCmds() {
    }

    /** Stream id: two UNSIGNED 64-bit numbers (raw bit patterns); stored in Postgres with the sign bit flipped so bigint order == unsigned order. */
    record Id(long ms, long seq) implements Comparable<Id> {
        static final Id ZERO = new Id(0, 0);
        static final Id MAX = new Id(-1L, -1L);

        static Id fromDb(long dbMs, long dbSeq) {
            return new Id(dbMs ^ Long.MIN_VALUE, dbSeq ^ Long.MIN_VALUE);
        }

        long dbMs() {
            return ms ^ Long.MIN_VALUE;
        }

        long dbSeq() {
            return seq ^ Long.MIN_VALUE;
        }

        @Override
        public int compareTo(Id o) {
            int c = Long.compareUnsigned(ms, o.ms);
            return c != 0 ? c : Long.compareUnsigned(seq, o.seq);
        }

        String fmt() {
            return Long.toUnsignedString(ms) + "-" + Long.toUnsignedString(seq);
        }

        Id next() {
            return seq == -1L ? new Id(ms + 1, 0) : new Id(ms, seq + 1);
        }

        Id prev() {
            return seq == 0 ? new Id(ms - 1, -1L) : new Id(ms, seq - 1);
        }
    }

    /** Stream counters kept in keys.sv. */
    static final class Meta {
        Id last = Id.ZERO;
        long added;
        Id maxDeleted = Id.ZERO;

        static Meta parse(byte[] sv) {
            Meta m = new Meta();
            if (sv == null || sv.length == 0) {
                return m;
            }
            String[] p = new String(sv, StandardCharsets.US_ASCII).split(" ");
            m.last = new Id(Long.parseUnsignedLong(p[0]), Long.parseUnsignedLong(p[1]));
            m.added = Long.parseLong(p[2]);
            m.maxDeleted = new Id(Long.parseUnsignedLong(p[3]), Long.parseUnsignedLong(p[4]));
            return m;
        }

        byte[] encode() {
            return (Long.toUnsignedString(last.ms()) + " " + Long.toUnsignedString(last.seq()) + " " + added + " " + Long.toUnsignedString(maxDeleted.ms()) + " " + Long.toUnsignedString(maxDeleted.seq())).getBytes(StandardCharsets.US_ASCII);
        }
    }

    record Entry(Id id, byte[] packed) {
    }

    static void register(Cmd.Registry r) {
        r.add("xadd", -5, "write denyoom fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xadd);
        r.add("xlen", 2, "readonly fast", 1, 1, 1, "@read @stream @fast", (x, a) -> {
            long n = x.count(a[1], Ctx.T_STREAM);
            return n < 0 ? 0L : n;
        });
        r.add("xrange", -4, "readonly", 1, 1, 1, "@read @stream @slow", (x, a) -> xrange(x, a, false));
        r.add("xrevrange", -4, "readonly", 1, 1, 1, "@read @stream @slow", (x, a) -> xrange(x, a, true));
        r.add("xdel", -3, "write fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xdel);
        r.add("xtrim", -4, "write", 1, 1, 1, "@write @stream @slow", StreamCmds::xtrim);
        r.add("xsetid", -3, "write denyoom fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xsetid);
        r.add("xread", -4, "readonly blocking movablekeys", 0, 0, 0, "@read @stream @slow @blocking", (x, a) -> xread(x, a, false))
                .keys(a -> streamKeys(a, false)).blocking(a -> blockMs(a, false), Resp.NIL_ARRAY);
        r.add("xreadgroup", -7, "write blocking movablekeys", 0, 0, 0, "@write @stream @slow @blocking", (x, a) -> xread(x, a, true))
                .keys(a -> streamKeys(a, true)).blocking(a -> blockMs(a, true), Resp.NIL_ARRAY);
        r.add("xgroup", -2, "write", 2, 2, 1, "@write @stream @slow", StreamCmds::xgroup);
        r.add("xack", -4, "write fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xack);
        r.add("xpending", -3, "readonly", 1, 1, 1, "@read @stream @slow", StreamCmds::xpending);
        r.add("xclaim", -6, "write fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xclaim);
        r.add("xautoclaim", -6, "write fast", 1, 1, 1, "@write @stream @fast", StreamCmds::xautoclaim);
        r.add("xinfo", -2, "readonly", 2, 2, 1, "@read @stream @slow", StreamCmds::xinfo);
    }

    // ------------------------------------------------------------------------------------------
    // ids and packing
    // ------------------------------------------------------------------------------------------

    private static RedisError badId() {
        return RedisError.err("Invalid stream ID specified as stream command argument");
    }

    /** Parses "ms-seq" or "ms"; {@code missingSeq} is used when only the ms part is given. */
    static Id parseId(byte[] b, long missingSeq) {
        String s = new String(b, StandardCharsets.ISO_8859_1);
        try {
            int dash = s.indexOf('-');
            if (dash < 0) {
                return new Id(parseU(s), missingSeq);
            }
            return new Id(parseU(s.substring(0, dash)), parseU(s.substring(dash + 1)));
        } catch (NumberFormatException e) {
            throw badId();
        }
    }

    private static long parseU(String s) {
        if (s.isEmpty() || s.length() > 20 || !s.chars().allMatch(Character::isDigit)) {
            throw new NumberFormatException();
        }
        return Long.parseUnsignedLong(s);
    }

    /** Range start bound: "-" = 0-0, "(id" exclusive. */
    private static Id rangeStart(byte[] b) {
        if (b.length == 1 && b[0] == '-') {
            return Id.ZERO;
        }
        if (b.length == 1 && b[0] == '+') {
            return Id.MAX;
        }
        if (b.length > 0 && b[0] == '(') {
            Id id = parseId(Arrays.copyOfRange(b, 1, b.length), 0);
            if (id.equals(Id.MAX)) {
                throw RedisError.err("invalid start ID for the interval");
            }
            return id.next();
        }
        return parseId(b, 0);
    }

    private static Id rangeEnd(byte[] b) {
        if (b.length == 1 && b[0] == '+') {
            return Id.MAX;
        }
        if (b.length == 1 && b[0] == '-') {
            return Id.ZERO;
        }
        if (b.length > 0 && b[0] == '(') {
            Id id = parseId(Arrays.copyOfRange(b, 1, b.length), Long.MAX_VALUE);
            if (id.equals(Id.ZERO)) {
                throw RedisError.err("invalid end ID for the interval");
            }
            return id.prev();
        }
        return parseId(b, Long.MAX_VALUE);
    }

    static byte[] pack(byte[][] a, int from) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(bo);
        o.writeInt(a.length - from);
        for (int i = from; i < a.length; i++) {
            o.writeInt(a[i].length);
            o.write(a[i]);
        }
        return bo.toByteArray();
    }

    static List<Object> unpack(byte[] p) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(p));
            int n = in.readInt();
            List<Object> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                out.add(b);
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object entryReply(Entry e) {
        return new ArrayList<Object>(List.of(e.id().fmt().getBytes(StandardCharsets.US_ASCII), unpack(e.packed())));
    }

    private static List<Entry> readEntries(Ctx x, byte[] k, String where, String order, long limit, Object... extra) throws Exception {
        Object[] args = new Object[3 + extra.length];
        args[0] = x.db;
        args[1] = k;
        System.arraycopy(extra, 0, args, 2, extra.length);
        args[args.length - 1] = limit;
        return x.list("SELECT ms, seq, f FROM warp_redis_stream_entries WHERE db = ? AND k = ? " + where + " ORDER BY ms " + order + ", seq " + order
                + " LIMIT ?", rs -> new Entry(Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getBytes(3)), args);
    }

    private static void saveMeta(Ctx x, byte[] k, Meta m) throws Exception {
        x.update("UPDATE warp_redis_keys SET sv = ? WHERE db = ? AND k = ?", m.encode(), x.db, k);
    }

    private static Meta loadMeta(Ctx x, byte[] k) throws Exception {
        return Meta.parse(x.one("SELECT sv FROM warp_redis_keys WHERE db = ? AND k = ?", rs -> rs.getBytes(1), x.db, k));
    }

    // ------------------------------------------------------------------------------------------
    // XADD / XTRIM / XDEL
    // ------------------------------------------------------------------------------------------

    private static Object xadd(Ctx x, byte[][] a) throws Exception {
        int i = 2;
        boolean noMk = false;
        TrimSpec trimSpec = null;
        for (; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "NOMKSTREAM")) {
                noMk = true;
            } else if (Num.eq(o, "MAXLEN") || Num.eq(o, "MINID")) {
                Object[] r = parseTrim(a, i);
                trimSpec = (TrimSpec) r[0];
                i = (Integer) r[1];
            } else {
                break;
            }
        }
        if (i >= a.length) {
            throw RedisError.syntax();
        }
        byte[] idArg = a[i++];
        int fv = a.length - i;
        if (fv < 2 || fv % 2 != 0) {
            throw RedisError.arity("xadd");
        }
        final boolean autoMs = idArg.length == 1 && idArg[0] == '*';
        boolean autoSeq = false;
        Id given = null;
        if (!autoMs) {
            String s = new String(idArg, StandardCharsets.ISO_8859_1);
            if (s.endsWith("-*")) {
                autoSeq = true;
                given = new Id(parseId(Arrays.copyOf(idArg, idArg.length - 2), 0).ms(), 0);
            } else {
                given = parseId(idArg, 0);
                if (given.equals(Id.ZERO)) {
                    throw RedisError.err("The ID specified in XADD must be greater than 0-0");
                }
            }
        }
        final byte[] packed = pack(a, i);
        final boolean fautoSeq = autoSeq;
        final Id fgiven = given;
        final TrimSpec ftrim = trimSpec;
        final boolean fnomk = noMk;
        return x.atomic(() -> {
            Ctx.Meta km;
            if (fnomk) {
                km = x.lockExisting(a[1], Ctx.T_STREAM);
                if (km == null) {
                    return null;
                }
            } else {
                km = x.lockOrCreate(a[1], Ctx.T_STREAM);
            }
            Meta m = Meta.parse(km.sv());
            if (m.last.equals(Id.MAX)) {
                throw RedisError.err("The stream has exhausted the last possible ID, unable to add more items");
            }
            Id id;
            if (autoMs) {
                long now = x.now;
                id = Long.compareUnsigned(now, m.last.ms()) > 0 ? new Id(now, 0) : m.last.next();
            } else if (fautoSeq) {
                if (Long.compareUnsigned(fgiven.ms(), m.last.ms()) < 0) {
                    throw RedisError.err("The ID specified in XADD is equal or smaller than the target stream top item");
                }
                if (fgiven.ms() == m.last.ms()) {
                    if (m.last.seq() == -1L) {
                        throw RedisError.err("The ID specified in XADD is equal or smaller than the target stream top item");
                    }
                    id = new Id(fgiven.ms(), m.last.seq() + 1);
                } else {
                    id = new Id(fgiven.ms(), 0);
                }
                if (id.equals(Id.ZERO)) {
                    id = new Id(0, 1);
                }
            } else {
                id = fgiven;
                if (id.compareTo(m.last) <= 0 && (km.n() > 0 || m.added > 0 || !m.last.equals(Id.ZERO))) {
                    throw RedisError.err("The ID specified in XADD is equal or smaller than the target stream top item");
                }
            }
            x.update("INSERT INTO warp_redis_stream_entries (db, k, ms, seq, f) VALUES (?, ?, ?, ?, ?)", x.db, a[1], id.dbMs(), id.dbSeq(), packed);
            m.last = id;
            m.added++;
            x.adjust(a[1], 1);
            saveMeta(x, a[1], m);
            trim(x, a[1], ftrim);
            x.wake(a[1]);
            return id.fmt().getBytes(StandardCharsets.US_ASCII);
        });
    }

    /** MAXLEN/MINID [=|~] threshold [LIMIT n]. {@code approx} trims only whole "nodes" of 100 entries like Redis' radix tree. */
    record TrimSpec(boolean maxlen, long threshold, Id minId, boolean approx, long limit) {
    }

    private static final long NODE = 100;

    /** Parses a trim clause starting at a[i] (MAXLEN or MINID); returns {spec, index of its last argument}. */
    private static Object[] parseTrim(byte[][] a, int i) {
        boolean maxlen = Num.eq(a[i], "MAXLEN");
        i++;
        boolean approx = false;
        if (i < a.length && (Num.eq(a[i], "=") || Num.eq(a[i], "~"))) {
            approx = Num.eq(a[i], "~");
            i++;
        }
        if (i >= a.length) {
            throw RedisError.syntax();
        }
        long thr = 0;
        Id min = null;
        if (maxlen) {
            Long t = Num.parseLong(a[i]);
            if (t == null) {
                throw RedisError.notInt();
            }
            if (t < 0) {
                throw RedisError.err("The MAXLEN argument must be >= 0.");
            }
            thr = t;
        } else {
            min = parseId(a[i], 0);
        }
        long limit = -1;
        if (i + 2 < a.length && Num.eq(a[i + 1], "LIMIT")) {
            Long l = Num.parseLong(a[i + 2]);
            if (l == null) {
                throw RedisError.notInt();
            }
            if (l < 0) {
                throw RedisError.err("The LIMIT argument must be >= 0.");
            }
            if (!approx) {
                throw RedisError.err("syntax error, LIMIT cannot be used without the special ~ option");
            }
            limit = l;
            i += 2;
        }
        return new Object[] {new TrimSpec(maxlen, thr, min, approx, limit), i};
    }

    private static long trim(Ctx x, byte[] k, TrimSpec t) throws Exception {
        if (t == null) {
            return 0;
        }
        long toRemove;
        if (t.maxlen()) {
            long n = x.scalar("SELECT n FROM warp_redis_keys WHERE db = ? AND k = ?", x.db, k);
            toRemove = n - t.threshold();
        } else {
            toRemove = x.scalar("SELECT count(*) FROM warp_redis_stream_entries WHERE db = ? AND k = ? AND (ms, seq) < (?, ?)", x.db, k,
                    t.minId().dbMs(), t.minId().dbSeq());
        }
        if (toRemove <= 0) {
            return 0;
        }
        if (t.approx()) {
            long cap = t.limit() < 0 ? NODE * 100 : t.limit();
            toRemove = Math.min(toRemove / NODE * NODE, cap / NODE * NODE);
            if (toRemove <= 0) {
                return 0;
            }
        }
        long removed = x.one("WITH d AS (DELETE FROM warp_redis_stream_entries e USING (SELECT ms, seq FROM warp_redis_stream_entries WHERE db = ? AND k = ? "
                + "ORDER BY ms, seq LIMIT ?) r WHERE e.db = ? AND e.k = ? AND e.ms = r.ms AND e.seq = r.seq RETURNING e.ms, e.seq) "
                + "SELECT count(*) FROM d", rs -> rs.getLong(1), x.db, k, toRemove, x.db, k);
        if (removed > 0) {
            x.adjust(k, -removed);
        }
        return removed;
    }

    private static Object xtrim(Ctx x, byte[][] a) throws Exception {
        if (!Num.eq(a[2], "MAXLEN") && !Num.eq(a[2], "MINID")) {
            throw RedisError.syntax();
        }
        Object[] r = parseTrim(a, 2);
        TrimSpec spec = (TrimSpec) r[0];
        if ((Integer) r[1] + 1 < a.length) {
            throw RedisError.syntax();
        }
        return x.atomic(() -> {
            Ctx.Meta km = x.lockExisting(a[1], Ctx.T_STREAM);
            if (km == null) {
                return 0L;
            }
            return trim(x, a[1], spec);
        });
    }

    private static Object xdel(Ctx x, byte[][] a) throws Exception {
        List<Id> ids = new ArrayList<>();
        for (int i = 2; i < a.length; i++) {
            ids.add(parseId(a[i], 0));
        }
        return x.atomic(() -> {
            Ctx.Meta km = x.lockExisting(a[1], Ctx.T_STREAM);
            if (km == null) {
                return 0L;
            }
            Meta m = Meta.parse(km.sv());
            long removed = 0;
            for (Id id : ids) {
                int n = x.update("DELETE FROM warp_redis_stream_entries WHERE db = ? AND k = ? AND ms = ? AND seq = ?", x.db, a[1], id.dbMs(), id.dbSeq());
                if (n > 0) {
                    removed++;
                    if (id.compareTo(m.maxDeleted) > 0) {
                        m.maxDeleted = id;
                    }
                }
            }
            if (removed > 0) {
                x.adjust(a[1], -removed);
                saveMeta(x, a[1], m);
            }
            return removed;
        });
    }

    private static Object xsetid(Ctx x, byte[][] a) throws Exception {
        Id id = parseId(a[2], 0);
        Long added = null;
        Id maxDel = null;
        for (int i = 3; i < a.length; i++) {
            if (Num.eq(a[i], "ENTRIESADDED") && i + 1 < a.length) {
                added = Num.parseLong(a[++i]);
                if (added == null) {
                    throw RedisError.notInt();
                }
                if (added < 0) {
                    throw RedisError.err("entries_added must be positive");
                }
            } else if (Num.eq(a[i], "MAXDELETEDID") && i + 1 < a.length) {
                maxDel = parseId(a[++i], 0);
            } else {
                throw RedisError.syntax();
            }
        }
        final Long fadded = added;
        final Id fmd = maxDel;
        return x.atomic(() -> {
            Ctx.Meta km = x.lockExisting(a[1], Ctx.T_STREAM);
            if (km == null) {
                throw new RedisError(RedisError.NO_KEY);
            }
            Meta m = Meta.parse(km.sv());
            if (id.compareTo(m.maxDeleted) < 0) {
                throw RedisError.err("The ID specified in XSETID is smaller than current max_deleted_entry_id");
            }
            Id top = x.one("SELECT ms, seq FROM warp_redis_stream_entries WHERE db = ? AND k = ? ORDER BY ms DESC, seq DESC LIMIT 1",
                    rs -> Id.fromDb(rs.getLong(1), rs.getLong(2)), x.db, a[1]);
            if (top != null && id.compareTo(top) < 0) {
                throw RedisError.err("The ID specified in XSETID is smaller than the target stream top item");
            }
            if (fadded != null && fadded < km.n()) {
                throw RedisError.err("The entries_added specified in XSETID is smaller than the target stream length");
            }
            if (fmd != null && fmd.compareTo(id) > 0) {
                throw RedisError.err("The ID specified in XSETID is smaller than the provided max_deleted_entry_id");
            }
            m.last = id;
            if (fadded != null) {
                m.added = fadded;
            }
            if (fmd != null) {
                m.maxDeleted = fmd;
            }
            saveMeta(x, a[1], m);
            return Resp.OK;
        });
    }

    // ------------------------------------------------------------------------------------------
    // XRANGE
    // ------------------------------------------------------------------------------------------

    private static Object xrange(Ctx x, byte[][] a, boolean rev) throws Exception {
        Id lo = rev ? rangeStart(a[3]) : rangeStart(a[2]);
        Id hi = rev ? rangeEnd(a[2]) : rangeEnd(a[3]);
        long count = Long.MAX_VALUE;
        boolean zeroCount = false;
        if (a.length > 4) {
            if (a.length != 6 || !Num.eq(a[4], "COUNT")) {
                throw RedisError.syntax();
            }
            Long c = Num.parseLong(a[5]);
            if (c == null) {
                throw RedisError.notInt();
            }
            count = c < 0 ? 0 : c;
            if (count == 0) {
                zeroCount = true;
            }
        }
        Ctx.Meta m = x.get(a[1]);
        if (m != null && m.type() != Ctx.T_STREAM) {
            throw RedisError.wrongType();
        }
        if (zeroCount) {
            return Resp.NIL_ARRAY;
        }
        if (m == null || lo.compareTo(hi) > 0) {
            return new ArrayList<>();
        }
        List<Entry> es = readEntries(x, a[1], "AND (ms, seq) >= (?, ?) AND (ms, seq) <= (?, ?)", rev ? "DESC" : "ASC", count,
                lo.dbMs(), lo.dbSeq(), hi.dbMs(), hi.dbSeq());
        List<Object> out = new ArrayList<>();
        for (Entry e : es) {
            out.add(entryReply(e));
        }
        return out;
    }

    // ------------------------------------------------------------------------------------------
    // XREAD / XREADGROUP
    // ------------------------------------------------------------------------------------------

    private static int streamsAt(byte[][] a) {
        for (int i = 1; i < a.length; i++) {
            if (Num.eq(a[i], "STREAMS")) {
                return i;
            }
        }
        return -1;
    }

    private static int[] streamKeys(byte[][] a, boolean group) {
        int s = streamsAt(a);
        if (s < 0) {
            return new int[0];
        }
        int n = a.length - s - 1;
        if (n <= 0 || n % 2 != 0) {
            return new int[0];
        }
        int[] r = new int[n / 2];
        for (int i = 0; i < r.length; i++) {
            r[i] = s + 1 + i;
        }
        return r;
    }

    private static long blockMs(byte[][] a, boolean group) {
        for (int i = 1; i < a.length; i++) {
            if (Num.eq(a[i], "STREAMS")) {
                break;
            }
            if (Num.eq(a[i], "BLOCK") && i + 1 < a.length) {
                return ListCmds.timeoutOf(a[i + 1], true);
            }
        }
        return -1;
    }

    private static Object xread(Ctx x, byte[][] a, boolean group) throws Exception {
        long count = Long.MAX_VALUE;
        boolean noack = false;
        byte[] gname = null;
        byte[] cname = null;
        int streams = -1;
        for (int i = 1; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "COUNT") && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null) {
                    throw RedisError.notInt();
                }
                count = c <= 0 ? Long.MAX_VALUE : c;
            } else if (Num.eq(o, "BLOCK") && i + 1 < a.length) {
                ListCmds.timeoutOf(a[++i], true);
            } else if (Num.eq(o, "NOACK") && group) {
                noack = true;
            } else if (Num.eq(o, "GROUP") && group && i + 2 < a.length) {
                gname = a[++i];
                cname = a[++i];
            } else if (Num.eq(o, "STREAMS")) {
                streams = i;
                break;
            } else {
                throw RedisError.syntax();
            }
        }
        if (streams < 0) {
            throw RedisError.syntax();
        }
        if (group && gname == null) {
            throw RedisError.err("Missing GROUP option for XREADGROUP");
        }
        int n = a.length - streams - 1;
        if (n <= 0 || n % 2 != 0) {
            throw RedisError.err("Unbalanced '" + (group ? "xreadgroup" : "xread") + "' list of streams: for each stream key an ID or '$' must be specified.");
        }
        n /= 2;
        final long fcount = count;
        final boolean fnoack = noack;
        final byte[] fg = gname;
        final byte[] fc = cname;
        final int fstreams = streams;
        final int fn = n;
        Map<String, Object> state = x.session.callState;
        return x.atomic(() -> {
            List<Object> results = new ArrayList<>();
            for (int s = 0; s < fn; s++) {
                byte[] key = a[fstreams + 1 + s];
                byte[] idArg = a[fstreams + 1 + fn + s];
                Ctx.Meta km = group ? x.lockExisting(key, Ctx.T_STREAM) : (x.get(key));
                if (km != null && km.type() != Ctx.T_STREAM) {
                    throw RedisError.wrongType();
                }
                if (group) {
                    if (km == null) {
                        throw new RedisError("NOGROUP No such key '" + Num.str(key) + "' or consumer group '" + Num.str(fg)
                                + "' in XREADGROUP with GROUP option");
                    }
                    Object[] g = x.one("SELECT last_ms, last_seq, coalesce(entries_read, -1) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?",
                            rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getLong(3)}, x.db, key, fg);
                    if (g == null) {
                        throw new RedisError("NOGROUP No such key '" + Num.str(key) + "' or consumer group '" + Num.str(fg)
                                + "' in XREADGROUP with GROUP option");
                    }
                    ensureConsumer(x, key, fg, fc);
                    List<Entry> es;
                    if (idArg.length == 1 && idArg[0] == '>') {
                        Id last = (Id) g[0];
                        es = readEntries(x, key, "AND (ms, seq) > (?, ?)", "ASC", fcount, last.dbMs(), last.dbSeq());
                        if (!es.isEmpty()) {
                            Id newLast = es.get(es.size() - 1).id();
                            x.update("UPDATE warp_redis_stream_groups SET last_ms = ?, last_seq = ?, entries_read = coalesce(entries_read, 0) + ? "
                                    + "WHERE db = ? AND k = ? AND g = ?", newLast.dbMs(), newLast.dbSeq(), (long) es.size(), x.db, key, fg);
                            if (!fnoack) {
                                long[] ms = new long[es.size()];
                                long[] sq = new long[es.size()];
                                for (int e = 0; e < ms.length; e++) {
                                    ms[e] = es.get(e).id().dbMs();
                                    sq[e] = es.get(e).id().dbSeq();
                                }
                                x.update("INSERT INTO warp_redis_stream_pel (db, k, g, ms, seq, c, delivered, cnt) SELECT ?, ?, ?, u.ms, u.sq, ?, ?, 1 "
                                        + "FROM unnest(?::int8[], ?::int8[]) AS u(ms, sq) ON CONFLICT (db, k, g, ms, seq) DO UPDATE SET c = EXCLUDED.c, "
                                        + "delivered = EXCLUDED.delivered, cnt = warp_redis_stream_pel.cnt + 1", x.db, key, fg, fc, x.now, ms, sq);
                            }
                            x.update("UPDATE warp_redis_stream_consumers SET active = ?, seen = ? WHERE db = ? AND k = ? AND g = ? AND c = ?",
                                    x.now, x.now, x.db, key, fg, fc);
                        }
                        if (es.isEmpty()) {
                            continue;
                        }
                        List<Object> ents = new ArrayList<>();
                        for (Entry e : es) {
                            ents.add(entryReply(e));
                        }
                        results.add(new ArrayList<Object>(List.of(key, ents)));
                        continue;
                    }
                    // history: this consumer's own pending entries after the given id
                    Id from = parseId(idArg, 0);
                    List<Object[]> pend = x.list("SELECT p.ms, p.seq, e.f FROM warp_redis_stream_pel p LEFT JOIN warp_redis_stream_entries e "
                            + "ON e.db = p.db AND e.k = p.k AND e.ms = p.ms AND e.seq = p.seq WHERE p.db = ? AND p.k = ? AND p.g = ? AND p.c = ? "
                            + "AND (p.ms, p.seq) > (?, ?) ORDER BY p.ms, p.seq LIMIT ?",
                            rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getBytes(3)}, x.db, key, fg, fc, from.dbMs(), from.dbSeq(), fcount);
                    List<Object> ents = new ArrayList<>();
                    for (Object[] p : pend) {
                        Id id = (Id) p[0];
                        ents.add(new ArrayList<Object>(Arrays.asList(id.fmt().getBytes(StandardCharsets.US_ASCII), p[1] == null ? null : unpack((byte[]) p[1]))));
                    }
                    // a group read with an explicit id always answers (possibly with an empty list)
                    results.add(new ArrayList<Object>(List.of(key, ents)));
                    continue;
                }
                // XREAD
                if (km == null) {
                    if (idArg.length == 1 && idArg[0] == '$') {
                        state.putIfAbsent("xread$" + s, Id.ZERO);
                    }
                    continue;
                }
                Id from;
                if (idArg.length == 1 && idArg[0] == '$') {
                    String sk = "xread$" + s;
                    Object saved = state.get(sk);
                    if (saved == null) {
                        saved = Meta.parse(x.one("SELECT sv FROM warp_redis_keys WHERE db = ? AND k = ?", rs -> rs.getBytes(1), x.db, key)).last;
                        state.put(sk, saved);
                    }
                    from = (Id) saved;
                } else if (idArg.length == 1 && idArg[0] == '+') {
                    // last entry only
                    List<Entry> last = readEntries(x, key, "", "DESC", 1);
                    if (last.isEmpty()) {
                        continue;
                    }
                    results.add(new ArrayList<Object>(List.of(key, new ArrayList<Object>(List.of(entryReply(last.get(0)))))));
                    continue;
                } else {
                    from = parseId(idArg, 0);
                }
                List<Entry> es = readEntries(x, key, "AND (ms, seq) > (?, ?)", "ASC", fcount, from.dbMs(), from.dbSeq());
                if (es.isEmpty()) {
                    continue;
                }
                List<Object> ents = new ArrayList<>();
                for (Entry e : es) {
                    ents.add(entryReply(e));
                }
                results.add(new ArrayList<Object>(List.of(key, ents)));
            }
            if (results.isEmpty()) {
                return Cmd.NOT_READY;
            }
            if (x.resp3) {
                List<Object> flat = new ArrayList<>();
                for (Object r : results) {
                    List<?> pair = (List<?>) r;
                    flat.add(pair.get(0));
                    flat.add(pair.get(1));
                }
                return new Resp.RMap(flat);
            }
            return results;
        });
    }

    private static void ensureConsumer(Ctx x, byte[] key, byte[] g, byte[] c) throws Exception {
        x.update("INSERT INTO warp_redis_stream_consumers (db, k, g, c, seen, active) VALUES (?, ?, ?, ?, ?, -1) ON CONFLICT DO NOTHING",
                x.db, key, g, c, x.now);
        x.update("UPDATE warp_redis_stream_consumers SET seen = ? WHERE db = ? AND k = ? AND g = ? AND c = ?", x.now, x.db, key, g, c);
    }

    // ------------------------------------------------------------------------------------------
    // XGROUP
    // ------------------------------------------------------------------------------------------

    private static RedisError noGroup(byte[] key, byte[] g) {
        return new RedisError("NOGROUP No such consumer group '" + Num.str(g) + "' for key name '" + Num.str(key) + "'");
    }

    private static RedisError subSyntax(byte[] sub, String cmd) {
        return new RedisError("ERR unknown subcommand or wrong number of arguments for '" + Num.str(sub) + "'. Try " + cmd + " HELP.");
    }

    private static Object xgroup(Ctx x, byte[][] a) throws Exception {
        String sub = Num.upper(a[1]);
        switch (sub) {
            case "HELP":
                return Resp.help("XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "CREATE <key> <groupname> <id|$> [option]",
                        "CREATECONSUMER <key> <groupname> <consumer>", "DELCONSUMER <key> <groupname> <consumer>", "DESTROY <key> <groupname>",
                        "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]", "HELP");
            case "CREATE": {
                if (a.length < 5) {
                    throw RedisError.arity("xgroup|create");
                }
                boolean mk = false;
                Long er = null;
                for (int i = 5; i < a.length; i++) {
                    if (Num.eq(a[i], "MKSTREAM")) {
                        mk = true;
                    } else if (Num.eq(a[i], "ENTRIESREAD") && i + 1 < a.length) {
                        er = Num.parseLong(a[++i]);
                        if (er == null) {
                            throw RedisError.notInt();
                        }
                        if (er < 0 && er != -1) {
                            throw RedisError.err("value for ENTRIESREAD must be positive or -1");
                        }
                    } else {
                        throw subSyntax(a[1], "XGROUP");
                    }
                }
                final boolean fmk = mk;
                final Long fer = er;
                return x.atomic(() -> {
                    Ctx.Meta km = fmk ? x.lockOrCreate(a[2], Ctx.T_STREAM) : x.lockExisting(a[2], Ctx.T_STREAM);
                    if (km == null) {
                        throw RedisError.err("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use "
                                + "the MKSTREAM option to create an empty stream automatically.");
                    }
                    Id id = a[4].length == 1 && a[4][0] == '$' ? Meta.parse(km.sv()).last : parseId(a[4], 0);
                    int n = x.update("INSERT INTO warp_redis_stream_groups (db, k, g, last_ms, last_seq, entries_read) VALUES (?, ?, ?, ?, ?, ?) "
                            + "ON CONFLICT DO NOTHING", x.db, a[2], a[3], id.dbMs(), id.dbSeq(),
                            fer == null || fer < 0 ? Ctx.NULL_BIGINT : (Object) fer);
                    if (n == 0) {
                        throw new RedisError("BUSYGROUP Consumer Group name already exists");
                    }
                    return Resp.OK;
                });
            }
            case "SETID": {
                if (a.length < 5) {
                    throw RedisError.arity("xgroup|setid");
                }
                Long er = null;
                for (int i = 5; i < a.length; i++) {
                    if (Num.eq(a[i], "ENTRIESREAD") && i + 1 < a.length) {
                        er = Num.parseLong(a[++i]);
                        if (er == null) {
                            throw RedisError.notInt();
                        }
                    } else {
                        throw subSyntax(a[1], "XGROUP");
                    }
                }
                final Long fer = er;
                return x.atomic(() -> {
                    Ctx.Meta km = x.lockExisting(a[2], Ctx.T_STREAM);
                    if (km == null) {
                        throw RedisError.err("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use "
                                + "the MKSTREAM option to create an empty stream automatically.");
                    }
                    Id id = a[4].length == 1 && a[4][0] == '$' ? Meta.parse(km.sv()).last : parseId(a[4], 0);
                    int n = x.update("UPDATE warp_redis_stream_groups SET last_ms = ?, last_seq = ?, entries_read = ? WHERE db = ? AND k = ? AND g = ?",
                            id.dbMs(), id.dbSeq(), fer == null || fer < 0 ? Ctx.NULL_BIGINT : (Object) fer, x.db, a[2], a[3]);
                    if (n == 0) {
                        throw noGroup(a[2], a[3]);
                    }
                    return Resp.OK;
                });
            }
            case "DESTROY": {
                if (a.length != 4) {
                    throw RedisError.arity("xgroup|destroy");
                }
                return x.atomic(() -> {
                    if (x.lockExisting(a[2], Ctx.T_STREAM) == null) {
                        throw RedisError.err("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use "
                                + "the MKSTREAM option to create an empty stream automatically.");
                    }
                    return (long) x.update("DELETE FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[2], a[3]);
                });
            }
            case "CREATECONSUMER": {
                if (a.length != 5) {
                    throw RedisError.arity("xgroup|createconsumer");
                }
                return x.atomic(() -> {
                    if (x.lockExisting(a[2], Ctx.T_STREAM) == null) {
                        throw RedisError.err("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use "
                                + "the MKSTREAM option to create an empty stream automatically.");
                    }
                    if (x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[2], a[3]) == 0) {
                        throw noGroup(a[2], a[3]);
                    }
                    return (long) x.update("INSERT INTO warp_redis_stream_consumers (db, k, g, c, seen, active) VALUES (?, ?, ?, ?, ?, -1) ON CONFLICT DO NOTHING",
                            x.db, a[2], a[3], a[4], x.now);
                });
            }
            case "DELCONSUMER": {
                if (a.length != 5) {
                    throw RedisError.arity("xgroup|delconsumer");
                }
                return x.atomic(() -> {
                    if (x.lockExisting(a[2], Ctx.T_STREAM) == null) {
                        throw RedisError.err("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use "
                                + "the MKSTREAM option to create an empty stream automatically.");
                    }
                    if (x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[2], a[3]) == 0) {
                        throw noGroup(a[2], a[3]);
                    }
                    long pending = x.scalar("SELECT count(*) FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND c = ?", x.db, a[2], a[3], a[4]);
                    x.update("DELETE FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND c = ?", x.db, a[2], a[3], a[4]);
                    x.update("DELETE FROM warp_redis_stream_consumers WHERE db = ? AND k = ? AND g = ? AND c = ?", x.db, a[2], a[3], a[4]);
                    return pending;
                });
            }
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try XGROUP HELP.");
        }
    }

    private static Object xack(Ctx x, byte[][] a) throws Exception {
        List<Id> ids = new ArrayList<>();
        for (int i = 3; i < a.length; i++) {
            ids.add(parseId(a[i], 0));
        }
        return x.atomic(() -> {
            if (x.lockExisting(a[1], Ctx.T_STREAM) == null) {
                return 0L;
            }
            long n = 0;
            for (Id id : ids) {
                n += x.update("DELETE FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?", x.db, a[1], a[2], id.dbMs(), id.dbSeq());
            }
            return n;
        });
    }

    private static Object xpending(Ctx x, byte[][] a) throws Exception {
        long minIdle = 0;
        int i = 3;
        boolean extended = a.length > 3;
        if (extended && Num.eq(a[3], "IDLE")) {
            if (a.length < 5) {
                throw RedisError.syntax();
            }
            Long v = Num.parseLong(a[4]);
            if (v == null) {
                throw RedisError.notInt();
            }
            minIdle = v;
            i = 5;
        }
        Id lo = null;
        Id hi = null;
        long count = 0;
        byte[] consumer = null;
        if (extended) {
            if (a.length - i < 3 || a.length - i > 4) {
                throw RedisError.syntax();
            }
            lo = rangeStart(a[i]);
            hi = rangeEnd(a[i + 1]);
            Long c = Num.parseLong(a[i + 2]);
            if (c == null) {
                throw RedisError.notInt();
            }
            count = Math.max(0, c);
            if (a.length - i == 4) {
                consumer = a[i + 3];
            }
        }
        Ctx.Meta km = x.get(a[1]);
        if (km != null && km.type() != Ctx.T_STREAM) {
            throw RedisError.wrongType();
        }
        if (km == null || x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[1], a[2]) == 0) {
            throw new RedisError("NOGROUP No such key '" + Num.str(a[1]) + "' or consumer group '" + Num.str(a[2]) + "'");
        }
        if (!extended) {
            long total = x.scalar("SELECT count(*) FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ?", x.db, a[1], a[2]);
            if (total == 0) {
                return new ArrayList<Object>(Arrays.asList(0L, null, null, null));
            }
            Object[] mm = x.one("SELECT (SELECT ms FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? ORDER BY ms, seq LIMIT 1), "
                    + "(SELECT seq FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? ORDER BY ms, seq LIMIT 1), "
                    + "(SELECT ms FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? ORDER BY ms DESC, seq DESC LIMIT 1), "
                    + "(SELECT seq FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? ORDER BY ms DESC, seq DESC LIMIT 1)",
                    rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)).fmt(), Id.fromDb(rs.getLong(3), rs.getLong(4)).fmt()},
                    x.db, a[1], a[2], x.db, a[1], a[2], x.db, a[1], a[2], x.db, a[1], a[2]);
            List<Object> per = new ArrayList<>();
            for (Object[] c : x.list("SELECT c, count(*) FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? GROUP BY c ORDER BY c",
                    rs -> new Object[] {rs.getBytes(1), rs.getLong(2)}, x.db, a[1], a[2])) {
                per.add(new ArrayList<Object>(List.of(c[0], Long.toString((Long) c[1]).getBytes(StandardCharsets.US_ASCII))));
            }
            return new ArrayList<Object>(List.of(total, ((String) mm[0]).getBytes(StandardCharsets.US_ASCII),
                    ((String) mm[1]).getBytes(StandardCharsets.US_ASCII), per));
        }
        List<Object> args = new ArrayList<>(List.of(x.db, a[1], a[2], lo.dbMs(), lo.dbSeq(), hi.dbMs(), hi.dbSeq(), x.now, minIdle));
        String cond = "";
        if (consumer != null) {
            cond = " AND c = ?";
            args.add(consumer);
        }
        args.add(count);
        List<Object[]> rows = x.list("SELECT ms, seq, c, delivered, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? "
                + "AND (ms, seq) >= (?, ?) AND (ms, seq) <= (?, ?) AND (? - delivered) >= ? " + cond + " ORDER BY ms, seq LIMIT ?",
                rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getBytes(3), rs.getLong(4), rs.getLong(5)}, args.toArray());
        List<Object> out = new ArrayList<>();
        for (Object[] r : rows) {
            out.add(new ArrayList<Object>(List.of(((Id) r[0]).fmt().getBytes(StandardCharsets.US_ASCII), r[1], Math.max(0, x.now - (Long) r[2]), r[3])));
        }
        return out;
    }

    private static Object xclaim(Ctx x, byte[][] a) throws Exception {
        Long minIdle = Num.parseLong(a[4]);
        if (minIdle == null) {
            throw RedisError.err("Invalid min-idle-time argument for XCLAIM");
        }
        int i = 5;
        List<Id> ids = new ArrayList<>();
        while (i < a.length) {
            try {
                ids.add(parseId(a[i], 0));
                i++;
            } catch (RedisError e) {
                break;
            }
        }
        if (ids.isEmpty()) {
            throw badId();
        }
        Long idle = null;
        Long time = null;
        Long retry = null;
        boolean force = false;
        boolean justId = false;
        for (; i < a.length; i++) {
            byte[] o = a[i];
            if (Num.eq(o, "FORCE")) {
                force = true;
            } else if (Num.eq(o, "JUSTID")) {
                justId = true;
            } else if (Num.eq(o, "IDLE") && i + 1 < a.length) {
                idle = Num.parseLong(a[++i]);
                if (idle == null) {
                    throw RedisError.err("Invalid IDLE option argument for XCLAIM");
                }
            } else if (Num.eq(o, "TIME") && i + 1 < a.length) {
                time = Num.parseLong(a[++i]);
                if (time == null) {
                    throw RedisError.err("Invalid TIME option argument for XCLAIM");
                }
            } else if (Num.eq(o, "RETRYCOUNT") && i + 1 < a.length) {
                retry = Num.parseLong(a[++i]);
                if (retry == null) {
                    throw RedisError.err("Invalid RETRYCOUNT option argument for XCLAIM");
                }
            } else if (Num.eq(o, "LASTID") && i + 1 < a.length) {
                parseId(a[++i], 0);
            } else {
                throw RedisError.err("Unrecognized XCLAIM option '" + Num.str(o) + "'");
            }
        }
        final Long fidle = idle;
        final Long ftime = time;
        final Long fretry = retry;
        final boolean fforce = force;
        final boolean fjust = justId;
        return x.atomic(() -> {
            Ctx.Meta km = x.lockExisting(a[1], Ctx.T_STREAM);
            if (km == null || x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[1], a[2]) == 0) {
                throw new RedisError("NOGROUP No such key '" + Num.str(a[1]) + "' or consumer group '" + Num.str(a[2]) + "'");
            }
            ensureConsumer(x, a[1], a[2], a[3]);
            List<Object> out = new ArrayList<>();
            boolean claimedAny = false;
            for (Id id : ids) {
                Object[] pel = x.one("SELECT delivered, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ? FOR UPDATE",
                        rs -> new Object[] {rs.getLong(1), rs.getLong(2)}, x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                Entry entry = firstEntry(x, a[1], id);
                if (entry == null) {
                    if (pel != null) {
                        x.update("DELETE FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?", x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                    }
                    continue;
                }
                if (pel == null) {
                    if (!fforce) {
                        continue;
                    }
                    x.update("INSERT INTO warp_redis_stream_pel (db, k, g, ms, seq, c, delivered, cnt) VALUES (?, ?, ?, ?, ?, ?, ?, 0)", x.db, a[1], a[2],
                            id.dbMs(), id.dbSeq(), a[3], x.now);
                    pel = new Object[] {x.now, 0L};
                } else if (x.now - (Long) pel[0] < minIdle) {
                    continue;
                }
                long delivered = ftime != null ? ftime : fidle != null ? x.now - fidle : x.now;
                long cnt = fretry != null ? fretry : (fjust ? (Long) pel[1] : (Long) pel[1] + 1);
                x.update("UPDATE warp_redis_stream_pel SET c = ?, delivered = ?, cnt = ? WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?",
                        a[3], delivered, cnt, x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                claimedAny = true;
                out.add(fjust ? id.fmt().getBytes(StandardCharsets.US_ASCII) : entryReply(entry));
            }
            if (claimedAny) {
                x.update("UPDATE warp_redis_stream_consumers SET active = ? WHERE db = ? AND k = ? AND g = ? AND c = ?", x.now, x.db, a[1], a[2], a[3]);
            }
            return out;
        });
    }

    private static Entry firstEntry(Ctx x, byte[] k, Id id) throws Exception {
        List<Entry> es = readEntries(x, k, "AND ms = ? AND seq = ?", "ASC", 1, id.dbMs(), id.dbSeq());
        return es.isEmpty() ? null : es.get(0);
    }

    private static Object xautoclaim(Ctx x, byte[][] a) throws Exception {
        Long minIdle = Num.parseLong(a[4]);
        if (minIdle == null) {
            throw RedisError.err("Invalid min-idle-time argument for XAUTOCLAIM");
        }
        Id start = rangeStart(a[5]);
        long count = 100;
        boolean justId = false;
        for (int i = 6; i < a.length; i++) {
            if (Num.eq(a[i], "COUNT") && i + 1 < a.length) {
                Long c = Num.parseLong(a[++i]);
                if (c == null || c < 1 || c > Long.MAX_VALUE / 10) {
                    throw RedisError.err("COUNT must be > 0");
                }
                count = c;
            } else if (Num.eq(a[i], "JUSTID")) {
                justId = true;
            } else {
                throw RedisError.syntax();
            }
        }
        final long fcount = count;
        final boolean fjust = justId;
        return x.atomic(() -> {
            Ctx.Meta km = x.lockExisting(a[1], Ctx.T_STREAM);
            if (km == null || x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[1], a[2]) == 0) {
                throw new RedisError("NOGROUP No such key '" + Num.str(a[1]) + "' or consumer group '" + Num.str(a[2]) + "'");
            }
            ensureConsumer(x, a[1], a[2], a[3]);
            List<Object[]> pel = x.list("SELECT ms, seq, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND (ms, seq) >= (?, ?) "
                    + "ORDER BY ms, seq", rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getLong(3)}, x.db, a[1], a[2], start.dbMs(), start.dbSeq());
            List<Object> claimed = new ArrayList<>();
            List<Object> deleted = new ArrayList<>();
            Id next = Id.ZERO;
            long attempts = 0;
            boolean any = false;
            int idx = 0;
            for (; idx < pel.size() && attempts < fcount; idx++) {
                Id id = (Id) pel.get(idx)[0];
                Object[] cur = x.one("SELECT delivered FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?",
                        rs -> new Object[] {rs.getLong(1)}, x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                attempts++;
                Entry e = firstEntry(x, a[1], id);
                if (e == null) {
                    x.update("DELETE FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?", x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                    deleted.add(id.fmt().getBytes(StandardCharsets.US_ASCII));
                    continue;
                }
                if (x.now - (Long) cur[0] < minIdle) {
                    continue;
                }
                x.update("UPDATE warp_redis_stream_pel SET c = ?, delivered = ?, cnt = cnt + ? WHERE db = ? AND k = ? AND g = ? AND ms = ? AND seq = ?",
                        a[3], x.now, fjust ? 0 : 1, x.db, a[1], a[2], id.dbMs(), id.dbSeq());
                any = true;
                claimed.add(fjust ? id.fmt().getBytes(StandardCharsets.US_ASCII) : entryReply(e));
            }
            if (idx < pel.size()) {
                next = (Id) pel.get(idx)[0];
            }
            if (any) {
                x.update("UPDATE warp_redis_stream_consumers SET active = ? WHERE db = ? AND k = ? AND g = ? AND c = ?", x.now, x.db, a[1], a[2], a[3]);
            }
            return new ArrayList<Object>(List.of(next.fmt().getBytes(StandardCharsets.US_ASCII), claimed, deleted));
        });
    }

    // ------------------------------------------------------------------------------------------
    // XINFO
    // ------------------------------------------------------------------------------------------

    private static Object xinfo(Ctx x, byte[][] a) throws Exception {
        String sub = Num.upper(a[1]);
        if (sub.equals("HELP")) {
            return Resp.help("XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "CONSUMERS <key> <groupname>",
                    "GROUPS <key>", "STREAM <key> [FULL [COUNT <count>]]", "HELP");
        }
        if (a.length < 3) {
            throw RedisError.arity("xinfo|" + sub.toLowerCase());
        }
        Ctx.Meta km = x.get(a[2]);
        if (km == null) {
            throw new RedisError(RedisError.NO_KEY);
        }
        if (km.type() != Ctx.T_STREAM) {
            throw RedisError.wrongType();
        }
        Meta m = loadMeta(x, a[2]);
        switch (sub) {
            case "STREAM": {
                boolean full = a.length > 3 && Num.eq(a[3], "FULL");
                long count = 10;
                if (full && a.length > 4) {
                    if (a.length != 6 || !Num.eq(a[4], "COUNT")) {
                        throw subSyntax(a[1], "XINFO");
                    }
                    Long c = Num.parseLong(a[5]);
                    if (c == null) {
                        throw RedisError.notInt();
                    }
                    count = c <= 0 ? Long.MAX_VALUE : c;
                } else if (!full && a.length > 3) {
                    throw subSyntax(a[1], "XINFO");
                }
                List<Entry> first = readEntries(x, a[2], "", "ASC", full ? count : 1);
                List<Entry> last = full ? List.of() : readEntries(x, a[2], "", "DESC", 1);
                long groups = x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ?", x.db, a[2]);
                List<Object> f = new ArrayList<>();
                f.add("length");
                f.add(km.n());
                f.add("radix-tree-keys");
                f.add(km.n() == 0 ? 0L : 1L);
                f.add("radix-tree-nodes");
                f.add(km.n() == 0 ? 1L : 2L);
                f.add("last-generated-id");
                f.add(m.last.fmt().getBytes(StandardCharsets.US_ASCII));
                f.add("max-deleted-entry-id");
                f.add(m.maxDeleted.fmt().getBytes(StandardCharsets.US_ASCII));
                f.add("entries-added");
                f.add(m.added);
                f.add("recorded-first-entry-id");
                f.add((first.isEmpty() ? Id.ZERO : first.get(0).id()).fmt().getBytes(StandardCharsets.US_ASCII));
                if (!full) {
                    f.add("groups");
                    f.add(groups);
                    f.add("first-entry");
                    f.add(first.isEmpty() ? null : entryReply(first.get(0)));
                    f.add("last-entry");
                    f.add(last.isEmpty() ? null : entryReply(last.get(0)));
                } else {
                    List<Object> es = new ArrayList<>();
                    for (Entry e : first) {
                        es.add(entryReply(e));
                    }
                    f.add("entries");
                    f.add(es);
                    List<Object> gl = new ArrayList<>();
                    for (Object[] g : groupRows(x, a[2], m)) {
                        List<Object> gf = new ArrayList<>();
                        gf.add("name");
                        gf.add(g[0]);
                        gf.add("last-delivered-id");
                        gf.add(((Id) g[1]).fmt().getBytes(StandardCharsets.US_ASCII));
                        gf.add("entries-read");
                        gf.add(g[2]);
                        gf.add("lag");
                        gf.add(g[3]);
                        gf.add("pel-count");
                        gf.add(g[5]);
                        List<Object> pl = new ArrayList<>();
                        for (Object[] p : x.list("SELECT ms, seq, c, delivered, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? ORDER BY ms, seq LIMIT ?",
                                rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getBytes(3), rs.getLong(4), rs.getLong(5)}, x.db, a[2], g[0], count)) {
                            pl.add(new ArrayList<Object>(List.of(((Id) p[0]).fmt().getBytes(StandardCharsets.US_ASCII), p[1], p[2], p[3])));
                        }
                        gf.add("pending");
                        gf.add(pl);
                        List<Object> cl = new ArrayList<>();
                        for (Object[] c : consumerRows(x, a[2], (byte[]) g[0])) {
                            List<Object> cf = new ArrayList<>();
                            cf.add("name");
                            cf.add(c[0]);
                            cf.add("seen-time");
                            cf.add(c[1]);
                            cf.add("active-time");
                            cf.add(c[2]);
                            cf.add("pel-count");
                            cf.add(c[3]);
                            cf.add("pending");
                            List<Object> cpl = new ArrayList<>();
                            for (Object[] p : x.list("SELECT ms, seq, delivered, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ? AND c = ? ORDER BY ms, seq LIMIT ?",
                                    rs -> new Object[] {Id.fromDb(rs.getLong(1), rs.getLong(2)), rs.getLong(3), rs.getLong(4)}, x.db, a[2], g[0], c[0], count)) {
                                cpl.add(new ArrayList<Object>(List.of(((Id) p[0]).fmt().getBytes(StandardCharsets.US_ASCII), p[1], p[2])));
                            }
                            cf.add(cpl);
                            cl.add(new Resp.RMap(cf));
                        }
                        gf.add("consumers");
                        gf.add(cl);
                        gl.add(new Resp.RMap(gf));
                    }
                    f.add("groups");
                    f.add(gl);
                }
                return new Resp.RMap(f);
            }
            case "GROUPS": {
                List<Object> out = new ArrayList<>();
                for (Object[] g : groupRows(x, a[2], m)) {
                    List<Object> f = new ArrayList<>();
                    f.add("name");
                    f.add(g[0]);
                    f.add("consumers");
                    f.add(g[4]);
                    f.add("pending");
                    f.add(g[5]);
                    f.add("last-delivered-id");
                    f.add(((Id) g[1]).fmt().getBytes(StandardCharsets.US_ASCII));
                    f.add("entries-read");
                    f.add(g[2]);
                    f.add("lag");
                    f.add(g[3]);
                    out.add(new Resp.RMap(f));
                }
                return out;
            }
            case "CONSUMERS": {
                if (a.length != 4) {
                    throw RedisError.arity("xinfo|consumers");
                }
                if (x.scalar("SELECT count(*) FROM warp_redis_stream_groups WHERE db = ? AND k = ? AND g = ?", x.db, a[2], a[3]) == 0) {
                    throw noGroup(a[2], a[3]);
                }
                List<Object> out = new ArrayList<>();
                for (Object[] c : consumerRows(x, a[2], a[3])) {
                    List<Object> f = new ArrayList<>();
                    f.add("name");
                    f.add(c[0]);
                    f.add("pending");
                    f.add(c[3]);
                    f.add("idle");
                    f.add(Math.max(0, x.now - (Long) c[1]));
                    f.add("inactive");
                    f.add((Long) c[2] < 0 ? -1L : Math.max(0, x.now - (Long) c[2]));
                    out.add(new Resp.RMap(f));
                }
                return out;
            }
            default:
                throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try XINFO HELP.");
        }
    }

    /** {name, lastId, entriesRead|null, lag|null, consumers, pending}. */
    private static List<Object[]> groupRows(Ctx x, byte[] key, Meta m) throws Exception {
        List<Object[]> raw = x.list("SELECT g, last_ms, last_seq, entries_read FROM warp_redis_stream_groups WHERE db = ? AND k = ? ORDER BY g",
                rs -> new Object[] {rs.getBytes(1), Id.fromDb(rs.getLong(2), rs.getLong(3)), rs.getObject(4)}, x.db, key);
        List<Object[]> out = new ArrayList<>();
        for (Object[] g : raw) {
            Id last = (Id) g[1];
            long lag = x.scalar("SELECT count(*) FROM warp_redis_stream_entries WHERE db = ? AND k = ? AND (ms, seq) > (?, ?)", x.db, key, last.dbMs(), last.dbSeq());
            long consumers = x.scalar("SELECT count(*) FROM warp_redis_stream_consumers WHERE db = ? AND k = ? AND g = ?", x.db, key, g[0]);
            long pending = x.scalar("SELECT count(*) FROM warp_redis_stream_pel WHERE db = ? AND k = ? AND g = ?", x.db, key, g[0]);
            out.add(new Object[] {g[0], last, g[2], lag, consumers, pending});
        }
        return out;
    }

    /** {name, seen, active, pending}. */
    private static List<Object[]> consumerRows(Ctx x, byte[] key, byte[] group) throws Exception {
        return x.list("SELECT c.c, c.seen, c.active, (SELECT count(*) FROM warp_redis_stream_pel p WHERE p.db = c.db AND p.k = c.k AND p.g = c.g AND p.c = c.c) "
                + "FROM warp_redis_stream_consumers c WHERE c.db = ? AND c.k = ? AND c.g = ? ORDER BY c.c",
                rs -> new Object[] {rs.getBytes(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)}, x.db, key, group);
    }
}
