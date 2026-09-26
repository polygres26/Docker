package com.sayonora.wire.rediswire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/** Generic key-space commands: DEL, EXISTS, TYPE, RENAME, COPY, MOVE, expiry, KEYS/SCAN, RANDOMKEY, OBJECT, DUMP/RESTORE, SORT. */
final class KeyCmds {

    private static final long SHARD_SHIFT = 46;

    private KeyCmds() {
    }

    static void register(Cmd.Registry r) {
        r.add("del", -2, "write", 1, -1, 1, "@keyspace @write @slow", KeyCmds::del);
        r.add("unlink", -2, "write fast", 1, -1, 1, "@keyspace @write @fast", KeyCmds::del);
        r.add("exists", -2, "readonly fast", 1, -1, 1, "@keyspace @read @fast", KeyCmds::exists);
        r.add("touch", -2, "readonly fast", 1, -1, 1, "@keyspace @read @fast", KeyCmds::touch);
        r.add("type", 2, "readonly fast", 1, 1, 1, "@keyspace @read @fast", KeyCmds::type);
        r.add("rename", 3, "write", 1, 2, 1, "@keyspace @write @slow", (x, a) -> rename(x, a, false));
        r.add("renamenx", 3, "write fast", 1, 2, 1, "@keyspace @write @fast", (x, a) -> rename(x, a, true));
        r.add("copy", -3, "write denyoom", 1, 2, 1, "@keyspace @write @slow", KeyCmds::copy);
        r.add("move", 3, "write fast", 1, 1, 1, "@keyspace @write @fast", KeyCmds::move);
        r.add("expire", -3, "write fast", 1, 1, 1, "@keyspace @write @fast", (x, a) -> expire(x, a, "expire", false, false));
        r.add("pexpire", -3, "write fast", 1, 1, 1, "@keyspace @write @fast", (x, a) -> expire(x, a, "pexpire", true, false));
        r.add("expireat", -3, "write fast", 1, 1, 1, "@keyspace @write @fast", (x, a) -> expire(x, a, "expireat", false, true));
        r.add("pexpireat", -3, "write fast", 1, 1, 1, "@keyspace @write @fast", (x, a) -> expire(x, a, "pexpireat", true, true));
        r.add("ttl", 2, "readonly fast", 1, 1, 1, "@keyspace @read @fast", (x, a) -> ttl(x, a[1], false, false));
        r.add("pttl", 2, "readonly fast", 1, 1, 1, "@keyspace @read @fast", (x, a) -> ttl(x, a[1], true, false));
        r.add("expiretime", 2, "readonly fast", 1, 1, 1, "@keyspace @read @fast", (x, a) -> ttl(x, a[1], false, true));
        r.add("pexpiretime", 2, "readonly fast", 1, 1, 1, "@keyspace @read @fast", (x, a) -> ttl(x, a[1], true, true));
        r.add("persist", 2, "write fast", 1, 1, 1, "@keyspace @write @fast", KeyCmds::persist);
        r.add("keys", 2, "readonly", 0, 0, 0, "@keyspace @read @dangerous @slow", KeyCmds::keys);
        r.add("scan", -2, "readonly", 0, 0, 0, "@keyspace @read @slow", KeyCmds::scan);
        r.add("randomkey", 1, "readonly", 0, 0, 0, "@keyspace @read @slow", KeyCmds::randomKey);
        r.add("dbsize", 1, "readonly fast", 0, 0, 0, "@keyspace @read @fast", KeyCmds::dbsize);
        r.add("flushdb", -1, "write", 0, 0, 0, "@keyspace @write @slow @dangerous", (x, a) -> flush(x, a, false));
        r.add("flushall", -1, "write", 0, 0, 0, "@keyspace @write @slow @dangerous", (x, a) -> flush(x, a, true));
        r.add("swapdb", 3, "write fast", 0, 0, 0, "@keyspace @write @fast @dangerous", KeyCmds::swapdb);
        r.add("object", -2, "readonly", 2, 2, 1, "@keyspace @read @slow", KeyCmds::object);
        r.add("dump", 2, "readonly", 1, 1, 1, "@keyspace @read @slow", KeyCmds::dump);
        r.add("restore", -4, "write denyoom", 1, 1, 1, "@keyspace @write @slow @dangerous", KeyCmds::restore);
        r.add("sort", -2, "write denyoom movablekeys", 1, 1, 1, "@write @set @sortedset @list @slow @dangerous",
                (x, a) -> sort(x, a, false)).keys(a -> sortKeys(a));
        r.add("sort_ro", -2, "readonly", 1, 1, 1, "@read @set @sortedset @list @slow @dangerous", (x, a) -> sort(x, a, true));
    }

    static String typeName(int t) {
        return Ctx.TYPE_NAMES[t];
    }

    private static Object del(Ctx x, byte[][] a) throws Exception {
        byte[][] keys = Arrays.copyOfRange(a, 1, a.length);
        return x.one("WITH d AS (DELETE FROM warp_redis_keys WHERE db = ? AND k = ANY(?) RETURNING exp) "
                + "SELECT count(*) FILTER (WHERE exp IS NULL OR exp > ?) FROM d", rs -> rs.getLong(1), x.db, keys, x.now);
    }

    private static Object exists(Ctx x, byte[][] a) throws Exception {
        byte[][] keys = Arrays.copyOfRange(a, 1, a.length);
        Set<Hub.BK> live = new HashSet<>();
        for (byte[] k : x.list("SELECT k FROM warp_redis_keys WHERE db = ? AND k = ANY(?) AND (exp IS NULL OR exp > ?)",
                rs -> rs.getBytes(1), x.db, keys, x.now)) {
            live.add(new Hub.BK(k));
        }
        long n = 0;
        for (byte[] k : keys) {
            if (live.contains(new Hub.BK(k))) {
                n++;
            }
        }
        return n;
    }

    private static Object touch(Ctx x, byte[][] a) throws Exception {
        return exists(x, a);
    }

    private static Object type(Ctx x, byte[][] a) throws Exception {
        Ctx.Meta m = x.get(a[1]);
        return new Resp.Simple(m == null ? "none" : typeName(m.type()));
    }

    private static Object rename(Ctx x, byte[][] a, boolean nx) throws Exception {
        byte[] src = a[1];
        byte[] dst = a[2];
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(src);
            if (m == null) {
                throw new RedisError(RedisError.NO_KEY);
            }
            if (Arrays.equals(src, dst)) {
                return nx ? (Object) 0L : Resp.OK;
            }
            if (nx) {
                x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND exp <= ?", x.db, dst, x.now);
                if (x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ?", x.db, dst) > 0) {
                    return 0L;
                }
            } else {
                x.delete(dst);
            }
            x.update("UPDATE warp_redis_keys SET k = ?, slot = ? WHERE db = ? AND k = ?", dst, Slot.of(dst), x.db, src);
            if (m.type() == Ctx.T_LIST || m.type() == Ctx.T_ZSET || m.type() == Ctx.T_STREAM) {
                x.wake(dst);
            }
            return nx ? (Object) 1L : Resp.OK;
        });
    }

    private static Object copy(Ctx x, byte[][] a) throws Exception {
        boolean replace = false;
        int destDb = x.db;
        for (int i = 3; i < a.length; i++) {
            if (Num.eq(a[i], "REPLACE")) {
                replace = true;
            } else if (Num.eq(a[i], "DB") && i + 1 < a.length) {
                Long d = Num.parseLong(a[++i]);
                if (d == null) {
                    throw RedisError.notInt();
                }
                if (d < 0 || d >= x.store.options().databases()) {
                    throw RedisError.err("DB index is out of range");
                }
                destDb = d.intValue();
            } else {
                throw RedisError.syntax();
            }
        }
        final boolean frep = replace;
        final int ddb = destDb;
        byte[] src = a[1];
        byte[] dst = a[2];
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(src);
            if (m == null) {
                return 0L;
            }
            if (ddb == x.db && Arrays.equals(src, dst)) {
                throw RedisError.err("source and destination objects are the same");
            }
            x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND exp <= ?", ddb, dst, x.now);
            if (x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ?", ddb, dst) > 0) {
                if (!frep) {
                    return 0L;
                }
                x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ?", ddb, dst);
            }
            x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot, n) SELECT ?, ?, type, exp, sv, ?, n FROM warp_redis_keys "
                    + "WHERE db = ? AND k = ?", ddb, dst, Slot.of(dst), x.db, src);
            copyChildren(x, m.type(), src, dst, ddb);
            if (m.type() == Ctx.T_LIST || m.type() == Ctx.T_ZSET || m.type() == Ctx.T_STREAM) {
                x.wake(dst);
            }
            return 1L;
        });
    }

    private static void copyChildren(Ctx x, int type, byte[] src, byte[] dst, int ddb) throws Exception {
        switch (type) {
            case Ctx.T_HASH -> x.update("INSERT INTO warp_redis_hashes (db, k, f, v) SELECT ?, ?, f, v FROM warp_redis_hashes WHERE db = ? AND k = ?", ddb, dst, x.db, src);
            case Ctx.T_LIST -> x.update("INSERT INTO warp_redis_lists (db, k, pos, v) SELECT ?, ?, pos, v FROM warp_redis_lists WHERE db = ? AND k = ?", ddb, dst, x.db, src);
            case Ctx.T_SET -> x.update("INSERT INTO warp_redis_sets (db, k, m) SELECT ?, ?, m FROM warp_redis_sets WHERE db = ? AND k = ?", ddb, dst, x.db, src);
            case Ctx.T_ZSET -> x.update("INSERT INTO warp_redis_zsets (db, k, m, score) SELECT ?, ?, m, score FROM warp_redis_zsets WHERE db = ? AND k = ?", ddb, dst, x.db, src);
            case Ctx.T_STREAM -> {
                x.update("INSERT INTO warp_redis_stream_entries (db, k, ms, seq, f) SELECT ?, ?, ms, seq, f FROM warp_redis_stream_entries WHERE db = ? AND k = ?", ddb, dst, x.db, src);
                x.update("INSERT INTO warp_redis_stream_groups (db, k, g, last_ms, last_seq, entries_read) SELECT ?, ?, g, last_ms, last_seq, entries_read FROM warp_redis_stream_groups WHERE db = ? AND k = ?", ddb, dst, x.db, src);
                x.update("INSERT INTO warp_redis_stream_consumers (db, k, g, c, seen, active) SELECT ?, ?, g, c, seen, active FROM warp_redis_stream_consumers WHERE db = ? AND k = ?", ddb, dst, x.db, src);
                x.update("INSERT INTO warp_redis_stream_pel (db, k, g, ms, seq, c, delivered, cnt) SELECT ?, ?, g, ms, seq, c, delivered, cnt FROM warp_redis_stream_pel WHERE db = ? AND k = ?", ddb, dst, x.db, src);
            }
            default -> {
            }
        }
    }

    private static Object move(Ctx x, byte[][] a) throws Exception {
        Long d = Num.parseLong(a[2]);
        if (d == null) {
            throw RedisError.notInt();
        }
        if (d < 0 || d >= x.store.options().databases()) {
            throw RedisError.err("DB index is out of range");
        }
        if (d == x.db) {
            throw RedisError.err("source and destination objects are the same");
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(a[1]);
            if (m == null) {
                return 0L;
            }
            x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND exp <= ?", d.intValue(), a[1], x.now);
            if (x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ?", d.intValue(), a[1]) > 0) {
                return 0L;
            }
            x.update("UPDATE warp_redis_keys SET db = ? WHERE db = ? AND k = ?", d.intValue(), x.db, a[1]);
            return 1L;
        });
    }

    private static Object expire(Ctx x, byte[][] a, String cmd, boolean millis, boolean absolute) throws Exception {
        Long t = Num.parseLong(a[2]);
        if (t == null) {
            throw RedisError.notInt();
        }
        boolean nx = false;
        boolean xx = false;
        boolean gt = false;
        boolean lt = false;
        for (int i = 3; i < a.length; i++) {
            if (Num.eq(a[i], "NX")) {
                nx = true;
            } else if (Num.eq(a[i], "XX")) {
                xx = true;
            } else if (Num.eq(a[i], "GT")) {
                gt = true;
            } else if (Num.eq(a[i], "LT")) {
                lt = true;
            } else {
                throw RedisError.err("Unsupported option " + Num.str(a[i]));
            }
        }
        if (nx && (xx || gt || lt)) {
            throw RedisError.err("NX and XX, GT or LT options at the same time are not compatible");
        }
        if (gt && lt) {
            throw RedisError.err("GT and LT options at the same time are not compatible");
        }
        String bad = "ERR invalid expire time in '" + cmd + "' command";
        long abs;
        if (!millis) {
            if (t > Long.MAX_VALUE / 1000 || t < Long.MIN_VALUE / 1000) {
                throw new RedisError(bad);
            }
            abs = t * 1000;
        } else {
            abs = t;
        }
        if (!absolute) {
            long base = x.now;
            abs = abs + base;
            if ((t > 0 && abs < base) || (t < 0 && abs > base)) {
                throw new RedisError(bad);
            }
        }
        final long exp = abs;
        final boolean fnx = nx;
        final boolean fxx = xx;
        final boolean fgt = gt;
        final boolean flt = lt;
        if (!nx && !xx && !gt && !lt && exp > x.now) {
            return x.update("UPDATE warp_redis_keys SET exp = ?, ver = nextval('warp_redis_ver') WHERE db = ? AND k = ? "
                    + "AND (exp IS NULL OR exp > ?)", exp, x.db, a[1], x.now) > 0 ? 1L : 0L;
        }
        return x.atomic(() -> {
            Ctx.Meta m = x.lock(a[1]);
            if (m == null) {
                return 0L;
            }
            Long cur = m.exp();
            if ((fnx && cur != null) || (fxx && cur == null) || (fgt && (cur == null || exp <= cur))
                    || (flt && cur != null && exp >= cur)) {
                return 0L;
            }
            if (exp <= x.now) {
                x.delete(a[1]);
            } else {
                x.update("UPDATE warp_redis_keys SET exp = ? WHERE db = ? AND k = ?", exp, x.db, a[1]);
            }
            return 1L;
        });
    }

    private static Object ttl(Ctx x, byte[] k, boolean ms, boolean abs) throws Exception {
        Ctx.Meta m = x.get(k);
        if (m == null) {
            return -2L;
        }
        if (m.exp() == null) {
            return -1L;
        }
        long v = abs ? m.exp() : Math.max(0, m.exp() - x.now);
        return ms ? v : (v + 500) / 1000;
    }

    private static Object persist(Ctx x, byte[][] a) throws Exception {
        return x.update("UPDATE warp_redis_keys SET exp = NULL, ver = nextval('warp_redis_ver') WHERE db = ? AND k = ? "
                + "AND exp IS NOT NULL AND exp > ?", x.db, a[1], x.now) > 0 ? 1L : 0L;
    }

    // ------------------------------------------------------------------------------------------
    // KEYS / SCAN / RANDOMKEY / DBSIZE / FLUSH
    // ------------------------------------------------------------------------------------------

    private static Object keys(Ctx x, byte[][] a) throws Exception {
        x.requireFanOut();
        byte[] pat = a[1];
        List<Object> out = new ArrayList<>();
        for (int s = 0; s < x.shardCount(); s++) {
            x.useShard(s);
            for (byte[] k : x.list("SELECT k FROM warp_redis_keys WHERE db = ? AND (exp IS NULL OR exp > ?) ORDER BY k",
                    rs -> rs.getBytes(1), x.db, x.now)) {
                if (Glob.matches(pat, k)) {
                    out.add(k);
                }
            }
        }
        return out;
    }

    private static Object scan(Ctx x, byte[][] a) throws Exception {
        x.requireFanOut();
        Long cursor = parseCursor(a[1]);
        byte[] match = null;
        long count = 10;
        String type = null;
        for (int i = 2; i < a.length; i++) {
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
            } else if (Num.eq(a[i], "TYPE") && i + 1 < a.length) {
                type = Num.str(a[++i]).toLowerCase();
            } else {
                throw RedisError.syntax();
            }
        }
        int n = x.shardCount();
        int shard = (int) (cursor >>> SHARD_SHIFT);
        long lastId = cursor & ((1L << SHARD_SHIFT) - 1);
        if (shard >= n) {
            throw RedisError.err("invalid cursor");
        }
        int typeCode = -1;
        if (type != null) {
            typeCode = Arrays.asList(Ctx.TYPE_NAMES).indexOf(type);
            if (typeCode < 0) {
                // unknown type name: Redis returns an empty page
                return List.of("0", new ArrayList<>());
            }
        }
        List<Object> keys = new ArrayList<>();
        x.useShard(shard);
        final int tc = typeCode;
        List<Object[]> rows = x.list("SELECT id, k FROM warp_redis_keys WHERE db = ? AND id > ? AND (exp IS NULL OR exp > ?) "
                + "AND (? < 0 OR type = ?) ORDER BY id LIMIT ?",
                rs -> new Object[] {rs.getLong(1), rs.getBytes(2)}, x.db, lastId, x.now, tc, tc, count);
        long next;
        for (Object[] row : rows) {
            if (match == null || Glob.matches(match, (byte[]) row[1])) {
                keys.add(row[1]);
            }
        }
        if (rows.size() < count) {
            next = shard + 1 >= n ? 0 : ((long) (shard + 1) << SHARD_SHIFT);
        } else {
            next = ((long) shard << SHARD_SHIFT) | (Long) rows.get(rows.size() - 1)[0];
        }
        List<Object> out = new ArrayList<>();
        out.add(Long.toString(next));
        out.add(keys);
        return out;
    }

    static long parseCursor(byte[] b) {
        // unsigned 64-bit decimal
        try {
            if (b.length == 0 || b.length > 20) {
                throw new NumberFormatException();
            }
            for (byte c : b) {
                if (c < '0' || c > '9') {
                    throw new NumberFormatException();
                }
            }
            return Long.parseUnsignedLong(Num.str(b));
        } catch (NumberFormatException e) {
            throw RedisError.err("invalid cursor");
        }
    }

    private static Object randomKey(Ctx x, byte[][] a) throws Exception {
        x.requireFanOut();
        int n = x.shardCount();
        int start = java.util.concurrent.ThreadLocalRandom.current().nextInt(n);
        for (int i = 0; i < n; i++) {
            x.useShard((start + i) % n);
            byte[] k = x.one("SELECT k FROM warp_redis_keys WHERE db = ? AND (exp IS NULL OR exp > ?) AND id >= "
                    + "(SELECT floor(random() * coalesce(max(id), 0))::bigint FROM warp_redis_keys) ORDER BY id LIMIT 1",
                    rs -> rs.getBytes(1), x.db, x.now);
            if (k == null) {
                k = x.one("SELECT k FROM warp_redis_keys WHERE db = ? AND (exp IS NULL OR exp > ?) LIMIT 1",
                        rs -> rs.getBytes(1), x.db, x.now);
            }
            if (k != null) {
                return k;
            }
        }
        return null;
    }

    private static Object dbsize(Ctx x, byte[][] a) throws Exception {
        x.requireFanOut();
        long total = 0;
        for (int s = 0; s < x.shardCount(); s++) {
            x.useShard(s);
            total += x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND (exp IS NULL OR exp > ?)", x.db, x.now);
        }
        return total;
    }

    private static Object flush(Ctx x, byte[][] a, boolean all) throws Exception {
        if (a.length > 2 || (a.length == 2 && !Num.eq(a[1], "ASYNC") && !Num.eq(a[1], "SYNC"))) {
            throw RedisError.syntax();
        }
        x.requireFanOut();
        for (int s = 0; s < x.shardCount(); s++) {
            x.useShard(s);
            if (all) {
                x.update("DELETE FROM warp_redis_keys");
            } else {
                x.update("DELETE FROM warp_redis_keys WHERE db = ?", x.db);
            }
        }
        return Resp.OK;
    }

    private static Object swapdb(Ctx x, byte[][] a) throws Exception {
        Long d1 = Num.parseLong(a[1]);
        Long d2 = Num.parseLong(a[2]);
        if (d1 == null) {
            throw RedisError.err("invalid first DB index");
        }
        if (d2 == null) {
            throw RedisError.err("invalid second DB index");
        }
        int max = x.store.options().databases();
        if (d1 < 0 || d1 >= max || d2 < 0 || d2 >= max) {
            throw RedisError.err("DB index is out of range");
        }
        x.requireFanOut();
        for (int s = 0; s < x.shardCount(); s++) {
            x.useShard(s);
            x.atomic(() -> {
                x.update("UPDATE warp_redis_keys SET db = -1 WHERE db = ?", d1.intValue());
                x.update("UPDATE warp_redis_keys SET db = ? WHERE db = ?", d1.intValue(), d2.intValue());
                x.update("UPDATE warp_redis_keys SET db = ? WHERE db = -1", d2.intValue());
                return null;
            });
        }
        return Resp.OK;
    }

    // ------------------------------------------------------------------------------------------
    // OBJECT
    // ------------------------------------------------------------------------------------------

    private static Object object(Ctx x, byte[][] a) throws Exception {
        String sub = Num.upper(a[1]);
        if (sub.equals("HELP")) {
            return Resp.help("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", "ENCODING <key>", "FREQ <key>",
                    "IDLETIME <key>", "REFCOUNT <key>", "HELP");
        }
        if (!List.of("ENCODING", "FREQ", "IDLETIME", "REFCOUNT").contains(sub)) {
            throw new RedisError("ERR unknown subcommand '" + Num.str(a[1]) + "'. Try OBJECT HELP.");
        }
        if (a.length != 3) {
            throw RedisError.arity("object|" + sub.toLowerCase());
        }
        Ctx.Meta m = x.get(a[2]);
        if (m == null) {
            return null;
        }
        switch (sub) {
            case "REFCOUNT":
                return 1L;
            case "IDLETIME":
                return 0L;
            case "FREQ":
                throw RedisError.err("An LFU maxmemory policy is not selected, access frequency not tracked. Please note "
                        + "that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            default:
                return encoding(x, a[2], m.type());
        }
    }

    static String encoding(Ctx x, byte[] k, int type) throws Exception {
        switch (type) {
            case Ctx.T_STRING: {
                byte[] v = StringCmds.getString(x, k);
                if (v != null && v.length <= 20 && Num.parseLong(v) != null) {
                    return "int";
                }
                return v != null && v.length <= 44 ? "embstr" : "raw";
            }
            case Ctx.T_HASH:
                return x.scalar("SELECT count(*) FROM warp_redis_hashes WHERE db = ? AND k = ?", x.db, k) <= 128 ? "listpack" : "hashtable";
            case Ctx.T_LIST:
                return x.scalar("SELECT count(*) FROM warp_redis_lists WHERE db = ? AND k = ?", x.db, k) <= 128 ? "listpack" : "quicklist";
            case Ctx.T_SET: {
                long n = x.scalar("SELECT count(*) FROM warp_redis_sets WHERE db = ? AND k = ?", x.db, k);
                boolean ints = x.scalar("SELECT count(*) FROM warp_redis_sets WHERE db = ? AND k = ? AND convert_from(m, 'SQL_ASCII') !~ '^-?(0|[1-9][0-9]{0,18})$'", x.db, k) == 0;
                return ints && n <= 512 ? "intset" : n <= 128 ? "listpack" : "hashtable";
            }
            case Ctx.T_ZSET:
                return x.scalar("SELECT count(*) FROM warp_redis_zsets WHERE db = ? AND k = ?", x.db, k) <= 128 ? "listpack" : "skiplist";
            default:
                return "stream";
        }
    }

    // ------------------------------------------------------------------------------------------
    // DUMP / RESTORE (own serialization format, "WRD1")
    // ------------------------------------------------------------------------------------------

    private static Object dump(Ctx x, byte[][] a) throws Exception {
        Ctx.Meta m = x.get(a[1]);
        if (m == null) {
            return null;
        }
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(bo);
        o.writeBytes("WRD1");
        o.writeByte(m.type());
        byte[] k = a[1];
        switch (m.type()) {
            case Ctx.T_STRING -> writeBytes(o, StringCmds.getString(x, k));
            case Ctx.T_HASH -> {
                List<byte[][]> rows = x.list("SELECT f, v FROM warp_redis_hashes WHERE db = ? AND k = ? ORDER BY f",
                        rs -> new byte[][] {rs.getBytes(1), rs.getBytes(2)}, x.db, k);
                o.writeInt(rows.size());
                for (byte[][] r : rows) {
                    writeBytes(o, r[0]);
                    writeBytes(o, r[1]);
                }
            }
            case Ctx.T_LIST -> {
                List<byte[]> rows = x.list("SELECT v FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos", rs -> rs.getBytes(1), x.db, k);
                o.writeInt(rows.size());
                for (byte[] r : rows) {
                    writeBytes(o, r);
                }
            }
            case Ctx.T_SET -> {
                List<byte[]> rows = x.list("SELECT m FROM warp_redis_sets WHERE db = ? AND k = ? ORDER BY m", rs -> rs.getBytes(1), x.db, k);
                o.writeInt(rows.size());
                for (byte[] r : rows) {
                    writeBytes(o, r);
                }
            }
            case Ctx.T_ZSET -> {
                List<Object[]> rows = x.list("SELECT m, score FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY score, m",
                        rs -> new Object[] {rs.getBytes(1), rs.getDouble(2)}, x.db, k);
                o.writeInt(rows.size());
                for (Object[] r : rows) {
                    writeBytes(o, (byte[]) r[0]);
                    o.writeDouble((Double) r[1]);
                }
            }
            default -> {
                writeBytes(o, m.sv() == null ? x.one("SELECT sv FROM warp_redis_keys WHERE db = ? AND k = ?", rs -> rs.getBytes(1), x.db, k) : m.sv());
                List<Object[]> rows = x.list("SELECT ms, seq, f FROM warp_redis_stream_entries WHERE db = ? AND k = ? ORDER BY ms, seq",
                        rs -> new Object[] {rs.getLong(1), rs.getLong(2), rs.getBytes(3)}, x.db, k);
                o.writeInt(rows.size());
                for (Object[] r : rows) {
                    o.writeLong((Long) r[0]);
                    o.writeLong((Long) r[1]);
                    writeBytes(o, (byte[]) r[2]);
                }
                List<Object[]> groups = x.list("SELECT g, last_ms, last_seq, coalesce(entries_read, -1) FROM warp_redis_stream_groups WHERE db = ? AND k = ?",
                        rs -> new Object[] {rs.getBytes(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)}, x.db, k);
                o.writeInt(groups.size());
                for (Object[] g : groups) {
                    writeBytes(o, (byte[]) g[0]);
                    o.writeLong((Long) g[1]);
                    o.writeLong((Long) g[2]);
                    o.writeLong((Long) g[3]);
                }
            }
        }
        o.flush();
        byte[] body = bo.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(body);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(body);
        new DataOutputStream(out).writeLong(crc.getValue());
        return out.toByteArray();
    }

    private static void writeBytes(DataOutputStream o, byte[] b) throws IOException {
        byte[] v = b == null ? new byte[0] : b;
        o.writeInt(v.length);
        o.write(v);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > in.available()) {
            throw new IOException("bad length");
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }

    private static Object restore(Ctx x, byte[][] a) throws Exception {
        Long ttl = Num.parseLong(a[2]);
        if (ttl == null) {
            throw RedisError.notInt();
        }
        if (ttl < 0) {
            throw RedisError.err("Invalid TTL value, must be >= 0");
        }
        boolean replace = false;
        boolean absttl = false;
        for (int i = 4; i < a.length; i++) {
            if (Num.eq(a[i], "REPLACE")) {
                replace = true;
            } else if (Num.eq(a[i], "ABSTTL")) {
                absttl = true;
            } else if ((Num.eq(a[i], "IDLETIME") || Num.eq(a[i], "FREQ")) && i + 1 < a.length) {
                if (Num.parseLong(a[++i]) == null) {
                    throw RedisError.notInt();
                }
            } else {
                throw RedisError.syntax();
            }
        }
        final boolean frep0 = replace;
        if (!frep0 && x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ? AND (exp IS NULL OR exp > ?)", x.db, a[1], x.now) > 0) {
            throw new RedisError("BUSYKEY Target key name already exists.");
        }
        byte[] p = a[3];
        String badPayload = "ERR DUMP payload version or checksum are wrong";
        if (p.length < 4 + 1 + 8) {
            throw RedisError.err(badPayload.substring(4));
        }
        CRC32 crc = new CRC32();
        crc.update(p, 0, p.length - 8);
        long want = new DataInputStream(new ByteArrayInputStream(p, p.length - 8, 8)).readLong();
        if (crc.getValue() != want || p[0] != 'W' || p[1] != 'R' || p[2] != 'D' || p[3] != '1') {
            throw new RedisError(badPayload);
        }
        Long exp = ttl == 0 ? null : (absttl ? ttl : x.now + ttl);
        final boolean frep = replace;
        return x.atomic(() -> {
            if (exp != null && exp <= x.now) {
                x.delete(a[1]);
                return Resp.OK;
            }
            x.update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND exp <= ?", x.db, a[1], x.now);
            if (x.scalar("SELECT count(*) FROM warp_redis_keys WHERE db = ? AND k = ?", x.db, a[1]) > 0) {
                if (!frep) {
                    throw new RedisError("BUSYKEY Target key name already exists.");
                }
                x.delete(a[1]);
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(p, 4, p.length - 4 - 8));
            int type = in.readUnsignedByte();
            byte[] k = a[1];
            byte[] sv = type == Ctx.T_STRING ? readBytes(in) : null;
            if (type == Ctx.T_STREAM) {
                sv = readBytes(in);
            }
            x.update("INSERT INTO warp_redis_keys (db, k, type, exp, sv, slot) VALUES (?, ?, ?, ?, ?, ?)", x.db, k, type,
                    exp == null ? Ctx.NULL_BIGINT : exp, sv == null ? Ctx.NULL_BYTEA : sv, Slot.of(k));

            switch (type) {
                case Ctx.T_STRING -> {
                }
                case Ctx.T_HASH -> {
                    int n = in.readInt();
                    for (int i = 0; i < n; i++) {
                        x.update("INSERT INTO warp_redis_hashes (db, k, f, v) VALUES (?, ?, ?, ?)", x.db, k, readBytes(in), readBytes(in));
                    }
                }
                case Ctx.T_LIST -> {
                    int n = in.readInt();
                    for (int i = 0; i < n; i++) {
                        x.update("INSERT INTO warp_redis_lists (db, k, pos, v) VALUES (?, ?, ?, ?)", x.db, k, (i + 1L) * ListCmds.STEP, readBytes(in));
                    }
                }
                case Ctx.T_SET -> {
                    int n = in.readInt();
                    for (int i = 0; i < n; i++) {
                        x.update("INSERT INTO warp_redis_sets (db, k, m) VALUES (?, ?, ?)", x.db, k, readBytes(in));
                    }
                }
                case Ctx.T_ZSET -> {
                    int n = in.readInt();
                    for (int i = 0; i < n; i++) {
                        x.update("INSERT INTO warp_redis_zsets (db, k, m, score) VALUES (?, ?, ?, ?)", x.db, k, readBytes(in), in.readDouble());
                    }
                }
                default -> {
                    int n = in.readInt();
                    for (int i = 0; i < n; i++) {
                        x.update("INSERT INTO warp_redis_stream_entries (db, k, ms, seq, f) VALUES (?, ?, ?, ?, ?)", x.db, k, in.readLong(), in.readLong(), readBytes(in));
                    }
                    int g = in.readInt();
                    for (int i = 0; i < g; i++) {
                        byte[] name = readBytes(in);
                        long lms = in.readLong();
                        long lseq = in.readLong();
                        long er = in.readLong();
                        x.update("INSERT INTO warp_redis_stream_groups (db, k, g, last_ms, last_seq, entries_read) VALUES (?, ?, ?, ?, ?, ?)",
                                x.db, k, name, lms, lseq, er < 0 ? Ctx.NULL_BIGINT : (Object) er);
                    }
                }
            }
            String countTable = switch (type) {
                case Ctx.T_HASH -> "warp_redis_hashes";
                case Ctx.T_LIST -> "warp_redis_lists";
                case Ctx.T_SET -> "warp_redis_sets";
                case Ctx.T_ZSET -> "warp_redis_zsets";
                case Ctx.T_STREAM -> "warp_redis_stream_entries";
                default -> null;
            };
            if (countTable != null) {
                x.update("UPDATE warp_redis_keys SET n = (SELECT count(*) FROM " + countTable + " WHERE db = ? AND k = ?) "
                        + "WHERE db = ? AND k = ?", x.db, k, x.db, k);
            }
            if (type == Ctx.T_LIST || type == Ctx.T_ZSET || type == Ctx.T_STREAM) {
                x.wake(k);
            }
            return Resp.OK;
        });
    }

    // ------------------------------------------------------------------------------------------
    // SORT
    // ------------------------------------------------------------------------------------------

    private static int[] sortKeys(byte[][] a) {
        List<Integer> ks = new ArrayList<>();
        ks.add(1);
        for (int i = 2; i + 1 < a.length; i++) {
            if (Num.eq(a[i], "STORE")) {
                ks.add(i + 1);
                break;
            }
        }
        int[] r = new int[ks.size()];
        for (int i = 0; i < r.length; i++) {
            r[i] = ks.get(i);
        }
        return r;
    }

    private static Object sort(Ctx x, byte[][] a, boolean ro) throws Exception {
        byte[] by = null;
        long off = 0;
        long cnt = -1;
        boolean desc = false;
        boolean alpha = false;
        byte[] store = null;
        List<byte[]> gets = new ArrayList<>();
        for (int i = 2; i < a.length; i++) {
            if (Num.eq(a[i], "ASC")) {
                desc = false;
            } else if (Num.eq(a[i], "DESC")) {
                desc = true;
            } else if (Num.eq(a[i], "ALPHA")) {
                alpha = true;
            } else if (Num.eq(a[i], "LIMIT") && i + 2 < a.length) {
                Long o = Num.parseLong(a[i + 1]);
                Long c = Num.parseLong(a[i + 2]);
                if (o == null || c == null) {
                    throw RedisError.notInt();
                }
                off = o;
                cnt = c;
                i += 2;
            } else if (Num.eq(a[i], "BY") && i + 1 < a.length) {
                by = a[++i];
            } else if (Num.eq(a[i], "GET") && i + 1 < a.length) {
                gets.add(a[++i]);
            } else if (Num.eq(a[i], "STORE") && i + 1 < a.length && !ro) {
                store = a[++i];
            } else {
                throw RedisError.syntax();
            }
        }
        Ctx.Meta m = x.get(a[1]);
        List<byte[]> elems = new ArrayList<>();
        if (m != null) {
            switch (m.type()) {
                case Ctx.T_LIST -> elems.addAll(x.list("SELECT v FROM warp_redis_lists WHERE db = ? AND k = ? ORDER BY pos", rs -> rs.getBytes(1), x.db, a[1]));
                case Ctx.T_SET -> elems.addAll(x.list("SELECT m FROM warp_redis_sets WHERE db = ? AND k = ?", rs -> rs.getBytes(1), x.db, a[1]));
                case Ctx.T_ZSET -> elems.addAll(x.list("SELECT m FROM warp_redis_zsets WHERE db = ? AND k = ? ORDER BY score, m", rs -> rs.getBytes(1), x.db, a[1]));
                default -> throw RedisError.wrongType();
            }
        }
        final boolean dontSort = by != null && new String(by, java.nio.charset.StandardCharsets.ISO_8859_1).indexOf('*') < 0;
        final boolean falpha = alpha;
        final byte[] fby = by;
        List<Object[]> keyed = new ArrayList<>();
        for (byte[] e : elems) {
            byte[] sk = e;
            if (fby != null && !dontSort) {
                sk = lookupPattern(x, fby, e);
            }
            Object key = null;
            if (!dontSort) {
                if (falpha) {
                    key = sk == null ? null : sk;
                } else {
                    Double d = sk == null ? Double.valueOf(0) : Num.parseDouble(sk);
                    if (d == null) {
                        throw RedisError.err("One or more scores can't be converted into double");
                    }
                    key = d;
                }
            }
            keyed.add(new Object[] {e, key});
        }
        if (!dontSort) {
            final boolean fdesc = desc;
            keyed.sort((p, q) -> {
                int c;
                if (falpha) {
                    byte[] pk = (byte[]) p[1];
                    byte[] qk = (byte[]) q[1];
                    c = pk == null ? (qk == null ? 0 : -1) : qk == null ? 1 : Arrays.compareUnsigned(pk, qk);
                } else {
                    c = Double.compare((Double) p[1], (Double) q[1]);
                }
                return fdesc ? -c : c;
            });
        }
        int from = (int) Math.min(Math.max(0, off), keyed.size());
        int to = cnt < 0 ? keyed.size() : (int) Math.min(keyed.size(), from + Math.max(0, cnt));
        List<Object> out = new ArrayList<>();
        for (Object[] e : keyed.subList(from, to)) {
            if (gets.isEmpty()) {
                out.add(e[0]);
            } else {
                for (byte[] g : gets) {
                    out.add(g.length == 1 && g[0] == '#' ? e[0] : lookupPattern(x, g, (byte[]) e[0]));
                }
            }
        }
        if (store != null) {
            final byte[] fstore = store;
            final List<Object> fout = out;
            return x.atomic(() -> {
                x.delete(fstore);
                if (fout.isEmpty()) {
                    return 0L;
                }
                x.lockOrCreate(fstore, Ctx.T_LIST);
                long pos = 0;
                for (Object o : fout) {
                    pos += ListCmds.STEP;
                    x.update("INSERT INTO warp_redis_lists (db, k, pos, v) VALUES (?, ?, ?, ?)", x.db, fstore, pos, o == null ? new byte[0] : o);
                }
                x.adjust(fstore, fout.size());
                x.wake(fstore);
                return (long) fout.size();
            });
        }
        return out;
    }

    private static byte[] lookupPattern(Ctx x, byte[] pattern, byte[] elem) throws Exception {
        String p = new String(pattern, java.nio.charset.StandardCharsets.ISO_8859_1);
        int star = p.indexOf('*');
        if (star < 0) {
            return null;
        }
        String field = null;
        int arrow = p.indexOf("->", star);
        String keyPart = p;
        if (arrow >= 0) {
            keyPart = p.substring(0, arrow);
            field = p.substring(arrow + 2);
        }
        int s = keyPart.indexOf('*');
        byte[] pre = keyPart.substring(0, s).getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
        byte[] post = keyPart.substring(s + 1).getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
        byte[] key = new byte[pre.length + elem.length + post.length];
        System.arraycopy(pre, 0, key, 0, pre.length);
        System.arraycopy(elem, 0, key, pre.length, elem.length);
        System.arraycopy(post, 0, key, pre.length + elem.length, post.length);
        if (field != null) {
            return x.one("SELECT h.v FROM warp_redis_hashes h JOIN warp_redis_keys k ON k.db = h.db AND k.k = h.k "
                    + "WHERE h.db = ? AND h.k = ? AND h.f = ? AND (k.exp IS NULL OR k.exp > ?)", rs -> rs.getBytes(1),
                    x.db, key, field.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1), x.now);
        }
        try {
            return StringCmds.getString(x, key);
        } catch (RedisError e) {
            return null;
        }
    }
}
