package com.sayonora.warp.rediswire;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-command execution context: the session, the shard (Postgres host) the command runs on and, lazily, one pooled
 * connection to it. The connection is only held for the duration of the command (or of an EXEC) and returned to the
 * pool in {@link #close()}; nothing here survives a client's idle time or a blocking wait.
 */
final class Ctx implements AutoCloseable {

    static final int T_STRING = 0;
    static final int T_HASH = 1;
    static final int T_LIST = 2;
    static final int T_SET = 3;
    static final int T_ZSET = 4;
    static final int T_STREAM = 5;

    static final String[] TYPE_NAMES = {"string", "hash", "list", "set", "zset", "stream"};

    /** SQL NULL of a given JDBC type, for binding. */
    record Nul(int sqlType) {
    }

    static final Nul NULL_BIGINT = new Nul(Types.BIGINT);
    static final Nul NULL_BYTEA = new Nul(Types.BINARY);

    interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    interface Body<T> {
        T run() throws Exception;
    }

    /** Key metadata; {@code exp} is epoch ms or null; {@code created} is true for a key this command just created. */
    record Meta(int type, Long exp, long ver, byte[] sv, long n, boolean created) {
    }

    final RedisStore store;
    final Session session;
    long now;
    boolean resp3;
    int db;
    boolean tx;
    private List<String> hosts;
    private int shard;
    private Connection conn;
    private int connShard = -1;
    /** keys pushed to during this command: wake blocked clients after commit. */
    private final Map<String, byte[]> wakes = new LinkedHashMap<>();
    long waitedNanos;
    /** set by a command body to roll the surrounding {@link #atomic} transaction back instead of committing. */
    boolean rollbackOnly;
    private int depth;

    Ctx(RedisStore store, Session session) {
        this.store = store;
        this.session = session;
        this.hosts = session.hosts();
        this.db = session.db;
        this.resp3 = session.resp3;
        this.now = System.currentTimeMillis();
    }

    List<String> hosts() {
        return hosts;
    }

    int shard() {
        return shard;
    }

    int shardCount() {
        return hosts.size();
    }

    /** Guard for commands that fan out over every shard: not possible inside one EXEC transaction. */
    void requireFanOut() {
        if (tx && hosts.size() > 1) {
            throw RedisError.err("this command spans every shard and cannot run inside MULTI/EXEC when the store is sharded");
        }
    }

    String host() {
        return hosts.get(shard);
    }

    int shardOfKey(byte[] key) {
        return Slot.shardOf(Slot.of(key), hosts.size());
    }

    /** Switches to shard {@code i}; the previous connection goes back to the pool unless inside an EXEC. */
    void useShard(int i) {
        if (i != shard) {
            if (tx) {
                throw new RedisError(RedisError.CROSSSLOT);
            }
            release();
            shard = i;
        }
    }

    Connection c() throws SQLException {
        if (conn != null && connShard == shard) {
            return conn;
        }
        release();
        String h = hosts.get(shard);
        conn = store.open(h);
        connShard = shard;
        return conn;
    }

    private void release() {
        if (conn != null) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException ignored) {
                // returned to the pool below either way (Hikari evicts a broken one)
            }
            try {
                conn.close();
            } catch (SQLException ignored) {
                // nothing to do
            }
            conn = null;
            connShard = -1;
        }
    }

    @Override
    public void close() {
        release();
    }

    // ------------------------------------------------------------------------------------------
    // JDBC helpers
    // ------------------------------------------------------------------------------------------

    private void bind(PreparedStatement ps, Object[] args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            Object a = args[i];
            int idx = i + 1;
            switch (a) {
                case null -> ps.setNull(idx, Types.BINARY);
                case byte[] b -> ps.setBytes(idx, b);
                case Long l -> ps.setLong(idx, l);
                case Integer n -> ps.setInt(idx, n);
                case Double d -> ps.setDouble(idx, d);
                case String s -> ps.setString(idx, s);
                case Boolean b -> ps.setBoolean(idx, b);
                case Nul n -> ps.setNull(idx, n.sqlType());
                case byte[][] arr -> {
                    Object[] o = arr;
                    Array sa = c().createArrayOf("bytea", o);
                    ps.setArray(idx, sa);
                }
                case long[] arr -> {
                    Long[] o = new Long[arr.length];
                    for (int j = 0; j < arr.length; j++) {
                        o[j] = arr[j];
                    }
                    ps.setArray(idx, c().createArrayOf("int8", o));
                }
                case double[] arr -> {
                    Double[] o = new Double[arr.length];
                    for (int j = 0; j < arr.length; j++) {
                        o[j] = arr[j];
                    }
                    ps.setArray(idx, c().createArrayOf("float8", o));
                }
                default -> ps.setObject(idx, a);
            }
        }
    }

    int update(String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c().prepareStatement(sql)) {
            bind(ps, args);
            return ps.executeUpdate();
        }
    }

    <T> T one(String sql, RowMapper<T> m, Object... args) throws SQLException {
        try (PreparedStatement ps = c().prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? m.map(rs) : null;
            }
        }
    }

    <T> List<T> list(String sql, RowMapper<T> m, Object... args) throws SQLException {
        try (PreparedStatement ps = c().prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(m.map(rs));
                }
                return out;
            }
        }
    }

    /** First column of the first row as long, {@code 0} when there is no row or it is NULL. */
    long scalar(String sql, Object... args) throws SQLException {
        Long v = one(sql, rs -> rs.getLong(1), args);
        return v == null ? 0 : v;
    }

    /** Runs {@code body} in one Postgres transaction (or inline when already inside an EXEC). */
    <T> T atomic(Body<T> body) throws Exception {
        if (tx || depth > 0) {
            return body.run();
        }
        depth++;
        Connection cn = c();
        cn.setAutoCommit(false);
        try {
            rollbackOnly = false;
            T r = body.run();
            if (rollbackOnly) {
                cn.rollback();
                wakes.clear();
                wakeDbs.clear();
                rollbackOnly = false;
                return r;
            }
            flushWakes();
            cn.commit();
            fireWakes();
            return r;
        } catch (Throwable t) {
            try {
                cn.rollback();
            } catch (SQLException ignored) {
                // connection is broken anyway
            }
            wakes.clear();
            throw t;
        } finally {
            depth--;
            try {
                cn.setAutoCommit(true);
            } catch (SQLException ignored) {
                // evicted by the pool
            }
        }
    }

    /** Begin an EXEC transaction on the current shard. */
    void begin() throws SQLException {
        c().setAutoCommit(false);
        tx = true;
    }

    void commit() throws SQLException {
        flushWakes();
        c().commit();
        c().setAutoCommit(true);
        tx = false;
        fireWakes();
    }

    void rollback() {
        try {
            if (conn != null) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
        } catch (SQLException ignored) {
            // nothing to do
        }
        tx = false;
        wakes.clear();
    }

    // ------------------------------------------------------------------------------------------
    // key primitives
    // ------------------------------------------------------------------------------------------

    /** Live key metadata (no lock); {@code null} when missing or expired. */
    Meta get(byte[] k) throws SQLException {
        return one("SELECT type, exp, ver, n FROM warp_redis_keys WHERE db = ? AND k = ? AND (exp IS NULL OR exp > ?)",
                rs -> new Meta(rs.getInt(1), (Long) rs.getObject(2), rs.getLong(3), null, rs.getLong(4), false), db, k, now);
    }

    /** Locks a live key (row lock until commit) and gives it a fresh version; {@code null} when missing/expired. */
    Meta lock(byte[] k) throws SQLException {
        return one("UPDATE warp_redis_keys SET ver = nextval('warp_redis_ver') WHERE db = ? AND k = ? "
                + "AND (exp IS NULL OR exp > ?) RETURNING type, exp, ver, sv, n",
                rs -> new Meta(rs.getInt(1), (Long) rs.getObject(2), rs.getLong(3), rs.getBytes(4), rs.getLong(5), false), db, k, now);
    }

    /** Locks the key for a write of {@code type}, creating it when missing; WRONGTYPE for another type. */
    Meta lockOrCreate(byte[] k, int type) throws SQLException {
        for (int i = 0; i < 5; i++) {
            Meta m = lock(k);
            if (m != null) {
                if (m.type() != type) {
                    throw RedisError.wrongType();
                }
                return m;
            }
            update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND exp <= ?", db, k, now);
            if (update("INSERT INTO warp_redis_keys (db, k, type, slot) VALUES (?, ?, ?, ?) ON CONFLICT (db, k) DO NOTHING",
                    db, k, type, Slot.of(k)) == 1) {
                return new Meta(type, null, 0, null, 0, true);
            }
        }
        throw RedisError.err("could not lock key (concurrent modification)");
    }

    /** Locks a live key of {@code type}; {@code null} when missing; WRONGTYPE for another type. */
    Meta lockExisting(byte[] k, int type) throws SQLException {
        Meta m = lock(k);
        if (m != null && m.type() != type) {
            throw RedisError.wrongType();
        }
        return m;
    }

    /** Deletes the key (children cascade) whether or not it is expired; true when a row was removed. */
    boolean delete(byte[] k) throws SQLException {
        return update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ?", db, k) > 0;
    }

    /** Live keys among {@code keys} (deduplicated) with their types; expired or missing keys are absent. */
    Map<Hub.BK, Integer> liveTypes(byte[][] keys) throws SQLException {
        Map<Hub.BK, Integer> out = new LinkedHashMap<>();
        for (Object[] r : list("SELECT k, type FROM warp_redis_keys WHERE db = ? AND k = ANY(?) AND (exp IS NULL OR exp > ?)",
                rs -> new Object[] {rs.getBytes(1), rs.getInt(2)}, db, keys, now)) {
            out.put(new Hub.BK((byte[]) r[0]), (Integer) r[1]);
        }
        return out;
    }

    /** Adds {@code delta} to the element counter of a container key; returns the new count. */
    long adjust(byte[] k, long delta) throws SQLException {
        Long n = one("UPDATE warp_redis_keys SET n = n + ? WHERE db = ? AND k = ? RETURNING n", rs -> rs.getLong(1), delta, db, k);
        return n == null ? 0 : n;
    }

    /** Removes the key row when its container has no elements left (counter reached zero). */
    void deleteIfEmpty(byte[] k) throws SQLException {
        update("DELETE FROM warp_redis_keys WHERE db = ? AND k = ? AND type NOT IN (0, 5) AND n <= 0", db, k);
    }

    /** Element count of a live container key of {@code type}: {@code -1} when the key is missing; WRONGTYPE otherwise. */
    long count(byte[] k, int type) throws SQLException {
        Meta m = get(k);
        if (m == null) {
            return -1;
        }
        if (m.type() != type) {
            throw RedisError.wrongType();
        }
        return m.n();
    }

    /** Purges every expired key of this shard (used before scans/counts that must not see dead rows). */
    void purgeExpired() throws SQLException {
        update("DELETE FROM warp_redis_keys WHERE exp IS NOT NULL AND exp <= ?", now);
    }

    /** A "typed read": one statement returning the key's type and, when it has {@code type}, the child rows. */
    record TypedRead<T>(boolean exists, int type, List<T> rows) {
        void expect(int t) {
            if (exists && type != t) {
                throw RedisError.wrongType();
            }
        }
    }

    /**
     * Reads a key and its children consistently in ONE statement. {@code childSql} is a full SELECT using {@code ?}
     * placeholders (its arguments follow db, k, now); it is run laterally only when the key has {@code type}. Its FIRST
     * column must never be NULL; mappers read child columns from index 2 (index 1 is the key type).
     */
    <T> TypedRead<T> typedRead(byte[] k, int type, String childSql, RowMapper<T> mapper, Object... childArgs)
            throws SQLException {
        String sql = "SELECT m.type, r.* FROM (SELECT type FROM warp_redis_keys WHERE db = ? AND k = ? "
                + "AND (exp IS NULL OR exp > ?)) m LEFT JOIN LATERAL (" + childSql + ") r ON m.type = " + type;
        Object[] args = new Object[3 + childArgs.length];
        args[0] = db;
        args[1] = k;
        args[2] = now;
        System.arraycopy(childArgs, 0, args, 3, childArgs.length);
        int type0 = -1;
        boolean exists = false;
        List<T> rows = new ArrayList<>();
        try (PreparedStatement ps = c().prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    exists = true;
                    type0 = rs.getInt(1);
                    if (type0 == type && rs.getObject(2) != null) {
                        rows.add(mapper.map(rs));
                    }
                }
            }
        }
        return new TypedRead<>(exists, type0, rows);
    }

    // ------------------------------------------------------------------------------------------
    // blocking-client wake-ups
    // ------------------------------------------------------------------------------------------

    /** Records that {@code k} received elements: blocked clients (any Warp node) are woken after commit. */
    void wake(byte[] k) {
        wakes.put(db + ":" + java.util.Arrays.hashCode(k), k);
        wakeDbs.put(db + ":" + java.util.Arrays.hashCode(k), db);
    }

    private final Map<String, Integer> wakeDbs = new LinkedHashMap<>();

    private void flushWakes() throws SQLException {
        for (Map.Entry<String, byte[]> e : wakes.entrySet()) {
            int d = wakeDbs.get(e.getKey());
            try (PreparedStatement ps = c().prepareStatement("SELECT pg_notify('warp_redis_keys', ?)")) {
                ps.setString(1, RedisStore.wakeId(d, e.getValue()));
                ps.execute();
            }
        }
    }

    private void fireWakes() {
        if (wakes.isEmpty()) {
            return;
        }
        for (Map.Entry<String, byte[]> e : wakes.entrySet()) {
            store.hub().signalKey(RedisStore.wakeId(wakeDbs.get(e.getKey()), e.getValue()));
        }
        wakes.clear();
        wakeDbs.clear();
    }
}
