package com.sayonora.wire.cqlwire;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** All SQL of the CQL store: the schema catalog (home host) and the cell table (partition-hash shards). */
final class CqlStore {

    /** One stored cell as read back. */
    static final class Rec {
        long token;
        byte[] pk;
        byte[] ck;
        byte[] ckv;
        String col;
        byte[] path;
        byte[] val;
        long ts;
        /** Remaining TTL in seconds, -1 = none. */
        long ttl = -1;
    }

    /** One cell to write. {@code ttlSec <= 0} = no TTL. */
    static final class Cell {
        final byte[] ck;
        final byte[] ckv;
        final String col;
        final byte[] path;
        final byte[] val;
        final long ts;
        final int ttlSec;

        Cell(byte[] ck, byte[] ckv, String col, byte[] path, byte[] val, long ts, int ttlSec) {
            this.ck = ck;
            this.ckv = ckv;
            this.col = col;
            this.path = path;
            this.val = val;
            this.ts = ts;
            this.ttlSec = ttlSec;
        }
    }

    /** Selection of cells of a table. */
    static final class Scan {
        Long token;
        byte[] pk;
        Long tokLo, tokHi;
        boolean tokLoIncl = true, tokHiIncl = true;
        byte[] ckLo, ckHi;
        boolean reversed;
        Long resumeToken;
        byte[] resumePk, resumeCk;
        int limit = 100;
        /** Also report partitions that have only static cells. */
        boolean staticRows;
    }

    private static final String LIVE = "(expires_at IS NULL OR expires_at > now())";
    private final CqlShards shards;

    CqlStore(CqlShards shards) {
        this.shards = shards;
    }

    CqlShards shards() {
        return shards;
    }

    // ---------------------------------------------------------------- catalog

    List<String[]> loadSchema(String home) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT kind, ks, name, spec FROM warp_cql_schema ORDER BY seq");
                    ResultSet rs = ps.executeQuery()) {
                List<String[]> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new String[] {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)});
                }
                return out;
            }
        });
    }

    void putSchema(String home, String kind, String ks, String name, String spec) {
        shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cql_schema (kind, ks, name, spec) VALUES (?, ?, ?, ?) "
                    + "ON CONFLICT (kind, ks, name) DO UPDATE SET spec = EXCLUDED.spec")) {
                ps.setString(1, kind);
                ps.setString(2, ks);
                ps.setString(3, name);
                ps.setString(4, spec);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Inserts only when absent; false when the entry exists (used for IF NOT EXISTS races). */
    boolean insertSchema(String home, String kind, String ks, String name, String spec) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cql_schema (kind, ks, name, spec) VALUES (?, ?, ?, ?) "
                    + "ON CONFLICT (kind, ks, name) DO NOTHING")) {
                ps.setString(1, kind);
                ps.setString(2, ks);
                ps.setString(3, name);
                ps.setString(4, spec);
                return ps.executeUpdate() == 1;
            }
        });
    }

    void deleteSchema(String home, String kind, String ks, String name) {
        shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_schema WHERE kind = ? AND ks = ? AND name = ?")) {
                ps.setString(1, kind);
                ps.setString(2, ks);
                ps.setString(3, name);
                ps.executeUpdate();
            }
            return null;
        });
    }

    void deleteKeyspaceSchema(String home, String ks) {
        shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_schema WHERE ks = ?")) {
                ps.setString(1, ks);
                ps.executeUpdate();
            }
            return null;
        });
    }

    // ---------------------------------------------------------------- writes (inside a caller's transaction or autocommit)

    void lockPartition(Connection c, UUID tid, byte[] pk) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
            ps.setString(1, tid + CqlType.hex(pk));
            ps.executeQuery().close();
        }
    }

    void upsert(Connection c, UUID tid, long token, byte[] pk, List<Cell> cells) throws SQLException {
        if (cells.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cql_cells (tid, token, pk, ck, ckv, col, path, val, ts, expires_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? > 0 THEN now() + (? * interval '1 second') END) "
                + "ON CONFLICT (tid, token, pk, ck, col, path) DO UPDATE SET val = EXCLUDED.val, ts = EXCLUDED.ts, "
                + "expires_at = EXCLUDED.expires_at "
                + "WHERE warp_cql_cells.expires_at <= now() OR warp_cql_cells.ts < EXCLUDED.ts OR (warp_cql_cells.ts = EXCLUDED.ts AND "
                + "(EXCLUDED.val IS NULL OR (warp_cql_cells.val IS NOT NULL AND warp_cql_cells.val <= EXCLUDED.val)))")) {
            for (Cell x : cells) {
                ps.setObject(1, tid);
                ps.setLong(2, token);
                ps.setBytes(3, pk);
                ps.setBytes(4, x.ck);
                ps.setBytes(5, x.ckv);
                ps.setString(6, x.col);
                ps.setBytes(7, x.path);
                ps.setBytes(8, x.val);
                ps.setLong(9, x.ts);
                ps.setInt(10, x.ttlSec);
                ps.setInt(11, x.ttlSec);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** Atomically adds {@code delta} to a counter cell. */
    void counterAdd(Connection c, UUID tid, long token, byte[] pk, byte[] ck, byte[] ckv, String col, long delta, long ts) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_cql_cells (tid, token, pk, ck, ckv, col, path, val, ts) "
                + "VALUES (?, ?, ?, ?, ?, ?, ''::bytea, int8send(?::bigint), ?) "
                + "ON CONFLICT (tid, token, pk, ck, col, path) DO UPDATE SET "
                + "val = int8send((((('x' || encode(warp_cql_cells.val, 'hex'))::bit(64)::bigint)::numeric + ?::numeric + 9223372036854775808) "
                + "% 18446744073709551616 - 9223372036854775808)::bigint), ts = GREATEST(warp_cql_cells.ts, EXCLUDED.ts)")) {
            ps.setObject(1, tid);
            ps.setLong(2, token);
            ps.setBytes(3, pk);
            ps.setBytes(4, ck);
            ps.setBytes(5, ckv);
            ps.setString(6, col);
            ps.setLong(7, delta);
            ps.setLong(8, ts);
            ps.setLong(9, delta);
            ps.executeUpdate();
        }
    }

    /**
     * Deletes cells of one partition. {@code ckLo} (inclusive) / {@code ckHi} (exclusive) bound the clustering key when non-null,
     * {@code col} / {@code path} narrow to a column / collection element, {@code maxTs} keeps cells written after it.
     */
    int delete(Connection c, UUID tid, long token, byte[] pk, byte[] ckLo, byte[] ckHi, String col, byte[] path, Long maxTs)
            throws SQLException {
        StringBuilder sql = new StringBuilder("DELETE FROM warp_cql_cells WHERE tid = ? AND token = ? AND pk = ?");
        if (ckLo != null) {
            sql.append(" AND ck >= ?");
        }
        if (ckHi != null) {
            sql.append(" AND ck < ?");
        }
        if (col != null) {
            sql.append(" AND col = ?");
        }
        if (path != null) {
            sql.append(" AND path = ?");
        }
        if (maxTs != null) {
            sql.append(" AND ts <= ?");
        }
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            int i = 1;
            ps.setObject(i++, tid);
            ps.setLong(i++, token);
            ps.setBytes(i++, pk);
            if (ckLo != null) {
                ps.setBytes(i++, ckLo);
            }
            if (ckHi != null) {
                ps.setBytes(i++, ckHi);
            }
            if (col != null) {
                ps.setString(i++, col);
            }
            if (path != null) {
                ps.setBytes(i++, path);
            }
            if (maxTs != null) {
                ps.setLong(i, maxTs);
            }
            return ps.executeUpdate();
        }
    }

    /** Deletes the cells of a column's collection elements (path > '') - used to overwrite a collection. */
    void deleteElements(Connection c, UUID tid, long token, byte[] pk, byte[] ck, String col) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_cells WHERE tid = ? AND token = ? AND pk = ? AND ck = ? AND col = ?")) {
            ps.setObject(1, tid);
            ps.setLong(2, token);
            ps.setBytes(3, pk);
            ps.setBytes(4, ck);
            ps.setString(5, col);
            ps.executeUpdate();
        }
    }

    // ---------------------------------------------------------------- reads

    private static Rec rec(ResultSet rs) throws SQLException {
        Rec r = new Rec();
        r.token = rs.getLong(1);
        r.pk = rs.getBytes(2);
        r.ck = rs.getBytes(3);
        r.ckv = rs.getBytes(4);
        r.col = rs.getString(5);
        r.path = rs.getBytes(6);
        r.val = rs.getBytes(7);
        r.ts = rs.getLong(8);
        double ttl = rs.getDouble(9);
        r.ttl = rs.wasNull() ? -1 : Math.max(1, (long) Math.ceil(ttl));
        return r;
    }

    private static final String COLS = "c.token, c.pk, c.ck, c.ckv, c.col, c.path, c.val, c.ts, EXTRACT(EPOCH FROM (c.expires_at - now()))";

    /** Cells of up to {@code scan.limit} rows (distinct token, pk, ck) in (token, pk, ck) order, then col, path. */
    List<Rec> scan(Connection c, UUID tid, Scan s) throws SQLException {
        StringBuilder w = new StringBuilder("tid = ? AND " + LIVE);
        List<Object> args = new ArrayList<>();
        args.add(tid);
        if (s.token != null) {
            w.append(" AND token = ? AND pk = ?");
            args.add(s.token);
            args.add(s.pk);
        }
        if (s.tokLo != null) {
            w.append(s.tokLoIncl ? " AND token >= ?" : " AND token > ?");
            args.add(s.tokLo);
        }
        if (s.tokHi != null) {
            w.append(s.tokHiIncl ? " AND token <= ?" : " AND token < ?");
            args.add(s.tokHi);
        }
        if (s.ckLo != null) {
            w.append(" AND ck >= ?");
            args.add(s.ckLo);
        }
        if (s.ckHi != null) {
            w.append(" AND ck < ?");
            args.add(s.ckHi);
        }
        boolean bounded = s.ckLo != null || s.ckHi != null;
        if (s.staticRows && !bounded) {
            w.append(" AND (ck > ''::bytea OR (ck = ''::bytea AND NOT EXISTS (SELECT 1 FROM warp_cql_cells r WHERE r.tid = warp_cql_cells.tid "
                    + "AND r.token = warp_cql_cells.token AND r.pk = warp_cql_cells.pk AND r.ck > ''::bytea AND r.val IS NOT NULL "
                    + "AND r.col <> E'\\001row' AND r.col NOT LIKE E'%\\002' AND r.col <> E'\\003rt' AND (r.expires_at IS NULL OR r.expires_at > now()))))");
        } else {
            w.append(" AND ck > ''::bytea");
        }
        if (s.resumePk != null) {
            if (s.token != null) {
                w.append(s.reversed ? " AND ck < ?" : " AND ck > ?");
                args.add(s.resumeCk);
            } else {
                w.append(" AND (token, pk, ck) > (?, ?, ?)");
                args.add(s.resumeToken);
                args.add(s.resumePk);
                args.add(s.resumeCk);
            }
        }
        String dir = s.reversed ? " DESC" : "";
        String sql = "WITH k AS (SELECT DISTINCT token, pk, ck FROM warp_cql_cells WHERE " + w + " ORDER BY token, pk, ck" + dir
                + " LIMIT ?) SELECT " + COLS + " FROM warp_cql_cells c JOIN k USING (token, pk, ck) WHERE c.tid = ? AND "
                + "(c.expires_at IS NULL OR c.expires_at > now()) ORDER BY c.token, c.pk, c.ck" + dir + ", c.col, c.path";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            for (Object a : args) {
                bind(ps, i++, a);
            }
            ps.setInt(i++, s.limit);
            ps.setObject(i, tid);
            try (ResultSet rs = ps.executeQuery()) {
                List<Rec> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rec(rs));
                }
                return out;
            }
        }
    }

    private static void bind(PreparedStatement ps, int i, Object a) throws SQLException {
        if (a instanceof byte[] b) {
            ps.setBytes(i, b);
        } else if (a instanceof Long l) {
            ps.setLong(i, l);
        } else {
            ps.setObject(i, a);
        }
    }

    /** All live cells of one partition, optionally of one exact clustering key (X'' selects the static row). */
    List<Rec> partition(Connection c, UUID tid, long token, byte[] pk, byte[] ckExact) throws SQLException {
        String sql = "SELECT " + COLS + " FROM warp_cql_cells c WHERE c.tid = ? AND c.token = ? AND c.pk = ? "
                + (ckExact == null ? "" : "AND c.ck = ? ") + "AND (c.expires_at IS NULL OR c.expires_at > now()) ORDER BY c.ck, c.col, c.path";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, tid);
            ps.setLong(2, token);
            ps.setBytes(3, pk);
            if (ckExact != null) {
                ps.setBytes(4, ckExact);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Rec> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rec(rs));
                }
                return out;
            }
        }
    }

    /** Range tombstone cells (column {@link Rows#RT_COL}) of several partitions. */
    List<Rec> rangeTombstones(Connection c, UUID tid, List<long[]> tokens, List<byte[]> pks) throws SQLException {
        if (pks.isEmpty()) {
            return List.of();
        }
        StringBuilder sql = new StringBuilder("SELECT " + COLS + " FROM warp_cql_cells c WHERE c.tid = ? AND c.col = E'\\003rt' AND (c.token, c.pk) IN (");
        for (int i = 0; i < pks.size(); i++) {
            sql.append(i > 0 ? ", (?, ?)" : "(?, ?)");
        }
        sql.append(')');
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            int i = 1;
            ps.setObject(i++, tid);
            for (int j = 0; j < pks.size(); j++) {
                ps.setLong(i++, tokens.get(j)[0]);
                ps.setBytes(i++, pks.get(j));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Rec> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rec(rs));
                }
                return out;
            }
        }
    }

    /** Writes a range tombstone over clustering keys [lo, hi) ({@code hi == null}: to the end of the partition; {@code lo} empty: the whole partition). */
    void rangeTombstone(Connection c, UUID tid, long token, byte[] pk, byte[] lo, byte[] hi, long ts) throws SQLException {
        upsert(c, tid, token, pk, List.of(new Cell(lo, new byte[0], Rows.RT_COL, hi == null ? new byte[0] : hi, hi == null ? new byte[0] : new byte[] {1}, ts, Dml.GC_GRACE)));
    }

    /** Static cells of several partitions. */
    List<Rec> statics(Connection c, UUID tid, List<long[]> tokens, List<byte[]> pks) throws SQLException {
        if (pks.isEmpty()) {
            return List.of();
        }
        StringBuilder sql = new StringBuilder("SELECT " + COLS + " FROM warp_cql_cells c WHERE c.tid = ? AND c.ck = ''::bytea AND "
                + "(c.expires_at IS NULL OR c.expires_at > now()) AND (c.token, c.pk) IN (");
        for (int i = 0; i < pks.size(); i++) {
            sql.append(i > 0 ? ", (?, ?)" : "(?, ?)");
        }
        sql.append(')');
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            int i = 1;
            ps.setObject(i++, tid);
            for (int j = 0; j < pks.size(); j++) {
                ps.setLong(i++, tokens.get(j)[0]);
                ps.setBytes(i++, pks.get(j));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Rec> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rec(rs));
                }
                return out;
            }
        }
    }

    // ---------------------------------------------------------------- maintenance

    void dropTable(String host, UUID tid) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_cells WHERE tid = ?")) {
                ps.setObject(1, tid);
                ps.executeUpdate();
            }
            return null;
        });
    }

    void dropColumn(String host, UUID tid, String col) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_cells WHERE tid = ? AND col = ?")) {
                ps.setObject(1, tid);
                ps.setString(2, col);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Deletes expired cells (a tombstone expires after gc_grace_seconds). */
    int sweep(String host) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_cql_cells WHERE expires_at IS NOT NULL AND expires_at <= now()")) {
                return ps.executeUpdate();
            }
        });
    }

    long count(String host) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_cql_cells"); ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        });
    }
}
