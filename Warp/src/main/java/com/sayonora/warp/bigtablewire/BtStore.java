package com.sayonora.warp.bigtablewire;

import com.google.common.hash.Hashing;
import com.sayonora.warp.bigtablewire.admin.v2.GcRule;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** All SQL of the Bigtable store: the table catalog (home host) and the cells (row-hash shards). */
final class BtStore {

    private final BtShards shards;

    BtStore(BtShards shards) {
        this.shards = shards;
    }

    BtShards shards() {
        return shards;
    }

    // ---------------------------------------------------------------- catalog (home host)

    byte[] tableSpec(String home, String name) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT spec FROM warp_bt_tables WHERE name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getBytes(1) : null;
                }
            }
        });
    }

    /** False when the table exists already. */
    boolean insertTable(String home, String name, String parent, byte[] spec) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO warp_bt_tables (name, parent, spec) VALUES (?, ?, ?) ON CONFLICT (name) DO NOTHING")) {
                ps.setString(1, name);
                ps.setString(2, parent);
                ps.setBytes(3, spec);
                return ps.executeUpdate() == 1;
            }
        });
    }

    @FunctionalInterface
    interface SpecUpdate {
        byte[] apply(byte[] current);
    }

    /** Read-modify-write of a table's spec under a row lock; null when the table does not exist. */
    byte[] updateSpec(String home, String name, SpecUpdate fn) {
        return shards.tx(home, c -> {
            byte[] cur;
            try (PreparedStatement ps = c.prepareStatement("SELECT spec FROM warp_bt_tables WHERE name = ? FOR UPDATE")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    cur = rs.getBytes(1);
                }
            }
            byte[] next = fn.apply(cur);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_bt_tables SET spec = ? WHERE name = ?")) {
                ps.setBytes(1, next);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return next;
        });
    }

    boolean deleteTable(String home, String name) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_tables WHERE name = ?")) {
                ps.setString(1, name);
                return ps.executeUpdate() == 1;
            }
        });
    }

    /** (name, spec) of every table of the parent, ordered by name, starting after {@code afterName} (may be null). */
    List<byte[][]> listTables(String home, String parent, String afterName, int limit) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name, spec FROM warp_bt_tables WHERE parent = ? "
                    + (afterName == null ? "" : "AND name > ? ") + "ORDER BY name LIMIT ?")) {
                int i = 1;
                ps.setString(i++, parent);
                if (afterName != null) {
                    ps.setString(i++, afterName);
                }
                ps.setInt(i, limit);
                List<byte[][]> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new byte[][] {rs.getString(1).getBytes(StandardCharsets.UTF_8), rs.getBytes(2)});
                    }
                }
                return out;
            }
        });
    }

    /** (name, spec) of every table whose catalog row is on this host. */
    List<byte[][]> listAllTables(String home) {
        return shards.conn(home, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name, spec FROM warp_bt_tables ORDER BY name");
                    ResultSet rs = ps.executeQuery()) {
                List<byte[][]> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new byte[][] {rs.getString(1).getBytes(StandardCharsets.UTF_8), rs.getBytes(2)});
                }
                return out;
            }
        });
    }

    // ---------------------------------------------------------------- bulk deletes (every shard)

    void deleteTableCells(String host, String table) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ?")) {
                ps.setString(1, table);
                ps.executeUpdate();
            }
            return null;
        });
    }

    void deleteFamilyCells(String host, String table, String family) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ? AND family = ?")) {
                ps.setString(1, table);
                ps.setString(2, family);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** Deletes the rows in [start, endExclusive) (null = unbounded). */
    void dropRange(String host, String table, byte[] start, byte[] endExclusive) {
        shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ?"
                    + (start == null ? "" : " AND row_key >= ?") + (endExclusive == null ? "" : " AND row_key < ?"))) {
                int i = 1;
                ps.setString(i++, table);
                if (start != null) {
                    ps.setBytes(i++, start);
                }
                if (endExclusive != null) {
                    ps.setBytes(i, endExclusive);
                }
                ps.executeUpdate();
            }
            return null;
        });
    }

    // ---------------------------------------------------------------- single-row primitives (inside a transaction)

    /** Serialises writers of one row (MutateRow, CheckAndMutateRow, ReadModifyWriteRow) on its host. */
    static long lockKey(String table, byte[] rowKey) {
        return Hashing.murmur3_128().newHasher().putString(table, StandardCharsets.UTF_8).putByte((byte) 0).putBytes(rowKey).hash().asLong();
    }

    static void lock(Connection c, long key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            ps.setLong(1, key);
            ps.execute();
        }
    }

    static List<BtCell> loadRow(Connection c, String table, byte[] rowKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT family, qual, ts, val FROM warp_bt_cells WHERE tbl = ? AND row_key = ? "
                + "ORDER BY family, qual, ts DESC")) {
            ps.setString(1, table);
            ps.setBytes(2, rowKey);
            List<BtCell> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new BtCell(rs.getString(1), rs.getBytes(2), rs.getLong(3), rs.getBytes(4), BtCell.NO_LABELS));
                }
            }
            return out;
        }
    }

    static void putCell(Connection c, String table, byte[] rowKey, String family, byte[] qual, long ts, byte[] value) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_bt_cells (tbl, row_key, family, qual, ts, val) VALUES (?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (tbl, row_key, family, qual, ts) DO UPDATE SET val = EXCLUDED.val")) {
            ps.setString(1, table);
            ps.setBytes(2, rowKey);
            ps.setString(3, family);
            ps.setBytes(4, qual);
            ps.setLong(5, ts);
            ps.setBytes(6, value);
            ps.executeUpdate();
        }
    }

    /** Deletes the cells of a column with start <= ts < end (end 0 = no upper bound). */
    static void deleteColumn(Connection c, String table, byte[] rowKey, String family, byte[] qual, long start, long end) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ? AND row_key = ? AND family = ? AND qual = ? "
                + "AND ts >= ?" + (end == 0 ? "" : " AND ts < ?"))) {
            ps.setString(1, table);
            ps.setBytes(2, rowKey);
            ps.setString(3, family);
            ps.setBytes(4, qual);
            ps.setLong(5, start);
            if (end != 0) {
                ps.setLong(6, end);
            }
            ps.executeUpdate();
        }
    }

    static void deleteFamily(Connection c, String table, byte[] rowKey, String family) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ? AND row_key = ? AND family = ?")) {
            ps.setString(1, table);
            ps.setBytes(2, rowKey);
            ps.setString(3, family);
            ps.executeUpdate();
        }
    }

    static void deleteRow(Connection c, String table, byte[] rowKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE tbl = ? AND row_key = ?")) {
            ps.setString(1, table);
            ps.setBytes(2, rowKey);
            ps.executeUpdate();
        }
    }

    // ---------------------------------------------------------------- scans

    /** A key segment of a scan: a range [start, endExclusive) (null = unbounded) or an explicit batch of row keys. */
    record Seg(byte[] start, byte[] endExclusive, List<byte[]> keys) {
        static Seg range(byte[] s, byte[] e) {
            return new Seg(s, e, null);
        }

        static Seg keys(List<byte[]> k) {
            return new Seg(null, null, k);
        }
    }

    /**
     * Pages through the rows of one shard in key order over a list of disjoint segments (already in scan order). A page is one
     * short query (a pooled connection is borrowed for it only); a row is only handed out once all its cells were read.
     */
    final class ShardScan {
        private final String host;
        private final String table;
        private final List<Seg> segs;
        private final boolean reversed;
        private final int pageCells;
        private int segIdx;
        private Object[] cursor; // last (row_key, family, qual, ts) read of the current segment
        private final List<BtCell> pending = new ArrayList<>();
        private byte[] pendingKey;
        private final java.util.ArrayDeque<BtRow> ready = new java.util.ArrayDeque<>();

        ShardScan(String host, String table, List<Seg> segs, boolean reversed, int pageCells) {
            this.host = host;
            this.table = table;
            this.segs = segs;
            this.reversed = reversed;
            this.pageCells = pageCells;
        }

        BtRow peek() {
            fill();
            return ready.peekFirst();
        }

        BtRow poll() {
            fill();
            return ready.pollFirst();
        }

        private void fill() {
            while (ready.isEmpty() && (segIdx < segs.size() || pendingKey != null)) {
                if (segIdx >= segs.size()) {
                    flushPending();
                    return;
                }
                fetchPage();
            }
        }

        private void flushPending() {
            if (pendingKey != null) {
                pending.sort(BtCell.ORDER);
                ready.add(new BtRow(pendingKey, new ArrayList<>(pending)));
                pending.clear();
                pendingKey = null;
            }
        }

        private void fetchPage() {
            Seg seg = segs.get(segIdx);
            StringBuilder sql = new StringBuilder("SELECT row_key, family, qual, ts, val FROM warp_bt_cells WHERE tbl = ?");
            if (seg.keys() != null) {
                sql.append(" AND row_key = ANY(?)");
            } else {
                if (seg.start() != null) {
                    sql.append(" AND row_key >= ?");
                }
                if (seg.endExclusive() != null) {
                    sql.append(" AND row_key < ?");
                }
            }
            if (cursor != null) {
                sql.append(reversed ? " AND (row_key, family, qual, ts) < (?, ?, ?, ?)" : " AND (row_key, family, qual, ts) > (?, ?, ?, ?)");
            }
            sql.append(reversed ? " ORDER BY row_key DESC, family DESC, qual DESC, ts DESC LIMIT ?" : " ORDER BY row_key, family, qual, ts LIMIT ?");
            List<Object[]> page = new ArrayList<>();
            shards.conn(host, c -> {
                try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                    int i = 1;
                    ps.setString(i++, table);
                    if (seg.keys() != null) {
                        Array a = c.createArrayOf("bytea", seg.keys().toArray(new byte[0][]));
                        ps.setArray(i++, a);
                    } else {
                        if (seg.start() != null) {
                            ps.setBytes(i++, seg.start());
                        }
                        if (seg.endExclusive() != null) {
                            ps.setBytes(i++, seg.endExclusive());
                        }
                    }
                    if (cursor != null) {
                        ps.setBytes(i++, (byte[]) cursor[0]);
                        ps.setString(i++, (String) cursor[1]);
                        ps.setBytes(i++, (byte[]) cursor[2]);
                        ps.setLong(i++, (Long) cursor[3]);
                    }
                    ps.setInt(i, pageCells);
                    ps.setFetchSize(pageCells);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            page.add(new Object[] {rs.getBytes(1), rs.getString(2), rs.getBytes(3), rs.getLong(4), rs.getBytes(5)});
                        }
                    }
                }
                return null;
            });
            for (Object[] r : page) {
                byte[] key = (byte[]) r[0];
                if (pendingKey != null && !Arrays.equals(pendingKey, key)) {
                    flushPending();
                }
                pendingKey = key;
                pending.add(new BtCell((String) r[1], (byte[]) r[2], (Long) r[3], (byte[]) r[4], BtCell.NO_LABELS));
            }
            if (page.size() < pageCells) {
                // segment exhausted: its last row is complete
                segIdx++;
                cursor = null;
                flushPending();
            } else {
                Object[] last = page.get(page.size() - 1);
                cursor = new Object[] {last[0], last[1], last[2], last[3]};
            }
        }
    }

    ShardScan scan(String host, String table, List<Seg> segs, boolean reversed, int pageCells) {
        return new ShardScan(host, table, segs, reversed, pageCells);
    }

    /** (row key, approximate stored bytes) of the table's rows on one host in key order, {@code after} exclusive, up to limit rows. */
    List<Object[]> rowSizes(String host, String table, byte[] after, int limit) {
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT row_key, sum(octet_length(val) + octet_length(qual) + octet_length(family) "
                    + "+ octet_length(row_key) + 8) FROM warp_bt_cells WHERE tbl = ?" + (after == null ? "" : " AND row_key > ?")
                    + " GROUP BY row_key ORDER BY row_key LIMIT ?")) {
                int i = 1;
                ps.setString(i++, table);
                if (after != null) {
                    ps.setBytes(i++, after);
                }
                ps.setInt(i, limit);
                List<Object[]> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Object[] {rs.getBytes(1), rs.getLong(2)});
                    }
                }
                return out;
            }
        });
    }

    // ---------------------------------------------------------------- garbage collection

    /** Applies a family's GC rule to every cell of the table on one host; returns the cells removed. */
    int gc(String host, String table, String family, GcRule rule, long nowMicros) {
        List<Object> params = new ArrayList<>();
        String expr = gcExpr(rule, nowMicros, params);
        if (expr == null) {
            return 0;
        }
        return shards.conn(host, c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_bt_cells WHERE ctid IN (SELECT ctid FROM ("
                    + "SELECT ctid, ts, row_number() OVER (PARTITION BY row_key, qual ORDER BY ts DESC) AS rn FROM warp_bt_cells "
                    + "WHERE tbl = ? AND family = ?) x WHERE " + expr + ")")) {
                int i = 1;
                ps.setString(i++, table);
                ps.setString(i++, family);
                for (Object p : params) {
                    ps.setLong(i++, (Long) p);
                }
                return ps.executeUpdate();
            }
        });
    }

    /** SQL predicate "this cell is garbage": union deletes when ANY rule says so, intersection when ALL do. */
    private static String gcExpr(GcRule r, long nowMicros, List<Object> params) {
        switch (r.getRuleCase()) {
            case MAX_NUM_VERSIONS:
                return "rn > " + Math.max(0, r.getMaxNumVersions());
            case MAX_AGE: {
                long micros = r.getMaxAge().getSeconds() * 1_000_000L + r.getMaxAge().getNanos() / 1000;
                params.add(nowMicros - micros);
                return "ts < ?";
            }
            case UNION:
            case INTERSECTION: {
                boolean union = r.getRuleCase() == GcRule.RuleCase.UNION;
                List<GcRule> subs = union ? r.getUnion().getRulesList() : r.getIntersection().getRulesList();
                List<String> parts = new ArrayList<>();
                for (GcRule s : subs) {
                    String e = gcExpr(s, nowMicros, params);
                    if (e != null) {
                        parts.add("(" + e + ")");
                    }
                }
                if (parts.isEmpty()) {
                    return null;
                }
                return String.join(union ? " OR " : " AND ", parts);
            }
            default:
                return null;
        }
    }
}
