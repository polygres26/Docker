package com.sayonora.warp.cqlwire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Assembly of stored cells into rows, and helpers over encoded keys. */
final class Rows {

    private Rows() {
    }

    /** One CQL row: values aligned with the table's column list. */
    static final class RowData {
        Object[] vals;
        long[] ts;
        long[] ttl;
        long token;
        byte[] pk;
        byte[] ck;
        boolean hasRegular;
        /** No live cell: the row is deleted (row tombstone) or only holds cell tombstones. Such rows are skipped by readers. */
        boolean dead;
        /** Per element write times / TTLs (seconds, -1 = none) of non-frozen collection columns, in element order. */
        long[][] elemTs;
        long[][] elemTtl;
    }

    /** Column name of the row tombstone cell written by a DELETE that carries an explicit timestamp. */
    static final String ROW_TOMB = "\u0001row";
    /** Suffix of the column name of a collection tombstone cell (the range tombstone written when a collection is overwritten or deleted). */
    static final String CTOMB = "\u0002";
    /** Column name of a range tombstone cell: ck = start (inclusive), path = end (exclusive, empty = to the end), ts = deletion time. */
    static final String RT_COL = "\u0003rt";

    static final byte[] STATIC_CK = new byte[0];
    static final byte[] SINGLE_CK = {1};

    static String pkKey(long token, byte[] pk) {
        return token + ":" + CqlType.hex(pk);
    }

    /** Lexicographic successor of a prefix: the smallest byte string greater than every string starting with it, or null if none. */
    static byte[] succ(byte[] p) {
        int n = p.length;
        while (n > 0 && p[n - 1] == (byte) 0xFF) {
            n--;
        }
        if (n == 0) {
            return null;
        }
        byte[] r = Arrays.copyOf(p, n);
        r[n - 1]++;
        return r;
    }

    /** The cells of one row (or of a partition's static row) that are alive: not shadowed by a row tombstone or a collection tombstone. */
    static List<CqlStore.Rec> live(List<CqlStore.Rec> group) {
        return live(group, Long.MIN_VALUE);
    }

    static List<CqlStore.Rec> live(List<CqlStore.Rec> group, long rangeTomb) {
        long tomb = rangeTomb;
        Map<String, Long> ctomb = null;
        for (CqlStore.Rec c : group) {
            if (c.col.equals(ROW_TOMB)) {
                tomb = Math.max(tomb, c.ts);
            } else if (c.col.endsWith(CTOMB)) {
                if (ctomb == null) {
                    ctomb = new java.util.HashMap<>();
                }
                ctomb.merge(c.col.substring(0, c.col.length() - 1), c.ts, Math::max);
            }
        }
        List<CqlStore.Rec> live = new ArrayList<>(group.size());
        for (CqlStore.Rec c : group) {
            if (c.col.equals(ROW_TOMB) || c.col.equals(RT_COL) || c.col.endsWith(CTOMB) || c.val == null || c.ts <= tomb) {
                continue;
            }
            if (ctomb != null && ctomb.containsKey(c.col) && c.ts <= ctomb.get(c.col)) {
                continue;
            }
            live.add(c);
        }
        return live;
    }

    private static void applyCells(Schema.Table t, RowData r, List<CqlStore.Rec> cells) {
        Map<Integer, TreeMap<byte[], CqlStore.Rec>> multi = new TreeMap<>();
        for (CqlStore.Rec c : cells) {
            if (c.col.isEmpty()) {
                r.hasRegular = true;
                continue;
            }
            if (c.val == null || c.col.equals(ROW_TOMB)) {
                continue;
            }
            Schema.Column col = t.col(c.col);
            if (col == null) {
                continue;
            }
            if (col.kind == Schema.Kind.REGULAR) {
                r.hasRegular = true;
            }
            int o = col.ordinal;
            if (col.type.isMultiCell()) {
                multi.computeIfAbsent(o, k -> new TreeMap<>(Arrays::compareUnsigned)).put(c.path, c);
                r.ts[o] = Math.max(r.ts[o], c.ts);
            } else {
                r.vals[o] = col.type.deserialize(c.val);
                r.ts[o] = c.ts;
                r.ttl[o] = c.ttl;
            }
        }
        for (var e : multi.entrySet()) {
            Schema.Column col = t.columns.get(e.getKey());
            CqlType et = col.type.args.get(0);
            List<Object[]> elems = new ArrayList<>(); // {element object, value object or null, rec}
            for (var en : e.getValue().entrySet()) {
                CqlStore.Rec rec = en.getValue();
                switch (col.type.k) {
                    case LIST -> elems.add(new Object[] {et.deserialize(rec.val), null, rec});
                    case SET -> elems.add(new Object[] {et.deserialize(en.getKey()), null, rec});
                    default -> elems.add(new Object[] {et.deserialize(en.getKey()), col.type.args.get(1).deserialize(rec.val), rec});
                }
            }
            if (col.type.k != CqlType.K.LIST) {
                elems.sort((x, y) -> et.compare(x[0], y[0]));
            }
            long[] ets = new long[elems.size()], ettl = new long[elems.size()];
            for (int i = 0; i < ets.length; i++) {
                CqlStore.Rec rec = (CqlStore.Rec) elems.get(i)[2];
                ets[i] = rec.ts;
                ettl[i] = rec.ttl;
            }
            if (r.elemTs == null) {
                r.elemTs = new long[r.vals.length][];
                r.elemTtl = new long[r.vals.length][];
            }
            r.elemTs[e.getKey()] = ets;
            r.elemTtl[e.getKey()] = ettl;
            switch (col.type.k) {
                case LIST -> {
                    List<Object> l = new ArrayList<>();
                    elems.forEach(x -> l.add(x[0]));
                    r.vals[e.getKey()] = l;
                }
                case SET -> {
                    TreeSet<Object> s = new TreeSet<>(et);
                    elems.forEach(x -> s.add(x[0]));
                    r.vals[e.getKey()] = s;
                }
                default -> {
                    TreeMap<Object, Object> m = new TreeMap<>(et);
                    elems.forEach(x -> m.put(x[0], x[1]));
                    r.vals[e.getKey()] = m;
                }
            }
        }
    }

    private static RowData blank(Schema.Table t, long token, byte[] pk) {
        RowData r = new RowData();
        r.vals = new Object[t.columns.size()];
        r.ts = new long[r.vals.length];
        r.ttl = new long[r.vals.length];
        Arrays.fill(r.ttl, -1);
        r.token = token;
        r.pk = pk;
        Object[] pv = t.partitionValues(pk);
        for (int i = 0; i < pv.length; i++) {
            r.vals[t.partition.get(i).ordinal] = pv[i];
        }
        return r;
    }

    /**
     * Rows from cells ordered by (token, pk, ck, col, path). {@code statics} holds the static-row cells of the partitions involved
     * (may also be contained in {@code recs} for static-only partitions).
     */
    static List<RowData> assemble(Schema.Table t, List<CqlStore.Rec> recs, List<CqlStore.Rec> statics) {
        return assemble(t, recs, statics, null);
    }

    /** Deletion time of the range tombstones of a partition that cover clustering key {@code ck} (Long.MIN_VALUE when none). */
    private static long covering(List<CqlStore.Rec> rts, byte[] ck) {
        long ts = Long.MIN_VALUE;
        if (rts != null) {
            for (CqlStore.Rec rt : rts) {
                boolean fromOk = Arrays.compareUnsigned(ck, rt.ck) >= 0;
                boolean toOk = rt.val == null || rt.val.length == 0 || Arrays.compareUnsigned(ck, rt.path) < 0;
                if (fromOk && toOk) {
                    ts = Math.max(ts, rt.ts);
                }
            }
        }
        return ts;
    }

    static List<RowData> assemble(Schema.Table t, List<CqlStore.Rec> recs, List<CqlStore.Rec> statics, List<CqlStore.Rec> rangeTombstones) {
        Map<String, List<CqlStore.Rec>> rtBy = new java.util.HashMap<>();
        if (rangeTombstones != null) {
            for (CqlStore.Rec rt : rangeTombstones) {
                rtBy.computeIfAbsent(pkKey(rt.token, rt.pk), k -> new ArrayList<>()).add(rt);
            }
        }
        Map<String, List<CqlStore.Rec>> staticBy = new java.util.HashMap<>();
        if (statics != null) {
            for (CqlStore.Rec s : statics) {
                staticBy.computeIfAbsent(pkKey(s.token, s.pk), k -> new ArrayList<>()).add(s);
            }
        }
        List<RowData> out = new ArrayList<>();
        int i = 0;
        while (i < recs.size()) {
            CqlStore.Rec first = recs.get(i);
            int j = i;
            while (j < recs.size() && recs.get(j).token == first.token && Arrays.equals(recs.get(j).pk, first.pk)
                    && Arrays.equals(recs.get(j).ck, first.ck)) {
                j++;
            }
            List<CqlStore.Rec> group = recs.subList(i, j);
            RowData r = blank(t, first.token, first.pk);
            r.ck = first.ck;
            boolean isStaticRow = first.ck.length == 0;
            byte[] ckvBytes = first.ckv;
            for (CqlStore.Rec g : group) {
                if (g.ckv != null && g.ckv.length > 0) {
                    ckvBytes = g.ckv;
                    break;
                }
            }
            if (!isStaticRow && ckvBytes.length > 0) {
                Object[] cv = t.clusteringFromValues(ckvBytes);
                for (int k = 0; k < cv.length; k++) {
                    r.vals[t.clustering.get(k).ordinal] = cv[k];
                }
            }
            List<CqlStore.Rec> rts = rtBy.get(pkKey(first.token, first.pk));
            List<CqlStore.Rec> st = staticBy.get(pkKey(first.token, first.pk));
            if (st != null && !isStaticRow) {
                applyCells(t, r, live(st, covering(rts, STATIC_CK)));
                r.hasRegular = false;
            }
            List<CqlStore.Rec> live = live(group, covering(rts, first.ck));
            r.dead = live.isEmpty();
            applyCells(t, r, live);
            out.add(r);
            i = j;
        }
        return out;
    }
}
