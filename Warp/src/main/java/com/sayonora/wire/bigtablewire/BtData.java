package com.sayonora.wire.bigtablewire;

import com.google.protobuf.BytesValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.sayonora.wire.bigtablewire.admin.v2.Table;
import com.sayonora.wire.bigtablewire.v2.CheckAndMutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.CheckAndMutateRowResponse;
import com.sayonora.wire.bigtablewire.v2.MutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowResponse;
import com.sayonora.wire.bigtablewire.v2.MutateRowsRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowsResponse;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRowRequest;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRowResponse;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRule;
import com.sayonora.wire.bigtablewire.v2.ReadRowsRequest;
import com.sayonora.wire.bigtablewire.v2.ReadRowsResponse;
import com.sayonora.wire.bigtablewire.v2.RowRange;
import com.sayonora.wire.bigtablewire.v2.RowSet;
import com.sayonora.wire.bigtablewire.v2.SampleRowKeysRequest;
import com.sayonora.wire.bigtablewire.v2.SampleRowKeysResponse;
import io.grpc.stub.ServerCallStreamObserver;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * The Bigtable data API over the Postgres store: ReadRows (row set, filter, limit, reversed; streamed page by page with the
 * pooled connection borrowed only for each page), MutateRow / MutateRows / CheckAndMutateRow / ReadModifyWriteRow (one row = one
 * local transaction serialised by a per-row advisory lock) and SampleRowKeys. Rows are sharded by hash of table name and row key,
 * so multi-row reads merge the shards' key-ordered streams.
 */
final class BtData {

    private static final int MAX_VALUE_CHUNK = 1 << 20;
    private static final int POINT_BATCH = 500;

    private final BtStore store;
    private final BtAdmin admin;
    private final BtShards shards;
    private final BtConfig cfg;

    BtData(BtStore store, BtAdmin admin, BtConfig cfg) {
        this.store = store;
        this.admin = admin;
        this.shards = store.shards();
        this.cfg = cfg;
    }

    private static String tableName(String tableName, String viewName) {
        if (tableName.isEmpty() && !viewName.isEmpty()) {
            throw BtException.unimplemented("authorized views are not supported");
        }
        return tableName;
    }

    private static long nowMicros() {
        return System.currentTimeMillis() * 1000L;
    }

    // =============================================================== ReadRows

    /** A key interval [lo, hi) with null meaning unbounded. */
    record Iv(byte[] lo, byte[] hi) {
        boolean point() {
            return lo != null && hi != null && hi.length == lo.length + 1 && hi[lo.length] == 0
                    && Arrays.equals(hi, 0, lo.length, lo, 0, lo.length);
        }
    }

    private static byte[] plusZero(byte[] k) {
        return Arrays.copyOf(k, k.length + 1);
    }

    /** Validates the row set and reduces it to sorted, disjoint intervals; null = the whole table. */
    static List<Iv> intervals(RowSet rs) {
        if (rs.getRowKeysCount() == 0 && rs.getRowRangesCount() == 0) {
            return null;
        }
        List<Iv> l = new ArrayList<>();
        int idx = 0;
        for (RowRange r : rs.getRowRangesList()) {
            byte[] s = switch (r.getStartKeyCase()) {
                case START_KEY_CLOSED -> r.getStartKeyClosed().toByteArray();
                case START_KEY_OPEN -> r.getStartKeyOpen().toByteArray();
                default -> new byte[0];
            };
            byte[] e = switch (r.getEndKeyCase()) {
                case END_KEY_CLOSED -> r.getEndKeyClosed().toByteArray();
                case END_KEY_OPEN -> r.getEndKeyOpen().toByteArray();
                default -> new byte[0];
            };
            if (s.length > 0 && e.length > 0 && Arrays.compareUnsigned(s, e) > 0) {
                throw BtException.invalid("Error in element #" + idx + ": "
                        + (r.getStartKeyCase() == RowRange.StartKeyCase.START_KEY_OPEN ? "start_key_open" : "start_key_closed")
                        + " must be less than "
                        + (r.getEndKeyCase() == RowRange.EndKeyCase.END_KEY_CLOSED ? "end_key_closed" : "end_key_open"));
            }
            byte[] lo = s.length == 0 ? null : (r.getStartKeyCase() == RowRange.StartKeyCase.START_KEY_OPEN ? plusZero(s) : s);
            byte[] hi = e.length == 0 ? null : (r.getEndKeyCase() == RowRange.EndKeyCase.END_KEY_CLOSED ? plusZero(e) : e);
            if (lo == null || hi == null || Arrays.compareUnsigned(lo, hi) < 0) {
                l.add(new Iv(lo, hi));
            }
            idx++;
        }
        for (ByteString k : rs.getRowKeysList()) {
            if (!k.isEmpty()) {
                byte[] kb = k.toByteArray();
                l.add(new Iv(kb, plusZero(kb)));
            }
        }
        l.sort((a, b) -> a.lo == null ? (b.lo == null ? 0 : -1) : b.lo == null ? 1 : Arrays.compareUnsigned(a.lo, b.lo));
        List<Iv> out = new ArrayList<>();
        for (Iv iv : l) {
            if (!out.isEmpty()) {
                Iv cur = out.get(out.size() - 1);
                if (cur.hi == null || (iv.lo != null && Arrays.compareUnsigned(iv.lo, cur.hi) <= 0)) {
                    byte[] hi = cur.hi == null || iv.hi == null ? null : (Arrays.compareUnsigned(iv.hi, cur.hi) > 0 ? iv.hi : cur.hi);
                    out.set(out.size() - 1, new Iv(cur.lo, hi));
                    continue;
                }
            }
            out.add(iv);
        }
        return out;
    }

    private List<BtStore.ShardScan> scans(String table, List<String> hosts, List<Iv> ivs, boolean reversed) {
        List<BtStore.ShardScan> out = new ArrayList<>();
        for (String host : hosts) {
            List<BtStore.Seg> segs = new ArrayList<>();
            if (ivs == null) {
                segs.add(BtStore.Seg.range(null, null));
            } else {
                List<byte[]> batch = new ArrayList<>();
                for (Iv iv : ivs) {
                    if (iv.point()) {
                        if (shards.owner(hosts, table, iv.lo).equals(host)) {
                            batch.add(iv.lo);
                            if (batch.size() >= POINT_BATCH) {
                                segs.add(BtStore.Seg.keys(batch));
                                batch = new ArrayList<>();
                            }
                        }
                    } else {
                        if (!batch.isEmpty()) {
                            segs.add(BtStore.Seg.keys(batch));
                            batch = new ArrayList<>();
                        }
                        segs.add(BtStore.Seg.range(iv.lo, iv.hi));
                    }
                }
                if (!batch.isEmpty()) {
                    segs.add(BtStore.Seg.keys(batch));
                }
            }
            if (segs.isEmpty()) {
                continue;
            }
            if (reversed) {
                java.util.Collections.reverse(segs);
            }
            out.add(store.scan(host, table, segs, reversed, cfg.scanPageCells));
        }
        return out;
    }

    void readRows(ReadRowsRequest req, ServerCallStreamObserver<ReadRowsResponse> out) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        Table table = admin.table(name);
        BtFilter.Node filter = BtFilter.compile(req.hasFilter() ? req.getFilter() : null);
        long limit = req.getRowsLimit();
        if (limit < 0) {
            throw BtException.invalid("Error in field 'rows_limit' : rows_limit must be >= 0");
        }
        List<Iv> ivs = intervals(req.getRows());
        boolean reversed = req.getReversed();
        List<String> hosts = admin.hostsOf(name);
        Comparator<byte[]> cmp = reversed ? (a, b) -> Arrays.compareUnsigned(b, a) : Arrays::compareUnsigned;
        List<BtStore.ShardScan> sc = scans(name, hosts, ivs, reversed);
        PriorityQueue<BtStore.ShardScan> pq = new PriorityQueue<>((a, b) -> cmp.compare(a.peek().key(), b.peek().key()));
        for (BtStore.ShardScan s : sc) {
            if (s.peek() != null) {
                pq.add(s);
            }
        }
        ChunkWriter w = new ChunkWriter(out, cfg.responseBytes);
        long sent = 0;
        while (!pq.isEmpty() && (limit == 0 || sent < limit)) {
            if (out.isCancelled()) {
                return;
            }
            BtStore.ShardScan s = pq.poll();
            BtRow row = s.poll();
            if (s.peek() != null) {
                pq.add(s);
            }
            BtRow live = liveCells(table, row);
            BtRow res = live == null ? null : BtFilter.apply(filter, live);
            if (res != null) {
                if (!w.row(res)) {
                    return;
                }
                sent++;
            }
        }
        w.finish();
        out.onCompleted();
    }

    /** Cells of families that no longer exist in the table are invisible (a dropped family is deleted asynchronously of readers). */
    private static BtRow liveCells(Table table, BtRow row) {
        boolean all = true;
        for (BtCell c : row.cells()) {
            if (!table.containsColumnFamilies(c.family())) {
                all = false;
                break;
            }
        }
        if (all) {
            return row;
        }
        List<BtCell> keep = new ArrayList<>();
        for (BtCell c : row.cells()) {
            if (table.containsColumnFamilies(c.family())) {
                keep.add(c);
            }
        }
        return keep.isEmpty() ? null : new BtRow(row.key(), keep);
    }

    /** Builds ReadRowsResponse messages of CellChunks (row key on a row's first chunk, family/qualifier only when they change). */
    private static final class ChunkWriter {
        private final ServerCallStreamObserver<ReadRowsResponse> out;
        private final int limit;
        private ReadRowsResponse.Builder cur = ReadRowsResponse.newBuilder();
        private int size;

        ChunkWriter(ServerCallStreamObserver<ReadRowsResponse> out, int limit) {
            this.out = out;
            this.limit = limit;
        }

        /** False when the client went away. */
        boolean row(BtRow r) {
            String fam = null;
            byte[] qual = null;
            List<BtCell> cells = r.cells();
            for (int i = 0; i < cells.size(); i++) {
                BtCell c = cells.get(i);
                boolean last = i == cells.size() - 1;
                ReadRowsResponse.CellChunk.Builder ch = ReadRowsResponse.CellChunk.newBuilder();
                if (i == 0) {
                    ch.setRowKey(ByteString.copyFrom(r.key()));
                }
                if (!c.family().equals(fam)) {
                    ch.setFamilyName(StringValue.of(c.family()));
                    ch.setQualifier(BytesValue.of(ByteString.copyFrom(c.qual())));
                } else if (!Arrays.equals(c.qual(), qual)) {
                    ch.setQualifier(BytesValue.of(ByteString.copyFrom(c.qual())));
                }
                fam = c.family();
                qual = c.qual();
                ch.setTimestampMicros(c.ts());
                ch.addAllLabels(c.labels());
                byte[] v = c.value();
                if (v.length <= MAX_VALUE_CHUNK) {
                    ch.setValue(ByteString.copyFrom(v));
                    if (last) {
                        ch.setCommitRow(true);
                    }
                    if (!add(ch.build())) {
                        return false;
                    }
                } else {
                    int off = 0;
                    boolean firstPiece = true;
                    while (off < v.length) {
                        int n = Math.min(MAX_VALUE_CHUNK, v.length - off);
                        ReadRowsResponse.CellChunk.Builder p = firstPiece ? ch : ReadRowsResponse.CellChunk.newBuilder();
                        p.setValue(ByteString.copyFrom(v, off, n));
                        off += n;
                        if (off < v.length) {
                            p.setValueSize(v.length);
                        } else if (last) {
                            p.setCommitRow(true);
                        }
                        firstPiece = false;
                        if (!add(p.build())) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        private boolean add(ReadRowsResponse.CellChunk ch) {
            cur.addChunks(ch);
            size += ch.getSerializedSize();
            return size < limit || flush();
        }

        private boolean flush() {
            if (cur.getChunksCount() == 0) {
                return true;
            }
            ReadRowsResponse m = cur.build();
            cur = ReadRowsResponse.newBuilder();
            size = 0;
            return send(out, m);
        }

        void finish() {
            flush();
        }
    }

    /** onNext honouring gRPC flow control; false when the call was cancelled. */
    static <T> boolean send(ServerCallStreamObserver<T> out, T msg) {
        while (!out.isReady()) {
            if (out.isCancelled()) {
                return false;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        if (out.isCancelled()) {
            return false;
        }
        out.onNext(msg);
        return true;
    }

    // =============================================================== mutations

    private static void requireKey(ByteString key) {
        if (key.isEmpty()) {
            throw BtException.invalid("Row keys must be non-empty");
        }
    }

    MutateRowResponse mutateRow(MutateRowRequest req) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        Table table = admin.table(name);
        requireKey(req.getRowKey());
        if (req.getMutationsCount() == 0) {
            throw BtException.invalid("No mutations provided");
        }
        List<BtMutations.Op> ops = BtMutations.compile(table, req.getMutationsList(), nowMicros());
        byte[] key = req.getRowKey().toByteArray();
        String host = shards.owner(admin.hostsOf(name), name, key);
        shards.tx(host, c -> {
            BtStore.lock(c, BtStore.lockKey(name, key));
            BtMutations.apply(c, name, key, ops);
            return null;
        });
        return MutateRowResponse.getDefaultInstance();
    }

    MutateRowsResponse mutateRows(MutateRowsRequest req) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        Table table = admin.table(name);
        int n = req.getEntriesCount();
        if (n == 0) {
            throw BtException.invalid("No mutations provided");
        }
        long now = nowMicros();
        List<BtException> err = new ArrayList<>(java.util.Collections.nCopies(n, null));
        List<List<BtMutations.Op>> ops = new ArrayList<>(java.util.Collections.nCopies(n, null));
        List<String> hosts = admin.hostsOf(name);
        Map<String, List<Integer>> byHost = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            MutateRowsRequest.Entry e = req.getEntries(i);
            if (e.getRowKey().isEmpty()) {
                err.set(i, new BtException(io.grpc.Status.Code.INTERNAL, "rpc error: code = InvalidArgument desc = Row keys must be non-empty"));
                continue;
            }
            try {
                ops.set(i, BtMutations.compile(table, e.getMutationsList(), now));
            } catch (BtException ex) {
                err.set(i, new BtException(io.grpc.Status.Code.INTERNAL, ex.getMessage()));
                continue;
            }
            if (!ops.get(i).isEmpty()) {
                byHost.computeIfAbsent(shards.owner(hosts, name, e.getRowKey().toByteArray()), h -> new ArrayList<>()).add(i);
            }
        }
        for (Map.Entry<String, List<Integer>> h : byHost.entrySet()) {
            List<Integer> idx = h.getValue();
            try {
                shards.tx(h.getKey(), c -> {
                    long[] locks = idx.stream().mapToLong(i -> BtStore.lockKey(name, req.getEntries(i).getRowKey().toByteArray())).sorted().distinct().toArray();
                    for (long l : locks) {
                        BtStore.lock(c, l);
                    }
                    for (int i : idx) {
                        BtMutations.apply(c, name, req.getEntries(i).getRowKey().toByteArray(), ops.get(i));
                    }
                    return null;
                });
            } catch (BtException ex) {
                for (int i : idx) {
                    err.set(i, ex);
                }
            }
        }
        MutateRowsResponse.Builder out = MutateRowsResponse.newBuilder();
        for (int i = 0; i < n; i++) {
            com.google.rpc.Status.Builder st = com.google.rpc.Status.newBuilder();
            if (err.get(i) != null) {
                st.setCode(err.get(i).code.value()).setMessage(err.get(i).getMessage());
            }
            out.addEntries(MutateRowsResponse.Entry.newBuilder().setIndex(i).setStatus(st));
        }
        return out.build();
    }

    CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest req) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        Table table = admin.table(name);
        requireKey(req.getRowKey());
        BtFilter.Node pred = req.hasPredicateFilter() ? BtFilter.compile(req.getPredicateFilter()) : BtFilter.PASS;
        long now = nowMicros();
        List<BtMutations.Op> yes = BtMutations.compile(table, req.getTrueMutationsList(), now);
        List<BtMutations.Op> no = BtMutations.compile(table, req.getFalseMutationsList(), now);
        byte[] key = req.getRowKey().toByteArray();
        String host = shards.owner(admin.hostsOf(name), name, key);
        boolean matched = shards.tx(host, c -> {
            BtStore.lock(c, BtStore.lockKey(name, key));
            List<BtCell> cells = BtStore.loadRow(c, name, key);
            boolean m = !cells.isEmpty() && !pred.apply(key, cells).isEmpty();
            List<BtMutations.Op> ops = m ? yes : no;
            if (!ops.isEmpty()) {
                BtMutations.apply(c, name, key, ops);
            }
            return m;
        });
        return CheckAndMutateRowResponse.newBuilder().setPredicateMatched(matched).build();
    }

    ReadModifyWriteRowResponse readModifyWriteRow(ReadModifyWriteRowRequest req) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        Table table = admin.table(name);
        requireKey(req.getRowKey());
        for (ReadModifyWriteRule r : req.getRulesList()) {
            BtMutations.requireFamily(table, r.getFamilyName());
            if (r.getRuleCase() == ReadModifyWriteRule.RuleCase.RULE_NOT_SET) {
                throw BtException.unknown("unknown RMW rule oneof <nil>");
            }
        }
        byte[] key = req.getRowKey().toByteArray();
        String host = shards.owner(admin.hostsOf(name), name, key);
        Map<String, BtCell> written = shards.tx(host, c -> {
            BtStore.lock(c, BtStore.lockKey(name, key));
            Map<String, BtCell> res = new LinkedHashMap<>();
            for (ReadModifyWriteRule r : req.getRulesList()) {
                byte[] qual = r.getColumnQualifier().toByteArray();
                BtCell latest = latest(c, name, key, r.getFamilyName(), qual);
                byte[] cur = latest == null ? new byte[0] : latest.value();
                byte[] next;
                if (r.getRuleCase() == ReadModifyWriteRule.RuleCase.APPEND_VALUE) {
                    next = concat(cur, r.getAppendValue().toByteArray());
                } else {
                    long base = 0;
                    if (latest != null) {
                        if (cur.length != 8) {
                            throw BtException.unknown("increment on non-64-bit value");
                        }
                        base = ByteBuffer.wrap(cur).getLong();
                    }
                    next = ByteBuffer.allocate(8).putLong(base + r.getIncrementAmount()).array();
                }
                long ts = Math.max(nowMicros(), latest == null ? 0 : latest.ts());
                BtStore.putCell(c, name, key, r.getFamilyName(), qual, ts, next);
                res.put(r.getFamilyName() + "\u0000" + new String(qual, java.nio.charset.StandardCharsets.ISO_8859_1),
                        new BtCell(r.getFamilyName(), qual, ts, next, BtCell.NO_LABELS));
            }
            return res;
        });
        List<BtCell> cells = new ArrayList<>(written.values());
        cells.sort(BtCell.ORDER);
        com.sayonora.wire.bigtablewire.v2.Row.Builder row = com.sayonora.wire.bigtablewire.v2.Row.newBuilder().setKey(req.getRowKey());
        com.sayonora.wire.bigtablewire.v2.Family.Builder fam = null;
        com.sayonora.wire.bigtablewire.v2.Column.Builder col = null;
        BtCell prev = null;
        for (BtCell c : cells) {
            if (fam == null || !fam.getName().equals(c.family())) {
                if (fam != null) {
                    fam.addColumns(col);
                    row.addFamilies(fam);
                }
                fam = com.sayonora.wire.bigtablewire.v2.Family.newBuilder().setName(c.family());
                col = null;
            } else if (!c.sameColumn(prev)) {
                fam.addColumns(col);
                col = null;
            }
            if (col == null) {
                col = com.sayonora.wire.bigtablewire.v2.Column.newBuilder().setQualifier(ByteString.copyFrom(c.qual()));
            }
            col.addCells(com.sayonora.wire.bigtablewire.v2.Cell.newBuilder().setTimestampMicros(c.ts()).setValue(ByteString.copyFrom(c.value())));
            prev = c;
        }
        if (fam != null) {
            fam.addColumns(col);
            row.addFamilies(fam);
        }
        return ReadModifyWriteRowResponse.newBuilder().setRow(row).build();
    }

    private static BtCell latest(java.sql.Connection c, String tbl, byte[] key, String family, byte[] qual) throws SQLException {
        try (var ps = c.prepareStatement("SELECT ts, val FROM warp_bt_cells WHERE tbl = ? AND row_key = ? AND family = ? AND qual = ? "
                + "ORDER BY ts DESC LIMIT 1")) {
            ps.setString(1, tbl);
            ps.setBytes(2, key);
            ps.setString(3, family);
            ps.setBytes(4, qual);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? new BtCell(family, qual, rs.getLong(1), rs.getBytes(2), BtCell.NO_LABELS) : null;
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] o = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, o, a.length, b.length);
        return o;
    }

    // =============================================================== SampleRowKeys

    /** A sorted stream of (row key, approximate bytes) of one host, fetched a page at a time. */
    private final class SizeIter {
        private final String host;
        private final String table;
        private List<Object[]> page = List.of();
        private int pos;
        private byte[] after;
        private boolean done;

        SizeIter(String host, String table) {
            this.host = host;
            this.table = table;
        }

        Object[] peek() {
            if (pos >= page.size() && !done) {
                page = store.rowSizes(host, table, after, 1000);
                pos = 0;
                if (page.size() < 1000) {
                    done = true;
                }
                if (!page.isEmpty()) {
                    after = (byte[]) page.get(page.size() - 1)[0];
                }
            }
            return pos < page.size() ? page.get(pos) : null;
        }

        Object[] next() {
            Object[] r = peek();
            pos++;
            return r;
        }
    }

    void sampleRowKeys(SampleRowKeysRequest req, ServerCallStreamObserver<SampleRowKeysResponse> out) {
        String name = tableName(req.getTableName(), req.getAuthorizedViewName());
        admin.table(name);
        List<SizeIter> its = new ArrayList<>();
        for (String h : admin.hostsOf(name)) {
            its.add(new SizeIter(h, name));
        }
        long total = 0;
        long since = 0;
        while (true) {
            SizeIter best = null;
            for (SizeIter it : its) {
                Object[] p = it.peek();
                if (p != null && (best == null || Arrays.compareUnsigned((byte[]) p[0], (byte[]) best.peek()[0]) < 0)) {
                    best = it;
                }
            }
            if (best == null) {
                break;
            }
            Object[] r = best.next();
            total += (Long) r[1];
            since += (Long) r[1];
            if (since >= cfg.sampleBytes) {
                since = 0;
                if (!send(out, SampleRowKeysResponse.newBuilder().setRowKey(ByteString.copyFrom((byte[]) r[0])).setOffsetBytes(total).build())) {
                    return;
                }
            }
        }
        if (send(out, SampleRowKeysResponse.newBuilder().setOffsetBytes(total).build())) {
            out.onCompleted();
        }
    }
}
