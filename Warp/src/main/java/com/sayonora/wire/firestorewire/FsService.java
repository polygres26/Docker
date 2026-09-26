package com.sayonora.wire.firestorewire;

import com.google.firestore.v1.AggregationResult;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.DeleteDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentMask;
import com.google.firestore.v1.GetDocumentRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.RunAggregationQueryRequest;
import com.google.firestore.v1.RunAggregationQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredAggregationQuery;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.TransactionOptions;
import com.google.firestore.v1.UpdateDocumentRequest;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.rpc.Status;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * The Firestore v1 operations (transport independent: gRPC and REST both call these). Every method takes the request message and
 * returns (or streams) the response message; failures are {@link FsException}s carrying the canonical status and message.
 */
final class FsService {

    static final long TX_TTL_MS = 270_000;

    final FsStore store;
    private final ConcurrentHashMap<ByteString, Tx> txs = new ConcurrentHashMap<>();
    private final SecureRandom rnd = new SecureRandom();

    FsService(FsStore store) {
        this.store = store;
    }

    /** An open transaction (held in memory of the node that began it). */
    static final class Tx {
        final ByteString id;
        final FsNames.Db db;
        final boolean readOnly;
        final long readAtUs;
        final long created = System.currentTimeMillis();
        final Map<String, Long> reads = new java.util.concurrent.ConcurrentHashMap<>();
        volatile boolean dirty;

        Tx(ByteString id, FsNames.Db db, boolean readOnly, long readAtUs) {
            this.id = id;
            this.db = db;
            this.readOnly = readOnly;
            this.readAtUs = readAtUs;
        }

        void read(String rel, long updateUs) {
            Long prev = reads.putIfAbsent(rel, updateUs);
            if (prev != null && prev != updateUs) {
                dirty = true;
            }
        }
    }

    // ------------------------------------------------------------------ transactions

    Tx begin(FsNames.Db db, TransactionOptions o) {
        boolean ro = o.getModeCase() == TransactionOptions.ModeCase.READ_ONLY;
        long at = 0;
        if (ro) {
            var r = o.getReadOnly();
            if (r.getConsistencySelectorCase() == TransactionOptions.ReadOnly.ConsistencySelectorCase.READ_TIME) {
                at = readTimeUs(r.getReadTime());
            } else {
                at = FsClock.wallMicros();
            }
        }
        byte[] id = new byte[16];
        rnd.nextBytes(id);
        Tx t = new Tx(ByteString.copyFrom(id), db, ro, at);
        txs.put(t.id, t);
        sweepTx();
        return t;
    }

    private void sweepTx() {
        long now = System.currentTimeMillis();
        for (Tx t : new ArrayList<>(txs.values())) {
            if (now - t.created > TX_TTL_MS) {
                finish(t.id);
            }
        }
    }

    /** Ids of transactions that were committed, rolled back or expired: using one is ABORTED, an id never issued is INVALID_ARGUMENT. */
    private final ConcurrentHashMap<ByteString, Long> finished = new ConcurrentHashMap<>();

    private void finish(ByteString id) {
        txs.remove(id);
        long now = System.currentTimeMillis();
        finished.put(id, now);
        if (finished.size() > 10_000) {
            finished.values().removeIf(t -> now - t > 3_600_000);
        }
    }

    Tx tx(ByteString id, FsNames.Db db) {
        Tx t = txs.get(id);
        if (t == null) {
            if (finished.containsKey(id)) {
                throw FsException.aborted("The referenced transaction has expired or is no longer valid.");
            }
            throw FsException.invalid("Invalid transaction.");
        }
        if (System.currentTimeMillis() - t.created > TX_TTL_MS) {
            finish(id);
            throw FsException.aborted("The referenced transaction has expired or is no longer valid.");
        }
        if (!t.db.equals(db)) {
            throw FsException.invalid("Invalid transaction.");
        }
        return t;
    }

    void rollback(FsNames.Db db, ByteString id) {
        Tx t = txs.get(id);
        if (t == null) {
            if (finished.containsKey(id)) {
                return; // rolling back a finished transaction is a no-op
            }
            throw FsException.invalid("Invalid transaction.");
        }
        if (!t.db.equals(db)) {
            throw FsException.invalid("Invalid transaction.");
        }
        finish(id);
    }

    /** Resolved read consistency: the transaction (or null) and the read-at time (or null = now). */
    private record Read(Tx tx, Long atUs, ByteString newTx) {
    }

    private long readTimeUs(Timestamp t) {
        long us = FsClock.micros(t);
        if (us < store.horizonUs()) {
            throw FsException.precondition("The requested 'read_time' is too old.");
        }
        if (us > FsClock.wallMicros() + 1_000_000L) {
            throw FsException.invalid("The requested 'read_time' cannot be in the future.");
        }
        return us;
    }

    private Read resolve(FsNames.Db db, ByteString txn, TransactionOptions newTx, Timestamp readTime) {
        if (txn != null && !txn.isEmpty()) {
            Tx t = tx(txn, db);
            return new Read(t, t.readOnly ? t.readAtUs : null, null);
        }
        if (newTx != null && newTx.getModeCase() != TransactionOptions.ModeCase.MODE_NOT_SET) {
            Tx t = begin(db, newTx);
            return new Read(t, t.readOnly ? t.readAtUs : null, t.id);
        }
        if (newTx != null) {
            Tx t = begin(db, newTx);
            return new Read(t, null, t.id);
        }
        if (readTime != null) {
            return new Read(null, readTimeUs(readTime), null);
        }
        return new Read(null, null, null);
    }

    private static void track(Read r, FsStore.Doc d) {
        if (r.tx != null && !r.tx.readOnly) {
            r.tx.read(d.rel, d.updateUs);
        }
    }

    private static void trackMissing(Read r, String rel) {
        if (r.tx != null && !r.tx.readOnly) {
            r.tx.read(rel, 0);
        }
    }

    // ------------------------------------------------------------------ point reads

    Document getDocument(GetDocumentRequest req) {
        FsNames.Loc l = FsNames.parseDoc(req.getName());
        Read r = resolve(l.db(), req.getConsistencySelectorCase() == GetDocumentRequest.ConsistencySelectorCase.TRANSACTION ? req.getTransaction() : null,
                null, req.getConsistencySelectorCase() == GetDocumentRequest.ConsistencySelectorCase.READ_TIME ? req.getReadTime() : null);
        FsStore.Doc d = store.get(l.db(), l.rel(), r.atUs);
        if (d == null) {
            trackMissing(r, l.rel());
            throw FsException.notFound("Document \"" + req.getName() + "\" not found.");
        }
        track(r, d);
        return FsWrites.masked(d.toProto(l.db()), req.hasMask() ? req.getMask().getFieldPathsList() : null);
    }

    void batchGet(BatchGetDocumentsRequest req, Consumer<BatchGetDocumentsResponse> out) {
        FsNames.Db db = FsNames.parseDb(req.getDatabase());
        Read r = resolve(db, req.getConsistencySelectorCase() == BatchGetDocumentsRequest.ConsistencySelectorCase.TRANSACTION ? req.getTransaction() : null,
                req.getConsistencySelectorCase() == BatchGetDocumentsRequest.ConsistencySelectorCase.NEW_TRANSACTION ? req.getNewTransaction() : null,
                req.getConsistencySelectorCase() == BatchGetDocumentsRequest.ConsistencySelectorCase.READ_TIME ? req.getReadTime() : null);
        List<String> rels = new ArrayList<>();
        for (String n : req.getDocumentsList()) {
            FsNames.Loc l = FsNames.parseDoc(n);
            if (!l.db().equals(db)) {
                throw FsException.invalid("Document name \"" + n + "\" does not lie within database \"" + db.name() + "\"");
            }
            rels.add(l.rel());
        }
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(rels));
        Map<String, FsStore.Doc> found = store.getMany(db, distinct, r.atUs);
        long readUs = r.atUs != null ? r.atUs : FsClock.wallMicros();
        if (r.newTx != null) {
            out.accept(BatchGetDocumentsResponse.newBuilder().setTransaction(r.newTx).build());
        }
        for (String rel : distinct) {
            BatchGetDocumentsResponse.Builder b = BatchGetDocumentsResponse.newBuilder().setReadTime(FsClock.ts(readUs));
            FsStore.Doc d = found.get(rel);
            if (d != null) {
                track(r, d);
                b.setFound(FsWrites.masked(d.toProto(db), req.hasMask() ? req.getMask().getFieldPathsList() : null));
            } else {
                trackMissing(r, rel);
                b.setMissing(db.docsRoot() + "/" + rel);
            }
            out.accept(b.build());
        }
    }

    // ------------------------------------------------------------------ list

    ListDocumentsResponse listDocuments(ListDocumentsRequest req) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        FsNames.Db db = parent.db();
        if (req.getCollectionId().isEmpty()) {
            throw FsException.invalid("Invalid collection id: must not be empty");
        }
        Read r = resolve(db, req.getConsistencySelectorCase() == ListDocumentsRequest.ConsistencySelectorCase.TRANSACTION ? req.getTransaction() : null,
                null, req.getConsistencySelectorCase() == ListDocumentsRequest.ConsistencySelectorCase.READ_TIME ? req.getReadTime() : null);
        int pageSize = req.getPageSize() > 0 ? Math.min(req.getPageSize(), 300) : 100;
        StructuredQuery.Builder q = StructuredQuery.newBuilder()
                .addFrom(StructuredQuery.CollectionSelector.newBuilder().setCollectionId(req.getCollectionId()));
        if (!req.getOrderBy().isBlank()) {
            for (String part : req.getOrderBy().split(",")) {
                String p = part.trim();
                boolean desc = false;
                if (p.toLowerCase().endsWith(" desc")) {
                    desc = true;
                    p = p.substring(0, p.length() - 5).trim();
                } else if (p.toLowerCase().endsWith(" asc")) {
                    p = p.substring(0, p.length() - 4).trim();
                }
                q.addOrderBy(StructuredQuery.Order.newBuilder().setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(p))
                        .setDirection(desc ? StructuredQuery.Direction.DESCENDING : StructuredQuery.Direction.ASCENDING));
            }
        }
        boolean hasName = false;
        for (var o : q.getOrderByList()) {
            hasName |= o.getField().getFieldPath().equals("__name__");
        }
        if (!hasName) {
            // the page token is a cursor over every ordering term, so the implicit __name__ must be explicit here
            q.addOrderBy(StructuredQuery.Order.newBuilder().setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__"))
                    .setDirection(q.getOrderByCount() > 0 ? q.getOrderBy(q.getOrderByCount() - 1).getDirection() : StructuredQuery.Direction.ASCENDING));
        }
        if (!req.getPageToken().isEmpty()) {
            try {
                q.setStartAt(Cursor.parseFrom(Base64.getUrlDecoder().decode(req.getPageToken())).toBuilder().setBefore(false));
            } catch (Exception e) {
                throw FsException.invalid("Invalid page token");
            }
        }
        q.setLimit(com.google.protobuf.Int32Value.of(pageSize + 1));
        FsQuery fq = new FsQuery(store, db, parent, q.build(), r.atUs);
        ListDocumentsResponse.Builder resp = ListDocumentsResponse.newBuilder();
        List<FsStore.Doc> docs = new ArrayList<>();
        Iterator<FsStore.Doc> it = fq.run();
        while (it.hasNext()) {
            docs.add(it.next());
        }
        boolean more = docs.size() > pageSize;
        if (more) {
            docs = docs.subList(0, pageSize);
        }
        boolean full = docs.size() == pageSize;
        List<String> maskPaths = req.hasMask() ? req.getMask().getFieldPathsList() : null;
        Map<String, Document> missing = new LinkedHashMap<>();
        if (req.getShowMissing() && !more && req.getPageToken().isEmpty()) {
            missing = missingDocs(db, parent.rel(), req.getCollectionId(), docs, r.atUs);
        }
        List<Object[]> merged = new ArrayList<>();
        for (FsStore.Doc d : docs) {
            track(r, d);
            merged.add(new Object[] {d.nameKey(), FsWrites.masked(d.toProto(db), maskPaths)});
        }
        for (Document m : missing.values()) {
            String rel = m.getName().substring(db.docsRoot().length() + 1);
            merged.add(new Object[] {FsNames.nameKey(rel), m});
        }
        merged.sort((a, b) -> java.util.Arrays.compareUnsigned((byte[]) a[0], (byte[]) b[0]));
        for (Object[] m : merged) {
            resp.addDocuments((Document) m[1]);
        }
        if (full) {
            FsStore.Doc last = docs.get(docs.size() - 1);
            Cursor.Builder c = Cursor.newBuilder();
            for (FsQuery.Term t : fq.terms) {
                c.addValues(fq.fieldValue(last, t.path()));
            }
            resp.setNextPageToken(Base64.getUrlEncoder().withoutPadding().encodeToString(c.build().toByteArray()));
        }
        return resp.build();
    }

    /** Nonexistent documents of a collection that have descendants (what showMissing lists). */
    private Map<String, Document> missingDocs(FsNames.Db db, String parentRel, String collId, List<FsStore.Doc> existing, Long atUs) {
        String collPath = parentRel.isEmpty() ? collId : parentRel + "/" + collId;
        int depth = FsNames.depth(collPath);
        Set<String> have = new HashSet<>();
        for (FsStore.Doc d : existing) {
            have.add(d.rel);
        }
        Map<String, Document> out = new LinkedHashMap<>();
        Iterator<FsStore.Doc> it = store.scan(db, FsStore.Scope.all(collPath), false, atUs, null, true, null, true);
        while (it.hasNext()) {
            FsStore.Doc d = it.next();
            List<String> segs = FsNames.segments(d.rel);
            if (segs.size() > depth + 1) {
                String child = String.join("/", segs.subList(0, depth + 1));
                if (!have.contains(child) && !out.containsKey(child)) {
                    out.put(child, Document.newBuilder().setName(db.docsRoot() + "/" + child).build());
                }
            }
        }
        return out;
    }

    ListCollectionIdsResponse listCollectionIds(ListCollectionIdsRequest req) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        Long at = req.getConsistencySelectorCase() == ListCollectionIdsRequest.ConsistencySelectorCase.READ_TIME ? readTimeUs(req.getReadTime()) : null;
        List<String> ids = store.collectionIds(parent.db(), parent.rel(), at);
        int size = req.getPageSize() > 0 ? req.getPageSize() : Integer.MAX_VALUE;
        String after = req.getPageToken().isEmpty() ? null : new String(Base64.getUrlDecoder().decode(req.getPageToken()), java.nio.charset.StandardCharsets.UTF_8);
        ListCollectionIdsResponse.Builder b = ListCollectionIdsResponse.newBuilder();
        int n = 0;
        String last = null;
        boolean more = false;
        for (String id : ids) {
            if (after != null && FsValues.compareStrings(id, after) <= 0) {
                continue;
            }
            if (n == size) {
                more = true;
                break;
            }
            b.addCollectionIds(id);
            last = id;
            n++;
        }
        if (more) {
            b.setNextPageToken(Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
        return b.build();
    }

    // ------------------------------------------------------------------ queries

    void runQuery(RunQueryRequest req, Consumer<RunQueryResponse> out) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        FsNames.Db db = parent.db();
        Read r = resolve(db, req.getConsistencySelectorCase() == RunQueryRequest.ConsistencySelectorCase.TRANSACTION ? req.getTransaction() : null,
                req.getConsistencySelectorCase() == RunQueryRequest.ConsistencySelectorCase.NEW_TRANSACTION ? req.getNewTransaction() : null,
                req.getConsistencySelectorCase() == RunQueryRequest.ConsistencySelectorCase.READ_TIME ? req.getReadTime() : null);
        if (req.getQueryTypeCase() != RunQueryRequest.QueryTypeCase.STRUCTURED_QUERY) {
            throw FsException.invalid("only structured queries are supported");
        }
        FsQuery fq = new FsQuery(store, db, parent, req.getStructuredQuery(), r.atUs);
        Iterator<FsStore.Doc> it = fq.run();
        long readUs = r.atUs != null ? r.atUs : FsClock.wallMicros();
        StructuredQuery sq = req.getStructuredQuery();
        if (r.newTx != null) {
            out.accept(RunQueryResponse.newBuilder().setTransaction(r.newTx).build());
        }
        // the last message of the stream carries done=true (like the official emulator), so keep one message in hand
        RunQueryResponse pending = null;
        while (it.hasNext()) {
            FsStore.Doc d = it.next();
            track(r, d);
            Document doc = d.toProto(db);
            if (sq.hasSelect()) {
                doc = doc.toBuilder().clearFields().putAllFields(fq.project(d.fields)).build();
            }
            RunQueryResponse resp = RunQueryResponse.newBuilder().setDocument(doc).setReadTime(FsClock.ts(readUs)).build();
            if (pending != null) {
                out.accept(pending);
            } else if (sq.getOffset() > 0 && sq.hasLimit()) {
                out.accept(RunQueryResponse.newBuilder().setReadTime(FsClock.ts(readUs)).setSkippedResults(sq.getOffset()).build());
            }
            pending = resp;
        }
        out.accept(pending != null ? pending.toBuilder().setDone(true).build()
                : RunQueryResponse.newBuilder().setReadTime(FsClock.ts(readUs)).setDone(true).build());
    }

    RunAggregationQueryResponse runAggregation(RunAggregationQueryRequest req) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        FsNames.Db db = parent.db();
        Read r = resolve(db, req.getConsistencySelectorCase() == RunAggregationQueryRequest.ConsistencySelectorCase.TRANSACTION ? req.getTransaction() : null,
                req.getConsistencySelectorCase() == RunAggregationQueryRequest.ConsistencySelectorCase.NEW_TRANSACTION ? req.getNewTransaction() : null,
                req.getConsistencySelectorCase() == RunAggregationQueryRequest.ConsistencySelectorCase.READ_TIME ? req.getReadTime() : null);
        if (req.getQueryTypeCase() != RunAggregationQueryRequest.QueryTypeCase.STRUCTURED_AGGREGATION_QUERY) {
            throw FsException.invalid("A structured aggregation query is required");
        }
        StructuredAggregationQuery aq = req.getStructuredAggregationQuery();
        if (aq.getAggregationsCount() == 0) {
            throw FsException.invalid("Aggregations can not be empty.");
        }
        if (aq.getAggregationsCount() > 5) {
            throw FsException.invalid("The maximum number of aggregations allowed in an aggregation query is 5. Received: " + aq.getAggregationsCount());
        }
        StructuredQuery sq = aq.getStructuredQuery();
        FsQuery fq = new FsQuery(store, db, parent, sq, r.atUs);
        AggState[] st = new AggState[aq.getAggregationsCount()];
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < st.length; i++) {
            var a = aq.getAggregations(i);
            String alias = a.getAlias().isEmpty() ? "field_" + (i + 1) : a.getAlias();
            if (aliases.contains(alias)) {
                throw FsException.invalid("Aggregation aliases contain duplicate alias: " + alias + ".");
            }
            aliases.add(alias);
            st[i] = new AggState(a);
        }
        boolean allCount = true;
        for (AggState s : st) {
            allCount &= s.kind == 0;
        }
        AggregationResult.Builder res = AggregationResult.newBuilder();
        boolean unfiltered = !sq.hasWhere() && !sq.hasStartAt() && !sq.hasEndAt() && sq.getOrderByCount() == 0 && sq.getOffset() == 0
                && r.tx == null && r.atUs == null && sq.getFromCount() == 1 && !sq.getFrom(0).getAllDescendants();
        if (allCount && unfiltered) {
            long n = store.count(db, fq.scope());
            if (sq.hasLimit()) {
                n = Math.min(n, sq.getLimit().getValue());
            }
            for (AggState s : st) {
                s.count = s.upTo >= 0 ? Math.min(n, s.upTo) : n;
            }
        } else {
            Iterator<FsStore.Doc> it = fq.run();
            while (it.hasNext()) {
                FsStore.Doc d = it.next();
                track(r, d);
                for (AggState s : st) {
                    s.add(d);
                }
            }
        }
        for (int i = 0; i < st.length; i++) {
            res.putAggregateFields(aliases.get(i), st[i].result());
        }
        RunAggregationQueryResponse.Builder b = RunAggregationQueryResponse.newBuilder().setResult(res)
                .setReadTime(FsClock.ts(r.atUs != null ? r.atUs : FsClock.wallMicros()));
        if (r.newTx != null) {
            b.setTransaction(r.newTx);
        }
        return b.build();
    }

    /** Running state of one aggregation. */
    private static final class AggState {
        final int kind; // 0 count, 1 sum, 2 avg
        final List<String> path;
        long upTo = -1;
        long count;
        long isum;
        double dsum;
        boolean isDouble;
        boolean overflow;
        long n;

        AggState(StructuredAggregationQuery.Aggregation a) {
            switch (a.getOperatorCase()) {
                case COUNT:
                    kind = 0;
                    path = null;
                    if (a.getCount().hasUpTo()) {
                        upTo = a.getCount().getUpTo().getValue();
                        if (upTo < 0) {
                            throw FsException.invalid("The `up_to` value in a COUNT aggregation must be greater than or equal to zero.");
                        }
                    }
                    break;
                case SUM:
                    kind = 1;
                    path = FsQuery.fieldPath(a.getSum().getField().getFieldPath());
                    break;
                case AVG:
                    kind = 2;
                    path = FsQuery.fieldPath(a.getAvg().getField().getFieldPath());
                    break;
                default:
                    throw FsException.invalid("Operator field in Aggregation is not set.");
            }
        }

        void add(FsStore.Doc d) {
            if (kind == 0) {
                if (upTo < 0 || count < upTo) {
                    count++;
                }
                return;
            }
            Value v = FsValues.get(d.fields, path);
            if (v == null) {
                return;
            }
            if (v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
                n++;
                if (!isDouble) {
                    long s = isum + v.getIntegerValue();
                    if (((isum ^ s) & (v.getIntegerValue() ^ s)) < 0) {
                        isDouble = true;
                        dsum = (double) isum + (double) v.getIntegerValue();
                    } else {
                        isum = s;
                    }
                } else {
                    dsum += v.getIntegerValue();
                }
            } else if (v.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
                n++;
                if (!isDouble) {
                    isDouble = true;
                    dsum = (double) isum;
                }
                dsum += v.getDoubleValue();
            }
        }

        Value result() {
            switch (kind) {
                case 0:
                    return FsValues.ofLong(count);
                case 1:
                    return isDouble ? FsValues.ofDouble(dsum) : FsValues.ofLong(isum);
                default:
                    if (n == 0) {
                        return FsValues.NULL;
                    }
                    return FsValues.ofDouble(isDouble ? dsum / n : (double) isum / n);
            }
        }
    }

    PartitionQueryResponse partitionQuery(PartitionQueryRequest req) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        if (req.getPartitionCount() < 0) {
            throw FsException.invalid("Invalid partition_count: " + req.getPartitionCount());
        }
        if (req.getQueryTypeCase() != PartitionQueryRequest.QueryTypeCase.STRUCTURED_QUERY) {
            throw FsException.invalid("only structured queries are supported");
        }
        StructuredQuery sq = req.getStructuredQuery();
        boolean group = sq.getFromCount() == 1 && sq.getFrom(0).getAllDescendants();
        if (!group) {
            throw FsException.invalid("Partition queries must be collection group queries (all_descendants = true).");
        }
        // Warp does not compute split points: a single partition (the whole query) is a valid answer for any partition_count.
        return PartitionQueryResponse.getDefaultInstance();
    }

    // ------------------------------------------------------------------ writes

    static String newId(SecureRandom rnd) {
        String a = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(20);
        for (int i = 0; i < 20; i++) {
            sb.append(a.charAt(rnd.nextInt(a.length())));
        }
        return sb.toString();
    }

    Document createDocument(CreateDocumentRequest req) {
        FsNames.Loc parent = FsNames.parseParent(req.getParent());
        FsNames.validateId(req.getCollectionId(), true);
        if (!req.getDocumentId().isEmpty()) {
            FsNames.validateId(req.getDocumentId(), false);
        }
        String id = req.getDocumentId().isEmpty() ? newId(rnd) : req.getDocumentId();
        String rel = (parent.rel().isEmpty() ? "" : parent.rel() + "/") + req.getCollectionId() + "/" + id;
        String name = parent.db().docsRoot() + "/" + rel;
        FsNames.parseDoc(name);
        Write w = Write.newBuilder().setUpdate(req.getDocument().toBuilder().setName(name).clearCreateTime().clearUpdateTime())
                .setCurrentDocument(Precondition.newBuilder().setExists(false)).build();
        Document out = commitOne(parent.db(), w);
        return FsWrites.masked(out, req.hasMask() ? req.getMask().getFieldPathsList() : null);
    }

    Document updateDocument(UpdateDocumentRequest req) {
        FsNames.Loc l = FsNames.parseDoc(req.getDocument().getName());
        Write.Builder w = Write.newBuilder().setUpdate(req.getDocument());
        if (req.hasUpdateMask()) {
            w.setUpdateMask(req.getUpdateMask());
        }
        if (req.hasCurrentDocument()) {
            w.setCurrentDocument(req.getCurrentDocument());
        }
        Document out = commitOne(l.db(), w.build());
        return FsWrites.masked(out, req.hasMask() ? req.getMask().getFieldPathsList() : null);
    }

    void deleteDocument(DeleteDocumentRequest req) {
        FsNames.Loc l = FsNames.parseDoc(req.getName());
        Write.Builder w = Write.newBuilder().setDelete(req.getName());
        if (req.hasCurrentDocument()) {
            w.setCurrentDocument(req.getCurrentDocument());
        }
        commitOne(l.db(), w.build());
    }

    /** A single-write commit that returns the resulting document (or an empty one after a delete). */
    private Document commitOne(FsNames.Db db, Write w) {
        String rel = FsWrites.target(db, w);
        try (FsStore.WriteTx tx = store.begin(db, List.of(rel))) {
            long commitUs = Math.max(FsClock.nowMicros(), tx.lastCommitUs + 1);
            FsWrites.Overlay o = new FsWrites.Overlay(db, commitUs, tx.current);
            FsWrites.apply(o, w);
            tx.applyAndCommit(o.changes(), commitUs);
            FsStore.Doc d = o.state.get(rel);
            return d == null ? Document.getDefaultInstance() : d.toProto(db);
        }
    }

    CommitResponse commit(CommitRequest req) {
        FsNames.Db db = FsNames.parseDb(req.getDatabase());
        Tx t = null;
        if (!req.getTransaction().isEmpty()) {
            t = tx(req.getTransaction(), db);
            if (t.readOnly) {
                throw FsException.invalid("Cannot modify entities in a read-only transaction.");
            }
        }
        List<String> rels = new ArrayList<>();
        for (Write w : req.getWritesList()) {
            rels.add(FsWrites.target(db, w));
        }
        Set<String> lockSet = new LinkedHashSet<>(rels);
        if (t != null) {
            lockSet.addAll(t.reads.keySet());
        }
        if (req.getWritesCount() == 0) {
            if (t != null) {
                finish(t.id);
            }
            return CommitResponse.getDefaultInstance();
        }
        try {
            return commitLocked(req, db, t, lockSet);
        } catch (FsException e) {
            if (t != null) {
                finish(t.id);
            }
            throw e;
        }
    }

    private CommitResponse commitLocked(CommitRequest req, FsNames.Db db, Tx t, Set<String> lockSet) {
        try (FsStore.WriteTx wt = store.begin(db, lockSet)) {
            if (t != null) {
                if (t.dirty) {
                    throw aborted();
                }
                for (var e : t.reads.entrySet()) {
                    FsStore.Doc cur = wt.current.get(e.getKey());
                    long have = cur == null ? 0 : cur.updateUs;
                    if (have != e.getValue()) {
                        throw aborted();
                    }
                }
            }
            long commitUs = Math.max(FsClock.nowMicros(), wt.lastCommitUs + 1);
            FsWrites.Overlay o = new FsWrites.Overlay(db, commitUs, wt.current);
            CommitResponse.Builder resp = CommitResponse.newBuilder().setCommitTime(FsClock.ts(commitUs));
            for (Write w : req.getWritesList()) {
                resp.addWriteResults(FsWrites.apply(o, w));
            }
            wt.applyAndCommit(o.changes(), commitUs);
            if (t != null) {
                finish(t.id);
            }
            return resp.build();
        }
    }

    private static FsException aborted() {
        return FsException.aborted("Aborted due to cross-transaction contention. This occurs when multiple transactions attempt to access "
                + "the same data, requiring Firestore to abort at least one in order to enforce serializability.");
    }

    BatchWriteResponse batchWrite(BatchWriteRequest req) {
        FsNames.Db db = FsNames.parseDb(req.getDatabase());
        Set<String> seen = new HashSet<>();
        for (Write w : req.getWritesList()) {
            String rel = FsWrites.target(db, w);
            if (!seen.add(rel)) {
                throw FsException.invalid("the same document cannot be written more than once in a single request");
            }
        }
        BatchWriteResponse.Builder out = BatchWriteResponse.newBuilder();
        for (Write w : req.getWritesList()) {
            try {
                CommitResponse cr = commit(CommitRequest.newBuilder().setDatabase(req.getDatabase()).addWrites(w).build());
                out.addWriteResults(cr.getWriteResults(0));
                out.addStatus(Status.newBuilder().setCode(0).build());
            } catch (FsException e) {
                out.addWriteResults(WriteResult.getDefaultInstance());
                out.addStatus(Status.newBuilder().setCode(e.code.value()).setMessage(e.getMessage()).build());
            }
        }
        return out.build();
    }

    static DocumentMask emptyMask() {
        return DocumentMask.getDefaultInstance();
    }
}
