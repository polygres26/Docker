package com.sayonora.warp.firestorewire;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentChange;
import com.google.firestore.v1.DocumentDelete;
import com.google.firestore.v1.DocumentRemove;
import com.google.firestore.v1.ExistenceFilter;
import com.google.firestore.v1.ListenRequest;
import com.google.firestore.v1.ListenResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Target;
import com.google.firestore.v1.TargetChange;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteRequest;
import com.google.firestore.v1.WriteResponse;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The two bidirectional Firestore streams. {@code Write}: handshake then atomic commits with stream tokens. {@code Listen}: document
 * and query targets with snapshots, resume tokens and a change feed read from the store's version log; streams wait on the
 * {@link FsHub} and hold no database connection between snapshots.
 */
final class FsStreams {

    private static final Logger log = LoggerFactory.getLogger(FsStreams.class);

    private final FsService svc;
    private final FsStore store;
    final FsHub hub;
    private final Map<String, Long> writeSessions = new ConcurrentHashMap<>();
    private final SecureRandom rnd = new SecureRandom();
    private final ExecutorService exec = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "firestorewire-listen");
        t.setDaemon(true);
        return t;
    });

    FsStreams(FsService svc, FsStore store) {
        this.svc = svc;
        this.store = store;
        this.hub = new FsHub(store);
    }

    void close() {
        hub.close();
        exec.shutdownNow();
    }

    static ByteString token(long us) {
        return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(us).array());
    }

    static long tokenUs(ByteString t) {
        return t.size() == 8 ? t.asReadOnlyByteBuffer().getLong() : -1;
    }

    // ------------------------------------------------------------------ Write

    StreamObserver<WriteRequest> write(StreamObserver<WriteResponse> out) {
        return new StreamObserver<>() {
            private String streamId;
            private FsNames.Db db;
            private long counter;
            private boolean closed;

            @Override
            public synchronized void onNext(WriteRequest r) {
                if (closed) {
                    return;
                }
                try {
                    if (streamId == null) {
                        db = FsNames.parseDb(r.getDatabase());
                        if (!r.getStreamId().isEmpty()) {
                            Long c = writeSessions.get(r.getStreamId());
                            if (c == null) {
                                throw FsException.invalid("Stream ID is invalid or expired.");
                            }
                            streamId = r.getStreamId();
                            counter = c;
                        } else {
                            if (r.getWritesCount() > 0) {
                                throw FsException.invalid("'writes' must not be set on an initial write request.");
                            }
                            byte[] b = new byte[24];
                            rnd.nextBytes(b);
                            streamId = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(b);
                            counter = 1;
                            writeSessions.put(streamId, counter);
                            out.onNext(WriteResponse.newBuilder().setStreamId(streamId).setStreamToken(token(counter)).build());
                            return;
                        }
                    }
                    if (r.getWritesCount() == 0) {
                        // an empty request acknowledges the last token; answered with the current token (like the official emulator)
                        out.onNext(WriteResponse.newBuilder().setStreamToken(token(counter)).build());
                        return;
                    }
                    if (r.getStreamToken().isEmpty()) {
                        throw FsException.invalid("Missing stream token: writes after the handshake must carry the last stream token.");
                    }
                    var cr = svc.commit(com.google.firestore.v1.CommitRequest.newBuilder().setDatabase(db.name()).addAllWrites(r.getWritesList()).build());
                    counter++;
                    writeSessions.put(streamId, counter);
                    out.onNext(WriteResponse.newBuilder().setStreamToken(token(counter))
                            .addAllWriteResults(cr.getWriteResultsList()).setCommitTime(cr.getCommitTime()).build());
                } catch (FsException e) {
                    closed = true;
                    out.onError(e.toStatus().asRuntimeException());
                } catch (RuntimeException e) {
                    log.error("firestorewire Write stream failed", e);
                    closed = true;
                    out.onError(io.grpc.Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
                }
            }

            @Override
            public synchronized void onError(Throwable t) {
                closed = true;
            }

            @Override
            public synchronized void onCompleted() {
                if (!closed) {
                    closed = true;
                    out.onCompleted();
                }
            }
        };
    }

    // ------------------------------------------------------------------ Listen

    StreamObserver<ListenRequest> listen(StreamObserver<ListenResponse> out) {
        ListenStream s = new ListenStream(out);
        if (out instanceof ServerCallStreamObserver<ListenResponse> so) {
            so.setOnCancelHandler(s::close);
        }
        return s;
    }

    private static final class TargetState {
        final int id;
        final boolean once;
        FsQuery query;
        Set<String> docs;
        final Map<String, Long> members = new LinkedHashMap<>();
        boolean removed;

        TargetState(int id, boolean once) {
            this.id = id;
            this.once = once;
        }
    }

    final class ListenStream implements StreamObserver<ListenRequest>, FsHub.Waker {

        private final StreamObserver<ListenResponse> out;
        private final Map<Integer, TargetState> targets = new LinkedHashMap<>();
        private final Map<String, Long> cursors = new HashMap<>();
        private FsNames.Db db;
        private long tokenUs;
        private boolean closed;
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private int nextAuto = 1;
        private boolean subscribed;

        ListenStream(StreamObserver<ListenResponse> out) {
            this.out = out;
        }

        @Override
        public void wake() {
            if (!closed && scheduled.compareAndSet(false, true)) {
                exec.execute(() -> {
                    scheduled.set(false);
                    processLocked();
                });
            }
        }

        private void send(ListenResponse r) {
            out.onNext(r);
        }

        @Override
        public synchronized void onNext(ListenRequest r) {
            if (closed) {
                return;
            }
            try {
                FsNames.Db d = FsNames.parseDb(r.getDatabase());
                if (db == null) {
                    db = d;
                } else if (!db.equals(d)) {
                    throw FsException.invalid("A Listen stream serves a single database.");
                }
                if (r.getTargetChangeCase() == ListenRequest.TargetChangeCase.ADD_TARGET) {
                    addTarget(r.getAddTarget());
                } else if (r.getTargetChangeCase() == ListenRequest.TargetChangeCase.REMOVE_TARGET) {
                    TargetState t = targets.remove(r.getRemoveTarget());
                    send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder()
                            .setTargetChangeType(TargetChange.TargetChangeType.REMOVE).addTargetIds(r.getRemoveTarget())).build());
                    if (targets.isEmpty()) {
                        hub.unsubscribe(this);
                        subscribed = false;
                        cursors.clear();
                    }
                    if (t != null) {
                        t.removed = true;
                    }
                } else {
                    throw FsException.invalid("ListenRequest must have add_target or remove_target");
                }
            } catch (FsException e) {
                closed = true;
                hub.unsubscribe(this);
                out.onError(e.toStatus().asRuntimeException());
            } catch (RuntimeException e) {
                log.error("firestorewire Listen failed", e);
                closed = true;
                hub.unsubscribe(this);
                out.onError(io.grpc.Status.INTERNAL.withDescription("Internal error: " + e).asRuntimeException());
            }
        }

        private void addTarget(Target t) {
            int id = t.getTargetId() != 0 ? t.getTargetId() : nextAuto();
            if (targets.containsKey(id)) {
                throw FsException.invalid("Target ID " + id + " is already in use.");
            }
            TargetState ts = new TargetState(id, t.getOnce());
            try {
                if (t.getTargetTypeCase() == Target.TargetTypeCase.QUERY) {
                    FsNames.Loc parent = FsNames.parseParent(t.getQuery().getParent());
                    if (!parent.db().equals(db)) {
                        throw FsException.invalid("Target query parent must be in the stream's database.");
                    }
                    ts.query = new FsQuery(store, db, parent, t.getQuery().getStructuredQuery(), null);
                } else if (t.getTargetTypeCase() == Target.TargetTypeCase.DOCUMENTS) {
                    ts.docs = new LinkedHashSet<>();
                    for (String n : t.getDocuments().getDocumentsList()) {
                        ts.docs.add(FsNames.parseDoc(n).rel());
                    }
                } else {
                    throw FsException.invalid("Target must have a query or documents");
                }
            } catch (FsException e) {
                send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder()
                        .setTargetChangeType(TargetChange.TargetChangeType.REMOVE).addTargetIds(id)
                        .setCause(com.google.rpc.Status.newBuilder().setCode(e.code.value()).setMessage(e.getMessage()))).build());
                return;
            }
            targets.put(id, ts);
            send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder()
                    .setTargetChangeType(TargetChange.TargetChangeType.ADD).addTargetIds(id)).build());
            if (!subscribed) {
                for (String h : store.hosts(db)) {
                    cursors.put(h, store.maxCommitUs(h, db));
                }
                hub.subscribe(this, db);
                subscribed = true;
            }
            long resumeUs = -1;
            if (t.getResumeTypeCase() == Target.ResumeTypeCase.RESUME_TOKEN) {
                resumeUs = tokenUs(t.getResumeToken());
            } else if (t.getResumeTypeCase() == Target.ResumeTypeCase.READ_TIME) {
                resumeUs = FsClock.micros(t.getReadTime());
            }
            if (resumeUs >= 0 && resumeUs < store.horizonUs()) {
                resumeUs = -1;
            }
            Map<String, FsStore.Doc> old = new LinkedHashMap<>();
            if (resumeUs >= 0) {
                for (FsStore.Doc d : snapshot(ts, resumeUs)) {
                    old.put(d.rel, d);
                }
            }
            long snapUs = Math.max(tokenUs, maxCursor());
            if (snapUs == 0) {
                snapUs = FsClock.wallMicros();
            }
            List<FsStore.Doc> now = snapshot(ts, null);
            Set<String> nowRels = new LinkedHashSet<>();
            if (ts.docs != null && resumeUs < 0) {
                // a documents target reports every requested document, in request order: present ones as changes, absent ones as deletes
                Map<String, FsStore.Doc> byRel = new HashMap<>();
                for (FsStore.Doc d : now) {
                    byRel.put(d.rel, d);
                }
                long rt = Math.max(snapUs, FsClock.wallMicros());
                for (String rel : ts.docs) {
                    FsStore.Doc d = byRel.get(rel);
                    if (d != null) {
                        nowRels.add(rel);
                        ts.members.put(rel, d.updateUs);
                        send(ListenResponse.newBuilder().setDocumentChange(DocumentChange.newBuilder().setDocument(d.toProto(db)).addTargetIds(id)).build());
                    } else {
                        send(ListenResponse.newBuilder().setDocumentDelete(DocumentDelete.newBuilder().setDocument(db.docsRoot() + "/" + rel)
                                .addRemovedTargetIds(id).setReadTime(FsClock.ts(rt))).build());
                    }
                }
                now = new ArrayList<>();
            }
            for (FsStore.Doc d : now) {
                nowRels.add(d.rel);
                ts.members.put(d.rel, d.updateUs);
                FsStore.Doc o = old.get(d.rel);
                if (o == null || o.updateUs != d.updateUs) {
                    send(ListenResponse.newBuilder().setDocumentChange(DocumentChange.newBuilder().setDocument(d.toProto(db)).addTargetIds(id)).build());
                }
            }
            long readTime = Math.max(snapUs, FsClock.wallMicros());
            for (var e : old.entrySet()) {
                if (!nowRels.contains(e.getKey())) {
                    String name = db.docsRoot() + "/" + e.getKey();
                    boolean exists = !store.getMany(db, List.of(e.getKey()), null).isEmpty();
                    if (exists) {
                        send(ListenResponse.newBuilder().setDocumentRemove(DocumentRemove.newBuilder().setDocument(name).addRemovedTargetIds(id)
                                .setReadTime(FsClock.ts(readTime))).build());
                    } else {
                        send(ListenResponse.newBuilder().setDocumentDelete(DocumentDelete.newBuilder().setDocument(name).addRemovedTargetIds(id)
                                .setReadTime(FsClock.ts(readTime))).build());
                    }
                }
            }
            if (resumeUs >= 0) {
                send(ListenResponse.newBuilder().setFilter(ExistenceFilter.newBuilder().setTargetId(id).setCount(now.size())).build());
            }
            tokenUs = Math.max(tokenUs, snapUs);
            send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder().setTargetChangeType(TargetChange.TargetChangeType.CURRENT)
                    .addTargetIds(id).setResumeToken(token(tokenUs)).setReadTime(FsClock.ts(readTime))).build());
            send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder().setTargetChangeType(TargetChange.TargetChangeType.NO_CHANGE)
                    .setResumeToken(token(tokenUs)).setReadTime(FsClock.ts(readTime))).build());
        }

        private int nextAuto() {
            while (targets.containsKey(nextAuto)) {
                nextAuto++;
            }
            return nextAuto++;
        }

        private long maxCursor() {
            long m = 0;
            for (long c : cursors.values()) {
                m = Math.max(m, c);
            }
            return m;
        }

        private List<FsStore.Doc> snapshot(TargetState ts, Long atUs) {
            if (ts.query != null) {
                List<FsStore.Doc> out = new ArrayList<>();
                FsQuery q = new FsQueryAt(ts.query, atUs).q;
                Iterator<FsStore.Doc> it = q.run();
                while (it.hasNext()) {
                    out.add(it.next());
                }
                return out;
            }
            List<FsStore.Doc> out = new ArrayList<>();
            Map<String, FsStore.Doc> m = store.getMany(db, ts.docs, atUs);
            for (String r : ts.docs) {
                if (m.containsKey(r)) {
                    out.add(m.get(r));
                }
            }
            return out;
        }

        /** Rebinds a parsed query to a read time. */
        private final class FsQueryAt {
            final FsQuery q;

            FsQueryAt(FsQuery base, Long atUs) {
                this.q = atUs == null ? base : base.at(atUs);
            }
        }

        private void processLocked() {
            synchronized (this) {
                if (closed || targets.isEmpty()) {
                    return;
                }
                try {
                    process();
                } catch (FsException e) {
                    log.debug("firestorewire Listen poll: {}", e.getMessage());
                } catch (RuntimeException e) {
                    log.warn("firestorewire Listen poll failed: {}", e.toString());
                }
            }
        }

        private void process() {
            List<FsStore.LogRow> rows = new ArrayList<>();
            Map<String, Long> newCursors = new HashMap<>();
            for (String h : store.hosts(db)) {
                long cur = cursors.getOrDefault(h, 0L);
                while (true) {
                    List<FsStore.LogRow> page = store.logSince(h, db, cur, 2000);
                    rows.addAll(page);
                    if (!page.isEmpty()) {
                        cur = page.get(page.size() - 1).commitUs();
                    }
                    if (page.size() < 2000) {
                        break;
                    }
                }
                newCursors.put(h, cur);
            }
            if (rows.isEmpty()) {
                return;
            }
            cursors.putAll(newCursors);
            rows.sort((a, b) -> a.commitUs() != b.commitUs() ? Long.compare(a.commitUs(), b.commitUs()) : Long.compare(a.seq(), b.seq()));
            Map<String, FsStore.LogRow> latest = new LinkedHashMap<>();
            long maxUs = 0;
            for (FsStore.LogRow r : rows) {
                latest.put(r.rel(), r);
                maxUs = Math.max(maxUs, r.commitUs());
            }
            // per document: which targets it enters/updates in and which it leaves
            Map<String, Set<Integer>> in = new LinkedHashMap<>();
            Map<String, Set<Integer>> outOf = new LinkedHashMap<>();
            for (TargetState ts : new ArrayList<>(targets.values())) {
                if (ts.query != null && ts.query.windowed()) {
                    boolean touches = false;
                    for (String rel : latest.keySet()) {
                        touches |= ts.query.inScope(rel) || ts.members.containsKey(rel);
                    }
                    if (!touches) {
                        continue;
                    }
                    Map<String, FsStore.Doc> fresh = new LinkedHashMap<>();
                    for (FsStore.Doc d : snapshot(ts, null)) {
                        fresh.put(d.rel, d);
                    }
                    for (var e : fresh.entrySet()) {
                        Long v = ts.members.get(e.getKey());
                        if (v == null || v != e.getValue().updateUs) {
                            in.computeIfAbsent(e.getKey(), k -> new LinkedHashSet<>()).add(ts.id);
                        }
                    }
                    for (String rel : new ArrayList<>(ts.members.keySet())) {
                        if (!fresh.containsKey(rel)) {
                            outOf.computeIfAbsent(rel, k -> new LinkedHashSet<>()).add(ts.id);
                        }
                    }
                    ts.members.clear();
                    for (var e : fresh.entrySet()) {
                        ts.members.put(e.getKey(), e.getValue().updateUs);
                    }
                    continue;
                }
                for (var e : latest.entrySet()) {
                    String rel = e.getKey();
                    FsStore.LogRow r = e.getValue();
                    boolean old = ts.members.containsKey(rel);
                    boolean nw;
                    if (r.deleted()) {
                        nw = false;
                    } else if (ts.query != null) {
                        nw = ts.query.accepts(r.doc());
                    } else {
                        nw = ts.docs.contains(rel);
                    }
                    if (old && !r.deleted() && r.commitUs() <= ts.members.get(rel)) {
                        continue;
                    }
                    if (old && r.deleted() && r.commitUs() < ts.members.get(rel)) {
                        continue;
                    }
                    if (nw) {
                        ts.members.put(rel, r.doc().updateUs);
                        in.computeIfAbsent(rel, k -> new LinkedHashSet<>()).add(ts.id);
                    } else if (old) {
                        ts.members.remove(rel);
                        outOf.computeIfAbsent(rel, k -> new LinkedHashSet<>()).add(ts.id);
                    }
                }
            }
            long readTime = Math.max(maxUs, FsClock.wallMicros());
            Set<String> rels = new LinkedHashSet<>(in.keySet());
            rels.addAll(outOf.keySet());
            boolean any = false;
            for (String rel : rels) {
                FsStore.LogRow r = latest.get(rel);
                Set<Integer> ins = in.getOrDefault(rel, Set.of());
                Set<Integer> outs = outOf.getOrDefault(rel, Set.of());
                if (r == null) {
                    // a windowed target changed membership for a document that had no log row in this batch
                    FsStore.Doc d = store.get(db, rel, null);
                    r = new FsStore.LogRow(0, rel, 0, d == null, d);
                }
                any = true;
                if (!ins.isEmpty()) {
                    DocumentChange.Builder dc = DocumentChange.newBuilder().setDocument(r.doc() != null ? r.doc().toProto(db)
                            : store.get(db, rel, null).toProto(db)).addAllTargetIds(ins).addAllRemovedTargetIds(outs);
                    send(ListenResponse.newBuilder().setDocumentChange(dc).build());
                } else if (r.deleted()) {
                    send(ListenResponse.newBuilder().setDocumentDelete(DocumentDelete.newBuilder().setDocument(db.docsRoot() + "/" + rel)
                            .addAllRemovedTargetIds(outs).setReadTime(FsClock.ts(readTime))).build());
                } else {
                    send(ListenResponse.newBuilder().setDocumentRemove(DocumentRemove.newBuilder().setDocument(db.docsRoot() + "/" + rel)
                            .addAllRemovedTargetIds(outs).setReadTime(FsClock.ts(readTime))).build());
                }
            }
            tokenUs = Math.max(tokenUs, maxUs);
            if (any) {
                send(ListenResponse.newBuilder().setTargetChange(TargetChange.newBuilder().setTargetChangeType(TargetChange.TargetChangeType.NO_CHANGE)
                        .setResumeToken(token(tokenUs)).setReadTime(FsClock.ts(readTime))).build());
            }
        }

        void close() {
            synchronized (this) {
                closed = true;
            }
            hub.unsubscribe(this);
        }

        @Override
        public void onError(Throwable t) {
            close();
        }

        @Override
        public void onCompleted() {
            boolean was;
            synchronized (this) {
                was = closed;
                closed = true;
            }
            hub.unsubscribe(this);
            if (!was) {
                out.onCompleted();
            }
        }
    }
}
