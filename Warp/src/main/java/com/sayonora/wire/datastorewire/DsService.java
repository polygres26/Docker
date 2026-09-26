package com.sayonora.wire.datastorewire;

import com.google.datastore.v1.AggregationQuery;
import com.google.datastore.v1.AggregationResult;
import com.google.datastore.v1.AggregationResultBatch;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.MutationResult;
import com.google.datastore.v1.PropertyTransform;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.ReserveIdsRequest;
import com.google.datastore.v1.ReserveIdsResponse;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RollbackResponse;
import com.google.datastore.v1.RunAggregationQueryRequest;
import com.google.datastore.v1.RunAggregationQueryResponse;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.TransactionOptions;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Datastore v1 operations (transport independent: gRPC and REST both call these). Transactions are optimistic: reads are
 * recorded with the entity versions they saw and validated, under the entity locks, at commit.
 */
final class DsService {

    static final long TX_TTL_MS = 270_000;
    private static final AtomicLong LAST = new AtomicLong();

    final DsStore store;
    private final ConcurrentHashMap<ByteString, Tx> txs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ByteString, Long> finished = new ConcurrentHashMap<>();
    private final SecureRandom rnd = new SecureRandom();

    DsService(DsStore store) {
        this.store = store;
    }

    static long nowMicros() {
        java.time.Instant i = java.time.Instant.now();
        long now = i.getEpochSecond() * 1_000_000L + i.getNano() / 1000;
        return LAST.updateAndGet(prev -> Math.max(prev + 1, now));
    }

    static Timestamp ts(long micros) {
        return Timestamp.newBuilder().setSeconds(Math.floorDiv(micros, 1_000_000L)).setNanos((int) Math.floorMod(micros, 1_000_000L) * 1000).build();
    }

    // ------------------------------------------------------------------ transactions

    static final class Tx {
        final ByteString id;
        final DsKeys.Part part;
        final boolean readOnly;
        final long created = System.currentTimeMillis();
        final Map<String, Long> reads = new ConcurrentHashMap<>();
        volatile boolean dirty;

        Tx(ByteString id, DsKeys.Part part, boolean readOnly) {
            this.id = id;
            this.part = part;
            this.readOnly = readOnly;
        }

        void read(String keyId, long version) {
            Long prev = reads.putIfAbsent(keyId, version);
            if (prev != null && prev != version) {
                dirty = true;
            }
        }
    }

    Tx begin(DsKeys.Part part, TransactionOptions o) {
        boolean ro = o.getModeCase() == TransactionOptions.ModeCase.READ_ONLY;
        byte[] id = new byte[16];
        rnd.nextBytes(id);
        Tx t = new Tx(ByteString.copyFrom(id), part, ro);
        txs.put(t.id, t);
        long now = System.currentTimeMillis();
        for (Tx x : new ArrayList<>(txs.values())) {
            if (now - x.created > TX_TTL_MS) {
                finish(x.id);
            }
        }
        return t;
    }

    private void finish(ByteString id) {
        txs.remove(id);
        finished.put(id, System.currentTimeMillis());
        if (finished.size() > 10_000) {
            long now = System.currentTimeMillis();
            finished.values().removeIf(t -> now - t > 3_600_000);
        }
    }

    Tx tx(ByteString id) {
        Tx t = txs.get(id);
        if (t == null) {
            if (finished.containsKey(id)) {
                throw DsException.invalid("transaction has expired or is invalid");
            }
            throw DsException.invalid("Invalid transaction.");
        }
        if (System.currentTimeMillis() - t.created > TX_TTL_MS) {
            finish(id);
            throw DsException.invalid("transaction has expired or is invalid");
        }
        return t;
    }

    RollbackResponse rollback(RollbackRequest r) {
        DsKeys.Part part = DsKeys.part(r.getProjectId(), r.getDatabaseId(), null);
        Tx t = tx(r.getTransaction());
        if (!t.part.project().equals(part.project())) {
            throw DsException.invalid("Invalid transaction.");
        }
        finish(t.id);
        return RollbackResponse.getDefaultInstance();
    }

    BeginTransactionResponse beginTransaction(BeginTransactionRequest r) {
        DsKeys.Part part = DsKeys.part(r.getProjectId(), r.getDatabaseId(), null);
        return BeginTransactionResponse.newBuilder().setTransaction(begin(part, r.getTransactionOptions()).id).build();
    }

    /** Resolved read consistency of a request. */
    private record Read(Tx tx, ByteString newTx) {
    }

    private Read resolve(DsKeys.Part part, ReadOptions ro) {
        switch (ro.getConsistencyTypeCase()) {
            case TRANSACTION:
                return new Read(tx(ro.getTransaction()), null);
            case NEW_TRANSACTION: {
                Tx t = begin(part, ro.getNewTransaction());
                return new Read(t, t.id);
            }
            case READ_TIME:
                throw DsException.unimplemented("Reading at a read_time is not supported by Warp's datastorewire (entities are read at the current state)");
            default:
                return new Read(null, null);
        }
    }

    private static void track(Read r, DsStore.KeyRef ref, long version) {
        if (r.tx != null && !r.tx.readOnly) {
            r.tx.read(ref.id(), version);
        }
    }

    // ------------------------------------------------------------------ lookup

    LookupResponse lookup(LookupRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), null);
        Read read = resolve(part, req.getReadOptions());
        Map<DsKeys.Part, List<Key>> byPart = new LinkedHashMap<>();
        List<DsStore.KeyRef> refs = new ArrayList<>();
        for (Key k : req.getKeysList()) {
            DsKeys.validate(k, false, true);
            DsKeys.Part kp = DsKeys.partOf(part, k);
            refs.add(new DsStore.KeyRef(kp, k, DsKeys.encode(k)));
        }
        Map<String, DsStore.Row> found = new LinkedHashMap<>();
        Map<DsKeys.Part, List<byte[]>> group = new LinkedHashMap<>();
        for (DsStore.KeyRef r : refs) {
            group.computeIfAbsent(r.part(), x -> new ArrayList<>()).add(r.bytes());
        }
        for (var e : group.entrySet()) {
            for (var row : store.getMany(e.getKey(), e.getValue()).entrySet()) {
                found.put(e.getKey().scope() + "|" + java.util.Base64.getEncoder().encodeToString(row.getKey().array()), row.getValue());
            }
        }
        LookupResponse.Builder b = LookupResponse.newBuilder();
        long readUs = nowMicros();
        for (DsStore.KeyRef r : refs) {
            DsStore.Row row = found.get(r.id());
            if (row != null) {
                track(read, r, row.version);
                b.addFound(EntityResult.newBuilder().setEntity(row.resultEntity()).setVersion(row.version)
                        .setCreateTime(ts(row.createUs)).setUpdateTime(ts(row.updateUs)));
            } else {
                track(read, r, 0);
                b.addMissing(EntityResult.newBuilder().setEntity(Entity.newBuilder().setKey(DsKeys.withPartition(r.key(), r.part()))).setVersion(readUs));
            }
        }
        b.setReadTime(ts(readUs));
        if (read.newTx != null) {
            b.setTransaction(read.newTx);
        }
        return b.build();
    }

    // ------------------------------------------------------------------ queries

    RunQueryResponse runQuery(RunQueryRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), req.getPartitionId());
        Read read = resolve(part, req.getReadOptions());
        Query query;
        if (req.getQueryTypeCase() == RunQueryRequest.QueryTypeCase.GQL_QUERY) {
            query = DsGql.parse(req.getGqlQuery(), part).query;
        } else if (req.getQueryTypeCase() == RunQueryRequest.QueryTypeCase.QUERY) {
            query = req.getQuery();
        } else {
            throw DsException.invalid("A query or gql_query is required.");
        }
        DsQuery dq = new DsQuery(store, part, query);
        DsQuery.Result res = dq.run();
        if (read.tx != null && !read.tx.readOnly) {
            for (EntityResult er : res.batch().getEntityResultsList()) {
                Key k = er.getEntity().getKey();
                read.tx.read(new DsStore.KeyRef(part, k, DsKeys.encode(k)).id(), er.getVersion());
            }
        }
        RunQueryResponse.Builder b = RunQueryResponse.newBuilder().setBatch(res.batch());
        if (req.getQueryTypeCase() == RunQueryRequest.QueryTypeCase.GQL_QUERY) {
            b.setQuery(query);
        }
        if (read.newTx != null) {
            b.setTransaction(read.newTx);
        }
        return b.build();
    }

    RunAggregationQueryResponse runAggregation(RunAggregationQueryRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), req.getPartitionId());
        Read read = resolve(part, req.getReadOptions());
        if (req.getQueryTypeCase() != RunAggregationQueryRequest.QueryTypeCase.AGGREGATION_QUERY) {
            throw DsException.invalid("An aggregation_query is required.");
        }
        AggregationQuery aq = req.getAggregationQuery();
        if (aq.getAggregationsCount() == 0) {
            throw DsException.invalid("Aggregations can not be empty.");
        }
        if (aq.getAggregationsCount() > 5) {
            throw DsException.invalid("The maximum number of aggregations allowed in an aggregation query is 5. Received: " + aq.getAggregationsCount());
        }
        DsQuery dq = new DsQuery(store, part, aq.getNestedQuery());
        long[] count = new long[aq.getAggregationsCount()];
        Agg[] aggs = new Agg[aq.getAggregationsCount()];
        List<String> aliases = new ArrayList<>();
        boolean allCount = true;
        for (int i = 0; i < aggs.length; i++) {
            AggregationQuery.Aggregation a = aq.getAggregations(i);
            String alias = a.getAlias().isEmpty() ? "property_" + (i + 1) : a.getAlias();
            if (aliases.contains(alias)) {
                throw DsException.invalid("Aggregation aliases contain duplicate alias: " + alias + ".");
            }
            aliases.add(alias);
            aggs[i] = new Agg(a);
            allCount &= aggs[i].kind == 0;
        }
        if (allCount && dq.plainKindScan() && read.tx == null && dq.kind() != null && !dq.kind().startsWith("__")) {
            long n = store.count(part, dq.scopeForCount());
            for (Agg a : aggs) {
                a.count = a.upTo >= 0 ? Math.min(n, a.upTo) : n;
            }
        } else {
            Iterator<DsStore.Row> it = dq.entities();
            while (it.hasNext()) {
                DsStore.Row row = it.next();
                if (read.tx != null && !read.tx.readOnly) {
                    read.tx.read(new DsStore.KeyRef(part, row.key, row.keyBytes).id(), row.version);
                }
                for (Agg a : aggs) {
                    a.add(row);
                }
            }
        }
        AggregationResult.Builder res = AggregationResult.newBuilder();
        for (int i = 0; i < aggs.length; i++) {
            res.putAggregateProperties(aliases.get(i), aggs[i].result());
        }
        RunAggregationQueryResponse.Builder b = RunAggregationQueryResponse.newBuilder().setBatch(AggregationResultBatch.newBuilder()
                .addAggregationResults(res).setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS).setReadTime(ts(nowMicros())));
        if (read.newTx != null) {
            b.setTransaction(read.newTx);
        }
        return b.build();
    }

    private static final class Agg {
        final int kind; // 0 count, 1 sum, 2 avg
        final String prop;
        long upTo = -1;
        long count;
        long isum;
        double dsum;
        boolean isDouble;
        long n;

        Agg(AggregationQuery.Aggregation a) {
            switch (a.getOperatorCase()) {
                case COUNT:
                    kind = 0;
                    prop = null;
                    if (a.getCount().hasUpTo()) {
                        upTo = a.getCount().getUpTo().getValue();
                        if (upTo < 0) {
                            throw DsException.invalid("The `up_to` value in a COUNT aggregation must be greater than or equal to zero.");
                        }
                    }
                    break;
                case SUM:
                    kind = 1;
                    prop = a.getSum().getProperty().getName();
                    break;
                case AVG:
                    kind = 2;
                    prop = a.getAvg().getProperty().getName();
                    break;
                default:
                    throw DsException.invalid("Operator field in Aggregation is not set.");
            }
        }

        void add(DsStore.Row row) {
            if (kind == 0) {
                if (upTo < 0 || count < upTo) {
                    count++;
                }
                return;
            }
            Value v = row.entity.getPropertiesMap().get(prop);
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
                    return Value.newBuilder().setIntegerValue(count).build();
                case 1:
                    return isDouble ? Value.newBuilder().setDoubleValue(dsum).build() : Value.newBuilder().setIntegerValue(isum).build();
                default:
                    return n == 0 ? DsValues.nullValue() : Value.newBuilder().setDoubleValue(isDouble ? dsum / n : (double) isum / n).build();
            }
        }
    }

    // ------------------------------------------------------------------ ids

    AllocateIdsResponse allocateIds(AllocateIdsRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), null);
        for (Key k : req.getKeysList()) {
            DsKeys.validate(k, true, false);
            if (DsKeys.complete(k)) {
                throw DsException.invalid("Key path element must not be complete: " + DsKeys.describe(k));
            }
        }
        AllocateIdsResponse.Builder b = AllocateIdsResponse.newBuilder();
        Map<DsKeys.Part, List<Integer>> byPart = new LinkedHashMap<>();
        for (int i = 0; i < req.getKeysCount(); i++) {
            byPart.computeIfAbsent(DsKeys.partOf(part, req.getKeys(i)), x -> new ArrayList<>()).add(i);
        }
        Key[] out = new Key[req.getKeysCount()];
        for (var e : byPart.entrySet()) {
            long first = store.allocate(e.getKey(), e.getValue().size());
            long id = first;
            for (int i : e.getValue()) {
                out[i] = withLastId(req.getKeys(i), id++, e.getKey());
            }
        }
        for (Key k : out) {
            b.addKeys(k);
        }
        return b.build();
    }

    ReserveIdsResponse reserveIds(ReserveIdsRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), null);
        for (Key k : req.getKeysList()) {
            DsKeys.validate(k, false, false);
        }
        for (Key k : req.getKeysList()) {
            DsKeys.Part kp = DsKeys.partOf(part, k);
            long max = 0;
            for (Key.PathElement e : k.getPathList()) {
                if (e.getIdTypeCase() == Key.PathElement.IdTypeCase.ID) {
                    max = Math.max(max, e.getId());
                }
            }
            if (max > 0) {
                store.reserve(kp, max);
            }
        }
        return ReserveIdsResponse.getDefaultInstance();
    }

    private static Key withLastId(Key k, long id, DsKeys.Part part) {
        Key.Builder b = k.toBuilder();
        int last = b.getPathCount() - 1;
        b.setPath(last, b.getPath(last).toBuilder().setId(id));
        return b.setPartitionId(part.toProto()).build();
    }

    // ------------------------------------------------------------------ commit

    CommitResponse commit(CommitRequest req) {
        DsKeys.Part part = DsKeys.part(req.getProjectId(), req.getDatabaseId(), null);
        boolean hasTx = req.getTransactionSelectorCase() == CommitRequest.TransactionSelectorCase.TRANSACTION;
        boolean single = req.getTransactionSelectorCase() == CommitRequest.TransactionSelectorCase.SINGLE_USE_TRANSACTION;
        boolean nonTx = req.getMode() == CommitRequest.Mode.NON_TRANSACTIONAL;
        if (nonTx && (hasTx || single)) {
            throw DsException.invalid("non-transaction commit cannot specify a transaction");
        }
        if (!nonTx && !hasTx && !single) {
            throw DsException.invalid("transactional commit requires a transaction");
        }
        Tx tx = null;
        if (hasTx) {
            tx = tx(req.getTransaction());
            if (tx.readOnly) {
                throw DsException.invalid("Cannot modify entities in a read-only transaction.");
            }
        }
        try {
            return commitBody(req, part, tx, nonTx);
        } catch (DsException e) {
            if (tx != null) {
                finish(tx.id);
            }
            throw e;
        }
    }

    private CommitResponse commitBody(CommitRequest req, DsKeys.Part part, Tx tx, boolean nonTx) {
        List<Mutation> muts = req.getMutationsList();
        if (muts.isEmpty()) {
            if (tx != null) {
                finish(tx.id);
            }
            return CommitResponse.getDefaultInstance();
        }
        // validate and resolve keys (allocating ids for incomplete ones)
        List<DsStore.KeyRef> refs = new ArrayList<>();
        List<Boolean> allocated = new ArrayList<>();
        List<Entity> entities = new ArrayList<>();
        Map<DsKeys.Part, List<Integer>> toAllocate = new LinkedHashMap<>();
        Key[] keys = new Key[muts.size()];
        DsKeys.Part[] parts = new DsKeys.Part[muts.size()];
        for (int i = 0; i < muts.size(); i++) {
            Mutation m = muts.get(i);
            Key k;
            switch (m.getOperationCase()) {
                case INSERT:
                    k = m.getInsert().getKey();
                    break;
                case UPDATE:
                    k = m.getUpdate().getKey();
                    break;
                case UPSERT:
                    k = m.getUpsert().getKey();
                    break;
                case DELETE:
                    k = m.getDelete();
                    break;
                default:
                    throw DsException.invalid("Mutation is missing operation.");
            }
            boolean canAllocate = m.getOperationCase() == Mutation.OperationCase.INSERT || m.getOperationCase() == Mutation.OperationCase.UPSERT;
            DsKeys.validate(k, canAllocate, false);
            parts[i] = DsKeys.partOf(part, k);
            keys[i] = k;
            if (!DsKeys.complete(k)) {
                toAllocate.computeIfAbsent(parts[i], x -> new ArrayList<>()).add(i);
            }
        }
        boolean[] wasAllocated = new boolean[muts.size()];
        for (var e : toAllocate.entrySet()) {
            long id = store.allocate(e.getKey(), e.getValue().size());
            for (int i : e.getValue()) {
                keys[i] = withLastId(keys[i], id++, e.getKey());
                wasAllocated[i] = true;
            }
        }
        for (int i = 0; i < muts.size(); i++) {
            refs.add(new DsStore.KeyRef(parts[i], keys[i], DsKeys.encode(keys[i])));
        }
        if (nonTx) {
            Set<String> seen = new HashSet<>();
            for (DsStore.KeyRef r : refs) {
                if (!seen.add(r.id())) {
                    throw DsException.invalid("A non-transactional commit may not contain multiple mutations affecting the same entity.");
                }
            }
        }
        Set<DsStore.KeyRef> lockSet = new LinkedHashSet<>(refs);
        Map<String, DsStore.KeyRef> extra = new LinkedHashMap<>();
        if (tx != null) {
            // read-set keys must be locked too so their versions can be validated
            for (String kid : tx.reads.keySet()) {
                boolean have = false;
                for (DsStore.KeyRef r : refs) {
                    have |= r.id().equals(kid);
                }
                if (!have) {
                    extra.put(kid, parseKeyId(kid, part));
                }
            }
            lockSet.addAll(extra.values());
        }
        try (DsStore.WriteTx wt = store.begin(lockSet)) {
            if (tx != null) {
                if (tx.dirty) {
                    throw aborted();
                }
                for (var e : tx.reads.entrySet()) {
                    DsStore.Row cur = wt.current.get(e.getKey());
                    if ((cur == null ? 0 : cur.version) != e.getValue()) {
                        throw aborted();
                    }
                }
            }
            long commitUs = nowMicros();
            Map<String, DsStore.Row> state = new LinkedHashMap<>(wt.current);
            Map<String, DsStore.Change> changes = new LinkedHashMap<>();
            CommitResponse.Builder resp = CommitResponse.newBuilder().setCommitTime(ts(commitUs));
            int indexUpdates = 0;
            for (int i = 0; i < muts.size(); i++) {
                Mutation m = muts.get(i);
                DsStore.KeyRef ref = refs.get(i);
                DsStore.Row cur = state.get(ref.id());
                MutationResult.Builder mr = MutationResult.newBuilder();
                if (wasAllocated[i]) {
                    mr.setKey(DsKeys.withPartition(keys[i], parts[i]));
                }
                // conflict detection
                boolean conflict = false;
                if (m.getConflictDetectionStrategyCase() == Mutation.ConflictDetectionStrategyCase.BASE_VERSION) {
                    if (m.getOperationCase() == Mutation.OperationCase.INSERT && m.getBaseVersion() > 0) {
                        throw DsException.invalid("Cannot insert an entity with a base version greater than zero");
                    }
                    conflict = m.getBaseVersion() != (cur == null ? 0 : cur.version);
                } else if (m.getConflictDetectionStrategyCase() == Mutation.ConflictDetectionStrategyCase.UPDATE_TIME) {
                    conflict = cur == null || DsValues.micros(m.getUpdateTime()) != cur.updateUs;
                }
                if (conflict) {
                    mr.setConflictDetected(true);
                    if (cur != null) {
                        mr.setVersion(cur.version);
                    }
                    resp.addMutationResults(mr);
                    continue;
                }
                switch (m.getOperationCase()) {
                    case INSERT:
                        if (cur != null) {
                            throw DsException.exists("entity already exists");
                        }
                        break;
                    case UPDATE:
                        if (cur == null) {
                            throw DsException.notFound("no entity to update");
                        }
                        break;
                    default:
                        break;
                }
                long version = Math.max(commitUs, cur == null ? 0 : cur.version + 1);
                if (m.getOperationCase() == Mutation.OperationCase.DELETE) {
                    state.remove(ref.id());
                    changes.put(ref.id(), new DsStore.Change(ref, null, version, 0, 0));
                    mr.setVersion(version).setUpdateTime(ts(commitUs));
                } else {
                    Entity e = m.getOperationCase() == Mutation.OperationCase.INSERT ? m.getInsert()
                            : m.getOperationCase() == Mutation.OperationCase.UPDATE ? m.getUpdate() : m.getUpsert();
                    Entity norm = DsValues.validateAndNormalize(e.toBuilder().setKey(keys[i].toBuilder().clearPartitionId()).build(), parts[i]);
                    List<Value> tr = new ArrayList<>();
                    if (m.getPropertyTransformsCount() > 0) {
                        norm = applyTransforms(norm, m.getPropertyTransformsList(), cur == null ? null : cur.entity, commitUs, tr);
                    }
                    long created = cur == null ? commitUs : cur.createUs;
                    DsStore.Row nr = new DsStore.Row(parts[i], keys[i].toBuilder().clearPartitionId().build(), ref.bytes(), norm, version, created, commitUs);
                    state.put(ref.id(), nr);
                    changes.put(ref.id(), new DsStore.Change(ref, norm, version, created, commitUs));
                    mr.setVersion(version).setCreateTime(ts(created)).setUpdateTime(ts(commitUs)).addAllTransformResults(tr);
                    indexUpdates += 2 + norm.getPropertiesCount();
                }
                resp.addMutationResults(mr);
            }
            resp.setIndexUpdates(indexUpdates);
            wt.applyAndCommit(new ArrayList<>(changes.values()));
            if (tx != null) {
                finish(tx.id);
            }
            return resp.build();
        }
    }

    private static DsStore.KeyRef parseKeyId(String kid, DsKeys.Part part) {
        int bar = kid.indexOf('|');
        String scope = kid.substring(0, bar);
        byte[] bytes = java.util.Base64.getDecoder().decode(kid.substring(bar + 1));
        String[] sp = scope.split("/", -1);
        DsKeys.Part p = new DsKeys.Part(sp[0], sp[1], sp.length > 2 ? sp[2] : "");
        return new DsStore.KeyRef(p, decodeKey(bytes), bytes);
    }

    /** Inverse of {@link DsKeys#encode}. */
    static Key decodeKey(byte[] b) {
        Key.Builder k = Key.newBuilder();
        int i = 0;
        while (i < b.length) {
            int s = i;
            while (b[i] != 0) {
                i++;
            }
            String kind = new String(b, s, i - s, java.nio.charset.StandardCharsets.UTF_8);
            i++;
            Key.PathElement.Builder e = Key.PathElement.newBuilder().setKind(kind);
            if (b[i] == 1) {
                i++;
                long biased = 0;
                for (int j = 0; j < 8; j++) {
                    biased = (biased << 8) | (b[i++] & 0xFF);
                }
                e.setId(biased ^ Long.MIN_VALUE);
            } else {
                i++;
                int ns = i;
                while (b[i] != 0) {
                    i++;
                }
                e.setName(new String(b, ns, i - ns, java.nio.charset.StandardCharsets.UTF_8));
                i++;
            }
            k.addPath(e);
        }
        return k.build();
    }

    private static DsException aborted() {
        return DsException.aborted("too much contention on these datastore entities. please try again.");
    }

    // ------------------------------------------------------------------ property transforms

    private Entity applyTransforms(Entity e, List<PropertyTransform> transforms, Entity before, long commitUs, List<Value> results) {
        Map<String, Value> props = new LinkedHashMap<>(e.getPropertiesMap());
        for (PropertyTransform t : transforms) {
            String p = t.getProperty();
            if (p.isEmpty()) {
                throw DsException.invalid("Invalid empty property path string.");
            }
            Value cur = props.get(p);
            Value nv;
            switch (t.getTransformTypeCase()) {
                case SET_TO_SERVER_VALUE:
                    nv = Value.newBuilder().setTimestampValue(ts(commitUs)).build();
                    break;
                case INCREMENT: {
                    Value op = t.getIncrement();
                    if (!DsValues.isNumber(op)) {
                        throw DsException.invalid("Input must be int64 or double.");
                    }
                    if (cur == null || !DsValues.isNumber(cur)) {
                        nv = op;
                    } else if (cur.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE && op.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
                        long a = cur.getIntegerValue();
                        long b = op.getIntegerValue();
                        long r = a + b;
                        if (((a ^ r) & (b ^ r)) < 0) {
                            r = a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
                        }
                        nv = Value.newBuilder().setIntegerValue(r).build();
                    } else {
                        nv = Value.newBuilder().setDoubleValue(num(cur) + num(op)).build();
                    }
                    break;
                }
                case MAXIMUM:
                case MINIMUM: {
                    boolean max = t.getTransformTypeCase() == PropertyTransform.TransformTypeCase.MAXIMUM;
                    Value op = max ? t.getMaximum() : t.getMinimum();
                    if (!DsValues.isNumber(op)) {
                        throw DsException.invalid("Input must be int64 or double.");
                    }
                    if (cur == null || !DsValues.isNumber(cur)) {
                        nv = op;
                    } else {
                        double a = num(cur);
                        double b = num(op);
                        if (Double.isNaN(a)) {
                            nv = cur;
                        } else if (Double.isNaN(b)) {
                            nv = op;
                        } else {
                            nv = (max ? b > a : b < a) ? op : cur;
                        }
                    }
                    break;
                }
                case APPEND_MISSING_ELEMENTS:
                case REMOVE_ALL_FROM_ARRAY: {
                    boolean append = t.getTransformTypeCase() == PropertyTransform.TransformTypeCase.APPEND_MISSING_ELEMENTS;
                    List<Value> arg = (append ? t.getAppendMissingElements() : t.getRemoveAllFromArray()).getValuesList();
                    List<Value> list = new ArrayList<>(cur != null && cur.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE
                            ? cur.getArrayValue().getValuesList() : List.of());
                    if (append) {
                        for (Value x : arg) {
                            boolean present = false;
                            for (Value y : list) {
                                present |= DsValues.rank(x) == DsValues.rank(y) && DsValues.compare(x, y) == 0;
                            }
                            if (!present) {
                                list.add(x);
                            }
                        }
                    } else {
                        list.removeIf(y -> arg.stream().anyMatch(x -> DsValues.rank(x) == DsValues.rank(y) && DsValues.compare(x, y) == 0));
                    }
                    nv = Value.newBuilder().setArrayValue(com.google.datastore.v1.ArrayValue.newBuilder().addAllValues(list)).build();
                    results.add(DsValues.nullValue());
                    props.put(p, nv);
                    continue;
                }
                default:
                    throw DsException.invalid("Operation type must be specified for property transformation.");
            }
            props.put(p, nv);
            results.add(nv);
        }
        return e.toBuilder().clearProperties().putAllProperties(props).build();
    }

    private static double num(Value v) {
        return v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? (double) v.getIntegerValue() : v.getDoubleValue();
    }
}
