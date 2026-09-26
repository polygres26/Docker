package com.sayonora.wire.datastorewire;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Runs a Datastore {@link Query}: kind / kindless / metadata scopes, filter trees (property filters with any-element semantics on
 * array properties, HAS_ANCESTOR, composite AND/OR), the effective ordering (explicit orders, the inequality property, then
 * {@code __key__}), projections (one row per index entry of array properties), distinct-on, cursors, offset and limit.
 */
final class DsQuery {

    private static final String KEY = "__key__";

    private final DsStore store;
    private final DsKeys.Part part;
    private final Query q;
    private final String kind;
    private final Node where;
    private final List<Ord> orders = new ArrayList<>();
    private final List<String> projection = new ArrayList<>();
    private final String signature;
    /** Top-level AND'ed property filters per property (used to restrict which array elements sort and project). */
    private final Map<String, List<Node>> restrictions = new LinkedHashMap<>();
    private Key ancestor;

    record Ord(String prop, boolean desc) {
    }

    /** A parsed filter node. */
    static final class Node {
        int type; // 0 property, 1 AND, 2 OR
        PropertyFilter.Operator op;
        String prop;
        Value value;
        List<Node> kids = new ArrayList<>();
        Key ancestor;
    }

    DsQuery(DsStore store, DsKeys.Part part, Query q) {
        this.store = store;
        this.part = part;
        this.q = q;
        if (q.hasLimit() && q.getLimit().getValue() < 0) {
            throw DsException.invalid("Limit must be non-negative.");
        }
        if (q.getOffset() < 0) {
            throw DsException.invalid("Offset must be non-negative.");
        }
        if (q.hasFindNearest()) {
            throw DsException.unimplemented("find_nearest (vector search) is not supported by Warp's datastorewire");
        }
        if (q.getKindCount() > 1) {
            throw DsException.invalid("Only one kind is allowed in a query.");
        }
        this.kind = q.getKindCount() == 0 ? null : q.getKind(0).getName();
        this.where = q.hasFilter() ? parse(q.getFilter()) : null;
        if (where != null) {
            collectRestrictions(where);
            findAncestor(where);
        }
        boolean metadata = kind != null && (kind.equals("__kind__") || kind.equals("__namespace__"));
        if (kind == null && where != null && hasNonKey(where)) {
            throw DsException.invalid("kind is required for non-__key__ filters");
        }
        for (PropertyOrder o : q.getOrderList()) {
            boolean desc = o.getDirection() == PropertyOrder.Direction.DESCENDING;
            String p = o.getProperty().getName();
            if ((kind == null || metadata) && (!p.equals(KEY) || desc)) {
                throw DsException.invalid("kind is required for all orders except __key__ ascending");
            }
            orders.add(new Ord(p, desc));
        }
        // effective ordering: explicit, else the inequality property, then __key__
        if (orders.isEmpty() && where != null) {
            String ineq = inequalityProperty(where);
            if (ineq != null && !ineq.equals(KEY)) {
                orders.add(new Ord(ineq, false));
            }
        }
        if (orders.isEmpty() && !q.getProjectionList().isEmpty() && where == null) {
            // a projection query is ordered by the projected properties (in index order)
            for (var p : q.getProjectionList()) {
                if (!p.getProperty().getName().equals(KEY) && kind != null) {
                    orders.add(new Ord(p.getProperty().getName(), false));
                }
            }
        }
        boolean hasKey = false;
        for (Ord o : orders) {
            hasKey |= o.prop().equals(KEY);
        }
        if (!hasKey) {
            orders.add(new Ord(KEY, false));
        }
        for (var p : q.getProjectionList()) {
            String n = p.getProperty().getName();
            if (projection.contains(n)) {
                throw DsException.invalid("cannot project a property multiple times");
            }
            projection.add(n);
        }
        for (String p : projection) {
            for (Node n : restrictions.getOrDefault(p, List.of())) {
                if (n.op == PropertyFilter.Operator.EQUAL) {
                    throw DsException.invalid("cannot use projection on a property with an equality filter");
                }
            }
        }
        if (kind == null && !projection.isEmpty() && !(projection.size() == 1 && projection.get(0).equals(KEY))) {
            throw DsException.invalid("kind is required for non-__key__ projections");
        }
        StringBuilder sig = new StringBuilder(kind == null ? "" : kind);
        for (Ord o : orders) {
            sig.append('|').append(o.prop()).append(o.desc() ? '-' : '+');
        }
        this.signature = sig.toString();
    }

    // ------------------------------------------------------------------ filter parsing

    private Node parse(Filter f) {
        Node n = new Node();
        switch (f.getFilterTypeCase()) {
            case COMPOSITE_FILTER: {
                var c = f.getCompositeFilter();
                if (c.getFiltersCount() == 0) {
                    throw DsException.invalid("A composite filter must have at least one sub-filter.");
                }
                if (c.getOp() == com.google.datastore.v1.CompositeFilter.Operator.OPERATOR_UNSPECIFIED) {
                    throw DsException.invalid("a composite filter must specify an operator");
                }
                n.type = c.getOp() == com.google.datastore.v1.CompositeFilter.Operator.AND ? 1 : 2;
                for (Filter k : c.getFiltersList()) {
                    n.kids.add(parse(k));
                }
                return n;
            }
            case PROPERTY_FILTER: {
                var pf = f.getPropertyFilter();
                n.prop = pf.getProperty().getName();
                n.op = pf.getOp();
                n.value = pf.getValue();
                if (n.op == PropertyFilter.Operator.OPERATOR_UNSPECIFIED) {
                    throw DsException.invalid("a property filter must specify an operator");
                }
                switch (n.op) {
                    case HAS_ANCESTOR:
                        if (!n.prop.equals(KEY)) {
                            throw DsException.invalid("property must be __key__");
                        }
                        if (n.value.getValueTypeCase() != Value.ValueTypeCase.KEY_VALUE) {
                            throw DsException.invalid("HAS_ANCESTOR requires a key value");
                        }
                        DsKeys.validate(n.value.getKeyValue(), false, true);
                        n.ancestor = n.value.getKeyValue();
                        break;
                    case IN:
                    case NOT_IN:
                        if (n.value.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE || n.value.getArrayValue().getValuesCount() == 0) {
                            throw DsException.invalid("'" + n.op.name() + "' property filter for '" + n.prop + "' requires a non-empty ArrayValue");
                        }
                        break;
                    default:
                        if (n.value.getValueTypeCase() == Value.ValueTypeCase.ENTITY_VALUE) {
                            throw DsException.invalid("An entity value is not allowed");
                        }
                        if (n.value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
                            throw DsException.invalid("A list value is not allowed");
                        }
                }
                if (n.prop.equals(KEY) && n.op != PropertyFilter.Operator.HAS_ANCESTOR) {
                    List<Value> vs = n.value.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE ? n.value.getArrayValue().getValuesList() : List.of(n.value);
                    for (Value v : vs) {
                        if (v.getValueTypeCase() != Value.ValueTypeCase.KEY_VALUE) {
                            throw DsException.invalid("__key__ filter value must be a key");
                        }
                    }
                }
                return n;
            }
            default:
                throw DsException.invalid("a filter must have exactly one of its fields set");
        }
    }

    private void collectRestrictions(Node n) {
        if (n.type == 1) {
            for (Node k : n.kids) {
                collectRestrictions(k);
            }
        } else if (n.type == 0 && n.op != PropertyFilter.Operator.HAS_ANCESTOR) {
            restrictions.computeIfAbsent(n.prop, k -> new ArrayList<>()).add(n);
        }
    }

    private void findAncestor(Node n) {
        if (n.type == 1) {
            for (Node k : n.kids) {
                findAncestor(k);
            }
        } else if (n.type == 0 && n.op == PropertyFilter.Operator.HAS_ANCESTOR && ancestor == null) {
            ancestor = n.ancestor;
        }
    }

    private static boolean hasNonKey(Node n) {
        if (n.type == 0) {
            return !n.prop.equals(KEY);
        }
        for (Node k : n.kids) {
            if (hasNonKey(k)) {
                return true;
            }
        }
        return false;
    }

    private static String inequalityProperty(Node n) {
        if (n.type == 0) {
            switch (n.op) {
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, NOT_EQUAL, NOT_IN:
                    return n.prop;
                default:
                    return null;
            }
        }
        String best = null;
        for (Node k : n.kids) {
            String s = inequalityProperty(k);
            if (s != null && (best == null || s.compareTo(best) < 0)) {
                best = s;
            }
        }
        return best;
    }

    // ------------------------------------------------------------------ evaluation

    private List<Value> candidates(DsStore.Row r, String prop) {
        if (prop.equals(KEY)) {
            return List.of(Value.newBuilder().setKeyValue(r.key).build());
        }
        return DsValues.indexed(r.entity, prop);
    }

    private boolean eq(Value a, Value b) {
        return DsValues.compare(a, b) == 0;
    }

    private boolean satisfies(Node n, Value cand) {
        Value v = n.value;
        switch (n.op) {
            case EQUAL:
                return eq(cand, v);
            case LESS_THAN:
                return DsValues.compare(cand, v) < 0;
            case LESS_THAN_OR_EQUAL:
                return DsValues.compare(cand, v) <= 0;
            case GREATER_THAN:
                return DsValues.compare(cand, v) > 0;
            case GREATER_THAN_OR_EQUAL:
                return DsValues.compare(cand, v) >= 0;
            case IN:
                for (Value x : v.getArrayValue().getValuesList()) {
                    if (eq(cand, x)) {
                        return true;
                    }
                }
                return false;
            default:
                return true;
        }
    }

    private boolean matches(DsStore.Row r, Node n) {
        if (n.type == 1) {
            for (Node k : n.kids) {
                if (!matches(r, k)) {
                    return false;
                }
            }
            return true;
        }
        if (n.type == 2) {
            for (Node k : n.kids) {
                if (matches(r, k)) {
                    return true;
                }
            }
            return false;
        }
        if (n.op == PropertyFilter.Operator.HAS_ANCESTOR) {
            return DsKeys.hasAncestor(r.key, n.ancestor);
        }
        List<Value> cands = candidates(r, n.prop);
        if (cands.isEmpty()) {
            return false;
        }
        switch (n.op) {
            case NOT_EQUAL:
                for (Value c : cands) {
                    if (eq(c, n.value)) {
                        return false;
                    }
                }
                return true;
            case NOT_IN:
                for (Value c : cands) {
                    for (Value x : n.value.getArrayValue().getValuesList()) {
                        if (eq(c, x)) {
                            return false;
                        }
                    }
                }
                return true;
            default:
                for (Value c : cands) {
                    if (satisfies(n, c)) {
                        return true;
                    }
                }
                return false;
        }
    }

    /** The index entries of {@code prop} that survive every AND'ed filter on it (what an index scan would visit). */
    private List<Value> restricted(DsStore.Row r, String prop) {
        List<Value> cands = candidates(r, prop);
        List<Node> rs = restrictions.get(prop);
        if (rs == null || cands.isEmpty()) {
            return cands;
        }
        Predicate<Value> ok = c -> {
            for (Node n : rs) {
                switch (n.op) {
                    case NOT_EQUAL:
                        if (eq(c, n.value)) {
                            return false;
                        }
                        break;
                    case NOT_IN:
                        for (Value x : n.value.getArrayValue().getValuesList()) {
                            if (eq(c, x)) {
                                return false;
                            }
                        }
                        break;
                    default:
                        if (!satisfies(n, c)) {
                            return false;
                        }
                }
            }
            return true;
        };
        List<Value> out = new ArrayList<>();
        for (Value c : cands) {
            if (ok.test(c)) {
                out.add(c);
            }
        }
        return out.isEmpty() ? cands : out;
    }

    // ------------------------------------------------------------------ rows

    /** A result row: an entity, or one index-entry combination of a projection. */
    private static final class Out {
        final DsStore.Row row;
        final Value[] sort;
        final Map<String, Value> proj;

        Out(DsStore.Row row, Value[] sort, Map<String, Value> proj) {
            this.row = row;
            this.sort = sort;
            this.proj = proj;
        }
    }

    private int compareSort(Value[] a, Value[] b) {
        for (int i = 0; i < orders.size(); i++) {
            int c = DsValues.compare(a[i], b[i]);
            if (c != 0) {
                return orders.get(i).desc() ? -c : c;
            }
        }
        return 0;
    }

    private static Value strip(Value v) {
        return v.getExcludeFromIndexes() ? v.toBuilder().clearExcludeFromIndexes().build() : v;
    }

    /** Expands one matching entity into result rows (a single one unless a projected property has several index entries). */
    private List<Out> expand(DsStore.Row r) {
        List<Map<String, Value>> projRows = new ArrayList<>();
        boolean keysOnly = projection.size() == 1 && projection.get(0).equals(KEY);
        if (!projection.isEmpty() && !keysOnly) {
            projRows.add(new LinkedHashMap<>());
            for (String p : projection) {
                if (p.equals(KEY)) {
                    continue;
                }
                List<Value> cands = restricted(r, p);
                List<Value> distinct = new ArrayList<>();
                for (Value c : cands) {
                    boolean seen = false;
                    for (Value d : distinct) {
                        seen |= eq(c, d) && DsValues.rank(c) == DsValues.rank(d);
                    }
                    if (!seen) {
                        distinct.add(c);
                    }
                }
                if (distinct.isEmpty()) {
                    return List.of();
                }
                List<Map<String, Value>> next = new ArrayList<>();
                for (Map<String, Value> base : projRows) {
                    for (Value c : distinct) {
                        Map<String, Value> m = new LinkedHashMap<>(base);
                        m.put(p, strip(c).toBuilder().setMeaning(18).build());
                        next.add(m);
                    }
                }
                projRows = next;
            }
        } else {
            projRows.add(null);
        }
        List<Out> out = new ArrayList<>();
        for (Map<String, Value> pr : projRows) {
            Value[] sort = new Value[orders.size()];
            boolean ok = true;
            for (int i = 0; i < sort.length && ok; i++) {
                Ord o = orders.get(i);
                if (o.prop().equals(KEY)) {
                    sort[i] = Value.newBuilder().setKeyValue(r.key).build();
                } else if (pr != null && pr.containsKey(o.prop())) {
                    sort[i] = pr.get(o.prop());
                } else {
                    List<Value> cands = restricted(r, o.prop());
                    if (cands.isEmpty()) {
                        ok = false;
                        break;
                    }
                    Value best = cands.get(0);
                    for (Value c : cands) {
                        int cmp = DsValues.compare(c, best);
                        if (o.desc() ? cmp > 0 : cmp < 0) {
                            best = c;
                        }
                    }
                    sort[i] = best;
                }
            }
            if (ok) {
                out.add(new Out(r, sort, pr));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ cursors

    private ByteString cursor(Out o) {
        Entity.Builder e = Entity.newBuilder().setKey(o.row.key);
        e.putProperties("s", Value.newBuilder().setStringValue(signature).build());
        ArrayValue.Builder vs = ArrayValue.newBuilder();
        for (Value v : o.sort) {
            vs.addValues(v);
        }
        e.putProperties("v", Value.newBuilder().setArrayValue(vs).build());
        return e.build().toByteString();
    }

    private ByteString emptyCursor() {
        return Entity.newBuilder().putProperties("s", Value.newBuilder().setStringValue(signature).build())
                .putProperties("v", Value.newBuilder().setArrayValue(ArrayValue.getDefaultInstance()).build()).build().toByteString();
    }

    /** The sort tuple of a cursor; an empty tuple means "before everything". */
    private Value[] parseCursor(ByteString c) {
        try {
            Entity e = Entity.parseFrom(c);
            Value s = e.getPropertiesMap().get("s");
            Value v = e.getPropertiesMap().get("v");
            if (s == null || v == null || v.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
                throw DsException.invalid("Error parsing protocol message");
            }
            if (!s.getStringValue().equals(signature)) {
                throw DsException.invalid("cursor does not match query");
            }
            return v.getArrayValue().getValuesList().toArray(new Value[0]);
        } catch (InvalidProtocolBufferException ex) {
            throw DsException.invalid("Error parsing protocol message");
        }
    }

    private int cmpTuple(Value[] a, Value[] cursor) {
        if (cursor.length == 0) {
            return 1;
        }
        return compareSort(a, cursor);
    }

    // ------------------------------------------------------------------ execution

    /** Result of a query run. */
    record Result(QueryResultBatch batch) {
    }

    Result run() {
        if (kind != null && (kind.equals("__kind__") || kind.equals("__namespace__"))) {
            return runMetadata();
        }
        Value[] start = q.getStartCursor().isEmpty() ? null : parseCursor(q.getStartCursor());
        Value[] end = q.getEndCursor().isEmpty() ? null : parseCursor(q.getEndCursor());
        long offset = q.getOffset();
        long limit = q.hasLimit() ? q.getLimit().getValue() : -1;

        DsStore.Scope scope = new DsStore.Scope(kind, ancestor == null ? null : DsKeys.encode(ancestor));
        // a scope on a partition other than the query's finds nothing
        if (ancestor != null && !DsKeys.partOf(part, ancestor).scope().equals(part.scope())) {
            return new Result(empty(start));
        }
        Iterator<DsStore.Row> it = store.scan(part, scope, false, null, true, null, true);
        boolean keyOrderOnly = orders.size() == 1 && !orders.get(0).desc() && projection.isEmpty();
        boolean keysOnly = projection.size() == 1 && projection.get(0).equals(KEY);

        List<Out> rows = new ArrayList<>();
        List<Out> streamed = null;
        if (keyOrderOnly && q.getDistinctOnCount() == 0) {
            // store order is the answer: filter, apply cursors, offset and limit lazily
            streamed = new ArrayList<>();
        }
        long skipped = 0;
        Out lastSkipped = null;
        Out lastOut = null;
        boolean limitReached = false;
        boolean beyondEnd = false;
        if (streamed != null) {
            while (it.hasNext()) {
                DsStore.Row r = it.next();
                if (where != null && !matches(r, where)) {
                    continue;
                }
                Value[] sort = {Value.newBuilder().setKeyValue(r.key).build()};
                if (start != null && cmpTuple(sort, start) <= 0) {
                    continue;
                }
                if (end != null && end.length > 0 && cmpTuple(sort, end) > 0 || end != null && end.length == 0) {
                    beyondEnd = true;
                    break;
                }
                Out o = new Out(r, sort, null);
                if (skipped < offset) {
                    skipped++;
                    lastSkipped = o;
                    continue;
                }
                if (limit >= 0 && streamed.size() >= limit) {
                    limitReached = true;
                    break;
                }
                streamed.add(o);
            }
            rows = streamed;
        } else {
            Comparator<Out> cmp = (x, y) -> compareSort(x.sort, y.sort);
            int cap = limit >= 0 && offset + limit < 100_000 && q.getDistinctOnCount() == 0 && start == null && end == null
                    ? (int) (offset + limit) : -1;
            PriorityQueue<Out> heap = cap > 0 ? new PriorityQueue<>(cmp.reversed()) : null;
            List<Out> all = new ArrayList<>();
            while (it.hasNext()) {
                DsStore.Row r = it.next();
                if (where != null && !matches(r, where)) {
                    continue;
                }
                for (Out o : expand(r)) {
                    if (heap != null) {
                        if (heap.size() < cap) {
                            heap.add(o);
                        } else if (cmp.compare(o, heap.peek()) < 0) {
                            heap.poll();
                            heap.add(o);
                        }
                    } else {
                        all.add(o);
                    }
                }
            }
            if (heap != null) {
                all.addAll(heap);
            }
            all.sort(cmp);
            Set<String> seen = new HashSet<>();
            for (Out o : all) {
                if (start != null && cmpTuple(o.sort, start) <= 0) {
                    continue;
                }
                if (end != null && (end.length == 0 || cmpTuple(o.sort, end) > 0)) {
                    beyondEnd = true;
                    break;
                }
                if (q.getDistinctOnCount() > 0 && !seen.add(distinctKey(o))) {
                    continue;
                }
                if (skipped < offset) {
                    skipped++;
                    lastSkipped = o;
                    continue;
                }
                if (limit >= 0 && rows.size() >= limit) {
                    limitReached = true;
                    break;
                }
                rows.add(o);
            }
        }
        QueryResultBatch.Builder b = QueryResultBatch.newBuilder();
        b.setEntityResultType(keysOnly ? EntityResult.ResultType.KEY_ONLY
                : projection.isEmpty() ? EntityResult.ResultType.FULL : EntityResult.ResultType.PROJECTION);
        for (Out o : rows) {
            EntityResult.Builder er = EntityResult.newBuilder().setCursor(cursor(o));
            Key key = DsKeys.withPartition(o.row.key, part);
            if (keysOnly) {
                er.setEntity(Entity.newBuilder().setKey(key));
            } else if (o.proj != null) {
                er.setEntity(Entity.newBuilder().setKey(key).putAllProperties(o.proj));
            } else {
                er.setEntity(o.row.resultEntity());
            }
            if (!keysOnly && o.proj == null) {
                er.setVersion(o.row.version).setCreateTime(DsService.ts(o.row.createUs)).setUpdateTime(DsService.ts(o.row.updateUs));
            }
            b.addEntityResults(er);
            lastOut = o;
        }
        b.setSkippedResults((int) skipped);
        if (lastSkipped != null) {
            b.setSkippedCursor(cursor(lastSkipped));
        }
        if (lastOut != null) {
            b.setEndCursor(cursor(lastOut));
        } else if (lastSkipped != null) {
            b.setEndCursor(cursor(lastSkipped));
        } else {
            b.setEndCursor(start != null ? q.getStartCursor() : emptyCursor());
        }
        b.setMoreResults(limitReached || (limit >= 0 && rows.size() == limit) ? QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_LIMIT
                : beyondEnd ? QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_CURSOR : QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);
        b.setSnapshotVersion(DsService.nowMicros());
        b.setReadTime(DsService.ts(DsService.nowMicros()));
        return new Result(b.build());
    }

    private String distinctKey(Out o) {
        StringBuilder sb = new StringBuilder();
        for (var d : q.getDistinctOnList()) {
            String p = d.getName();
            Value v = o.proj != null && o.proj.containsKey(p) ? o.proj.get(p) : null;
            if (v == null) {
                List<Value> c = restricted(o.row, p);
                v = c.isEmpty() ? DsValues.nullValue() : c.get(0);
            }
            sb.append(p).append('=').append(DsValues.rank(v)).append(':').append(v.toBuilder().clearMeaning().clearExcludeFromIndexes().build()).append(';');
        }
        return sb.toString();
    }

    private QueryResultBatch empty(Value[] start) {
        return QueryResultBatch.newBuilder().setEntityResultType(EntityResult.ResultType.FULL)
                .setEndCursor(start != null ? q.getStartCursor() : emptyCursor()).setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS)
                .setSnapshotVersion(DsService.nowMicros()).setReadTime(DsService.ts(DsService.nowMicros())).build();
    }

    /** {@code __kind__} and {@code __namespace__} metadata queries: key-only entities, optionally filtered by {@code __key__}. */
    private Result runMetadata() {
        boolean kinds = kind.equals("__kind__");
        List<Key> keys = new ArrayList<>();
        if (kinds) {
            for (String k : store.kinds(part)) {
                keys.add(Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("__kind__").setName(k)).build());
            }
        } else {
            for (String ns : store.namespaces(part)) {
                Key.PathElement.Builder e = Key.PathElement.newBuilder().setKind("__namespace__");
                if (ns.isEmpty()) {
                    e.setId(1);
                } else {
                    e.setName(ns);
                }
                keys.add(Key.newBuilder().addPath(e).build());
            }
        }
        keys.sort(DsKeys::compare);
        QueryResultBatch.Builder b = QueryResultBatch.newBuilder().setEntityResultType(EntityResult.ResultType.FULL);
        long offset = q.getOffset();
        long limit = q.hasLimit() ? q.getLimit().getValue() : -1;
        long skipped = 0;
        int returned = 0;
        for (Key k : keys) {
            DsStore.Row r = new DsStore.Row(part, k, DsKeys.encode(k), Entity.newBuilder().setKey(k).build(), 1, 0, 0);
            if (where != null && !matches(r, where)) {
                continue;
            }
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (limit >= 0 && returned >= limit) {
                break;
            }
            b.addEntityResults(EntityResult.newBuilder().setEntity(Entity.newBuilder().setKey(DsKeys.withPartition(k, part)))
                    .setCursor(cursor(new Out(r, new Value[] {Value.newBuilder().setKeyValue(k).build()}, null))));
            returned++;
        }
        b.setSkippedResults((int) skipped);
        b.setEndCursor(returned > 0 ? b.getEntityResults(returned - 1).getCursor() : emptyCursor());
        b.setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);
        b.setSnapshotVersion(DsService.nowMicros());
        b.setReadTime(DsService.ts(DsService.nowMicros()));
        return new Result(b.build());
    }

    // ------------------------------------------------------------------ aggregation input

    /** The matching entities (offset/limit applied like the query would) for aggregation. */
    Iterator<DsStore.Row> entities() {
        Result r = run();
        List<DsStore.Row> rows = new ArrayList<>();
        for (EntityResult er : r.batch().getEntityResultsList()) {
            Key k = er.getEntity().getKey().toBuilder().clearPartitionId().build();
            rows.add(new DsStore.Row(part, k, DsKeys.encode(k), er.getEntity(), er.getVersion(), DsValues.micros(er.getCreateTime()),
                    DsValues.micros(er.getUpdateTime())));
        }
        return rows.iterator();
    }

    Query query() {
        return q;
    }

    String kind() {
        return kind;
    }

    boolean plainKindScan() {
        return where == null && q.getOffset() == 0 && !q.hasLimit() && q.getStartCursor().isEmpty() && q.getEndCursor().isEmpty()
                && projection.isEmpty() && q.getDistinctOnCount() == 0 && ancestor == null;
    }

    DsStore.Scope scopeForCount() {
        return new DsStore.Scope(kind, null);
    }
}
