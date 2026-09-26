package com.sayonora.wire.firestorewire;

import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.TreeSet;

/**
 * Executes a Firestore {@link StructuredQuery}: scope resolution, filter evaluation with Firestore's type-bracketed
 * comparison, the effective ordering (explicit order-by, then the fields of inequality filters, then the implicit
 * {@code __name__}), cursors, offset and limit. Documents are streamed from the store in {@code __name__} order and, when
 * another ordering is requested, collected (a bounded top-k heap when a limit is present).
 */
final class FsQuery {

    static final List<String> NAME = List.of("__name__");

    /** One resolved ordering term. */
    record Term(List<String> path, boolean desc) {
        boolean isName() {
            return path.size() == 1 && path.get(0).equals("__name__");
        }
    }

    private final FsStore store;
    private final FsNames.Db db;
    private final StructuredQuery q;
    private final FsNames.Loc parent;
    private final Long atUs;
    final List<Term> terms;
    private final StructuredQuery.Filter where;

    FsQuery(FsStore store, FsNames.Db db, FsNames.Loc parent, StructuredQuery q, Long atUs) {
        this.store = store;
        this.db = db;
        this.parent = parent;
        this.q = q;
        this.atUs = atUs;
        this.where = q.hasWhere() ? q.getWhere() : null;
        validateShape();
        this.terms = effectiveTerms();
        validateCursors();
    }

    // ------------------------------------------------------------------ validation and planning

    private void validateShape() {
        if (q.getFromCount() > 1) {
            throw FsException.invalid("StructuredQuery.from cannot have more than one collection selector.");
        }
        if (q.hasLimit() && q.getLimit().getValue() < 0) {
            throw FsException.invalid("limit is negative");
        }
        if (q.getOffset() < 0) {
            throw FsException.invalid("offset is negative");
        }
        if (q.hasFindNearest()) {
            var fn = q.getFindNearest();
            if (!fn.hasLimit() || fn.getLimit().getValue() < 1 || fn.getLimit().getValue() > 1000) {
                throw FsException.invalid("find_nearest requires a limit between 1 and 1000.");
            }
            if (!FsValues.isVector(fn.getQueryVector())) {
                throw FsException.invalid("find_nearest requires a vector query_vector.");
            }
            if (fn.getDistanceMeasure() == StructuredQuery.FindNearest.DistanceMeasure.DISTANCE_MEASURE_UNSPECIFIED) {
                throw FsException.invalid("find_nearest requires a distance_measure.");
            }
        }
        if (where != null) {
            validateFilter(where);
        }
    }

    private void validateFilter(StructuredQuery.Filter f) {
        int[] counts = new int[2]; // [not-equality style filters, array-contains]
        countFilters(f, counts);
        if (counts[0] > 1) {
            throw FsException.invalid("Only a single 'NOT_EQUAL', 'NOT_IN', 'IS_NOT_NAN', or 'IS_NOT_NULL' filter allowed per query.");
        }
        if (counts[1] > 1) {
            throw FsException.precondition("Only a single array-contains clause is allowed in a query");
        }
        checkFilter(f);
    }

    private void countFilters(StructuredQuery.Filter f, int[] counts) {
        switch (f.getFilterTypeCase()) {
            case COMPOSITE_FILTER:
                for (var x : f.getCompositeFilter().getFiltersList()) {
                    countFilters(x, counts);
                }
                break;
            case FIELD_FILTER:
                switch (f.getFieldFilter().getOp()) {
                    case NOT_EQUAL, NOT_IN -> counts[0]++;
                    case ARRAY_CONTAINS -> counts[1]++;
                    default -> {
                    }
                }
                break;
            case UNARY_FILTER:
                if (f.getUnaryFilter().getOp() == StructuredQuery.UnaryFilter.Operator.IS_NOT_NAN
                        || f.getUnaryFilter().getOp() == StructuredQuery.UnaryFilter.Operator.IS_NOT_NULL) {
                    counts[0]++;
                }
                break;
            default:
                break;
        }
    }

    private void checkFilter(StructuredQuery.Filter f) {
        switch (f.getFilterTypeCase()) {
            case COMPOSITE_FILTER: {
                var c = f.getCompositeFilter();
                if (c.getOp() == StructuredQuery.CompositeFilter.Operator.OPERATOR_UNSPECIFIED) {
                    throw FsException.invalid("Unknown CompositeFilter operator.");
                }
                for (var x : c.getFiltersList()) {
                    checkFilter(x);
                }
                break;
            }
            case FIELD_FILTER: {
                var ff = f.getFieldFilter();
                fieldPath(ff.getField().getFieldPath());
                switch (ff.getOp()) {
                    case OPERATOR_UNSPECIFIED:
                        throw FsException.invalid("Unknown FieldFilter operator.");
                    case IN:
                    case NOT_IN:
                    case ARRAY_CONTAINS_ANY:
                        if (ff.getValue().getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
                            throw FsException.invalid("'" + opName(ff.getOp()) + "' requires an ArrayValue.");
                        }
                        if (ff.getValue().getArrayValue().getValuesCount() == 0) {
                            throw FsException.invalid("'" + opName(ff.getOp()) + "' requires an non-empty ArrayValue.");
                        }
                        break;
                    default:
                        break;
                }
                break;
            }
            case UNARY_FILTER: {
                var u = f.getUnaryFilter();
                if (u.getOp() == StructuredQuery.UnaryFilter.Operator.OPERATOR_UNSPECIFIED) {
                    throw FsException.invalid("Unknown UnaryFilter operator.");
                }
                fieldPath(u.getField().getFieldPath());
                break;
            }
            default:
                throw FsException.invalid("Unknown Filter type.");
        }
    }

    private static String opName(StructuredQuery.FieldFilter.Operator op) {
        return op.name();
    }

    static List<String> fieldPath(String p) {
        if (p.equals("__name__")) {
            return NAME;
        }
        return FsValues.parseFieldPath(p);
    }

    private void collectInequalityFields(StructuredQuery.Filter f, TreeSet<String> out, Map<String, List<String>> paths) {
        switch (f.getFilterTypeCase()) {
            case COMPOSITE_FILTER:
                for (var x : f.getCompositeFilter().getFiltersList()) {
                    collectInequalityFields(x, out, paths);
                }
                break;
            case FIELD_FILTER: {
                var ff = f.getFieldFilter();
                switch (ff.getOp()) {
                    case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, NOT_EQUAL, NOT_IN: {
                        List<String> p = fieldPath(ff.getField().getFieldPath());
                        String k = FsValues.joinPath(p);
                        out.add(k);
                        paths.put(k, p);
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case UNARY_FILTER: {
                var u = f.getUnaryFilter();
                if (u.getOp() == StructuredQuery.UnaryFilter.Operator.IS_NOT_NAN || u.getOp() == StructuredQuery.UnaryFilter.Operator.IS_NOT_NULL) {
                    List<String> p = fieldPath(u.getField().getFieldPath());
                    String k = FsValues.joinPath(p);
                    out.add(k);
                    paths.put(k, p);
                }
                break;
            }
            default:
                break;
        }
    }

    private List<Term> effectiveTerms() {
        List<Term> out = new ArrayList<>();
        boolean lastDesc = false;
        for (var o : q.getOrderByList()) {
            List<String> p = fieldPath(o.getField().getFieldPath());
            boolean desc = o.getDirection() == StructuredQuery.Direction.DESCENDING;
            for (Term t : out) {
                if (t.path().equals(p)) {
                    throw FsException.invalid("order by clause cannot contain duplicate fields " + o.getField().getFieldPath());
                }
            }
            out.add(new Term(p, desc));
            lastDesc = desc;
        }
        if (where != null) {
            TreeSet<String> ineq = new TreeSet<>();
            Map<String, List<String>> paths = new LinkedHashMap<>();
            collectInequalityFields(where, ineq, paths);
            for (String k : ineq) {
                List<String> p = paths.get(k);
                boolean present = false;
                for (Term t : out) {
                    if (t.path().equals(p)) {
                        present = true;
                    }
                }
                if (!present) {
                    out.add(new Term(p, false));
                    lastDesc = false;
                }
            }
        }
        boolean hasName = false;
        for (Term t : out) {
            hasName |= t.isName();
        }
        if (!hasName) {
            out.add(new Term(NAME, lastDesc));
        }
        return out;
    }

    private void validateCursors() {
        for (Cursor c : new Cursor[] {q.hasStartAt() ? q.getStartAt() : null, q.hasEndAt() ? q.getEndAt() : null}) {
            if (c == null) {
                continue;
            }
            if (c.getValuesCount() > q.getOrderByCount()) {
                throw FsException.invalid("Cursor has too many values.");
            }
            for (int i = 0; i < c.getValuesCount(); i++) {
                if (terms.get(i).isName() && c.getValues(i).getValueTypeCase() != Value.ValueTypeCase.REFERENCE_VALUE) {
                    throw FsException.invalid("Cursor __key__ value is not a document reference.");
                }
            }
        }
    }

    // ------------------------------------------------------------------ scope

    FsStore.Scope scope() {
        String parentRel = parent.rel();
        if (q.getFromCount() == 0) {
            return FsStore.Scope.children(parentRel);
        }
        var from = q.getFrom(0);
        if (from.getAllDescendants()) {
            return new FsStore.Scope(null, from.getCollectionId().isEmpty() ? null : from.getCollectionId(), parentRel, null);
        }
        if (from.getCollectionId().isEmpty()) {
            throw FsException.invalid("Query must specify a collection id");
        }
        return FsStore.Scope.collection(parentRel.isEmpty() ? from.getCollectionId() : parentRel + "/" + from.getCollectionId());
    }

    // ------------------------------------------------------------------ filter evaluation

    private boolean matches(FsStore.Doc d, StructuredQuery.Filter f) {
        switch (f.getFilterTypeCase()) {
            case COMPOSITE_FILTER: {
                var c = f.getCompositeFilter();
                boolean and = c.getOp() == StructuredQuery.CompositeFilter.Operator.AND;
                if (c.getFiltersCount() == 0) {
                    return true;
                }
                for (var x : c.getFiltersList()) {
                    boolean m = matches(d, x);
                    if (and && !m) {
                        return false;
                    }
                    if (!and && m) {
                        return true;
                    }
                }
                return and;
            }
            case FIELD_FILTER:
                return fieldMatches(d, f.getFieldFilter());
            case UNARY_FILTER:
                return unaryMatches(d, f.getUnaryFilter());
            default:
                return true;
        }
    }

    Value fieldValue(FsStore.Doc d, List<String> path) {
        if (path == NAME || path.equals(NAME)) {
            return FsValues.ofRef(db.docsRoot() + "/" + d.rel);
        }
        return FsValues.get(d.fields, path);
    }

    private static boolean isNull(Value v) {
        return v.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE;
    }

    /** Equality as filters see it: null and NaN are never equal to anything (IEEE), 1 == 1.0. */
    private static boolean eq(Value a, Value b) {
        if (isNull(a) || isNull(b) || FsValues.isNaN(a) || FsValues.isNaN(b)) {
            return false;
        }
        return FsValues.typeOrder(a) == FsValues.typeOrder(b) && FsValues.compare(a, b) == 0;
    }

    private boolean unaryMatches(FsStore.Doc d, StructuredQuery.UnaryFilter u) {
        Value v = fieldValue(d, fieldPath(u.getField().getFieldPath()));
        if (v == null) {
            return false;
        }
        switch (u.getOp()) {
            case IS_NAN:
                return FsValues.isNaN(v);
            case IS_NULL:
                return isNull(v);
            case IS_NOT_NAN:
                return !isNull(v) && !FsValues.isNaN(v);
            case IS_NOT_NULL:
                return !isNull(v);
            default:
                return false;
        }
    }

    private boolean fieldMatches(FsStore.Doc d, StructuredQuery.FieldFilter ff) {
        Value v = fieldValue(d, fieldPath(ff.getField().getFieldPath()));
        if (v == null) {
            return false;
        }
        Value c = ff.getValue();
        boolean cNullOrNan = isNull(c) || FsValues.isNaN(c);
        switch (ff.getOp()) {
            case EQUAL:
                return eq(v, c);
            case NOT_EQUAL:
                return !isNull(c) && !isNull(v) && !eq(v, c);
            case LESS_THAN:
                return !cNullOrNan && !FsValues.isNaN(v) && sameBracket(v, c) && FsValues.compare(v, c) < 0;
            case LESS_THAN_OR_EQUAL:
                return !cNullOrNan && !FsValues.isNaN(v) && sameBracket(v, c) && FsValues.compare(v, c) <= 0;
            case GREATER_THAN:
                return !cNullOrNan && !FsValues.isNaN(v) && sameBracket(v, c) && FsValues.compare(v, c) > 0;
            case GREATER_THAN_OR_EQUAL:
                return !cNullOrNan && !FsValues.isNaN(v) && sameBracket(v, c) && FsValues.compare(v, c) >= 0;
            case ARRAY_CONTAINS:
                if (v.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
                    return false;
                }
                for (Value x : v.getArrayValue().getValuesList()) {
                    if (eq(x, c)) {
                        return true;
                    }
                }
                return false;
            case IN:
                for (Value x : c.getArrayValue().getValuesList()) {
                    if (eq(v, x)) {
                        return true;
                    }
                }
                return false;
            case NOT_IN:
                if (isNull(v)) {
                    return false;
                }
                for (Value x : c.getArrayValue().getValuesList()) {
                    if (isNull(x)) {
                        return false;
                    }
                }
                for (Value x : c.getArrayValue().getValuesList()) {
                    if (eq(v, x)) {
                        return false;
                    }
                }
                return true;
            case ARRAY_CONTAINS_ANY:
                if (v.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
                    return false;
                }
                for (Value x : v.getArrayValue().getValuesList()) {
                    for (Value y : c.getArrayValue().getValuesList()) {
                        if (eq(x, y)) {
                            return true;
                        }
                    }
                }
                return false;
            default:
                return false;
        }
    }

    /** Inequality filters only match values in the constant's type bracket (numbers, incl. int vs double, share one). */
    private static boolean sameBracket(Value a, Value b) {
        return FsValues.typeOrder(a) == FsValues.typeOrder(b);
    }

    // ------------------------------------------------------------------ ordering and cursors

    /** The values a document contributes to each ordering term, or null when it lacks an order-by field. */
    private Value[] keys(FsStore.Doc d) {
        Value[] k = new Value[terms.size()];
        for (int i = 0; i < k.length; i++) {
            Value v = fieldValue(d, terms.get(i).path());
            if (v == null) {
                return null;
            }
            k[i] = v;
        }
        return k;
    }

    private int compareKeys(Value[] a, Value[] b, int n) {
        for (int i = 0; i < n; i++) {
            int c = FsValues.compare(a[i], b[i]);
            if (c != 0) {
                return terms.get(i).desc() ? -c : c;
            }
        }
        return 0;
    }

    private boolean afterStart(Value[] k) {
        if (!q.hasStartAt()) {
            return true;
        }
        Cursor c = q.getStartAt();
        Value[] cv = c.getValuesList().toArray(new Value[0]);
        int cmp = compareKeys(k, cv, cv.length);
        return c.getBefore() ? cmp >= 0 : cmp > 0;
    }

    private boolean beforeEnd(Value[] k) {
        if (!q.hasEndAt()) {
            return true;
        }
        Cursor c = q.getEndAt();
        Value[] cv = c.getValuesList().toArray(new Value[0]);
        int cmp = compareKeys(k, cv, cv.length);
        return c.getBefore() ? cmp < 0 : cmp <= 0;
    }

    /** True when the whole ordering is {@code __name__} alone, so the store's order is the answer and scans can stop early. */
    private boolean nameOnly() {
        return terms.size() == 1 && terms.get(0).isName();
    }

    /** The same query bound to a read time. */
    FsQuery at(Long us) {
        return new FsQuery(store, db, parent, q, us);
    }

    // ------------------------------------------------------------------ point evaluation (Listen)

    /** True when limit or offset make membership depend on the other documents (Listen then re-runs the query). */
    boolean windowed() {
        return q.hasLimit() || q.getOffset() > 0;
    }

    /** Whether a document lies in the query's scope, ignoring filters. */
    boolean inScope(String rel) {
        FsStore.Scope s = scope();
        String cp = FsNames.collPath(rel);
        if (s.collPath() != null) {
            return cp.equals(s.collPath());
        }
        if (s.collId() != null && !FsNames.collId(rel).equals(s.collId())) {
            return false;
        }
        if (s.directDepth() != null && FsNames.depth(rel) != s.directDepth()) {
            return false;
        }
        String base = s.descendantsOf();
        return base == null || base.isEmpty() || rel.startsWith(base + "/");
    }

    /** Filter, order-by existence and cursors (no limit/offset) for one document. */
    boolean accepts(FsStore.Doc d) {
        if (!inScope(d.rel)) {
            return false;
        }
        if (where != null && !matches(d, where)) {
            return false;
        }
        Value[] k = keys(d);
        return k != null && afterStart(k) && beforeEnd(k);
    }

    // ------------------------------------------------------------------ execution

    /** Vector search: the {@code limit} documents whose vector field is nearest to the query vector (brute force over the matches). */
    private Iterator<FsStore.Doc> runNearest() {
        var fn = q.getFindNearest();
        double[] qv = vec(fn.getQueryVector());
        List<String> path = fieldPath(fn.getVectorField().getFieldPath());
        boolean dot = fn.getDistanceMeasure() == StructuredQuery.FindNearest.DistanceMeasure.DOT_PRODUCT;
        Iterator<FsStore.Doc> it = matching(scope());
        List<Object[]> scored = new ArrayList<>();
        while (it.hasNext()) {
            FsStore.Doc d = it.next();
            Value v = FsValues.get(d.fields, path);
            if (v == null || !FsValues.isVector(v)) {
                continue;
            }
            double[] dv = vec(v);
            if (dv.length != qv.length) {
                continue;
            }
            double dist = distance(fn.getDistanceMeasure(), qv, dv);
            if (fn.hasDistanceThreshold()) {
                double t = fn.getDistanceThreshold().getValue();
                if (dot ? dist < t : dist > t) {
                    continue;
                }
            }
            scored.add(new Object[] {dist, d});
        }
        scored.sort((a, b) -> {
            int c = dot ? Double.compare((double) b[0], (double) a[0]) : Double.compare((double) a[0], (double) b[0]);
            return c != 0 ? c : java.util.Arrays.compareUnsigned(((FsStore.Doc) a[1]).nameKey(), ((FsStore.Doc) b[1]).nameKey());
        });
        List<FsStore.Doc> out = new ArrayList<>();
        int limit = fn.getLimit().getValue();
        for (Object[] s : scored) {
            if (out.size() >= limit) {
                break;
            }
            FsStore.Doc d = (FsStore.Doc) s[1];
            if (!fn.getDistanceResultField().isEmpty()) {
                d = new FsStore.Doc(d.rel, FsValues.set(d.fields, FsValues.parseFieldPath(fn.getDistanceResultField()), 0,
                        FsValues.ofDouble((double) s[0])), d.createUs, d.updateUs);
            }
            out.add(d);
        }
        return out.iterator();
    }

    private static double[] vec(Value v) {
        var vs = v.getMapValue().getFieldsMap().get("value").getArrayValue().getValuesList();
        double[] out = new double[vs.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = vs.get(i).getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? vs.get(i).getIntegerValue() : vs.get(i).getDoubleValue();
        }
        return out;
    }

    static double distance(StructuredQuery.FindNearest.DistanceMeasure m, double[] a, double[] b) {
        double dot = 0;
        double na = 0;
        double nb = 0;
        double sq = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
            sq += (a[i] - b[i]) * (a[i] - b[i]);
        }
        switch (m) {
            case EUCLIDEAN:
                return Math.sqrt(sq);
            case COSINE:
                return na == 0 || nb == 0 ? 1.0 : 1.0 - dot / (Math.sqrt(na) * Math.sqrt(nb));
            default:
                return dot;
        }
    }

    /** Streams the matching documents in final order with offset and limit applied. */
    Iterator<FsStore.Doc> run() {
        if (q.hasFindNearest()) {
            return runNearest();
        }
        FsStore.Scope scope = scope();
        long offset = q.getOffset();
        long limit = q.hasLimit() ? q.getLimit().getValue() : -1;
        Iterator<FsStore.Doc> matches = matching(scope);
        if (nameOnly()) {
            return window(matches, offset, limit);
        }
        Comparator<Object[]> cmp = (x, y) -> {
            int c = compareKeys((Value[]) x[0], (Value[]) y[0], terms.size());
            return c;
        };
        List<Object[]> sorted;
        if (limit >= 0 && offset + limit < 100_000) {
            int cap = (int) (offset + limit);
            if (cap == 0) {
                return List.<FsStore.Doc>of().iterator();
            }
            PriorityQueue<Object[]> heap = new PriorityQueue<>(cmp.reversed());
            while (matches.hasNext()) {
                FsStore.Doc d = matches.next();
                Object[] e = {keys(d), d};
                if (heap.size() < cap) {
                    heap.add(e);
                } else if (cmp.compare(e, heap.peek()) < 0) {
                    heap.poll();
                    heap.add(e);
                }
            }
            sorted = new ArrayList<>(heap);
        } else {
            sorted = new ArrayList<>();
            while (matches.hasNext()) {
                FsStore.Doc d = matches.next();
                sorted.add(new Object[] {keys(d), d});
            }
        }
        sorted.sort(cmp);
        List<FsStore.Doc> docs = new ArrayList<>(sorted.size());
        for (Object[] e : sorted) {
            docs.add((FsStore.Doc) e[1]);
        }
        return window(docs.iterator(), offset, limit);
    }

    /** Documents passing the filter, order-by existence and cursors, in store (or reversed store) order. */
    Iterator<FsStore.Doc> matching(FsStore.Scope scope) {
        boolean nameOnly = nameOnly();
        boolean desc = nameOnly && terms.get(0).desc();
        byte[] lo = null;
        byte[] hi = null;
        boolean loIncl = true;
        boolean hiIncl = true;
        if (nameOnly) {
            // push cursors on __name__ down as key bounds (asc: start is the lower bound; desc: start is the upper bound)
            Cursor s = q.hasStartAt() ? q.getStartAt() : null;
            Cursor e = q.hasEndAt() ? q.getEndAt() : null;
            byte[] sk = s != null && s.getValuesCount() == 1 ? nameKeyOf(s.getValues(0)) : null;
            byte[] ek = e != null && e.getValuesCount() == 1 ? nameKeyOf(e.getValues(0)) : null;
            if (!desc) {
                if (sk != null) {
                    lo = sk;
                    loIncl = s.getBefore();
                }
                if (ek != null) {
                    hi = ek;
                    hiIncl = !e.getBefore();
                }
            } else {
                if (sk != null) {
                    hi = sk;
                    hiIncl = s.getBefore();
                }
                if (ek != null) {
                    lo = ek;
                    loIncl = !e.getBefore();
                }
            }
        }
        Iterator<FsStore.Doc> raw = store.scan(db, scope, desc, atUs, lo, loIncl, hi, hiIncl);
        return new Iterator<>() {
            private FsStore.Doc next;

            private void advance() {
                while (next == null && raw.hasNext()) {
                    FsStore.Doc d = raw.next();
                    if (where != null && !FsQuery.this.matches(d, where)) {
                        continue;
                    }
                    Value[] k = keys(d);
                    if (k == null || !afterStart(k) || !beforeEnd(k)) {
                        continue;
                    }
                    next = d;
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return next != null;
            }

            @Override
            public FsStore.Doc next() {
                advance();
                if (next == null) {
                    throw new NoSuchElementException();
                }
                FsStore.Doc d = next;
                next = null;
                return d;
            }
        };
    }

    private byte[] nameKeyOf(Value ref) {
        String r = ref.getReferenceValue();
        String root = db.docsRoot() + "/";
        if (!r.startsWith(root)) {
            return null;
        }
        String rel = r.substring(root.length());
        return rel.isEmpty() ? null : FsNames.nameKey(rel);
    }

    private static Iterator<FsStore.Doc> window(Iterator<FsStore.Doc> it, long offset, long limit) {
        return new Iterator<>() {
            private long skipped;
            private long taken;

            @Override
            public boolean hasNext() {
                while (skipped < offset && it.hasNext()) {
                    it.next();
                    skipped++;
                }
                return (limit < 0 || taken < limit) && it.hasNext();
            }

            @Override
            public FsStore.Doc next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                taken++;
                return it.next();
            }
        };
    }

    // ------------------------------------------------------------------ projection

    /** Applies {@code select}; an empty projection keeps only the document name. */
    Map<String, Value> project(Map<String, Value> fields) {
        if (!q.hasSelect()) {
            return fields;
        }
        Map<String, Value> out = new LinkedHashMap<>();
        for (var fr : q.getSelect().getFieldsList()) {
            List<String> p = fieldPath(fr.getFieldPath());
            if (p == NAME) {
                continue;
            }
            Value v = FsValues.get(fields, p);
            if (v != null) {
                out = FsValues.set(out, p, 0, v);
            }
        }
        return out;
    }
}
