package com.sayonora.wire.gremlinwire;

import static com.sayonora.wire.gremlinwire.Engine.NONE;
import static com.sayonora.wire.gremlinwire.Engine.batched;
import static com.sayonora.wire.gremlinwire.Engine.byStrict;
import static com.sayonora.wire.gremlinwire.Engine.byValue;
import static com.sayonora.wire.gremlinwire.Engine.drain;
import static com.sayonora.wire.gremlinwire.Engine.exists;
import static com.sayonora.wire.gremlinwire.Engine.filter;
import static com.sayonora.wire.gremlinwire.Engine.flat;
import static com.sayonora.wire.gremlinwire.Engine.lazy;
import static com.sayonora.wire.gremlinwire.Engine.map;
import static com.sayonora.wire.gremlinwire.Engine.once;
import static com.sayonora.wire.gremlinwire.Engine.sub;

import com.sayonora.wire.gremlinwire.Engine.Ctx;
import com.sayonora.wire.gremlinwire.Engine.Step;
import com.sayonora.wire.gremlinwire.Engine.Tr;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The step implementations of the interpreter (see {@link Engine} for traversers, context and helpers). */
final class Steps {

    private Steps() {
    }

    private static G.GremlinError err(String m) {
        return G.GremlinError.script(m);
    }

    static String[] strs(Object[] a) {
        List<String> l = new ArrayList<>();
        for (Object o : a) {
            if (o instanceof Collection<?> c) {
                for (Object x : c) {
                    l.add(String.valueOf(x));
                }
            } else if (o instanceof String s) {
                l.add(s);
            } else if (o != null && !(o instanceof G.Bytecode)) {
                l.add(String.valueOf(o));
            }
        }
        return l.toArray(new String[0]);
    }

    private static int intArg(Object o) {
        if (o instanceof Number n) {
            return n.intValue();
        }
        throw err("expected a number but got " + o);
    }

    private static long longArg(Object o) {
        if (o instanceof Number n) {
            return n.longValue();
        }
        throw err("expected a number but got " + o);
    }

    private static G.Scope scopeOf(Object[] a) {
        return a.length > 0 && a[0] instanceof G.Scope s ? s : G.Scope.global;
    }

    private static Object[] rest(Object[] a, int from) {
        return java.util.Arrays.copyOfRange(a, Math.min(from, a.length), a.length);
    }

    static Iterator<Tr> apply(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        switch (s.name) {
            // ---------------------------------------------------------------- sources
            case "V": {
                List<Object> ids = Engine.flatIds(a);
                if (ids.isEmpty() && a.length > 0) {
                    return filter(in, t -> false);
                }
                if (ids.isEmpty()) {
                    return flat(in, t -> lazy(() -> map(ctx.store.allVertices(GraphStore.VFilter.NONE), v -> t.child(v))));
                }
                return flat(in, t -> lazy(() -> map(ctx.store.vertices(ids).iterator(), v -> t.child(v))));
            }
            case "E": {
                List<Object> ids = Engine.flatIds(a);
                if (ids.isEmpty() && a.length > 0) {
                    return filter(in, t -> false);
                }
                if (ids.isEmpty()) {
                    return flat(in, t -> lazy(() -> map(ctx.store.allEdges(), e -> t.child(e))));
                }
                // TinkerGraph's edge id manager does not convert: an Integer/Short/Byte id never finds a (Long) edge id
                List<Object> strict = new ArrayList<>();
                for (Object id : ids) {
                    if (!(id instanceof Integer || id instanceof Short || id instanceof Byte)) {
                        strict.add(id);
                    }
                }
                return flat(in, t -> lazy(() -> map(ctx.store.edges(strict).iterator(), e -> t.child(e))));
            }
            case "inject": {
                // the injected values form one bulked traverser set: equal values merge into the position of their first occurrence
                java.util.LinkedHashMap<Object, Integer> bulks = new java.util.LinkedHashMap<>();
                for (Object v : Arrays2.list(a)) {
                    bulks.merge(v, 1, Integer::sum);
                }
                Iterator<Tr> up = filter(in, t -> t.path != null);
                return concat(lazy(() -> {
                    List<Tr> out = new ArrayList<>();
                    for (Map.Entry<Object, Integer> e : bulks.entrySet()) {
                        for (int i = 0; i < e.getValue(); i++) {
                            out.add(new Tr(e.getKey(), new Engine.Node(null, e.getKey(), Set.of()), ctx.hasSack ? ctx.sackInit : null, null));
                        }
                    }
                    return out.iterator();
                }), up);
            }
            case "addV":
                return Mutations.addV(s, in, ctx);
            case "addE":
                return Mutations.addE(s, in, ctx);
            case "mergeV":
                return Mutations.mergeV(s, in, ctx);
            case "mergeE":
                return Mutations.mergeE(s, in, ctx);
            case "property":
                return Mutations.property(s, in, ctx);
            case "drop":
                return Mutations.drop(in, ctx);

            // ---------------------------------------------------------------- navigation
            case "out":
                return adjacent(in, G.Direction.OUT, strs(a), ctx, true);
            case "in":
                return adjacent(in, G.Direction.IN, strs(a), ctx, true);
            case "both":
                return adjacent(in, G.Direction.BOTH, strs(a), ctx, true);
            case "outE":
                return adjacent(in, G.Direction.OUT, strs(a), ctx, false);
            case "inE":
                return adjacent(in, G.Direction.IN, strs(a), ctx, false);
            case "bothE":
                return adjacent(in, G.Direction.BOTH, strs(a), ctx, false);
            case "outV":
                return edgeEnds(in, ctx, true, false, false);
            case "inV":
                return edgeEnds(in, ctx, false, true, false);
            case "bothV":
                return edgeEnds(in, ctx, true, true, false);
            case "otherV":
                return edgeEnds(in, ctx, false, false, true);

            // ---------------------------------------------------------------- element access
            case "values": {
                String[] keys = strs(a);
                return flat(in, t -> map(Engine.propValues(t.v, keys).iterator(), v -> t.child(v)));
            }
            case "properties": {
                String[] keys = strs(a);
                return flat(in, t -> map(Engine.props(t.v, keys).iterator(), v -> t.child(v)));
            }
            case "valueMap":
                return map(in, t -> t.child(valueMap(t.v, s, true)));
            case "propertyMap":
                return map(in, t -> t.child(valueMap(t.v, s, false)));
            case "elementMap":
                return map(in, t -> t.child(elementMap(t.v, s)));
            case "id":
                return map(in, t -> {
                    Object v = Engine.token(t.v, G.T.id);
                    if (v == NONE) {
                        throw err("The provided traverser does not map to a value: " + t.v);
                    }
                    return t.child(v);
                });
            case "label":
                return map(in, t -> {
                    Object v = Engine.token(t.v, G.T.label);
                    if (v == NONE) {
                        throw err("The provided traverser does not map to a value: " + t.v);
                    }
                    return t.child(v);
                });
            case "key":
                return map(in, t -> t.child(need(Engine.token(t.v, G.T.key), t)));
            case "value":
                return map(in, t -> t.child(need(Engine.token(t.v, G.T.value), t)));
            case "constant": {
                Object c = a[0];
                return map(in, t -> t.child(c));
            }
            case "identity":
                return in;
            case "as": {
                List<String> ls = List.of(strs(a));
                return map(in, t -> t.labeled(ls));
            }
            case "select":
                return select(s, in, ctx);
            case "project":
                return project(s, in, ctx);
            case "loops": {
                String name = a.length > 0 && a[0] instanceof String n ? n : null;
                return map(in, t -> {
                    if (t.loops == null) {
                        if (ctx.hasRepeat) {
                            return t.child(0);
                        }
                        throw err("This traverser does not support loops: " + t.v);
                    }
                    Engine.Loop l = t.loops;
                    while (name != null && l != null && !name.equals(l.name)) {
                        l = l.next;
                    }
                    return t.child(l == null ? 0 : l.n);
                });
            }
            case "index":
                return map(in, t -> {
                    List<Object> pairs = new ArrayList<>();
                    if (t.v instanceof Collection<?> c) {
                        int i = 0;
                        for (Object x : c) {
                            pairs.add(new ArrayList<>(List.of(x == null ? (Object) "null" : x, i++)));
                        }
                    } else {
                        List<Object> pair = new ArrayList<>();
                        pair.add(t.v);
                        pair.add(0);
                        pairs.add(pair);
                    }
                    return t.child(pairs);
                });

            // ---------------------------------------------------------------- filters
            case "has":
                return filter(in, hasPredicate(a, ctx));
            case "hasNot": {
                Object k = a[0];
                if (k instanceof String key) {
                    return filter(in, t -> Engine.propValues(t.v, new String[] {key}).isEmpty());
                }
                if (k instanceof G.Bytecode bc) {
                    return filter(in, t -> !exists(bc, t, ctx));
                }
                throw err("hasNot() expects a property key");
            }
            case "hasLabel": {
                Predicate2 p = labelTest(a);
                return filter(in, t -> t.v instanceof G.Element e ? p.test(e.label()) : t.v instanceof G.VProp vp ? p.test(vp.key) : t.v instanceof G.Prop pr && p.test(pr.key));
            }
            case "hasId": {
                if (a.length == 1 && a[0] instanceof G.P p) {
                    return filter(in, t -> t.v instanceof G.Element e && Engine.test(p, e.id()));
                }
                Set<Object> ids = new HashSet<>();
                for (Object x : Engine.flatIds(a)) {
                    ids.add(GraphStore.canonId(x));
                }
                return filter(in, t -> t.v instanceof G.Element e && ids.contains(GraphStore.canonId(e.id())));
            }
            case "hasKey": {
                Predicate2 p = labelTest(a);
                return filter(in, t -> (t.v instanceof G.VProp vp && p.test(vp.key)) || (t.v instanceof G.Prop pr && p.test(pr.key)));
            }
            case "hasValue": {
                Object x = a.length == 1 ? a[0] : null;
                return filter(in, t -> {
                    Object v = t.v instanceof G.VProp vp ? vp.value : t.v instanceof G.Prop pr ? pr.value : NONE;
                    if (v == NONE) {
                        return false;
                    }
                    for (Object o : a) {
                        if (o instanceof G.P p ? Engine.test(p, v) : Cmp.eq(v, o)) {
                            return true;
                        }
                    }
                    return a.length == 0 && x == null;
                });
            }
            case "is": {
                Object x = a[0];
                return filter(in, t -> x instanceof G.P p ? Engine.test(p, t.v) : Cmp.eq(t.v, x));
            }
            case "where":
                return where(s, in, ctx);
            case "filter": {
                Object f = a[0];
                if (f instanceof G.Closure c) {
                    return filter(in, t -> truthy(ctx.call(c, Engine.viewOf(t))));
                }
                if (f instanceof G.P p) {
                    return filter(in, t -> Engine.test(p, t.v));
                }
                G.Bytecode bc = (G.Bytecode) f;
                return filter(in, t -> exists(bc, t, ctx));
            }
            case "not": {
                if (a[0] instanceof G.P p) {
                    G.P n = p.negate();
                    return filter(in, t -> Engine.test(n, t.v));
                }
                G.Bytecode bc = (G.Bytecode) a[0];
                return filter(in, t -> !exists(bc, t, ctx));
            }
            case "and": {
                if (a.length == 0) {
                    return in;
                }
                return filter(in, t -> {
                    for (Object x : a) {
                        if (!condition(x, t, ctx)) {
                            return false;
                        }
                    }
                    return true;
                });
            }
            case "or": {
                return filter(in, t -> {
                    for (Object x : a) {
                        if (condition(x, t, ctx)) {
                            return true;
                        }
                    }
                    return false;
                });
            }
            case "none", "discard":
                return filter(in, t -> false);
            case "coin": {
                double p = ((Number) a[0]).doubleValue();
                return filter(in, t -> ctx.rnd.nextDouble() < p);
            }
            case "simplePath":
                return filter(in, t -> isSimple(t));
            case "cyclicPath":
                return filter(in, t -> !isSimple(t));
            case "dedup":
                return dedup(s, in, ctx);
            case "limit":
                return limit(s, in, 0, false);
            case "skip":
                return skip(s, in);
            case "range":
                return range(s, in);
            case "tail":
                return tail(s, in);
            case "sample":
                return sample(s, in, ctx);
            case "timeLimit", "barrier", "with", "withSideEffect", "withSack", "withStrategies", "withBulk", "withPath":
                return in;

            // ---------------------------------------------------------------- map / branch
            case "map": {
                Object f = a[0];
                if (f instanceof G.Closure c) {
                    return map(in, t -> t.child(ctx.call(c, Engine.viewOf(t))));
                }
                G.Bytecode bc = (G.Bytecode) f;
                return flat(in, t -> {
                    Iterator<Tr> it = sub(bc, t, ctx);
                    if (!it.hasNext()) {
                        return Collections.emptyIterator();
                    }
                    return once(t.child(it.next().v));
                });
            }
            case "flatMap": {
                Object f = a[0];
                if (f instanceof G.Closure c) {
                    return flat(in, t -> {
                        Object r = ctx.call(c, Engine.viewOf(t));
                        return map(iter(r), x -> t.child(x));
                    });
                }
                G.Bytecode bc = (G.Bytecode) f;
                return flat(in, t -> map(sub(bc, t, ctx), r -> t.child(r.v)));
            }
            case "local": {
                G.Bytecode bc = (G.Bytecode) a[0];
                return flat(in, t -> sub(bc, t, ctx));
            }
            case "union": {
                return flat(in, t -> {
                    List<Iterator<Tr>> its = new ArrayList<>();
                    for (Object x : a) {
                        G.Bytecode bc = (G.Bytecode) x;
                        its.add(lazy(() -> sub(bc, t, ctx)));
                    }
                    return chain(its);
                });
            }
            case "coalesce": {
                return flat(in, t -> {
                    for (Object x : a) {
                        Iterator<Tr> it = sub((G.Bytecode) x, t, ctx);
                        if (it.hasNext()) {
                            // coalesce is a flat-map: each result extends the path by itself only
                            return map(it, r -> t.child(r.v));
                        }
                    }
                    return Collections.emptyIterator();
                });
            }
            case "optional": {
                G.Bytecode bc = (G.Bytecode) a[0];
                return flat(in, t -> {
                    Iterator<Tr> it = sub(bc, t, ctx);
                    return it.hasNext() ? it : once(t);
                });
            }
            case "choose":
                return choose(s, in, ctx);
            case "repeat":
                return repeat(s, in, ctx);
            case "sideEffect": {
                Object f = a[0];
                if (f instanceof G.Closure c) {
                    return map(in, t -> {
                        ctx.call(c, Engine.viewOf(t));
                        return t;
                    });
                }
                G.Bytecode bc = (G.Bytecode) f;
                return map(in, t -> {
                    Iterator<Tr> it = sub(bc, t, ctx);
                    while (it.hasNext()) {
                        it.next();
                    }
                    return t;
                });
            }

            // ---------------------------------------------------------------- reducing / collecting
            case "count":
                return count(s, in);
            case "sum", "min", "max", "mean":
                return numeric(s, in);
            case "fold":
                return fold(s, in, ctx);
            case "unfold":
                return flat(in, t -> map(unfoldIter(t.v), x -> t.child(x)));
            case "order":
                return order(s, in, ctx);
            case "group":
                return group(s, in, ctx);
            case "groupCount":
                return groupCount(s, in, ctx);
            case "path":
                return flat(in, t -> {
                    Object p = pathBy(t, s, ctx);
                    return p == NONE ? Collections.emptyIterator() : once(t.child(p));
                });
            case "tree":
                return tree(s, in, ctx);
            case "aggregate":
                return aggregate(s, in, ctx);
            case "cap":
                return cap(s, in, ctx);
            case "sack":
                return sack(s, in, ctx);
            case "math":
                return MathStep.apply(s, in, ctx);
            case "asString", "toLower", "toUpper", "trim", "lTrim", "rTrim", "length", "split", "replace", "substring", "concat", "reverse":
                return Strings.apply(s, in, ctx);
            case "explain", "profile", "subgraph", "match", "program", "pageRank", "peerPressure", "shortestPath", "connectedComponent", "io", "call":
                throw G.GremlinError.translation("The " + s.name + "() step is not supported by this Gremlin server");
            default:
                throw G.GremlinError.translation("No signature of method: DefaultGraphTraversal." + s.name + "() is applicable for the given arguments");
        }
    }

    interface Predicate2 {
        boolean test(String label);
    }

    private static Object need(Object v, Tr t) {
        if (v == NONE) {
            throw err("The provided traverser does not map to a value: " + t.v);
        }
        return v;
    }

    static boolean truthy(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Boolean b) {
            return b;
        }
        if (o instanceof Number n) {
            return n.doubleValue() != 0;
        }
        if (o instanceof Collection<?> c) {
            return !c.isEmpty();
        }
        if (o instanceof String s) {
            return !s.isEmpty();
        }
        return true;
    }

    private static boolean condition(Object x, Tr t, Ctx ctx) {
        if (x instanceof G.P p) {
            return Engine.test(p, t.v);
        }
        return exists((G.Bytecode) x, t, ctx);
    }

    /** Elementary iterable view of an arbitrary result value (script collection semantics). */
    static Iterator<Object> iter(Object r) {
        if (r == null) {
            return Collections.emptyIterator();
        }
        if (r instanceof Collection<?> c) {
            return new ArrayList<Object>(c).iterator();
        }
        if (r instanceof Map<?, ?> m) {
            List<Object> l = new ArrayList<>();
            for (Map.Entry<?, ?> e : m.entrySet()) {
                l.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()));
            }
            return l.iterator();
        }
        if (r instanceof Object[] arr) {
            return Arrays2.list(arr).iterator();
        }
        if (r instanceof Iterator<?> it) {
            @SuppressWarnings("unchecked")
            Iterator<Object> x = (Iterator<Object>) it;
            return x;
        }
        return once(r);
    }

    static Iterator<Object> unfoldIter(Object v) {
        if (v instanceof Map<?, ?> || v instanceof Collection<?> || v instanceof Object[] || v instanceof Iterator<?>) {
            return iter(v);
        }
        return once(v);
    }

    private static <A> Iterator<A> concat(Iterator<A> a, Iterator<A> b) {
        return chain(List.of(a, b));
    }

    static <A> Iterator<A> chain(List<Iterator<A>> its) {
        return flat(its.iterator(), i -> i);
    }

    /** Tiny helper (avoids importing java.util.Arrays under a clashing name). */
    static final class Arrays2 {
        static List<Object> list(Object[] a) {
            return new ArrayList<>(java.util.Arrays.asList(a));
        }
    }

    // ------------------------------------------------------------------ navigation

    private static Iterator<Tr> adjacent(Iterator<Tr> in, G.Direction dir, String[] labels, Ctx ctx, boolean vertices) {
        return batched(in, 128, chunk -> {
            List<Object> ids = new ArrayList<>();
            for (Tr t : chunk) {
                if (!(t.v instanceof G.Vertex v)) {
                    throw err("Cannot use a vertex step on a non-vertex: " + t.v);
                }
                ids.add(v.id);
            }
            Map<Object, List<G.Edge>> adj = ctx.store.adjacent(ids, dir, labels);
            Map<Object, G.Vertex> far = Collections.emptyMap();
            if (vertices) {
                Set<Object> farIds = new LinkedHashSet<>();
                for (Tr t : chunk) {
                    Object me = ((G.Vertex) t.v).id;
                    for (G.Edge e : adj.getOrDefault(me, List.of())) {
                        farIds.add(e.outId.equals(me) ? e.inId : e.outId);
                    }
                }
                far = new HashMap<>();
                for (G.Vertex v : ctx.store.vertices(new ArrayList<>(farIds))) {
                    far.put(v.id, v);
                }
            }
            List<Tr> out = new ArrayList<>();
            for (Tr t : chunk) {
                Object me = ((G.Vertex) t.v).id;
                List<G.Edge> es = adj.getOrDefault(me, List.of());
                if (dir == G.Direction.BOTH) {
                    // a self loop is reported once per direction by the store; keep both like TinkerGraph
                    es = new ArrayList<>(es);
                }
                for (G.Edge e : es) {
                    if (vertices) {
                        Object other = e.outId.equals(me) ? e.inId : e.outId;
                        G.Vertex ov = far.get(other);
                        if (ov != null) {
                            out.add(t.child(ov));
                        }
                    } else {
                        out.add(t.child(e));
                    }
                }
            }
            return out;
        });
    }

    private static Iterator<Tr> edgeEnds(Iterator<Tr> in, Ctx ctx, boolean out, boolean inn, boolean other) {
        return batched(in, 128, chunk -> {
            List<Object> ids = new ArrayList<>();
            for (Tr t : chunk) {
                if (!(t.v instanceof G.Edge e)) {
                    throw err("Cannot use an edge step on a non-edge: " + t.v);
                }
                if (other) {
                    Node2 prev = prevVertex(t);
                    if (prev == null) {
                        throw err("The path does not contain a vertex before the edge for otherV()");
                    }
                    ids.add(prev.v.id.equals(e.outId) ? e.inId : e.outId);
                } else {
                    if (out) {
                        ids.add(e.outId);
                    }
                    if (inn) {
                        ids.add(e.inId);
                    }
                }
            }
            Map<Object, G.Vertex> vs = new HashMap<>();
            for (G.Vertex v : ctx.store.vertices(ids)) {
                vs.put(v.id, v);
            }
            List<Tr> res = new ArrayList<>();
            for (Tr t : chunk) {
                G.Edge e = (G.Edge) t.v;
                List<Object> want = new ArrayList<>();
                if (other) {
                    Node2 prev = prevVertex(t);
                    want.add(prev.v.id.equals(e.outId) ? e.inId : e.outId);
                } else {
                    if (out) {
                        want.add(e.outId);
                    }
                    if (inn) {
                        want.add(e.inId);
                    }
                }
                for (Object id : want) {
                    G.Vertex v = vs.get(id);
                    if (v != null) {
                        res.add(t.child(v));
                    }
                }
            }
            return res;
        });
    }

    private record Node2(G.Vertex v) {
    }

    private static Node2 prevVertex(Tr t) {
        Engine.Node n = t.path == null ? null : t.path.parent;
        while (n != null) {
            if (n.obj instanceof G.Vertex v) {
                return new Node2(v);
            }
            n = n.parent;
        }
        return null;
    }

    // ------------------------------------------------------------------ has

    private static Predicate2 labelTest(Object[] a) {
        if (a.length == 1 && a[0] instanceof G.P p) {
            return l -> Engine.test(p, l);
        }
        Set<String> ls = new HashSet<>(List.of(strs(a)));
        return ls::contains;
    }

    private static java.util.function.Predicate<Tr> hasPredicate(Object[] a, Ctx ctx) {
        if (a.length == 1) {
            if (a[0] instanceof String key) {
                return t -> !Engine.propValues(t.v, new String[] {key}).isEmpty();
            }
            if (a[0] instanceof G.T tok) {
                return t -> Engine.token(t.v, tok) != NONE;
            }
            throw err("has() expects a property key");
        }
        if (a.length == 2) {
            return hasKv(a[0], a[1], ctx);
        }
        if (a.length == 3 && a[0] instanceof String label) {
            java.util.function.Predicate<Tr> kv = hasKv(a[1], a[2], ctx);
            return t -> t.v instanceof G.Element e && e.label().equals(label) && kv.test(t);
        }
        throw err("has() called with unsupported arguments");
    }

    private static boolean valueMatches(Object test, Object v) {
        if (test instanceof G.P p) {
            return Engine.test(p, v);
        }
        return Cmp.eq(v, test);
    }

    private static java.util.function.Predicate<Tr> hasKv(Object key, Object test, Ctx ctx) {
        if (key instanceof G.T tok) {
            return t -> {
                Object v = Engine.token(t.v, tok);
                if (v == NONE) {
                    return false;
                }
                if (tok == G.T.id && !(test instanceof G.P)) {
                    return Cmp.eq(GraphStore.canonId(v), GraphStore.canonId(test));
                }
                if (test instanceof G.Bytecode bc) {
                    return exists(bc, t.child(v), ctx);
                }
                return valueMatches(test, v);
            };
        }
        if (key == null) {
            return t -> false;
        }
        if (!(key instanceof String k)) {
            throw G.GremlinError.translation("has() expects a property key or T token");
        }
        String[] keys = {k};
        return t -> {
            if (!(t.v instanceof G.Element) && !(t.v instanceof G.VProp)) {
                return false;
            }
            List<Object> vals = Engine.propValues(t.v, keys);
            if (test instanceof G.Bytecode bc) {
                for (Object v : vals) {
                    if (exists(bc, t.child(v), ctx)) {
                        return true;
                    }
                }
                return false;
            }
            for (Object v : vals) {
                if (valueMatches(test, v)) {
                    return true;
                }
            }
            return false;
        };
    }

    private static boolean isSimple(Tr t) {
        Set<Object> seen = new HashSet<>();
        for (Engine.Node n = t.path; n != null; n = n.parent) {
            if (!seen.add(n.obj == null ? Engine.NONE : n.obj)) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ value maps

    private static Object valueMap(Object o, Step s, boolean unwrapMulti) {
        boolean tokens = false;
        boolean unwrapAll = false;
        List<Object> keysArgs = new ArrayList<>();
        for (Object x : s.args) {
            if (x instanceof Boolean) {
                tokens = true; // TinkerPop 3.8 includes the tokens for valueMap(false) as well
            } else if (x instanceof String) {
                keysArgs.add(x);
            }
        }
        for (Object[] w : s.with) {
            if (w.length > 0 && w[0] == G.WithOptions.tokens) {
                tokens = true;
            }
            if (w.length > 0 && w[0] instanceof String ws && ws.equals("~tinkerpop.valueMap.tokens")) {
                tokens = true;
            }
        }
        String[] keys = keysArgs.toArray(new String[0]);
        Map<Object, Object> m = new LinkedHashMap<>();
        if (o instanceof G.Vertex v) {
            if (tokens) {
                m.put(G.T.id, v.id);
                m.put(G.T.label, v.label);
            }
            List<String> ks = keys.length == 0 ? new ArrayList<>(v.props.keySet()) : List.of(keys);
            for (String k : ks) {
                List<G.VProp> l = v.vprops(k);
                if (l.isEmpty()) {
                    continue;
                }
                List<Object> vals = new ArrayList<>();
                for (G.VProp p : l) {
                    vals.add(unwrapMulti ? p.value : p);
                }
                m.put(k, vals);
            }
        } else if (o instanceof G.Edge e) {
            if (tokens) {
                m.put(G.T.id, e.id);
                m.put(G.T.label, e.label);
            }
            List<String> ks = keys.length == 0 ? new ArrayList<>(e.props.keySet()) : List.of(keys);
            for (String k : ks) {
                if (e.props.containsKey(k)) {
                    m.put(k, unwrapMulti ? e.props.get(k) : new G.Prop(k, e.props.get(k), e));
                }
            }
        } else if (o instanceof G.VProp p) {
            if (tokens) {
                m.put(G.T.id, p.id);
                m.put(G.T.key, p.key);
                m.put(G.T.value, p.value);
            }
            if (p.meta != null) {
                for (Map.Entry<String, Object> e : p.meta.entrySet()) {
                    m.put(e.getKey(), e.getValue());
                }
            }
        } else {
            throw err("The provided traverser does not map to a value: " + o);
        }
        return m;
    }

    private static Object elementMap(Object o, Step s) {
        String[] keys = strs(s.args);
        Map<Object, Object> m = new LinkedHashMap<>();
        if (o instanceof G.Vertex v) {
            m.put(G.T.id, v.id);
            m.put(G.T.label, v.label);
            List<String> ks = keys.length == 0 ? new ArrayList<>(v.props.keySet()) : List.of(keys);
            for (String k : ks) {
                List<G.VProp> l = v.vprops(k);
                if (!l.isEmpty()) {
                    m.put(k, l.get(l.size() - 1).value);
                }
            }
        } else if (o instanceof G.Edge e) {
            m.put(G.T.id, e.id);
            m.put(G.T.label, e.label);
            Map<Object, Object> in = new LinkedHashMap<>();
            in.put(G.T.id, e.inId);
            in.put(G.T.label, e.inLabel);
            Map<Object, Object> out = new LinkedHashMap<>();
            out.put(G.T.id, e.outId);
            out.put(G.T.label, e.outLabel);
            m.put(G.Direction.IN, in);
            m.put(G.Direction.OUT, out);
            List<String> ks = keys.length == 0 ? new ArrayList<>(e.props.keySet()) : List.of(keys);
            for (String k : ks) {
                if (e.props.containsKey(k)) {
                    m.put(k, e.props.get(k));
                }
            }
        } else if (o instanceof G.VProp p) {
            m.put(G.T.id, p.id);
            m.put(G.T.key, p.key);
            m.put(G.T.value, p.value);
        } else {
            throw err("The provided traverser does not map to a value: " + o);
        }
        return m;
    }

    // ------------------------------------------------------------------ select / project / path

    private static List<Object> labeled(Tr t, String key) {
        List<Object> out = new ArrayList<>();
        List<Engine.Node> nodes = new ArrayList<>();
        for (Engine.Node n = t.path; n != null; n = n.parent) {
            nodes.add(n);
        }
        Collections.reverse(nodes);
        for (Engine.Node n : nodes) {
            if (n.labels.contains(key)) {
                out.add(n.obj);
            }
        }
        return out;
    }

    private static Object scopeValue(Tr t, G.Pop pop, String key, Ctx ctx) {
        if (t.v instanceof Map<?, ?> m && m.containsKey(key)) {
            return m.get(key);
        }
        List<Object> vals = labeled(t, key);
        if (vals.isEmpty()) {
            if (ctx.side.containsKey(key)) {
                return ctx.side.get(key);
            }
            return NONE;
        }
        return switch (pop) {
            case first -> vals.get(0);
            case all -> vals;
            case mixed -> vals.size() == 1 ? vals.get(0) : vals;
            default -> vals.get(vals.size() - 1);
        };
    }

    private static Iterator<Tr> select(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        if (a.length == 1 && a[0] instanceof G.Column col) {
            return map(in, t -> {
                Object v = t.v;
                if (v instanceof Map<?, ?> m) {
                    return t.child(col == G.Column.keys ? new LinkedHashSet<>(m.keySet()) : new ArrayList<>(m.values()));
                }
                if (v instanceof Map.Entry<?, ?> e) {
                    return t.child(col == G.Column.keys ? e.getKey() : e.getValue());
                }
                throw err("The provided traverser does not map to a value: " + v);
            });
        }
        G.Pop pop = G.Pop.last;
        int from = 0;
        if (a.length > 0 && a[0] instanceof G.Pop p) {
            pop = p;
            from = 1;
        }
        G.Pop fpop = pop;
        String[] keys = strs(rest(a, from));
        if (keys.length == 0) {
            throw err("select() requires at least one key");
        }
        return flat(in, t -> {
            if (keys.length == 1) {
                Object v = scopeValue(t, fpop, keys[0], ctx);
                if (v == NONE) {
                    return Collections.emptyIterator();
                }
                if (!s.by.isEmpty()) {
                    v = byOn(s.by.get(0), t, v, ctx);
                    if (v == NONE) {
                        return Collections.emptyIterator();
                    }
                }
                return once(t.child(v));
            }
            Map<Object, Object> m = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object v = scopeValue(t, fpop, keys[i], ctx);
                if (v == NONE) {
                    return Collections.emptyIterator();
                }
                if (!s.by.isEmpty()) {
                    v = byOn(s.by.get(i % s.by.size()), t, v, ctx);
                    if (v == NONE) {
                        return Collections.emptyIterator();
                    }
                }
                m.put(keys[i], v);
            }
            return once(t.child(m));
        });
    }

    /** Applies a by() modulator to an arbitrary object (a labelled path value), keeping the traverser's path for sub traversals. */
    private static Object byOn(Object[] by, Tr t, Object v, Ctx ctx) {
        if (v instanceof List<?> l && by.length > 0 && !(by[0] instanceof G.Bytecode)) {
            List<Object> out = new ArrayList<>();
            for (Object x : l) {
                Object r = byValue(by, t.child(x), ctx);
                if (r != NONE) {
                    out.add(r);
                }
            }
            return out;
        }
        return byStrict(by, t.child(v), ctx);
    }

    private static Iterator<Tr> project(Step s, Iterator<Tr> in, Ctx ctx) {
        String[] keys = strs(s.args);
        return map(in, t -> {
            Map<Object, Object> m = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object[] by = i < s.by.size() ? s.by.get(i) : new Object[0];
                Object v = byStrict(by, t, ctx);
                if (v != NONE) {
                    m.put(keys[i], v);
                }
            }
            return t.child(m);
        });
    }

    static Object pathBy(Tr t, Step s, Ctx ctx) {
        G.Path p = Engine.pathOf(t.path);
        if (s.by.isEmpty()) {
            return p;
        }
        List<Object> objs = new ArrayList<>();
        for (int i = 0; i < p.objects.size(); i++) {
            Object[] by = s.by.get(i % s.by.size());
            Object o = p.objects.get(i);
            Object v = byStrict(by, new Tr(o, t.path, t.sack, t.loops).child(o), ctx);
            if (v == NONE) {
                return NONE;
            }
            objs.add(v);
        }
        return new G.Path(objs, p.labels);
    }

    private static Iterator<Tr> tree(Step s, Iterator<Tr> in, Ctx ctx) {
        String key = s.args.length > 0 && s.args[0] instanceof String k ? k : null;
        return lazy(() -> {
            G.Tree root = new G.Tree();
            List<Tr> all = drain(in);
            for (Tr t : all) {
                G.Path p = Engine.pathOf(t.path);
                G.Tree cur = root;
                for (int i = 0; i < p.objects.size(); i++) {
                    Object o = p.objects.get(i);
                    if (!s.by.isEmpty()) {
                        Object v = byStrict(s.by.get(i % s.by.size()), t.child(o), ctx);
                        if (v == NONE) {
                            break;
                        }
                        o = v;
                    }
                    G.Tree nxt = cur.get(o);
                    if (nxt == null) {
                        nxt = new G.Tree();
                        cur.put(o, nxt);
                    }
                    cur = nxt;
                }
            }
            if (key != null) {
                ctx.side.put(key, root);
                return all.iterator();
            }
            return once(new Tr(root, new Engine.Node(null, root, Set.of()), null, null));
        });
    }

    // ------------------------------------------------------------------ where

    private static Iterator<Tr> where(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        if (a.length == 1 && a[0] instanceof G.Bytecode bc) {
            return whereTraversal(bc, in, ctx);
        }
        G.P p;
        String startKey = null;
        if (a.length == 1 && a[0] instanceof G.P pp) {
            p = pp;
        } else if (a.length == 2 && a[0] instanceof String k && a[1] instanceof G.P pp) {
            startKey = k;
            p = pp;
        } else {
            throw err("where() called with unsupported arguments");
        }
        String sk = startKey;
        return filter(in, t -> {
            Object left = t.v;
            int byIdx = 0;
            if (sk != null) {
                left = scopeValue(t, G.Pop.last, sk, ctx);
                if (left == NONE) {
                    throw err("Neither the map, sideEffects, nor path has a " + sk + "-key: WherePredicateStep(" + sk + "," + p + ")");
                }
            }
            if (!s.by.isEmpty()) {
                Object[] b = s.by.get(0);
                left = byValue(b, t.child(left), ctx);
                if (left == NONE) {
                    return false;
                }
                byIdx = 1;
            }
            int bi = byIdx;
            boolean[] missing = {false};
            G.P resolved = Engine.mapArgs(p, x -> {
                if (x instanceof String label) {
                    Object v = scopeValue(t, G.Pop.last, label, ctx);
                    if (v == NONE) {
                        throw err("Neither the map, sideEffects, nor path has a " + label + "-key: WherePredicateStep(null," + p + ")");
                    }
                    if (!s.by.isEmpty()) {
                        Object[] b = s.by.get(bi % s.by.size());
                        Object r = byValue(b, t.child(v), ctx);
                        if (r == NONE) {
                            missing[0] = true;
                            return null;
                        }
                        return r;
                    }
                    return v;
                }
                return x;
            });
            return !missing[0] && Engine.test(resolved, left);
        });
    }

    private static Iterator<Tr> whereTraversal(G.Bytecode bc, Iterator<Tr> in, Ctx ctx) {
        List<Step> steps = Engine.steps(bc);
        String startLabel = null;
        String endLabel = null;
        List<Step> body = steps;
        if (!steps.isEmpty() && steps.get(0).name.equals("as") && steps.get(0).args.length == 1) {
            startLabel = (String) steps.get(0).args[0];
            body = steps.subList(1, steps.size());
        }
        if (!body.isEmpty() && body.get(body.size() - 1).name.equals("as") && body.get(body.size() - 1).args.length == 1) {
            endLabel = (String) body.get(body.size() - 1).args[0];
            body = body.subList(0, body.size() - 1);
        }
        String sl = startLabel;
        String el = endLabel;
        List<Step> fbody = body;
        return filter(in, t -> {
            Tr start = t;
            if (sl != null) {
                Object v = scopeValue(t, G.Pop.last, sl, ctx);
                if (v == NONE) {
                    throw err("Neither the map, sideEffects, nor path has a " + sl + "-key: WhereStartStep(" + sl + ")");
                } else {
                    start = new Tr(v, t.path, t.sack, t.loops);
                }
            }
            Iterator<Tr> it = Engine.run(fbody, once(start), ctx);
            if (el == null) {
                return it.hasNext();
            }
            Object want = scopeValue(t, G.Pop.last, el, ctx);
            while (it.hasNext()) {
                Tr r = it.next();
                if (want == NONE || Cmp.eq(r.v, want)) {
                    return true;
                }
            }
            return false;
        });
    }

    // ------------------------------------------------------------------ dedup / limit / range / tail / sample

    private static Iterator<Tr> dedup(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        if (a.length > 0 && a[0] instanceof G.Scope sc && sc == G.Scope.local) {
            return map(in, t -> {
                if (t.v instanceof Collection<?> c) {
                    Collection<Object> out = new LinkedHashSet<>();
                    Set<Object> seen = new HashSet<>();
                    for (Object x : c) {
                        Object k = s.by.isEmpty() ? x : byValue(s.by.get(0), t.child(x), ctx);
                        if (seen.add(k)) {
                            out.add(x);
                        }
                    }
                    return t.child(out);
                }
                return t;
            });
        }
        String[] labels = strs(a.length > 0 && a[0] instanceof G.Scope ? rest(a, 1) : a);
        Set<Object> seen = new HashSet<>();
        return filter(in, t -> {
            Object key;
            if (labels.length == 0) {
                key = s.by.isEmpty() ? t.v : byValue(s.by.get(0), t, ctx);
                if (key == NONE) {
                    return false;
                }
            } else {
                List<Object> ks = new ArrayList<>();
                for (int i = 0; i < labels.length; i++) {
                    Object v = scopeValue(t, G.Pop.last, labels[i], ctx);
                    if (v != NONE && !s.by.isEmpty()) {
                        v = byValue(s.by.get(i % s.by.size()), t.child(v), ctx);
                    }
                    ks.add(v);
                }
                key = ks;
            }
            return seen.add(key);
        });
    }

    private static Iterator<Tr> limit(Step s, Iterator<Tr> in, long skip, boolean unused) {
        Object[] a = s.args;
        G.Scope sc = scopeOf(a);
        long n = longArg(a[a.length - 1]);
        if (sc == G.Scope.local) {
            return map(in, t -> t.child(sliceLocal(t.v, 0, n)));
        }
        if (n < 0) {
            return in;
        }
        return new Iterator<>() {
            long c;

            @Override
            public boolean hasNext() {
                return c < n && in.hasNext();
            }

            @Override
            public Tr next() {
                c++;
                return in.next();
            }
        };
    }

    private static Iterator<Tr> skip(Step s, Iterator<Tr> in) {
        Object[] a = s.args;
        G.Scope sc = scopeOf(a);
        long n = Math.max(0, longArg(a[a.length - 1]));
        if (sc == G.Scope.local) {
            return map(in, t -> t.child(sliceLocal(t.v, n, Long.MAX_VALUE)));
        }
        return lazy(() -> {
            long i = 0;
            while (i < n && in.hasNext()) {
                in.next();
                i++;
            }
            return in;
        });
    }

    private static Iterator<Tr> range(Step s, Iterator<Tr> in) {
        Object[] a = s.args;
        G.Scope sc = scopeOf(a);
        long lo = Math.max(0, longArg(a[a.length - 2]));
        long hi = longArg(a[a.length - 1]);
        if (hi != -1 && hi < lo) {
            throw err("Not a legal range: [" + lo + ", " + hi + "]");
        }
        if (sc == G.Scope.local) {
            return map(in, t -> t.child(sliceLocal(t.v, lo, hi)));
        }
        return lazy(() -> {
            long i = 0;
            while (i < lo && in.hasNext()) {
                in.next();
                i++;
            }
            if (hi < 0) {
                return in;
            }
            long want = Math.max(0, hi - lo);
            return new Iterator<Tr>() {
                long c;

                @Override
                public boolean hasNext() {
                    return c < want && in.hasNext();
                }

                @Override
                public Tr next() {
                    c++;
                    return in.next();
                }
            };
        });
    }

    private static Object sliceLocal(Object v, long lo, long hi) {
        if (v instanceof Map<?, ?> m) {
            Map<Object, Object> out = new LinkedHashMap<>();
            long i = 0;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (i >= lo && (hi < 0 || i < hi)) {
                    out.put(e.getKey(), e.getValue());
                }
                i++;
            }
            return out;
        }
        if (v instanceof Collection<?> c) {
            Collection<Object> out = v instanceof Set ? new LinkedHashSet<>() : new ArrayList<>();
            long i = 0;
            for (Object x : c) {
                if (i >= lo && (hi < 0 || i < hi)) {
                    out.add(x);
                }
                i++;
            }
            return out;
        }
        return v;
    }

    private static Iterator<Tr> tail(Step s, Iterator<Tr> in) {
        Object[] a = s.args;
        G.Scope sc = scopeOf(a);
        long n = a.length > 0 && a[a.length - 1] instanceof Number x ? x.longValue() : 1;
        if (n < 0) {
            return filter(in, t -> false);
        }
        if (sc == G.Scope.local) {
            return map(in, t -> {
                Object v = t.v;
                int size = v instanceof Collection<?> c ? c.size() : v instanceof Map<?, ?> m ? m.size() : 0;
                return t.child(sliceLocal(v, Math.max(0, size - n), Long.MAX_VALUE));
            });
        }
        return lazy(() -> {
            java.util.ArrayDeque<Tr> q = new java.util.ArrayDeque<>();
            while (in.hasNext()) {
                q.addLast(in.next());
                if (q.size() > n) {
                    q.removeFirst();
                }
            }
            return q.iterator();
        });
    }

    private static Iterator<Tr> sample(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        if (a.length > 0 && a[0] instanceof G.Scope) {
            int n = intArg(a[1]);
            return map(in, t -> {
                if (t.v instanceof List<?> l) {
                    List<Object> c = new ArrayList<>(l);
                    Collections.shuffle(c, ctx.rnd);
                    return t.child(new ArrayList<>(c.subList(0, Math.min(n, c.size()))));
                }
                return t;
            });
        }
        int n = intArg(a[0]);
        return lazy(() -> {
            List<Tr> all = drain(in);
            Collections.shuffle(all, ctx.rnd);
            return new ArrayList<>(all.subList(0, Math.min(n, all.size()))).iterator();
        });
    }

    // ------------------------------------------------------------------ branching

    private static Iterator<Tr> choose(Step s, Iterator<Tr> in, Ctx ctx) {
        Object[] a = s.args;
        if (a.length == 1 || (a.length == 0)) {
            // choose(function/traversal).option(pick, traversal)...
            Object sel = a.length == 1 ? a[0] : null;
            return flat(in, t -> {
                Object key;
                if (sel instanceof G.Bytecode bc) {
                    Iterator<Tr> it = sub(bc, t, ctx);
                    key = it.hasNext() ? it.next().v : NONE;
                } else if (sel instanceof G.Closure c) {
                    key = ctx.call(c, Engine.viewOf(t));
                } else {
                    key = t.v;
                }
                if (key == NONE) {
                    return once(t);
                }
                G.Bytecode none = null;
                G.Bytecode any = null;
                List<G.Bytecode> matched = new ArrayList<>();
                for (Object[] opt : s.option) {
                    Object pick = opt[0];
                    G.Bytecode target = opt.length > 1 && opt[1] instanceof G.Bytecode b ? b : null;
                    if (pick == G.Pick.none) {
                        none = target;
                    } else if (pick == G.Pick.any) {
                        any = target;
                    } else if (key != NONE && (pick instanceof G.P p ? Engine.test(p, key) : Cmp.eq(key, pick))) {
                        matched.add(target);
                    }
                }
                if (matched.isEmpty() && none != null) {
                    matched.add(none);
                }
                if (any != null && !matched.isEmpty()) {
                    matched.add(any);
                }
                List<Iterator<Tr>> its = new ArrayList<>();
                for (G.Bytecode b : matched) {
                    if (b != null) {
                        its.add(sub(b, t, ctx));
                    }
                }
                if (its.isEmpty() && matched.isEmpty()) {
                    return Collections.emptyIterator();
                }
                return chain(its);
            });
        }
        return flat(in, t -> {
            boolean cond;
            Object c = a[0];
            if (c instanceof G.P p) {
                cond = Engine.test(p, t.v);
            } else if (c instanceof G.Closure cl) {
                cond = truthy(ctx.call(cl, Engine.viewOf(t)));
            } else {
                cond = exists((G.Bytecode) c, t, ctx);
            }
            if (cond) {
                return sub((G.Bytecode) a[1], t, ctx);
            }
            if (a.length > 2) {
                return sub((G.Bytecode) a[2], t, ctx);
            }
            return once(t);
        });
    }

    private static Iterator<Tr> repeat(Step s, Iterator<Tr> in, Ctx ctx) {
        G.Bytecode body = (G.Bytecode) (s.args.length > 1 ? s.args[1] : s.args[0]);
        String name = s.args.length > 1 ? (String) s.args[0] : null;
        Object[] preUntil = null;
        Object[] postUntil = null;
        Object[] preEmit = null;
        Object[] postEmit = null;
        long preTimes = -1;
        long postTimes = -1;
        for (Object[] m : s.pre) {
            switch ((String) m[0]) {
                case "until" -> preUntil = m;
                case "emit" -> preEmit = m;
                case "times" -> preTimes = longArg(m[1]);
                default -> {
                }
            }
        }
        for (Object[] m : s.post) {
            switch ((String) m[0]) {
                case "until" -> postUntil = m;
                case "emit" -> postEmit = m;
                case "times" -> postTimes = longArg(m[1]);
                default -> {
                }
            }
        }
        final Object[] fpu = preUntil;
        final Object[] fqu = postUntil;
        final Object[] fpe = preEmit;
        final Object[] fqe = postEmit;
        final long fpt = preTimes;
        final long fqt = postTimes;
        List<Step> bodySteps = Engine.steps(body);
        final boolean preMode = fpu != null || fpe != null || fpt >= 0;
        return flat(in, start -> lazy(() -> {
            List<Tr> outs = new ArrayList<>();
            List<Tr> cur = new ArrayList<>();
            cur.add(start.withLoops(new Engine.Loop(name, 0, start.loops)));
            int guard = 0;
            boolean initial = true;
            while (!cur.isEmpty()) {
                if (++guard > ctx.repeatLimit) {
                    throw err("repeat() exceeded the maximum number of iterations");
                }
                List<Tr> ready = new ArrayList<>();
                for (Tr t : cur) {
                    ctx.tick();
                    if (initial || preMode) {
                        if ((fpu != null && cond(fpu, t, ctx)) || (fpt >= 0 && t.loops.n >= fpt)) {
                            outs.add(exitLoop(t));
                            continue;
                        }
                        if (fpe != null && condEmit(fpe, t, ctx)) {
                            outs.add(exitLoop(t));
                        }
                    }
                    ready.add(t);
                }
                initial = false;
                List<Tr> next = new ArrayList<>();
                for (Tr t : ready) {
                    Iterator<Tr> it = Engine.run(bodySteps, once(t), ctx);
                    while (it.hasNext()) {
                        Tr r = it.next();
                        Engine.Loop l = r.loops;
                        Tr r2 = r.withLoops(new Engine.Loop(l.name, l.n + 1, l.next));
                        if ((fqu != null && cond(fqu, r2, ctx)) || (fqt >= 0 && r2.loops.n >= fqt)) {
                            outs.add(exitLoop(r2));
                        } else {
                            if (fqe != null && condEmit(fqe, r2, ctx)) {
                                outs.add(exitLoop(r2));
                            }
                            next.add(r2);
                        }
                    }
                }
                cur = next;
            }
            return outs.iterator();
        }));
    }

    private static Tr exitLoop(Tr t) {
        Engine.Loop l = t.loops;
        return t.withLoops(l == null ? null : l.next);
    }

    private static boolean cond(Object[] m, Tr t, Ctx ctx) {
        if (m.length < 2) {
            return true;
        }
        Object c = m[1];
        if (c instanceof G.P p) {
            return Engine.test(p, t.v);
        }
        if (c instanceof G.Closure cl) {
            return truthy(ctx.call(cl, Engine.viewOf(t)));
        }
        return exists((G.Bytecode) c, t, ctx);
    }

    private static boolean condEmit(Object[] m, Tr t, Ctx ctx) {
        return m.length < 2 || cond(m, t, ctx);
    }

    // ------------------------------------------------------------------ reducing

    private static Iterator<Tr> count(Step s, Iterator<Tr> in) {
        if (scopeOf(s.args) == G.Scope.local) {
            return map(in, t -> t.child(t.v instanceof Collection<?> c ? (long) c.size() : t.v instanceof Map<?, ?> m ? (long) m.size()
                    : t.v instanceof G.Path p ? (long) p.objects.size() : 1L));
        }
        return lazy(() -> {
            long n = 0;
            Tr last = null;
            while (in.hasNext()) {
                last = in.next();
                n++;
            }
            return once(new Tr(n, new Engine.Node(null, n, Set.of()), last == null ? null : last.sack, null));
        });
    }

    private static Number sumOf(Collection<?> c) {
        Number acc = null;
        for (Object o : c) {
            if (!(o instanceof Number n)) {
                throw err("Cannot sum non-numeric value " + o);
            }
            acc = acc == null ? (n instanceof Byte || n instanceof Short ? (Number) n.intValue() : n) : Cmp.add(acc, n);
        }
        return acc == null ? (Number) 0 : acc;
    }

    private static Object reduce(String name, Collection<?> c) {
        switch (name) {
            case "sum":
                return c.isEmpty() ? NONE : sumOf(c);
            case "mean": {
                if (c.isEmpty()) {
                    return NONE;
                }
                Number sum = sumOf(c);
                if (sum instanceof java.math.BigDecimal || sum instanceof java.math.BigInteger) {
                    return Cmp.big(sum).divide(java.math.BigDecimal.valueOf(c.size()), java.math.MathContext.DECIMAL128);
                }
                return sum.doubleValue() / c.size();
            }
            default: {
                Object best = null;
                boolean any = false;
                for (Object o : c) {
                    if (o == null) {
                        continue;
                    }
                    if (!any) {
                        best = o;
                        any = true;
                    } else {
                        int cmp = Cmp.order(o, best);
                        if (name.equals("min") ? cmp < 0 : cmp > 0) {
                            best = o;
                        }
                    }
                }
                return any ? best : NONE;
            }
        }
    }

    private static Iterator<Tr> numeric(Step s, Iterator<Tr> in) {
        String name = s.name;
        if (scopeOf(s.args) == G.Scope.local) {
            return flat(in, t -> {
                if (!(t.v instanceof Collection<?> c)) {
                    throw err("The provided traverser does not map to a value: " + t.v);
                }
                Object r = reduce(name, c);
                return r == NONE ? Collections.emptyIterator() : once(t.child(r));
            });
        }
        return lazy(() -> {
            List<Object> vals = new ArrayList<>();
            while (in.hasNext()) {
                Tr t = in.next();
                vals.add(t.v);
            }
            if (!name.equals("min") && !name.equals("max")) {
                for (Object v : vals) {
                    if (!(v instanceof Number)) {
                        throw err("The provided traverser does not map to a numeric value: " + v);
                    }
                }
            }
            Object r = reduce(name, vals);
            if (r == NONE) {
                return Collections.<Tr>emptyIterator();
            }
            return once(new Tr(r, new Engine.Node(null, r, Set.of()), null, null));
        });
    }

    private static Iterator<Tr> fold(Step s, Iterator<Tr> in, Ctx ctx) {
        return lazy(() -> {
            if (s.args.length == 2) {
                Object acc = s.args[0];
                Object op = s.args[1];
                while (in.hasNext()) {
                    Object v = in.next().v;
                    acc = applyOp(op, acc, v, ctx);
                }
                return once(new Tr(acc, new Engine.Node(null, acc, Set.of()), null, null));
            }
            List<Object> l = new ArrayList<>();
            while (in.hasNext()) {
                l.add(in.next().v);
            }
            return once(new Tr(l, new Engine.Node(null, l, Set.of()), null, null));
        });
    }

    static Object applyOp(Object op, Object a, Object b, Ctx ctx) {
        if (op instanceof G.Closure c) {
            return ctx.call(c, a, b);
        }
        if (!(op instanceof G.Operator o)) {
            throw err("unsupported operator " + op);
        }
        switch (o) {
            case sum, sumLong, minus, mult, div:
                if (b != null && !(b instanceof Number) || a != null && !(a instanceof Number)) {
                    Object bad = b != null && !(b instanceof Number) ? b : a;
                    throw err("class " + bad.getClass().getName() + " cannot be cast to class java.lang.Number");
                }
                if (a == null) {
                    return b;
                }
                if (b == null) {
                    return a;
                }
                return o == G.Operator.minus ? Cmp.sub((Number) a, (Number) b) : o == G.Operator.mult ? Cmp.mul((Number) a, (Number) b)
                        : o == G.Operator.div ? Cmp.div((Number) a, (Number) b) : Cmp.add((Number) a, (Number) b);
            case min:
                return a == null ? b : Cmp.order(a, b) <= 0 ? a : b;
            case max:
                return a == null ? b : Cmp.order(a, b) >= 0 ? a : b;
            case assign:
                return b;
            case and:
                return truthy(a) && truthy(b);
            case or:
                return truthy(a) || truthy(b);
            case addAll: {
                List<Object> r = new ArrayList<>();
                if (a instanceof Collection<?> x) {
                    r.addAll(x);
                } else if (a != null) {
                    r.add(a);
                }
                if (b instanceof Collection<?> y) {
                    r.addAll(y);
                } else if (b != null) {
                    r.add(b);
                }
                return r;
            }
            default:
                throw err("unsupported operator " + o);
        }
    }

    private static Iterator<Tr> order(Step s, Iterator<Tr> in, Ctx ctx) {
        boolean local = scopeOf(s.args) == G.Scope.local;
        List<Object[]> mods = s.by.isEmpty() ? Collections.singletonList(new Object[0]) : s.by;
        if (local) {
            return map(in, t -> {
                Object v = t.v;
                if (v instanceof Map<?, ?> m) {
                    List<Object> entries = new ArrayList<>();
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        entries.add(new java.util.AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()));
                    }
                    sortObjects(entries, mods.isEmpty() ? Collections.singletonList(new Object[] {G.Column.keys}) : mods, t, ctx, true);
                    Map<Object, Object> out = new LinkedHashMap<>();
                    for (Object e : entries) {
                        Map.Entry<?, ?> me = (Map.Entry<?, ?>) e;
                        out.put(me.getKey(), me.getValue());
                    }
                    return t.child(out);
                }
                if (v instanceof Collection<?> c) {
                    List<Object> l = new ArrayList<>(c);
                    sortObjects(l, mods, t, ctx, false);
                    return t.child(v instanceof Set ? new LinkedHashSet<>(l) : l);
                }
                return t;
            });
        }
        return lazy(() -> {
            List<Tr> all = drain(in);
            boolean shuffle = false;
            for (Object[] m : mods) {
                for (Object x : m) {
                    if (x == G.Order.shuffle) {
                        shuffle = true;
                    }
                }
            }
            if (shuffle) {
                Collections.shuffle(all, ctx.rnd);
                return all.iterator();
            }
            List<Object[]> keyed = new ArrayList<>();
            for (Tr t : all) {
                Object[] row = new Object[mods.size() + 1];
                boolean skip = false;
                for (int i = 0; i < mods.size(); i++) {
                    Object v = byStrict(mods.get(i), t, ctx);
                    if (v == NONE) {
                        skip = true; // a traverser without a by() value is filtered out
                    }
                    row[i] = v;
                }
                row[mods.size()] = t;
                if (!skip) {
                    keyed.add(row);
                }
            }
            List<Comparator<Object>> cmps = new ArrayList<>();
            for (Object[] m : mods) {
                cmps.add(comparatorOf(m, ctx));
            }
            keyed.sort((x, y) -> {
                for (int i = 0; i < mods.size(); i++) {
                    int c = cmps.get(i).compare(x[i], y[i]);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            });
            List<Tr> out = new ArrayList<>();
            for (Object[] r : keyed) {
                out.add((Tr) r[mods.size()]);
            }
            return out.iterator();
        });
    }

    private static Comparator<Object> comparatorOf(Object[] mod, Ctx ctx) {
        Object last = mod.length == 0 ? null : mod[mod.length - 1];
        if (last instanceof G.Closure c && (mod.length >= 2 || c.params() != null && c.params().size() == 2)) {
            return (p, q) -> ((Number) ctx.call(c, p, q)).intValue();
        }
        return Engine.comparator(mod);
    }

    private static void sortObjects(List<Object> l, List<Object[]> mods, Tr t, Ctx ctx, boolean entries) {
        List<Comparator<Object>> cmps = new ArrayList<>();
        for (Object[] m : mods) {
            cmps.add(comparatorOf(m, ctx));
        }
        Map<Object, Object[]> keys = new java.util.IdentityHashMap<>();
        for (Object o : l) {
            Object[] ks = new Object[mods.size()];
            for (int i = 0; i < mods.size(); i++) {
                Object v = byValue(mods.get(i), t.child(o), ctx);
                ks[i] = v == NONE ? null : v;
            }
            keys.put(o, ks);
        }
        l.sort((x, y) -> {
            Object[] kx = keys.get(x);
            Object[] ky = keys.get(y);
            for (int i = 0; i < mods.size(); i++) {
                int c = cmps.get(i).compare(kx[i], ky[i]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        });
    }

    private static boolean endsWithReducer(G.Bytecode bc) {
        List<Step> st = Engine.steps(bc);
        if (st.isEmpty()) {
            return false;
        }
        String n = st.get(st.size() - 1).name;
        return switch (n) {
            case "count", "sum", "min", "max", "mean", "fold", "group", "groupCount" -> true;
            default -> false;
        };
    }

    /** Value of a group's members under a by() value modulator. */
    private static Object groupValue(Object[] vby, List<Tr> members, Ctx ctx) {
        if (vby == null || vby.length == 0) {
            List<Object> l = new ArrayList<>();
            for (Tr m : members) {
                l.add(m.v);
            }
            return l;
        }
        if (vby[0] instanceof G.Bytecode bc) {
            List<Tr> out = drain(Engine.run(Engine.steps(bc), new ArrayList<>(members).iterator(), ctx));
            if (endsWithReducer(bc)) {
                return out.isEmpty() ? NONE : out.get(0).v;
            }
            List<Object> l = new ArrayList<>();
            for (Tr r : out) {
                l.add(r.v);
            }
            return l;
        }
        List<Object> l = new ArrayList<>();
        for (Tr m : members) {
            Object v = byValue(vby, m, ctx);
            if (v != NONE) {
                l.add(v);
            }
        }
        return l;
    }

    private static Map<Object, Object> buildGroup(Step s, List<Tr> all, Ctx ctx) {
        Map<Object, List<Tr>> groups = new HashMap<>();
        Object[] kby = s.by.isEmpty() ? new Object[0] : s.by.get(0);
        for (Tr t : all) {
            Object k = byStrict(kby, t, ctx);
            if (k == NONE) {
                continue;
            }
            groups.computeIfAbsent(k, x -> new ArrayList<>()).add(t);
        }
        Map<Object, Object> out = new HashMap<>();
        for (Map.Entry<Object, List<Tr>> e : groups.entrySet()) {
            Object v = groupValue(s.by.size() > 1 ? s.by.get(1) : null, e.getValue(), ctx);
            if (v != NONE) {
                out.put(e.getKey(), v);
            }
        }
        return out;
    }

    private static Iterator<Tr> group(Step s, Iterator<Tr> in, Ctx ctx) {
        String key = s.args.length > 0 && s.args[0] instanceof String k ? k : null;
        return lazy(() -> {
            List<Tr> all = drain(in);
            Map<Object, Object> out = buildGroup(s, all, ctx);
            if (key != null) {
                ctx.side.put(key, out);
                return all.iterator();
            }
            return once(new Tr(out, new Engine.Node(null, out, Set.of()), null, null));
        });
    }

    private static Iterator<Tr> groupCount(Step s, Iterator<Tr> in, Ctx ctx) {
        String key = s.args.length > 0 && s.args[0] instanceof String k ? k : null;
        return lazy(() -> {
            List<Tr> all = drain(in);
            Map<Object, Object> out = new HashMap<>();
            Object[] kby = s.by.isEmpty() ? new Object[0] : s.by.get(0);
            for (Tr t : all) {
                Object k = byStrict(kby, t, ctx);
                if (k == NONE) {
                    continue;
                }
                out.merge(k, 1L, (x, y) -> (Long) x + (Long) y);
            }
            if (key != null) {
                ctx.side.put(key, out);
                return all.iterator();
            }
            return once(new Tr(out, new Engine.Node(null, out, Set.of()), null, null));
        });
    }

    private static Iterator<Tr> aggregate(Step s, Iterator<Tr> in, Ctx ctx) {
        boolean local = s.args.length > 0 && s.args[0] instanceof G.Scope;
        String key = (String) s.args[s.args.length - 1];
        boolean barrier = s.name.equals("aggregate");
        java.util.function.Consumer<Tr> add = t -> {
            Object v = s.by.isEmpty() ? t.v : byStrict(s.by.get(0), t, ctx);
            if (v == NONE) {
                return;
            }
            Object cur = ctx.side.get(key);
            if (cur instanceof G.BulkSet bs) {
                bs.add(v);
            } else if (cur instanceof Collection<?>) {
                @SuppressWarnings("unchecked")
                Collection<Object> c = (Collection<Object>) cur;
                c.add(v);
            } else {
                G.BulkSet bs = new G.BulkSet();
                bs.add(v);
                ctx.side.put(key, bs);
            }
        };
        if (!barrier) {
            return map(in, t -> {
                add.accept(t);
                return t;
            });
        }
        if (local) {
            return map(in, t -> {
                add.accept(t);
                return t;
            });
        }
        return lazy(() -> {
            List<Tr> all = drain(in);
            if (!ctx.side.containsKey(key)) {
                ctx.side.put(key, new G.BulkSet());
            }
            for (Tr t : all) {
                add.accept(t);
            }
            return all.iterator();
        });
    }

    private static Iterator<Tr> cap(Step s, Iterator<Tr> in, Ctx ctx) {
        String[] keys = strs(s.args);
        return lazy(() -> {
            while (in.hasNext()) {
                in.next();
            }
            Object v;
            for (String k : keys) {
                if (!ctx.side.containsKey(k)) {
                    throw err("The side-effect key does not exist in the side-effects: " + k);
                }
            }
            if (keys.length == 1) {
                v = ctx.side.get(keys[0]);
            } else {
                Map<Object, Object> m = new LinkedHashMap<>();
                for (String k : keys) {
                    m.put(k, ctx.side.get(k));
                }
                v = m;
            }
            return once(new Tr(v, new Engine.Node(null, v, Set.of()), null, null));
        });
    }

    private static Iterator<Tr> sack(Step s, Iterator<Tr> in, Ctx ctx) {
        if (s.args.length == 0) {
            return map(in, t -> t.child(t.sack));
        }
        Object op = s.args[0];
        return map(in, t -> {
            Object other = s.by.isEmpty() ? t.v : byStrict(s.by.get(0), t, ctx);
            if (other == NONE) {
                return t;
            }
            return t.withSack(applyOp(op, t.sack, other, ctx));
        });
    }
}
