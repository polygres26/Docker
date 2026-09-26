package com.sayonora.warp.gremlinwire;

import static com.sayonora.warp.gremlinwire.Engine.NONE;

import com.sayonora.warp.gremlinwire.Engine.Ctx;
import com.sayonora.warp.gremlinwire.Engine.Step;
import com.sayonora.warp.gremlinwire.Engine.Tr;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** addV / addE / property / drop / mergeV / mergeE. */
final class Mutations {

    private Mutations() {
    }

    private static final AtomicLong VPID = new AtomicLong(1L << 40);

    private static G.GremlinError err(String m) {
        return G.GremlinError.script(m);
    }

    /** One decoded property() call. */
    private record PropSpec(G.Cardinality card, Object key, Object value, List<Object> meta) {
    }

    private static List<PropSpec> specs(List<Object[]> calls) {
        List<PropSpec> out = new ArrayList<>();
        for (Object[] p : calls) {
            if (p.length == 1 && p[0] instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    out.add(new PropSpec(null, e.getKey(), e.getValue(), List.of()));
                }
                continue;
            }
            int i = 0;
            G.Cardinality card = null;
            if (p.length > 0 && p[0] instanceof G.Cardinality c) {
                card = c;
                i = 1;
            }
            if (p.length < i + 2) {
                throw err("property() requires a key and a value");
            }
            List<Object> meta = new ArrayList<>();
            for (int k = i + 2; k < p.length; k++) {
                meta.add(p[k]);
            }
            out.add(new PropSpec(card, p[i], p[i + 1], meta));
        }
        return out;
    }

    private static Object value(Object v, Tr t, Ctx ctx) {
        if (v instanceof G.Bytecode bc) {
            Iterator<Tr> it = Engine.sub(bc, t, ctx);
            return it.hasNext() ? it.next().v : NONE;
        }
        if (v instanceof G.Closure c) {
            return ctx.call(c, Engine.viewOf(t));
        }
        return v;
    }

    static void applyVertexProp(G.Vertex v, G.Cardinality card, String key, Object val, List<Object> meta) {
        if (val == null) {
            v.props.remove(key);
            return;
        }
        G.Cardinality c = card == null ? G.Cardinality.single : card;
        List<G.VProp> l = v.props.get(key);
        if (l == null || c == G.Cardinality.single) {
            l = new ArrayList<>();
            v.props.put(key, l);
        }
        if (c == G.Cardinality.set) {
            for (G.VProp p : l) {
                if (Cmp.eq(p.value, val) && p.value.getClass() == val.getClass()) {
                    return;
                }
            }
        }
        G.VProp p = new G.VProp(VPID.incrementAndGet(), key, val);
        p.vertex = v;
        if (!meta.isEmpty()) {
            p.meta = new LinkedHashMap<>();
            for (int i = 0; i + 1 < meta.size(); i += 2) {
                p.meta.put(String.valueOf(meta.get(i)), meta.get(i + 1));
            }
        }
        l.add(p);
    }

    // ------------------------------------------------------------------ addV

    static Iterator<Tr> addV(Step s, Iterator<Tr> in, Ctx ctx) {
        return Engine.flat(in, t -> Engine.lazy(() -> {
            ctx.needWrite("addV()");
            String label = "vertex";
            if (s.args.length > 0) {
                Object l = value(s.args[0], t, ctx);
                if (!(l instanceof String ls)) {
                    throw err("addV() label must be a String");
                }
                label = ls;
            }
            Object id = null;
            List<PropSpec> ps = specs(s.props);
            G.Vertex v = null;
            List<PropSpec> plain = new ArrayList<>();
            for (PropSpec p : ps) {
                if (p.key() == G.T.id) {
                    id = value(p.value(), t, ctx);
                } else if (p.key() == G.T.label) {
                    Object l = value(p.value(), t, ctx);
                    label = String.valueOf(l);
                } else {
                    plain.add(p);
                }
            }
            String flabel = label;
            for (int attempt = 0; attempt < 100; attempt++) {
                Object vid = id != null ? GraphStore.canonId(id) : (Object) ctx.store.nextId();
                v = new G.Vertex(vid, flabel);
                for (PropSpec p : plain) {
                    Object val = value(p.value(), t, ctx);
                    if (val == NONE) {
                        continue;
                    }
                    if (!(p.key() instanceof String k)) {
                        throw err("property key must be a String");
                    }
                    // properties folded into addV() default to list cardinality (unlike property() on an existing vertex)
                    applyVertexProp(v, p.card() == null ? G.Cardinality.list : p.card(), k, val, p.meta());
                }
                try {
                    ctx.store.addVertex(v);
                    break;
                } catch (G.GremlinError e) {
                    if (id != null || !String.valueOf(e.getMessage()).startsWith("Vertex with id already exists")) {
                        throw e;
                    }
                    v = null;
                }
            }
            if (v == null) {
                throw err("could not allocate a vertex id");
            }
            return Engine.once(t.child(v));
        }));
    }

    // ------------------------------------------------------------------ addE

    private static G.Vertex resolveVertex(Object spec, Tr t, Ctx ctx, String what) {
        Object v = spec;
        if (spec instanceof String label) {
            v = NONE;
            List<Object> found = new ArrayList<>();
            for (Engine.Node n = t.path; n != null; n = n.parent) {
                if (n.labels.contains(label)) {
                    found.add(n.obj);
                    break;
                }
            }
            if (!found.isEmpty()) {
                v = found.get(0);
            } else if (ctx.side.containsKey(label)) {
                v = ctx.side.get(label);
            }
        } else if (spec instanceof G.Bytecode bc) {
            Iterator<Tr> it = Engine.sub(bc, t, ctx);
            v = it.hasNext() ? it.next().v : NONE;
        }
        if (v == NONE) {
            throw err("The value provided by " + what + "() does not map to a Vertex: " + spec);
        }
        if (v instanceof G.Vertex vx) {
            return vx;
        }
        if (v instanceof Long || v instanceof Integer || v instanceof java.util.UUID) {
            G.Vertex f = ctx.store.vertex(v);
            if (f != null) {
                return f;
            }
        }
        throw err("The value provided by " + what + "() does not map to a Vertex: " + v);
    }

    static Iterator<Tr> addE(Step s, Iterator<Tr> in, Ctx ctx) {
        return Engine.flat(in, t -> Engine.lazy(() -> {
            ctx.needWrite("addE()");
            Object l = value(s.args.length > 0 ? s.args[0] : null, t, ctx);
            if (!(l instanceof String label)) {
                throw err("addE() label must be a String");
            }
            boolean hasCur = t.v instanceof G.Vertex;
            G.Vertex out;
            G.Vertex inn;
            if (!s.from.isEmpty()) {
                out = resolveVertex(s.from.get(0)[0], t, ctx, "from");
            } else if (hasCur) {
                out = (G.Vertex) t.v;
            } else {
                throw err("The value provided by from() does not map to a Vertex: null");
            }
            if (!s.to.isEmpty()) {
                inn = resolveVertex(s.to.get(0)[0], t, ctx, "to");
            } else if (hasCur && s.from.isEmpty()) {
                throw err("The value provided by to() does not map to a Vertex: null");
            } else if (hasCur) {
                inn = (G.Vertex) t.v;
            } else {
                throw err("The value provided by to() does not map to a Vertex: null");
            }
            List<G.Vertex> ends = ctx.store.vertices(List.of(out.id, inn.id));
            G.Vertex fo = null;
            G.Vertex fi = null;
            for (G.Vertex v : ends) {
                if (v.id.equals(out.id)) {
                    fo = v;
                }
                if (v.id.equals(inn.id)) {
                    fi = v;
                }
            }
            if (fo == null || fi == null) {
                throw err("Vertex not found for addE(): " + (fo == null ? out.id : inn.id));
            }
            Object id = null;
            LinkedHashMap<String, Object> props = new LinkedHashMap<>();
            String elabel = label;
            for (PropSpec p : specs(s.props)) {
                if (p.key() == G.T.id) {
                    id = value(p.value(), t, ctx);
                } else if (p.key() == G.T.label) {
                    elabel = String.valueOf(value(p.value(), t, ctx));
                } else if (p.key() instanceof String k) {
                    Object v = value(p.value(), t, ctx);
                    if (v != NONE && v != null) {
                        props.put(k, v);
                    }
                }
            }
            G.Edge e = null;
            for (int attempt = 0; attempt < 100; attempt++) {
                Object eid = id != null ? GraphStore.canonId(id) : (Object) ctx.store.nextId();
                e = new G.Edge(eid, elabel, fo.id, fo.label, fi.id, fi.label);
                e.props.putAll(props);
                if (id == null && ctx.store.edge(eid) != null) {
                    continue;
                }
                if (id != null && ctx.store.edge(eid) != null) {
                    throw err("Edge with id already exists: " + eid);
                }
                ctx.store.addEdge(e);
                break;
            }
            return Engine.once(t.child(e));
        }));
    }

    // ------------------------------------------------------------------ property

    static Iterator<Tr> property(Step s, Iterator<Tr> in, Ctx ctx) {
        List<PropSpec> ps = specs(Collections.singletonList(s.args));
        return Engine.map(in, t -> {
            ctx.needWrite("property()");
            for (PropSpec p : ps) {
                Object val = value(p.value(), t, ctx);
                if (val == NONE) {
                    continue;
                }
                if (!(p.key() instanceof String key)) {
                    throw err("property() key must be a String");
                }
                if (t.v instanceof G.Vertex v) {
                    ctx.store.updateVertex(v.id, fresh -> applyVertexProp(fresh, p.card(), key, val, p.meta()));
                    applyVertexProp(v, p.card(), key, val, p.meta());
                } else if (t.v instanceof G.Edge e) {
                    ctx.store.updateEdge(e.id, fresh -> setEdge(fresh, key, val));
                    setEdge(e, key, val);
                } else if (t.v instanceof G.VProp vp && vp.vertex != null) {
                    ctx.store.updateVertex(vp.vertex.id, fresh -> {
                        for (G.VProp q : fresh.vprops(vp.key)) {
                            if (Cmp.eq(q.value, vp.value)) {
                                if (q.meta == null) {
                                    q.meta = new LinkedHashMap<>();
                                }
                                q.meta.put(key, val);
                            }
                        }
                    });
                    if (vp.meta == null) {
                        vp.meta = new LinkedHashMap<>();
                    }
                    vp.meta.put(key, val);
                } else {
                    throw err("property() cannot be applied to " + t.v);
                }
            }
            return t;
        });
    }

    private static void setEdge(G.Edge e, String key, Object val) {
        if (val == null) {
            e.props.remove(key);
        } else {
            e.props.put(key, val);
        }
    }

    // ------------------------------------------------------------------ drop

    static Iterator<Tr> drop(Iterator<Tr> in, Ctx ctx) {
        return Engine.lazy(() -> {
            while (in.hasNext()) {
                ctx.needWrite("drop()");
                Tr t = in.next();
                Object o = t.v;
                if (o instanceof G.Vertex v) {
                    ctx.store.removeVertex(v.id);
                } else if (o instanceof G.Edge e) {
                    ctx.store.removeEdge(e.id);
                } else if (o instanceof G.VProp vp && vp.vertex != null) {
                    ctx.store.updateVertex(vp.vertex.id, fresh -> {
                        List<G.VProp> l = fresh.props.get(vp.key);
                        if (l != null) {
                            for (Iterator<G.VProp> i = l.iterator(); i.hasNext();) {
                                G.VProp q = i.next();
                                if (Cmp.eq(q.value, vp.value)) {
                                    i.remove();
                                    break;
                                }
                            }
                            if (l.isEmpty()) {
                                fresh.props.remove(vp.key);
                            }
                        }
                    });
                } else if (o instanceof G.Prop p && p.element instanceof G.Edge e) {
                    ctx.store.updateEdge(e.id, fresh -> fresh.props.remove(p.key));
                } else if (o instanceof G.Prop p && p.element instanceof G.VProp vp && vp.vertex != null) {
                    ctx.store.updateVertex(vp.vertex.id, fresh -> {
                        for (G.VProp q : fresh.vprops(vp.key)) {
                            if (q.meta != null && Cmp.eq(q.value, vp.value)) {
                                q.meta.remove(p.key);
                            }
                        }
                    });
                } else {
                    throw err("drop() cannot be applied to " + o);
                }
            }
            return Collections.emptyIterator();
        });
    }

    // ------------------------------------------------------------------ merge

    private static Map<Object, Object> mapArg(Step s, Tr t, Ctx ctx) {
        Object m = s.args.length > 0 ? s.args[0] : t.v;
        if (m instanceof G.Bytecode bc) {
            Iterator<Tr> it = Engine.sub(bc, t, ctx);
            m = it.hasNext() ? it.next().v : NONE;
        }
        if (!(m instanceof Map<?, ?> map)) {
            throw err("mergeV()/mergeE() expects a Map but got " + m);
        }
        @SuppressWarnings("unchecked")
        Map<Object, Object> r = (Map<Object, Object>) map;
        return r;
    }

    private static Map<Object, Object> option(Step s, G.Merge which, Tr t, Ctx ctx) {
        for (Object[] o : s.option) {
            if (o.length > 1 && o[0] == which) {
                Object m = o[1];
                if (m instanceof G.Bytecode bc) {
                    Iterator<Tr> it = Engine.sub(bc, t, ctx);
                    m = it.hasNext() ? it.next().v : null;
                }
                if (m instanceof Map<?, ?> mm) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> r = (Map<Object, Object>) mm;
                    return r;
                }
            }
        }
        return null;
    }

    static Iterator<Tr> mergeV(Step s, Iterator<Tr> in, Ctx ctx) {
        return Engine.flat(in, t -> Engine.lazy(() -> {
            ctx.needWrite("mergeV()");
            Map<Object, Object> search = mapArg(s, t, ctx);
            Map<Object, Object> onCreate = option(s, G.Merge.onCreate, t, ctx);
            Map<Object, Object> onMatch = option(s, G.Merge.onMatch, t, ctx);
            List<G.Vertex> matches = new ArrayList<>();
            Object sid = search.get(G.T.id);
            if (sid != null) {
                G.Vertex v = ctx.store.vertex(sid);
                if (v != null && vertexMatches(v, search)) {
                    matches.add(v);
                }
            } else {
                Map<String, String> sp = new LinkedHashMap<>();
                for (Map.Entry<Object, Object> e : search.entrySet()) {
                    if (e.getKey() instanceof String k && e.getValue() instanceof String sv) {
                        sp.put(k, sv);
                    }
                }
                Object lab = search.get(G.T.label);
                Iterator<G.Vertex> it = ctx.store.allVertices(new GraphStore.VFilter(lab instanceof String ls ? List.of(ls) : null, sp));
                while (it.hasNext()) {
                    G.Vertex v = it.next();
                    if (vertexMatches(v, search)) {
                        matches.add(v);
                    }
                }
            }
            List<Tr> out = new ArrayList<>();
            if (!matches.isEmpty()) {
                for (G.Vertex v : matches) {
                    if (onMatch != null) {
                        Map<Object, Object> upd = onMatch;
                        ctx.store.updateVertex(v.id, f -> setProps(f, upd));
                        setProps(v, upd);
                    }
                    out.add(t.child(v));
                }
                return out.iterator();
            }
            Map<Object, Object> create = new LinkedHashMap<>(search);
            if (onCreate != null) {
                create.putAll(onCreate);
            }
            Object id = create.get(G.T.id);
            Object lab = create.get(G.T.label);
            for (int attempt = 0; attempt < 100; attempt++) {
                G.Vertex v = new G.Vertex(id != null ? GraphStore.canonId(id) : (Object) ctx.store.nextId(), lab == null ? "vertex" : String.valueOf(lab));
                setProps(v, create);
                try {
                    ctx.store.addVertex(v);
                    out.add(t.child(v));
                    break;
                } catch (G.GremlinError e) {
                    if (id != null) {
                        throw e;
                    }
                }
            }
            return out.iterator();
        }));
    }

    private static boolean vertexMatches(G.Vertex v, Map<Object, Object> search) {
        for (Map.Entry<Object, Object> e : search.entrySet()) {
            if (e.getKey() == G.T.id) {
                if (!Cmp.eq(GraphStore.canonId(e.getValue()), v.id)) {
                    return false;
                }
            } else if (e.getKey() == G.T.label) {
                if (!v.label.equals(e.getValue())) {
                    return false;
                }
            } else if (e.getKey() instanceof String k) {
                boolean ok = false;
                for (G.VProp p : v.vprops(k)) {
                    if (Cmp.eq(p.value, e.getValue())) {
                        ok = true;
                    }
                }
                if (!ok) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void setProps(G.Vertex v, Map<Object, Object> m) {
        for (Map.Entry<Object, Object> e : m.entrySet()) {
            if (e.getKey() instanceof String k) {
                applyVertexProp(v, G.Cardinality.single, k, e.getValue(), List.of());
            }
        }
    }

    static Iterator<Tr> mergeE(Step s, Iterator<Tr> in, Ctx ctx) {
        return Engine.flat(in, t -> Engine.lazy(() -> {
            ctx.needWrite("mergeE()");
            Map<Object, Object> search = mapArg(s, t, ctx);
            Map<Object, Object> onCreate = option(s, G.Merge.onCreate, t, ctx);
            Map<Object, Object> onMatch = option(s, G.Merge.onMatch, t, ctx);
            Object outSpec = search.containsKey(G.Direction.OUT) ? search.get(G.Direction.OUT) : search.get(G.Merge.outV);
            Object inSpec = search.containsKey(G.Direction.IN) ? search.get(G.Direction.IN) : search.get(G.Merge.inV);
            List<G.Edge> matches = new ArrayList<>();
            Object oid = outSpec == null ? null : GraphStore.canonId(outSpec instanceof Map<?, ?> om ? om.get(G.T.id) : outSpec);
            Object iid = inSpec == null ? null : GraphStore.canonId(inSpec instanceof Map<?, ?> im ? im.get(G.T.id) : inSpec);
            Iterator<G.Edge> it = ctx.store.allEdges();
            while (it.hasNext()) {
                G.Edge e = it.next();
                if (oid != null && !e.outId.equals(oid) || iid != null && !e.inId.equals(iid)) {
                    continue;
                }
                boolean ok = true;
                for (Map.Entry<Object, Object> me : search.entrySet()) {
                    if (me.getKey() == G.T.label) {
                        ok &= e.label.equals(me.getValue());
                    } else if (me.getKey() == G.T.id) {
                        ok &= Cmp.eq(GraphStore.canonId(me.getValue()), e.id);
                    } else if (me.getKey() instanceof String k) {
                        ok &= e.props.containsKey(k) && Cmp.eq(e.props.get(k), me.getValue());
                    }
                }
                if (ok) {
                    matches.add(e);
                }
            }
            List<Tr> out = new ArrayList<>();
            if (!matches.isEmpty()) {
                for (G.Edge e : matches) {
                    if (onMatch != null) {
                        Map<Object, Object> upd = onMatch;
                        ctx.store.updateEdge(e.id, f -> upd.forEach((k, v) -> {
                            if (k instanceof String ks) {
                                setEdge(f, ks, v);
                            }
                        }));
                        upd.forEach((k, v) -> {
                            if (k instanceof String ks) {
                                setEdge(e, ks, v);
                            }
                        });
                    }
                    out.add(t.child(e));
                }
                return out.iterator();
            }
            Map<Object, Object> create = new LinkedHashMap<>(search);
            if (onCreate != null) {
                create.putAll(onCreate);
            }
            Object co = create.containsKey(G.Direction.OUT) ? create.get(G.Direction.OUT) : create.get(G.Merge.outV);
            Object ci = create.containsKey(G.Direction.IN) ? create.get(G.Direction.IN) : create.get(G.Merge.inV);
            G.Vertex ov = ctx.store.vertex(co instanceof Map<?, ?> m1 ? m1.get(G.T.id) : co);
            G.Vertex iv = ctx.store.vertex(ci instanceof Map<?, ?> m2 ? m2.get(G.T.id) : ci);
            if (ov == null || iv == null) {
                throw err("Vertex not found for mergeE()");
            }
            Object lab = create.get(G.T.label);
            Object id = create.get(G.T.id);
            G.Edge e = new G.Edge(id != null ? GraphStore.canonId(id) : (Object) ctx.store.nextId(), lab == null ? "edge" : String.valueOf(lab), ov.id, ov.label,
                    iv.id, iv.label);
            create.forEach((k, v) -> {
                if (k instanceof String ks) {
                    setEdge(e, ks, v);
                }
            });
            ctx.store.addEdge(e);
            out.add(t.child(e));
            return out.iterator();
        }));
    }
}
