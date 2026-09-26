package com.sayonora.wire.gremlinwire;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Heap graph for unit tests and the conformance harness's offline checks. Not persisted. */
final class MemGraph implements GraphStore {

    private final Map<Object, G.Vertex> vs = new LinkedHashMap<>();
    private final Map<Object, G.Edge> es = new LinkedHashMap<>();
    private final AtomicLong ids = new AtomicLong(1);

    @Override
    public synchronized G.Vertex vertex(Object id) {
        G.Vertex v = vs.get(GraphStore.canonId(id));
        return v == null ? null : copy(v);
    }

    static G.Vertex copy(G.Vertex v) {
        G.Vertex c = new G.Vertex(v.id, v.label);
        for (Map.Entry<String, List<G.VProp>> e : v.props.entrySet()) {
            List<G.VProp> l = new ArrayList<>();
            for (G.VProp p : e.getValue()) {
                G.VProp q = new G.VProp(p.id, p.key, p.value);
                q.meta = p.meta == null ? null : new LinkedHashMap<>(p.meta);
                q.vertex = c;
                l.add(q);
            }
            c.props.put(e.getKey(), l);
        }
        return c;
    }

    static G.Edge copy(G.Edge e) {
        G.Edge c = new G.Edge(e.id, e.label, e.outId, e.outLabel, e.inId, e.inLabel);
        c.props.putAll(e.props);
        return c;
    }

    @Override
    public synchronized List<G.Vertex> vertices(Collection<Object> ids) {
        List<G.Vertex> out = new ArrayList<>();
        for (Object id : ids) {
            G.Vertex v = vertex(id);
            if (v != null) {
                out.add(v);
            }
        }
        return out;
    }

    @Override
    public synchronized Iterator<G.Vertex> allVertices(VFilter f) {
        List<G.Vertex> out = new ArrayList<>();
        for (G.Vertex v : vs.values()) {
            out.add(copy(v));
        }
        out.sort((a, b) -> GraphStore.compareIds(a.id, b.id));
        return out.iterator();
    }

    @Override
    public synchronized G.Edge edge(Object id) {
        G.Edge e = es.get(GraphStore.canonId(id));
        return e == null ? null : copy(e);
    }

    @Override
    public synchronized List<G.Edge> edges(Collection<Object> ids) {
        List<G.Edge> out = new ArrayList<>();
        for (Object id : ids) {
            G.Edge e = edge(id);
            if (e != null) {
                out.add(e);
            }
        }
        return out;
    }

    @Override
    public synchronized Iterator<G.Edge> allEdges() {
        List<G.Edge> out = new ArrayList<>();
        for (G.Edge e : es.values()) {
            out.add(copy(e));
        }
        out.sort((a, b) -> GraphStore.compareIds(a.id, b.id));
        return out.iterator();
    }

    private static boolean labelOk(String[] labels, String l) {
        if (labels == null || labels.length == 0) {
            return true;
        }
        for (String s : labels) {
            if (s.equals(l)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized Map<Object, List<G.Edge>> adjacent(Collection<Object> vertexIds, G.Direction dir, String[] labels) {
        Map<Object, List<G.Edge>> out = new LinkedHashMap<>();
        for (Object raw : vertexIds) {
            Object id = GraphStore.canonId(raw);
            List<G.Edge> l = new ArrayList<>();
            if (dir != G.Direction.IN) {
                List<G.Edge> o = new ArrayList<>();
                for (G.Edge e : es.values()) {
                    if (e.outId.equals(id) && labelOk(labels, e.label)) {
                        o.add(copy(e));
                    }
                }
                l.addAll(GraphStore.tinkerOrder(o, labels));
            }
            if (dir != G.Direction.OUT) {
                List<G.Edge> o = new ArrayList<>();
                for (G.Edge e : es.values()) {
                    if (e.inId.equals(id) && labelOk(labels, e.label)) {
                        o.add(copy(e));
                    }
                }
                l.addAll(GraphStore.tinkerOrder(o, labels));
            }
            if (!l.isEmpty()) {
                out.put(id, l);
            }
        }
        return out;
    }

    @Override
    public long nextId() {
        return ids.getAndIncrement();
    }

    @Override
    public synchronized void addVertex(G.Vertex v) {
        if (vs.containsKey(v.id)) {
            throw G.GremlinError.script("Vertex with id already exists: " + v.id);
        }
        vs.put(v.id, copy(v));
    }

    @Override
    public synchronized void addEdge(G.Edge e) {
        if (!vs.containsKey(e.outId) || !vs.containsKey(e.inId)) {
            throw G.GremlinError.script("Cannot add edge: an endpoint vertex does not exist");
        }
        if (es.containsKey(e.id)) {
            throw G.GremlinError.script("Edge with id already exists: " + e.id);
        }
        es.put(e.id, copy(e));
    }

    @Override
    public synchronized void updateVertex(Object id, Consumer<G.Vertex> fn) {
        G.Vertex v = vs.get(GraphStore.canonId(id));
        if (v != null) {
            fn.accept(v);
        }
    }

    @Override
    public synchronized void updateEdge(Object id, Consumer<G.Edge> fn) {
        G.Edge e = es.get(GraphStore.canonId(id));
        if (e != null) {
            fn.accept(e);
        }
    }

    @Override
    public synchronized void removeVertex(Object id) {
        Object c = GraphStore.canonId(id);
        vs.remove(c);
        es.values().removeIf(e -> e.outId.equals(c) || e.inId.equals(c));
    }

    @Override
    public synchronized void removeEdge(Object id) {
        es.remove(GraphStore.canonId(id));
    }

    @Override
    public synchronized long vertexCount() {
        return vs.size();
    }

    @Override
    public synchronized long edgeCount() {
        return es.size();
    }

    @Override
    public synchronized Map<String, Long> vertexLabels() {
        Map<String, Long> m = new TreeMap<>();
        vs.values().forEach(v -> m.merge(v.label, 1L, Long::sum));
        return m;
    }

    @Override
    public synchronized Map<String, Long> edgeLabels() {
        Map<String, Long> m = new TreeMap<>();
        es.values().forEach(v -> m.merge(v.label, 1L, Long::sum));
        return m;
    }
}
