package com.sayonora.warp.gremlinwire;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Storage the traversal interpreter runs over. Two implementations: {@link MemGraph} (heap, unit tests) and {@link PgGraph}
 * (the Postgres property-graph store, sharded over the hosts of the backend set).
 */
interface GraphStore {

    /** Narrowing hints for a full vertex scan; the engine still re-applies every filter, so a store may ignore them. */
    record VFilter(List<String> labels, Map<String, String> stringProps) {
        static final VFilter NONE = new VFilter(null, Map.of());
    }

    G.Vertex vertex(Object id);

    /** The vertices that exist among {@code ids}, in the order of {@code ids}. */
    List<G.Vertex> vertices(Collection<Object> ids);

    Iterator<G.Vertex> allVertices(VFilter filter);

    G.Edge edge(Object id);

    List<G.Edge> edges(Collection<Object> ids);

    Iterator<G.Edge> allEdges();

    /**
     * Edges adjacent to each vertex id, by direction and labels (empty = any): OUT then IN for BOTH, edge insertion order within
     * a direction. Ids without edges are absent from the map.
     */
    Map<Object, List<G.Edge>> adjacent(Collection<Object> vertexIds, G.Direction dir, String[] labels);

    /** A fresh element id (a Long). */
    long nextId();

    /** @throws G.GremlinError when a vertex with the id exists */
    void addVertex(G.Vertex v);

    /** Both endpoint vertices must exist. */
    void addEdge(G.Edge e);

    /** Read-modify-write of a vertex row; {@code fn} runs on the freshly read vertex. */
    void updateVertex(Object id, Consumer<G.Vertex> fn);

    void updateEdge(Object id, Consumer<G.Edge> fn);

    /** Removes the vertex and every edge incident to it. */
    void removeVertex(Object id);

    void removeEdge(Object id);

    long vertexCount();

    long edgeCount();

    /** Label to element counts. */
    Map<String, Long> vertexLabels();

    Map<String, Long> edgeLabels();

    /**
     * Orders one vertex's edges of one direction the way TinkerGraph iterates them (a HashMap of label to a HashSet of edges; with
     * explicit labels, in the order the labels were given), so out()/in()/both() results come back in the same order as the
     * reference implementation.
     */
    static List<G.Edge> tinkerOrder(List<G.Edge> insertionOrder, String[] labels) {
        java.util.HashMap<String, java.util.HashSet<G.Edge>> m = new java.util.HashMap<>();
        for (G.Edge e : insertionOrder) {
            m.computeIfAbsent(e.label, k -> new java.util.HashSet<>()).add(e);
        }
        List<G.Edge> out = new java.util.ArrayList<>(insertionOrder.size());
        if (labels != null && labels.length > 0) {
            for (String l : labels) {
                java.util.HashSet<G.Edge> s = m.get(l);
                if (s != null) {
                    out.addAll(s);
                    m.remove(l);
                }
            }
        } else {
            for (java.util.HashSet<G.Edge> s : m.values()) {
                out.addAll(s);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ id helpers

    /** Scan order of ids: Long ids numerically first, then the others by their key text. */
    static int compareIds(Object a, Object b) {
        if (a instanceof Long x && b instanceof Long y) {
            return Long.compare(x, y);
        }
        if (a instanceof Long) {
            return -1;
        }
        if (b instanceof Long) {
            return 1;
        }
        return key(a).compareTo(key(b));
    }

    /** Element ids are Long, String or UUID; every integral number is a Long (like TinkerGraph's default id manager). */
    static Object canonId(Object id) {
        if (id instanceof Long || id instanceof String || id instanceof UUID) {
            return id;
        }
        if (id instanceof Integer || id instanceof Short || id instanceof Byte) {
            return ((Number) id).longValue();
        }
        if (id instanceof BigInteger b && b.bitLength() < 64) {
            return b.longValue();
        }
        if (id instanceof Double d && d == Math.rint(d) && !d.isInfinite()) {
            return d.longValue();
        }
        if (id instanceof G.Element e) {
            return canonId(e.id());
        }
        return String.valueOf(id);
    }

    static String key(Object canon) {
        if (canon instanceof Long l) {
            return "l:" + l;
        }
        if (canon instanceof UUID u) {
            return "u:" + u;
        }
        return "s:" + canon;
    }

    static Object idOf(String key) {
        char k = key.charAt(0);
        String v = key.substring(2);
        return k == 'l' ? (Object) Long.parseLong(v) : k == 'u' ? (Object) UUID.fromString(v) : v;
    }
}
