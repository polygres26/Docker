package com.sayonora.wire.gremlinwire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-process access to the Gremlin store for the MCP tools: runs a Gremlin script through the same interpreter and Postgres store
 * gremlinwire serves over the wire, read-only unless writes are allowed. Results are plain (untyped) JSON.
 */
public final class GremlinEmbedded {

    private final GraphStore store;

    public GremlinEmbedded(BackendRegistry registry) {
        this.store = new PgGraph(registry);
    }

    GremlinEmbedded(GraphStore store) {
        this.store = store;
    }

    private GraphStore graph() {
        return store;
    }

    /** Runs {@code script}; at most {@code maxResults} results are returned ({@code truncated} says when more existed). */
    public JsonObject query(String script, Map<String, Object> bindings, boolean allowWrites, int maxResults, long timeoutMs) {
        long deadline = timeoutMs > 0 ? System.nanoTime() + timeoutMs * 1_000_000L : 0;
        Script.Session ss = new Script.Session(graph(), !allowWrites, deadline);
        Script.Env env = Script.newRoot(ss, bindings);
        Object result = Script.eval(script, env);
        Iterator<Object> it = Processor.resultItems(result, ss, env);
        JsonArray out = new JsonArray();
        boolean truncated = false;
        while (it.hasNext()) {
            Object o = it.next();
            if (out.size() >= maxResults) {
                truncated = true;
                break;
            }
            out.add(GraphSon.UNTYPED.write(o));
        }
        JsonObject r = new JsonObject();
        r.add("results", out);
        r.addProperty("count", out.size());
        r.addProperty("truncated", truncated);
        return r;
    }

    public JsonObject labels() {
        JsonObject o = new JsonObject();
        o.add("vertexLabels", counts(graph().vertexLabels()));
        o.add("edgeLabels", counts(graph().edgeLabels()));
        return o;
    }

    private static JsonObject counts(Map<String, Long> m) {
        JsonObject o = new JsonObject();
        m.forEach(o::addProperty);
        return o;
    }

    public JsonObject counts() {
        JsonObject o = new JsonObject();
        o.addProperty("vertices", graph().vertexCount());
        o.addProperty("edges", graph().edgeCount());
        return o;
    }

    /** One vertex (with its properties) by id, or null. */
    public JsonElement vertex(Object id) {
        G.Vertex v = graph().vertex(id);
        return v == null ? null : GraphSon.UNTYPED.write(v);
    }

    /** Adds a vertex; {@code props} values become single-cardinality properties. */
    public JsonElement addVertex(String label, Object id, Map<String, Object> props) {
        G.Vertex v = new G.Vertex(id == null ? (Object) graph().nextId() : GraphStore.canonId(id), label);
        for (Map.Entry<String, Object> e : props.entrySet()) {
            Mutations.applyVertexProp(v, G.Cardinality.single, e.getKey(), e.getValue(), List.of());
        }
        graph().addVertex(v);
        return GraphSon.UNTYPED.write(v);
    }

    public JsonElement addEdge(String label, Object outId, Object inId, Map<String, Object> props) {
        List<G.Vertex> ends = graph().vertices(List.of(outId, inId));
        G.Vertex o = null;
        G.Vertex i = null;
        for (G.Vertex v : ends) {
            if (v.id.equals(GraphStore.canonId(outId))) {
                o = v;
            }
            if (v.id.equals(GraphStore.canonId(inId))) {
                i = v;
            }
        }
        if (o == null || i == null) {
            throw new IllegalArgumentException("both endpoint vertices must exist");
        }
        G.Edge e = new G.Edge(graph().nextId(), label, o.id, o.label, i.id, i.label);
        e.props.putAll(new LinkedHashMap<>(props));
        graph().addEdge(e);
        return GraphSon.UNTYPED.write(e);
    }

    public boolean dropVertex(Object id) {
        if (graph().vertex(id) == null) {
            return false;
        }
        graph().removeVertex(id);
        return true;
    }

    /** Converts a JSON element (tool argument) to a Java value. */
    public static Object fromJson(JsonElement e) {
        return GraphSon.V3.read(e);
    }
}
