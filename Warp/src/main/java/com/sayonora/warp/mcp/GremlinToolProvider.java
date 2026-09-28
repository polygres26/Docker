package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.gremlinwire.GremlinEmbedded;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Apache TinkerPop Gremlin vocabulary over Warp's hosted property graph: {@code gremlin_query} runs a Gremlin script through the
 * same interpreter and Postgres store gremlinwire serves over its WebSocket/HTTP endpoint, read-only (any mutating step is refused);
 * {@code gremlin_write} runs a script allowed to mutate and, like the other write tools, is hidden under {@code WARP_MCP_READ_ONLY}.
 * Results are plain JSON, bounded by {@code maxResults}.
 */
final class GremlinToolProvider extends StoreToolProvider {

    private static final int MAX_RESULTS = 1000;
    private static final long TIMEOUT_MS = 30_000;

    GremlinToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.GREMLINSTORE, describer, stores);
    }

    private GremlinEmbedded gremlin() {
        return stores.engine("gremlin", GremlinEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject script = str("Gremlin script, e.g. g.V().has('name','marko').out('knows').values('name').toList()");
        JsonObject bindings = obj("Variables available to the script (JSON values)");
        JsonObject max = num("Maximum results returned (default 100, at most " + MAX_RESULTS + ")");
        return List.of(
                new Tool("gremlin_query", "Run a Gremlin traversal script read-only (addV/addE/property/drop are refused). Returns the results as plain JSON.",
                        schema(List.of("script"), "script", script, "bindings", bindings, "maxResults", max), false),
                new Tool("gremlin_list_labels", "Vertex and edge labels with their element counts.", schema(List.of()), false),
                new Tool("gremlin_count", "Total number of vertices and edges.", schema(List.of()), false),
                new Tool("gremlin_get_vertex", "One vertex with its properties by id.", schema(List.of("id"), "id", prop("string", "Vertex id (number, string or UUID)")), false),
                new Tool("gremlin_write", "Run a Gremlin script that may add, change or drop vertices and edges.",
                        schema(List.of("script"), "script", script, "bindings", bindings, "maxResults", max), true),
                new Tool("gremlin_add_vertex", "Add a vertex with a label and properties.", schema(List.of("label"), "label", str("Vertex label"), "id",
                        prop("string", "Optional vertex id (default: generated)"), "properties", obj("Property name to value")), true),
                new Tool("gremlin_add_edge", "Add an edge between two existing vertices.", schema(List.of("label", "outId", "inId"), "label", str("Edge label"),
                        "outId", prop("string", "Id of the out (source) vertex"), "inId", prop("string", "Id of the in (target) vertex"), "properties",
                        obj("Property name to value")), true),
                new Tool("gremlin_drop_vertex", "Delete a vertex and its edges.", schema(List.of("id"), "id", prop("string", "Vertex id")), true));
    }

    private static Object id(JsonObject a, String key) {
        if (!a.has(key) || a.get(key).isJsonNull()) {
            throw new IllegalArgumentException("missing required argument: " + key);
        }
        JsonElement e = a.get(key);
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) {
            return e.getAsLong();
        }
        String s = e.getAsString();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            try {
                return java.util.UUID.fromString(s);
            } catch (IllegalArgumentException ex2) {
                return s;
            }
        }
    }

    private static Map<String, Object> map(JsonObject a, String key) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (a.has(key) && a.get(key).isJsonObject()) {
            for (Map.Entry<String, JsonElement> e : a.getAsJsonObject(key).entrySet()) {
                m.put(e.getKey(), GremlinEmbedded.fromJson(e.getValue()));
            }
        }
        return m;
    }

    @Override
    protected Outcome run(String tool, JsonObject args, Ctx ctx) throws Exception {
        switch (tool) {
            case "gremlin_query", "gremlin_write": {
                String script = requireString(args, "script");
                int max = limit(args, "maxResults", 100, MAX_RESULTS);
                return json(gremlin().query(script, map(args, "bindings"), tool.equals("gremlin_write"), max, TIMEOUT_MS));
            }
            case "gremlin_list_labels":
                return json(gremlin().labels());
            case "gremlin_count":
                return json(gremlin().counts());
            case "gremlin_get_vertex": {
                JsonElement v = gremlin().vertex(id(args, "id"));
                return v == null ? Outcome.error("vertex not found") : json(v);
            }
            case "gremlin_add_vertex":
                return json(gremlin().addVertex(requireString(args, "label"), args.has("id") && !args.get("id").isJsonNull() ? id(args, "id") : null,
                        map(args, "properties")));
            case "gremlin_add_edge":
                return json(gremlin().addEdge(requireString(args, "label"), id(args, "outId"), id(args, "inId"), map(args, "properties")));
            case "gremlin_drop_vertex":
                return gremlin().dropVertex(id(args, "id")) ? Outcome.ok("{\"dropped\":true}") : Outcome.error("vertex not found");
            default:
                return Outcome.error("unknown gremlin tool: " + tool);
        }
    }
}
