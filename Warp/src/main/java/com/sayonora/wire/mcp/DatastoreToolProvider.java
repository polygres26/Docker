package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.datastorewire.DatastoreEmbedded;
import java.util.List;
import java.util.Map;

/**
 * Google Cloud Datastore vocabulary (datastore_* tools: lookup, run query or GQL, count, upsert/insert/delete entities). Each
 * tool issues the equivalent Datastore v1 request (lookup, runQuery, runAggregationQuery, commit) to datastorewire's own
 * {@code DsService} in process, so keys, ancestors, id allocation, indexes and query semantics are the wire protocol's.
 * Entities go in and come out as plain JSON (see {@link TypedValues}); a key is {@code kind} plus {@code name} (string id) or
 * {@code id} (numeric id), optionally under {@code ancestors: [{kind, name|id}]}.
 */
final class DatastoreToolProvider extends StoreToolProvider {

    private static final int MAX_ENTITIES = 1000;
    private static final TypedValues TV = TypedValues.DATASTORE;
    private static final Map<String, String> OPS = Map.of("==", "EQUAL", "=", "EQUAL", "!=", "NOT_EQUAL", "<", "LESS_THAN",
            "<=", "LESS_THAN_OR_EQUAL", ">", "GREATER_THAN", ">=", "GREATER_THAN_OR_EQUAL", "in", "IN", "not-in", "NOT_IN");

    DatastoreToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.DATASTORE, describer, stores);
    }

    private DatastoreEmbedded ds() {
        return stores.engine("datastore", DatastoreEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject project = str("Project id (default: WARP_MCP_GCP_PROJECT, else \"warp-project\")");
        JsonObject ns = str("Namespace (default: the default namespace)");
        JsonObject kind = str("Entity kind");
        JsonObject name = str("Key name (string id)");
        JsonObject id = str("Key id (numeric id)");
        JsonObject anc = arr("Ancestor path: [{kind, name|id}, ...] from the root entity down");
        JsonObject props = obj("Entity properties as plain JSON (strings, numbers, booleans, null, arrays, objects)");
        JsonObject where = arr("Filters: [{property, op, value}] with op one of ==, !=, <, <=, >, >=, in, not-in");
        return List.of(
                new Tool("datastore_lookup", "Read entities by key: one key (kind + name|id) or keys: [{kind, name|id, ancestors}].",
                        schema(List.of(), "project", project, "namespace", ns, "kind", kind, "name", name, "id", id, "ancestors", anc,
                                "keys", arr("Several keys instead of kind/name/id")), false),
                new Tool("datastore_run_query", "Query a kind with filters and ordering, or run a GQL query string.",
                        schema(List.of(), "project", project, "namespace", ns, "kind", kind, "where", where,
                                "orderBy", arr("[{property, direction: ASCENDING|DESCENDING}]"), "limit", num("Max entities (default 100, max 1000)"),
                                "keysOnly", bool("Return only keys"), "startCursor", str("endCursor of the previous page"),
                                "gql", str("GQL query string instead of kind/where (e.g. SELECT * FROM Task WHERE done = false)"),
                                "ancestor", anc), false),
                new Tool("datastore_count", "Count the entities of a kind matching the filters.",
                        schema(List.of("kind"), "project", project, "namespace", ns, "kind", kind, "where", where), false),
                new Tool("datastore_upsert_entity", "Create or overwrite an entity; without name/id a numeric id is allocated.",
                        schema(List.of("kind", "properties"), "project", project, "namespace", ns, "kind", kind, "name", name, "id", id,
                                "ancestors", anc, "properties", props), true),
                new Tool("datastore_insert_entity", "Create an entity (fails when the key exists); without name/id a numeric id is allocated.",
                        schema(List.of("kind", "properties"), "project", project, "namespace", ns, "kind", kind, "name", name, "id", id,
                                "ancestors", anc, "properties", props), true),
                new Tool("datastore_delete_entity", "Delete an entity by key.", schema(List.of("kind"), "project", project, "namespace", ns,
                        "kind", kind, "name", name, "id", id, "ancestors", anc), true));
    }

    private JsonObject call(String project, String verb, JsonObject body) {
        DatastoreEmbedded.Response r = ds().call(project, verb, body.toString());
        if (r.status() >= 300) {
            String m = r.body();
            try {
                JsonObject e = JsonParser.parseString(m).getAsJsonObject().getAsJsonObject("error");
                m = e.get("status").getAsString() + ": " + e.get("message").getAsString();
            } catch (RuntimeException ignored) {
                // raw body
            }
            throw new IllegalStateException(m);
        }
        return r.body().isBlank() ? new JsonObject() : JsonParser.parseString(r.body()).getAsJsonObject();
    }

    private static JsonObject partition(JsonObject a) {
        JsonObject p = new JsonObject();
        p.addProperty("projectId", gcpProject(a));
        if (optString(a, "namespace") != null) {
            p.addProperty("namespaceId", optString(a, "namespace"));
        }
        return p;
    }

    private static JsonObject pathElement(JsonObject e) {
        JsonObject pe = new JsonObject();
        pe.addProperty("kind", requireString(e, "kind"));
        if (optString(e, "name") != null) {
            pe.addProperty("name", optString(e, "name"));
        } else if (optString(e, "id") != null) {
            pe.addProperty("id", optString(e, "id"));
        }
        return pe;
    }

    private static JsonObject key(JsonObject a, boolean allowIncomplete) {
        JsonObject k = new JsonObject();
        k.add("partitionId", partition(a));
        JsonArray path = new JsonArray();
        if (a.has("ancestors") && a.get("ancestors").isJsonArray()) {
            a.getAsJsonArray("ancestors").forEach(e -> path.add(pathElement(e.getAsJsonObject())));
        }
        JsonObject last = pathElement(a);
        if (!allowIncomplete && !last.has("name") && !last.has("id")) {
            throw new IllegalArgumentException("name or id is required");
        }
        path.add(last);
        k.add("path", path);
        return k;
    }

    private JsonObject plainKey(JsonObject k) {
        JsonObject o = new JsonObject();
        JsonArray path = k.has("path") ? k.getAsJsonArray("path") : new JsonArray();
        JsonArray anc = new JsonArray();
        for (int i = 0; i < path.size(); i++) {
            JsonObject pe = path.get(i).getAsJsonObject();
            if (i < path.size() - 1) {
                anc.add(pe);
            } else {
                o.add("kind", pe.get("kind"));
                if (pe.has("name")) {
                    o.add("name", pe.get("name"));
                }
                if (pe.has("id")) {
                    o.add("id", pe.get("id"));
                }
            }
        }
        if (anc.size() > 0) {
            o.add("ancestors", anc);
        }
        if (k.has("partitionId") && k.getAsJsonObject("partitionId").has("namespaceId")
                && !k.getAsJsonObject("partitionId").get("namespaceId").getAsString().isEmpty()) {
            o.add("namespace", k.getAsJsonObject("partitionId").get("namespaceId"));
        }
        return o;
    }

    private JsonObject entity(JsonObject e) {
        JsonObject o = new JsonObject();
        o.add("key", plainKey(e.getAsJsonObject("key")));
        o.add("properties", TV.plain(e.has("properties") ? e.getAsJsonObject("properties") : null));
        return o;
    }

    private JsonObject queryBody(JsonObject a, boolean forCount) {
        JsonObject q = new JsonObject();
        if (optString(a, "kind") != null) {
            JsonArray kinds = new JsonArray();
            JsonObject k = new JsonObject();
            k.addProperty("name", optString(a, "kind"));
            kinds.add(k);
            q.add("kind", kinds);
        }
        JsonArray filters = new JsonArray();
        if (a.has("where") && a.get("where").isJsonArray()) {
            for (JsonElement e : a.getAsJsonArray("where")) {
                JsonObject w = e.getAsJsonObject();
                String op = OPS.get(requireString(w, "op"));
                if (op == null) {
                    throw new IllegalArgumentException("unknown operator \"" + w.get("op").getAsString() + "\"; use " + OPS.keySet());
                }
                JsonObject pf = new JsonObject();
                JsonObject prop = new JsonObject();
                prop.addProperty("name", requireString(w, "property"));
                pf.add("property", prop);
                pf.addProperty("op", op);
                pf.add("value", TV.value(w.get("value")));
                JsonObject wrap = new JsonObject();
                wrap.add("propertyFilter", pf);
                filters.add(wrap);
            }
        }
        if (a.has("ancestor") && a.get("ancestor").isJsonArray() && !a.getAsJsonArray("ancestor").isEmpty()) {
            JsonObject ak = new JsonObject();
            ak.add("partitionId", partition(a));
            JsonArray path = new JsonArray();
            a.getAsJsonArray("ancestor").forEach(e -> path.add(pathElement(e.getAsJsonObject())));
            ak.add("path", path);
            JsonObject pf = new JsonObject();
            JsonObject prop = new JsonObject();
            prop.addProperty("name", "__key__");
            pf.add("property", prop);
            pf.addProperty("op", "HAS_ANCESTOR");
            JsonObject v = new JsonObject();
            v.add("keyValue", ak);
            pf.add("value", v);
            JsonObject wrap = new JsonObject();
            wrap.add("propertyFilter", pf);
            filters.add(wrap);
        }
        if (filters.size() == 1) {
            q.add("filter", filters.get(0));
        } else if (filters.size() > 1) {
            JsonObject comp = new JsonObject();
            comp.addProperty("op", "AND");
            comp.add("filters", filters);
            JsonObject wrap = new JsonObject();
            wrap.add("compositeFilter", comp);
            q.add("filter", wrap);
        }
        if (!forCount) {
            if (a.has("orderBy") && a.get("orderBy").isJsonArray()) {
                JsonArray order = new JsonArray();
                for (JsonElement e : a.getAsJsonArray("orderBy")) {
                    JsonObject o = e.getAsJsonObject();
                    JsonObject x = new JsonObject();
                    JsonObject prop = new JsonObject();
                    prop.addProperty("name", requireString(o, "property"));
                    x.add("property", prop);
                    x.addProperty("direction", optString(o, "direction") == null ? "ASCENDING" : optString(o, "direction").toUpperCase(java.util.Locale.ROOT));
                    order.add(x);
                }
                q.add("order", order);
            }
            q.addProperty("limit", limit(a, "limit", 100, MAX_ENTITIES));
            if (optBool(a, "keysOnly", false)) {
                JsonArray proj = new JsonArray();
                JsonObject pr = new JsonObject();
                JsonObject prop = new JsonObject();
                prop.addProperty("name", "__key__");
                pr.add("property", prop);
                proj.add(pr);
                q.add("projection", proj);
            }
            if (optString(a, "startCursor") != null) {
                q.addProperty("startCursor", optString(a, "startCursor"));
            }
        }
        return q;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        String project = gcpProject(a);
        switch (tool) {
            case "datastore_lookup": {
                JsonArray keys = new JsonArray();
                if (a.has("keys") && a.get("keys").isJsonArray()) {
                    for (JsonElement e : a.getAsJsonArray("keys")) {
                        JsonObject k = e.getAsJsonObject().deepCopy();
                        if (optString(k, "namespace") == null && optString(a, "namespace") != null) {
                            k.addProperty("namespace", optString(a, "namespace"));
                        }
                        if (optString(k, "project") == null) {
                            k.addProperty("project", project);
                        }
                        keys.add(key(k, false));
                    }
                } else {
                    keys.add(key(a, false));
                }
                JsonObject body = new JsonObject();
                body.add("keys", keys);
                JsonObject res = call(project, "lookup", body);
                JsonArray found = new JsonArray();
                JsonArray missing = new JsonArray();
                if (res.has("found")) {
                    res.getAsJsonArray("found").forEach(e -> found.add(entity(e.getAsJsonObject().getAsJsonObject("entity"))));
                }
                if (res.has("missing")) {
                    res.getAsJsonArray("missing").forEach(e -> missing.add(plainKey(e.getAsJsonObject().getAsJsonObject("entity").getAsJsonObject("key"))));
                }
                JsonObject o = new JsonObject();
                o.add("entities", found);
                o.add("missing", missing);
                return json(o);
            }
            case "datastore_run_query": {
                JsonObject body = new JsonObject();
                body.add("partitionId", partition(a));
                if (optString(a, "gql") != null) {
                    JsonObject g = new JsonObject();
                    g.addProperty("queryString", optString(a, "gql"));
                    g.addProperty("allowLiterals", true);
                    body.add("gqlQuery", g);
                } else {
                    if (optString(a, "kind") == null) {
                        throw new IllegalArgumentException("kind (or gql) is required");
                    }
                    body.add("query", queryBody(a, false));
                }
                JsonObject res = call(project, "runQuery", body);
                JsonObject batch = res.has("batch") ? res.getAsJsonObject("batch") : new JsonObject();
                JsonArray ents = new JsonArray();
                if (batch.has("entityResults")) {
                    batch.getAsJsonArray("entityResults").forEach(e -> ents.add(entity(e.getAsJsonObject().getAsJsonObject("entity"))));
                }
                JsonObject o = new JsonObject();
                o.add("entities", ents);
                o.addProperty("count", ents.size());
                o.add("moreResults", batch.has("moreResults") ? batch.get("moreResults") : JsonNull.INSTANCE);
                o.add("endCursor", batch.has("endCursor") ? batch.get("endCursor") : JsonNull.INSTANCE);
                return json(o);
            }
            case "datastore_count": {
                JsonObject body = new JsonObject();
                body.add("partitionId", partition(a));
                JsonObject agg = new JsonObject();
                agg.add("nestedQuery", queryBody(a, true));
                JsonObject count = new JsonObject();
                count.add("count", new JsonObject());
                count.addProperty("alias", "n");
                JsonArray aggs = new JsonArray();
                aggs.add(count);
                agg.add("aggregations", aggs);
                body.add("aggregationQuery", agg);
                JsonObject res = call(project, "runAggregationQuery", body);
                long n = 0;
                if (res.has("batch") && res.getAsJsonObject("batch").has("aggregationResults")) {
                    JsonObject r0 = res.getAsJsonObject("batch").getAsJsonArray("aggregationResults").get(0).getAsJsonObject();
                    n = Long.parseLong(r0.getAsJsonObject("aggregateProperties").getAsJsonObject("n").get("integerValue").getAsString());
                }
                JsonObject o = new JsonObject();
                o.addProperty("count", n);
                return json(o);
            }
            case "datastore_upsert_entity", "datastore_insert_entity": {
                if (!a.has("properties") || !a.get("properties").isJsonObject()) {
                    throw new IllegalArgumentException("properties must be an object");
                }
                boolean incomplete = optString(a, "name") == null && optString(a, "id") == null;
                JsonObject e = new JsonObject();
                e.add("key", key(a, true));
                e.add("properties", TV.fields(a.getAsJsonObject("properties")));
                JsonObject mutation = new JsonObject();
                mutation.add(tool.equals("datastore_insert_entity") || incomplete ? "insert" : "upsert", e);
                JsonArray ms = new JsonArray();
                ms.add(mutation);
                JsonObject body = new JsonObject();
                body.addProperty("mode", "NON_TRANSACTIONAL");
                body.add("mutations", ms);
                JsonObject res = call(project, "commit", body);
                JsonObject o = new JsonObject();
                JsonObject r0 = res.has("mutationResults") ? res.getAsJsonArray("mutationResults").get(0).getAsJsonObject() : new JsonObject();
                o.add("key", plainKey(r0.has("key") ? r0.getAsJsonObject("key") : e.getAsJsonObject("key")));
                o.addProperty("ok", true);
                return json(o);
            }
            case "datastore_delete_entity": {
                JsonObject mutation = new JsonObject();
                mutation.add("delete", key(a, false));
                JsonArray ms = new JsonArray();
                ms.add(mutation);
                JsonObject body = new JsonObject();
                body.addProperty("mode", "NON_TRANSACTIONAL");
                body.add("mutations", ms);
                call(project, "commit", body);
                JsonObject o = new JsonObject();
                o.addProperty("ok", true);
                return json(o);
            }
            default:
                return Outcome.error("unknown datastore tool: " + tool);
        }
    }
}
