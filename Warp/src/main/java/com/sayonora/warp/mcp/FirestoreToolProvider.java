package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.firestorewire.FirestoreEmbedded;
import java.util.List;
import java.util.Map;

/**
 * Google Cloud Firestore vocabulary (tool names follow the Firebase MCP server's Firestore tools: firestore_list_collections,
 * firestore_get_documents, firestore_list_documents, firestore_query_collection, firestore_add_document,
 * firestore_update_document, firestore_delete_document; plus set/run-query/count). Each tool issues the equivalent Firestore v1
 * REST request to firestorewire's own {@code FsService} in process (preconditions, transforms, structured queries and
 * aggregations are the wire protocol's). Documents go in and come out as plain JSON (see {@link TypedValues}); a document's
 * {@code path} is relative to the database root ({@code users/alice}).
 */
final class FirestoreToolProvider extends StoreToolProvider {

    private static final int MAX_DOCS = 1000;
    private static final TypedValues TV = TypedValues.FIRESTORE;
    private static final Map<String, String> OPS = Map.ofEntries(Map.entry("==", "EQUAL"), Map.entry("=", "EQUAL"),
            Map.entry("!=", "NOT_EQUAL"), Map.entry("<", "LESS_THAN"), Map.entry("<=", "LESS_THAN_OR_EQUAL"),
            Map.entry(">", "GREATER_THAN"), Map.entry(">=", "GREATER_THAN_OR_EQUAL"), Map.entry("array-contains", "ARRAY_CONTAINS"),
            Map.entry("in", "IN"), Map.entry("not-in", "NOT_IN"), Map.entry("array-contains-any", "ARRAY_CONTAINS_ANY"));

    FirestoreToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.FIRESTORE, describer, stores);
    }

    private FirestoreEmbedded fs() {
        return stores.engine("firestore", FirestoreEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject project = str("Project id (default: WARP_MCP_GCP_PROJECT, else \"warp-project\")");
        JsonObject database = str("Database id (default \"(default)\")");
        JsonObject collection = str("Collection path, e.g. users or users/alice/orders");
        JsonObject path = str("Document path, e.g. users/alice");
        JsonObject data = obj("Document fields as plain JSON (strings, numbers, booleans, null, arrays, objects; see the tool notes for timestamps)");
        JsonObject where = arr("Filters: [{field, op, value}] with op one of ==, !=, <, <=, >, >=, array-contains, in, not-in, array-contains-any");
        return List.of(
                new Tool("firestore_list_collections", "List collection ids at the database root, or under a document.",
                        schema(List.of(), "project", project, "database", database, "documentPath", str("Parent document path (default: root)")), false),
                new Tool("firestore_list_documents", "List the documents of a collection (paged).",
                        schema(List.of("collection"), "project", project, "database", database, "collection", collection,
                                "pageSize", num("Max documents (default 100, max 1000)"), "pageToken", str("nextPageToken of the previous page"),
                                "orderBy", str("e.g. \"name desc\"")), false),
                new Tool("firestore_get_documents", "Read documents by path.", schema(List.of("paths"), "project", project, "database", database,
                        "paths", strings("Document paths, e.g. [\"users/alice\"]")), false),
                new Tool("firestore_add_document", "Create a document with a generated id (or the given documentId).",
                        schema(List.of("collection", "data"), "project", project, "database", database, "collection", collection,
                                "data", data, "documentId", str("Explicit id")), true),
                new Tool("firestore_set_document", "Create or overwrite a document; with merge=true only the given top-level fields change.",
                        schema(List.of("path", "data"), "project", project, "database", database, "path", path, "data", data,
                                "merge", bool("Keep fields not mentioned in data")), true),
                new Tool("firestore_update_document", "Update fields of an existing document (fails when it does not exist).",
                        schema(List.of("path", "data"), "project", project, "database", database, "path", path, "data", data), true),
                new Tool("firestore_delete_document", "Delete a document.", schema(List.of("path"), "project", project, "database", database,
                        "path", path), true),
                new Tool("firestore_query_collection", "Query a collection with filters, ordering and a limit.",
                        schema(List.of("collection"), "project", project, "database", database, "collection", collection, "where", where,
                                "orderBy", arr("[{field, direction: ASCENDING|DESCENDING}]"), "limit", num("Max documents (default 100, max 1000)"),
                                "allDescendants", bool("Query every collection with this id, at any depth")), false),
                new Tool("firestore_run_query", "Run a raw Firestore structuredQuery (proto3 JSON) under a parent path.",
                        schema(List.of("structuredQuery"), "project", project, "database", database,
                                "parent", str("Parent document path (default: database root)"), "structuredQuery", obj("StructuredQuery")), false),
                new Tool("firestore_count", "Count the documents of a collection matching the filters.",
                        schema(List.of("collection"), "project", project, "database", database, "collection", collection, "where", where), false));
    }

    private static String root(JsonObject a) {
        String db = optString(a, "database") == null ? "(default)" : optString(a, "database");
        return "projects/" + gcpProject(a) + "/databases/" + db + "/documents";
    }

    private static String rootUrl(JsonObject a) {
        return "/v1/" + root(a);
    }

    private static String url(JsonObject a, String rel) {
        // the embedded REST entry takes the decoded path: no percent-encoding here
        return rootUrl(a) + (rel.isEmpty() ? "" : "/" + rel);
    }

    private JsonElement send(String method, String path, String query, JsonElement body) {
        FirestoreEmbedded.Response r = fs().request(method, path, query, body == null ? null : body.toString());
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
        return r.body().isBlank() ? new JsonObject() : JsonParser.parseString(r.body());
    }

    private JsonObject doc(JsonObject a, JsonObject d) {
        JsonObject o = new JsonObject();
        String name = d.get("name").getAsString();
        String prefix = root(a) + "/";
        String rel = name.startsWith(prefix) ? name.substring(prefix.length()) : name;
        o.addProperty("path", rel);
        o.addProperty("id", rel.substring(rel.lastIndexOf('/') + 1));
        o.add("data", TV.plain(d.has("fields") ? d.getAsJsonObject("fields") : null));
        o.add("createTime", d.get("createTime"));
        o.add("updateTime", d.get("updateTime"));
        return o;
    }

    private static String fieldPath(String k) {
        return k.matches("[A-Za-z_][A-Za-z0-9_]*") ? k : "`" + k.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    private static JsonObject fieldsDoc(JsonObject plain) {
        JsonObject d = new JsonObject();
        d.add("fields", TV.fields(plain));
        return d;
    }

    private static JsonObject dataArg(JsonObject a) {
        if (!a.has("data") || !a.get("data").isJsonObject()) {
            throw new IllegalArgumentException("data must be an object");
        }
        return a.getAsJsonObject("data");
    }

    /** parent (relative path of the containing document, "" for the root) and collection id of a collection path. */
    private static String[] split(String collection) {
        String c = collection.replaceAll("^/+|/+$", "");
        int i = c.lastIndexOf('/');
        return i < 0 ? new String[] {"", c} : new String[] {c.substring(0, i), c.substring(i + 1)};
    }

    private JsonObject structured(JsonObject a, boolean forCount) {
        String[] pc = split(requireString(a, "collection"));
        JsonObject sq = new JsonObject();
        JsonObject from = new JsonObject();
        from.addProperty("collectionId", pc[1]);
        if (optBool(a, "allDescendants", false)) {
            from.addProperty("allDescendants", true);
        }
        JsonArray fromArr = new JsonArray();
        fromArr.add(from);
        sq.add("from", fromArr);
        if (a.has("where") && a.get("where").isJsonArray() && !a.getAsJsonArray("where").isEmpty()) {
            JsonArray filters = new JsonArray();
            for (JsonElement e : a.getAsJsonArray("where")) {
                JsonObject w = e.getAsJsonObject();
                String op = OPS.get(requireString(w, "op"));
                if (op == null) {
                    throw new IllegalArgumentException("unknown operator \"" + w.get("op").getAsString() + "\"; use " + OPS.keySet());
                }
                JsonObject ff = new JsonObject();
                JsonObject field = new JsonObject();
                field.addProperty("fieldPath", requireString(w, "field"));
                ff.add("field", field);
                ff.addProperty("op", op);
                ff.add("value", TV.value(w.get("value")));
                JsonObject wrap = new JsonObject();
                wrap.add("fieldFilter", ff);
                filters.add(wrap);
            }
            JsonObject where = new JsonObject();
            if (filters.size() == 1) {
                where = filters.get(0).getAsJsonObject();
            } else {
                JsonObject comp = new JsonObject();
                comp.addProperty("op", "AND");
                comp.add("filters", filters);
                where.add("compositeFilter", comp);
            }
            sq.add("where", where);
        }
        if (!forCount) {
            if (a.has("orderBy") && a.get("orderBy").isJsonArray()) {
                JsonArray ob = new JsonArray();
                for (JsonElement e : a.getAsJsonArray("orderBy")) {
                    JsonObject o = e.getAsJsonObject();
                    JsonObject x = new JsonObject();
                    JsonObject field = new JsonObject();
                    field.addProperty("fieldPath", requireString(o, "field"));
                    x.add("field", field);
                    x.addProperty("direction", optString(o, "direction") == null ? "ASCENDING" : optString(o, "direction").toUpperCase(java.util.Locale.ROOT));
                    ob.add(x);
                }
                sq.add("orderBy", ob);
            }
            sq.addProperty("limit", limit(a, "limit", 100, MAX_DOCS));
        }
        return sq;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        switch (tool) {
            case "firestore_list_collections": {
                String parent = optString(a, "documentPath") == null ? "" : optString(a, "documentPath");
                JsonObject res = send("POST", url(a, parent) + ":listCollectionIds", null, new JsonObject()).getAsJsonObject();
                JsonObject o = new JsonObject();
                o.add("collectionIds", res.has("collectionIds") ? res.get("collectionIds") : new JsonArray());
                return json(o);
            }
            case "firestore_list_documents": {
                String q = "pageSize=" + limit(a, "pageSize", 100, MAX_DOCS)
                        + (optString(a, "pageToken") == null ? "" : "&pageToken=" + enc(optString(a, "pageToken")))
                        + (optString(a, "orderBy") == null ? "" : "&orderBy=" + enc(optString(a, "orderBy")));
                JsonObject res = send("GET", url(a, requireString(a, "collection").replaceAll("^/+|/+$", "")), q, null).getAsJsonObject();
                JsonArray docs = new JsonArray();
                if (res.has("documents")) {
                    res.getAsJsonArray("documents").forEach(d -> docs.add(doc(a, d.getAsJsonObject())));
                }
                JsonObject o = new JsonObject();
                o.add("documents", docs);
                o.add("nextPageToken", res.has("nextPageToken") ? res.get("nextPageToken") : JsonNull.INSTANCE);
                return json(o);
            }
            case "firestore_get_documents": {
                JsonArray names = new JsonArray();
                if (!a.has("paths") || !a.get("paths").isJsonArray()) {
                    throw new IllegalArgumentException("paths must be an array of document paths");
                }
                a.getAsJsonArray("paths").forEach(p -> names.add(root(a) + "/" + p.getAsString().replaceAll("^/+", "")));
                JsonObject body = new JsonObject();
                body.add("documents", names);
                JsonArray res = send("POST", rootUrl(a).replace("/documents", "") + "/documents:batchGet", null, body).getAsJsonArray();
                JsonArray found = new JsonArray();
                JsonArray missing = new JsonArray();
                for (JsonElement e : res) {
                    JsonObject r = e.getAsJsonObject();
                    if (r.has("found")) {
                        found.add(doc(a, r.getAsJsonObject("found")));
                    } else if (r.has("missing")) {
                        String m = r.get("missing").getAsString();
                        String prefix = root(a) + "/";
                        missing.add(m.startsWith(prefix) ? m.substring(prefix.length()) : m);
                    }
                }
                JsonObject o = new JsonObject();
                o.add("documents", found);
                o.add("missing", missing);
                return json(o);
            }
            case "firestore_add_document": {
                String coll = requireString(a, "collection").replaceAll("^/+|/+$", "");
                JsonObject d = send("POST", url(a, coll), optString(a, "documentId") == null ? null : "documentId=" + enc(optString(a, "documentId")),
                        fieldsDoc(dataArg(a))).getAsJsonObject();
                return json(doc(a, d));
            }
            case "firestore_set_document": {
                String p = requireString(a, "path").replaceAll("^/+|/+$", "");
                JsonObject data = dataArg(a);
                StringBuilder q = new StringBuilder();
                if (optBool(a, "merge", false)) {
                    for (String k : data.keySet()) {
                        q.append(q.length() == 0 ? "" : "&").append("updateMask.fieldPaths=").append(enc(fieldPath(k)));
                    }
                }
                JsonObject d = send("PATCH", url(a, p), q.length() == 0 ? null : q.toString(), fieldsDoc(data)).getAsJsonObject();
                return json(doc(a, d));
            }
            case "firestore_update_document": {
                String p = requireString(a, "path").replaceAll("^/+|/+$", "");
                JsonObject data = dataArg(a);
                StringBuilder q = new StringBuilder("currentDocument.exists=true");
                for (String k : data.keySet()) {
                    q.append("&updateMask.fieldPaths=").append(enc(fieldPath(k)));
                }
                JsonObject d = send("PATCH", url(a, p), q.toString(), fieldsDoc(data)).getAsJsonObject();
                return json(doc(a, d));
            }
            case "firestore_delete_document": {
                send("DELETE", url(a, requireString(a, "path").replaceAll("^/+|/+$", "")), null, null);
                JsonObject o = new JsonObject();
                o.addProperty("ok", true);
                return json(o);
            }
            case "firestore_query_collection", "firestore_run_query": {
                boolean raw = tool.equals("firestore_run_query");
                String parentRel = raw ? (optString(a, "parent") == null ? "" : optString(a, "parent").replaceAll("^/+|/+$", ""))
                        : split(requireString(a, "collection"))[0];
                JsonObject body = new JsonObject();
                if (raw) {
                    if (!a.has("structuredQuery") || !a.get("structuredQuery").isJsonObject()) {
                        throw new IllegalArgumentException("structuredQuery must be an object");
                    }
                    JsonObject sq = a.getAsJsonObject("structuredQuery").deepCopy();
                    if (!sq.has("limit")) {
                        sq.addProperty("limit", MAX_DOCS);
                    }
                    body.add("structuredQuery", sq);
                } else {
                    body.add("structuredQuery", structured(a, false));
                }
                JsonArray res = send("POST", url(a, parentRel) + ":runQuery", null, body).getAsJsonArray();
                JsonArray docs = new JsonArray();
                for (JsonElement e : res) {
                    if (e.getAsJsonObject().has("document")) {
                        docs.add(doc(a, e.getAsJsonObject().getAsJsonObject("document")));
                    }
                }
                JsonObject o = new JsonObject();
                o.add("documents", docs);
                o.addProperty("count", docs.size());
                return json(o);
            }
            case "firestore_count": {
                JsonObject body = new JsonObject();
                JsonObject agg = new JsonObject();
                agg.add("structuredQuery", structured(a, true));
                JsonObject count = new JsonObject();
                count.add("count", new JsonObject());
                count.addProperty("alias", "n");
                JsonArray aggs = new JsonArray();
                aggs.add(count);
                agg.add("aggregations", aggs);
                body.add("structuredAggregationQuery", agg);
                JsonArray res = send("POST", url(a, split(requireString(a, "collection"))[0]) + ":runAggregationQuery", null, body).getAsJsonArray();
                long n = 0;
                for (JsonElement e : res) {
                    JsonObject r = e.getAsJsonObject();
                    if (r.has("result") && r.getAsJsonObject("result").has("aggregateFields")) {
                        n = Long.parseLong(r.getAsJsonObject("result").getAsJsonObject("aggregateFields").getAsJsonObject("n").get("integerValue").getAsString());
                    }
                }
                JsonObject o = new JsonObject();
                o.addProperty("count", n);
                return json(o);
            }
            default:
                return Outcome.error("unknown firestore tool: " + tool);
        }
    }
}
