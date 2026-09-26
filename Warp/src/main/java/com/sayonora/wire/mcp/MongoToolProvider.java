package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.AdHocQueryRunner;
import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.mongowire.MongoWireEmbedded;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * MongoDB vocabulary (tool and argument names follow the official mongodb-mcp-server: list-databases,
 * list-collections, find, aggregate, count, insert-many, update-many, delete-many,
 * collection-schema). Runs through mongowire's own command dispatcher against the same
 * {@code "<db>"."<collection>"(id, doc jsonb)} tables, so documents written by pymongo are visible
 * here and vice versa. {@code connectionId} (from the official connect tool) is accepted and ignored:
 * there is exactly one connection, this gateway's store.
 */
final class MongoToolProvider implements BackendToolProvider {

    private static final int MAX_DOCS = 1000;
    private static final Map<String, String> UNSUPPORTED = Map.ofEntries(
            Map.entry("create-collection", "collections are created implicitly by the first insert-many"),
            Map.entry("drop-collection", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("drop-database", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("rename-collection", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("create-index", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("drop-index", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("collection-indexes", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("explain", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("db-stats", "not exposed by this gateway (use a MongoDB client against mongowire)"),
            Map.entry("collection-storage-size", "not exposed by this gateway (use a MongoDB client against mongowire)"));

    private static final String EMULATED_LIST_DATABASES_SQL = "SELECT DISTINCT c.table_schema FROM information_schema.columns c "
            + "JOIN information_schema.columns d ON d.table_schema = c.table_schema "
            + "AND d.table_name = c.table_name AND d.column_name = 'doc' AND d.data_type = 'jsonb' "
            + "WHERE c.column_name = 'id' AND c.data_type = 'text' "
            + "AND c.table_schema NOT IN ('public', 'pg_catalog', 'information_schema') ORDER BY 1";

    private final EmulatedStores stores;
    private final ExternalClients clients;

    MongoToolProvider(EmulatedStores stores, ExternalClients clients) {
        this.stores = stores;
        this.clients = clients;
    }

    @Override
    public BackendKind kind() {
        return BackendKind.MONGODB;
    }

    @Override
    public List<Tool> tools() {
        JsonObject db = str("Database name");
        JsonObject coll = str("Collection name");
        JsonObject filter = obj("Query filter (MongoDB Extended JSON)");
        return List.of(
                new Tool("list-databases", "List all databases.", schema(List.of()), false),
                new Tool("list-collections", "List all collections in a database.",
                        schema(List.of("database"), "database", db), false),
                new Tool("find", "Run a find query against a collection.",
                        schema(List.of("database", "collection"), "database", db, "collection", coll,
                                "filter", filter, "projection", obj("Fields to include/exclude"),
                                "sort", obj("Sort spec, e.g. {\"age\": -1}"), "limit", num("Max documents (default 10)"),
                                "skip", num("Documents to skip")), false),
                new Tool("aggregate", "Run an aggregation pipeline (all stages and expression operators mongowire supports).",
                        schema(List.of("database", "collection", "pipeline"), "database", db, "collection", coll,
                                "pipeline", arr("Aggregation stages")), false),
                new Tool("count", "Count documents matching a filter.",
                        schema(List.of("database", "collection"), "database", db, "collection", coll,
                                "query", filter), false),
                new Tool("collection-schema", "Describe a collection's schema, inferred by sampling documents.",
                        schema(List.of("database", "collection"), "database", db, "collection", coll), false),
                new Tool("insert-many", "Insert an array of documents into a collection.",
                        schema(List.of("database", "collection", "documents"), "database", db, "collection", coll,
                                "documents", arr("Documents to insert")), true),
                new Tool("update-many", "Update all documents matching a filter.",
                        schema(List.of("database", "collection", "update"), "database", db, "collection", coll,
                                "filter", filter, "update", obj("Update operators, e.g. {\"$set\": {...}}")), true),
                new Tool("delete-many", "Delete all documents matching a filter.",
                        schema(List.of("database", "collection"), "database", db, "collection", coll,
                                "filter", filter), true));
    }

    /** Runs one MongoDB command document ({@code $db} = database) and returns the reply document:
     * mongowire's dispatcher for the emulated store, a real driver connection for external backends. */
    interface MongoRunner {
        BsonDocument run(BsonDocument command);
    }

    private MongoRunner runnerFor(Ctx ctx) {
        if (ctx.backend().emulated()) {
            MongoWireEmbedded mongo = stores.mongo();
            if (mongo == null) {
                throw new IllegalStateException("mongowire is not running on this gateway, so there is no "
                        + "MongoDB-shaped store to serve this tool");
            }
            return mongo::run;
        }
        BackendTarget target = ctx.backend().target();
        com.mongodb.client.MongoClient client = clients.mongo(target);
        String pinned = ExternalClients.operand(target).get("database") instanceof String d && !d.isBlank() ? d : null;
        return command -> {
            BsonDocument cmd = command.clone();
            String db = cmd.containsKey("$db") ? cmd.getString("$db").getValue() : "admin";
            cmd.remove("$db");
            if (pinned != null && !pinned.equals(db) && !"admin".equals(db)) {
                BsonDocument err = new BsonDocument();
                err.put("ok", new org.bson.BsonDouble(0));
                err.put("errmsg", new BsonString("database \"" + db + "\" is outside this backend's configured "
                        + "database \"" + pinned + "\""));
                err.put("codeName", new BsonString("Unauthorized (42501)"));
                return err;
            }
            // real servers return a first batch of 101 documents unless told otherwise
            if (cmd.containsKey("find") && !cmd.containsKey("batchSize")) {
                cmd.put("batchSize", new BsonInt32(MAX_DOCS));
            }
            if (cmd.containsKey("aggregate") && cmd.containsKey("cursor") && cmd.get("cursor").isDocument()
                    && !cmd.getDocument("cursor").containsKey("batchSize")) {
                cmd.getDocument("cursor").put("batchSize", new BsonInt32(MAX_DOCS));
            }
            try {
                return client.getDatabase(db).runCommand(cmd, BsonDocument.class);
            } catch (com.mongodb.MongoCommandException e) {
                BsonDocument err = new BsonDocument();
                err.put("ok", new org.bson.BsonDouble(0));
                err.put("errmsg", new BsonString(e.getErrorMessage()));
                err.put("codeName", new BsonString(e.getErrorCodeName()));
                return err;
            } catch (com.mongodb.MongoException e) {
                throw new MongoToolException("MongoError: " + e.getMessage());
            }
        };
    }

    @Override
    public JsonObject describe(Ctx ctx) {
        MongoRunner mongo = runnerFor(ctx);
        JsonArray dbs = new JsonArray();
        for (String db : databases(mongo, ctx)) {
            JsonObject d = new JsonObject();
            d.addProperty("name", db);
            JsonArray cols = new JsonArray();
            BsonDocument cmd = new BsonDocument();
            cmd.put("listCollections", new BsonInt32(1));
            cmd.put("$db", new BsonString(db));
            for (BsonValue v : check(mongo.run(cmd)).getDocument("cursor").getArray("firstBatch")) {
                cols.add(v.asDocument().getString("name").getValue());
            }
            d.add("collections", cols);
            dbs.add(d);
        }
        JsonObject out = new JsonObject();
        out.add("databases", dbs);
        return out;
    }

    private List<String> databases(MongoRunner mongo, Ctx ctx) {
        List<String> names = new ArrayList<>();
        if (!ctx.backend().emulated()) {
            BsonDocument cmd = new BsonDocument();
            cmd.put("listDatabases", new BsonInt32(1));
            cmd.put("nameOnly", org.bson.BsonBoolean.TRUE);
            cmd.put("$db", new BsonString("admin"));
            for (BsonValue v : check(mongo.run(cmd)).getArray("databases")) {
                String n = v.asDocument().getString("name").getValue();
                if (!List.of("admin", "config", "local").contains(n)) {
                    names.add(n);
                }
            }
            Object pinned = ExternalClients.operand(ctx.backend().target()).get("database");
            if (pinned instanceof String p && !p.isBlank()) {
                names.removeIf(n -> !n.equals(p));
                if (names.isEmpty()) {
                    names.add(p);
                }
            }
        } else if (ctx.backend().emulated()) {
            AdHocQueryRunner.Result r = ctx.sql(EMULATED_LIST_DATABASES_SQL);
            if (!r.success()) {
                throw new MongoToolException("ERROR [" + r.sqlState() + "]: " + r.error());
            }
            r.rows().forEach(row -> names.add(String.valueOf(row.get(0))));
        }
        return names;
    }

    @Override
    public Outcome call(String tool, JsonObject a, Ctx ctx) throws Exception {
        if (UNSUPPORTED.containsKey(tool)) {
            return Outcome.error("UnsupportedOperation: " + tool + " -- " + UNSUPPORTED.get(tool));
        }
        MongoRunner mongo;
        try {
            mongo = runnerFor(ctx);
        } catch (IllegalStateException e) {
            return Outcome.error(e.getMessage());
        }
        try {
            switch (tool) {
                case "list-databases" -> {
                    JsonArray arr = new JsonArray();
                    databases(mongo, ctx).forEach(n -> {
                        JsonObject o = new JsonObject();
                        o.addProperty("name", n);
                        arr.add(o);
                    });
                    return Outcome.ok("Found " + arr.size() + " databases", arr.toString());
                }
                case "list-collections" -> {
                    BsonDocument reply = run(mongo, "listCollections", 1, a);
                    JsonArray arr = new JsonArray();
                    for (BsonValue v : reply.getDocument("cursor").getArray("firstBatch")) {
                        JsonObject o = new JsonObject();
                        o.addProperty("name", v.asDocument().getString("name").getValue());
                        arr.add(o);
                    }
                    return Outcome.ok("Found " + arr.size() + " collections", arr.toString());
                }
                case "find" -> {
                    return find(mongo, a);
                }
                case "aggregate" -> {
                    BsonDocument cmd = base("aggregate", a);
                    cmd.put("pipeline", BsonArray.parse(requireJson(a, "pipeline")));
                    cmd.put("cursor", new BsonDocument());
                    BsonDocument reply = check(mongo.run(cmd));
                    BsonArray batch = reply.getDocument("cursor").getArray("firstBatch");
                    return Outcome.ok("The aggregation resulted in " + batch.size() + " documents", ejsonArray(batch));
                }
                case "count" -> {
                    BsonDocument cmd = base("count", a);
                    String f = optJson(a, "query", "filter");
                    cmd.put("query", f == null ? new BsonDocument() : BsonDocument.parse(f));
                    BsonDocument reply = check(mongo.run(cmd));
                    return Outcome.ok("Found " + reply.getNumber("n").longValue() + " documents in the collection \""
                            + requireString(a, "collection") + "\"");
                }
                case "collection-schema" -> {
                    return schemaOf(mongo, a, ctx);
                }
                case "insert-many" -> {
                    BsonDocument cmd = base("insert", a);
                    BsonArray docs = BsonArray.parse(requireJson(a, "documents"));
                    cmd.put("documents", docs);
                    BsonDocument reply = check(mongo.run(cmd));
                    if (reply.containsKey("writeErrors")) {
                        return Outcome.error("insert-many failed for some documents: "
                                + reply.getArray("writeErrors").toString());
                    }
                    return Outcome.ok("Inserted " + reply.getNumber("n").intValue() + " document(s) into collection \""
                            + requireString(a, "collection") + "\"");
                }
                case "update-many" -> {
                    BsonDocument cmd = base("update", a);
                    BsonDocument spec = new BsonDocument();
                    String f = optJson(a, "filter", "query");
                    spec.put("q", f == null ? new BsonDocument() : BsonDocument.parse(f));
                    spec.put("u", BsonDocument.parse(requireJson(a, "update")));
                    spec.put("multi", org.bson.BsonBoolean.TRUE);
                    cmd.put("updates", new BsonArray(List.of(spec)));
                    BsonDocument reply = check(mongo.run(cmd));
                    return Outcome.ok("Matched " + reply.getNumber("n").intValue() + " document(s) and modified "
                            + reply.getNumber("nModified").intValue());
                }
                case "delete-many" -> {
                    BsonDocument cmd = base("delete", a);
                    BsonDocument spec = new BsonDocument();
                    String f = optJson(a, "filter", "query");
                    spec.put("q", f == null ? new BsonDocument() : BsonDocument.parse(f));
                    spec.put("limit", new BsonInt32(0));
                    cmd.put("deletes", new BsonArray(List.of(spec)));
                    BsonDocument reply = check(mongo.run(cmd));
                    return Outcome.ok("Deleted " + reply.getNumber("n").intValue() + " document(s)");
                }
                default -> {
                    return Outcome.error("unknown MongoDB tool: " + tool);
                }
            }
        } catch (MongoToolException e) {
            return Outcome.error(e.getMessage());
        } catch (IllegalArgumentException | org.bson.json.JsonParseException e) {
            return Outcome.error("invalid arguments: " + e.getMessage());
        }
    }

    private Outcome find(MongoRunner mongo, JsonObject a) {
        BsonDocument cmd = base("find", a);
        String f = optJson(a, "filter", null);
        cmd.put("filter", f == null ? new BsonDocument() : BsonDocument.parse(f));
        String sortJson = optJson(a, "sort", null);
        int skip = optInt(a, "skip") == null ? 0 : optInt(a, "skip");
        int limit = optInt(a, "limit") == null ? 10 : optInt(a, "limit");
        limit = Math.min(limit, MAX_DOCS);
        // Sorting/skipping happen here (the dispatcher applies the filter and limit only), so
        // fetch unbounded (capped) and then sort/skip/limit; without a sort the limit is pushed down.
        BsonDocument sort = sortJson == null ? null : BsonDocument.parse(sortJson);
        if (sort == null && skip == 0) {
            cmd.put("limit", new BsonInt32(limit));
        }
        BsonDocument reply = check(mongo.run(cmd));
        List<BsonDocument> docs = new ArrayList<>();
        reply.getDocument("cursor").getArray("firstBatch").forEach(v -> docs.add(v.asDocument()));
        if (sort != null) {
            Comparator<BsonDocument> cmp = (x, y) -> 0;
            for (Map.Entry<String, BsonValue> e : sort.entrySet()) {
                int dir = e.getValue().isNumber() && e.getValue().asNumber().intValue() < 0 ? -1 : 1;
                String field = e.getKey();
                Comparator<BsonDocument> c = (x, y) -> dir * compareValues(x.get(field), y.get(field));
                cmp = cmp.thenComparing(c);
            }
            docs.sort(cmp);
        }
        List<BsonDocument> page = docs.subList(Math.min(skip, docs.size()), docs.size());
        if (page.size() > limit) {
            page = page.subList(0, limit);
        }
        String projJson = optJson(a, "projection", null);
        BsonArray out = new BsonArray();
        for (BsonDocument d : page) {
            out.add(projJson == null ? d : project(d, BsonDocument.parse(projJson)));
        }
        return Outcome.ok("Found " + out.size() + " documents in the collection \"" + requireString(a, "collection")
                + "\"", ejsonArray(out));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(BsonValue x, BsonValue y) {
        if (x == null || y == null) {
            return x == y ? 0 : (x == null ? -1 : 1);
        }
        if (x.isNumber() && y.isNumber()) {
            return Double.compare(x.asNumber().doubleValue(), y.asNumber().doubleValue());
        }
        if (x.isString() && y.isString()) {
            return x.asString().getValue().compareTo(y.asString().getValue());
        }
        return x.toString().compareTo(y.toString());
    }

    private static BsonDocument project(BsonDocument d, BsonDocument proj) {
        boolean include = proj.values().stream().anyMatch(v -> v.isNumber() ? v.asNumber().intValue() != 0 : v.isBoolean() && v.asBoolean().getValue());
        BsonDocument out = new BsonDocument();
        for (Map.Entry<String, BsonValue> e : d.entrySet()) {
            BsonValue p = proj.get(e.getKey());
            boolean flagged = p != null && (p.isNumber() ? p.asNumber().intValue() != 0 : p.isBoolean() && p.asBoolean().getValue());
            boolean excluded = p != null && !flagged;
            if (include ? (flagged || (e.getKey().equals("_id") && !excluded)) : !excluded) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private Outcome schemaOf(MongoRunner mongo, JsonObject a, Ctx ctx) {
        String db = requireIdent(a, "database");
        String coll = requireIdent(a, "collection");
        List<JsonElement> docs = new ArrayList<>();
        if (ctx.backend().emulated()) {
            AdHocQueryRunner.Result r = ctx.sql("SELECT doc FROM " + MongoWireEmbedded.physicalTable(db, coll) + " LIMIT 1000");
            if (!r.success()) {
                return sqlError(r);
            }
            for (List<Object> row : r.rows()) {
                docs.add(cell(row.get(0)));
            }
        } else {
            BsonDocument cmd = base("find", a);
            cmd.put("filter", new BsonDocument());
            cmd.put("limit", new BsonInt32(1000));
            for (BsonValue v : check(mongo.run(cmd)).getDocument("cursor").getArray("firstBatch")) {
                docs.add(JsonParser.parseString(v.asDocument().toJson(org.bson.json.JsonWriterSettings.builder()
                        .outputMode(org.bson.json.JsonMode.RELAXED).build())));
            }
        }
        Map<String, Set<String>> types = new LinkedHashMap<>();
        for (JsonElement doc : docs) {
            if (doc.isJsonObject()) {
                collect("", doc.getAsJsonObject(), types, 0);
            }
        }
        JsonObject fields = new JsonObject();
        types.forEach((k, v) -> {
            JsonObject f = new JsonObject();
            JsonArray t = new JsonArray();
            v.forEach(t::add);
            f.add("types", t);
            fields.add(k, f);
        });
        JsonObject out = new JsonObject();
        out.addProperty("sampledDocuments", docs.size());
        out.add("fields", fields);
        return Outcome.ok("Found " + types.size() + " fields in the schema for \"" + db + "." + coll + "\"",
                out.toString());
    }

    private static void collect(String prefix, JsonObject o, Map<String, Set<String>> types, int depth) {
        for (Map.Entry<String, JsonElement> e : o.entrySet()) {
            String path = prefix + e.getKey();
            JsonElement v = e.getValue();
            String type = v.isJsonNull() ? "null" : v.isJsonArray() ? "array" : v.isJsonObject() ? "object"
                    : v.getAsJsonPrimitive().isBoolean() ? "boolean" : v.getAsJsonPrimitive().isNumber() ? "number"
                    : "string";
            types.computeIfAbsent(path, k -> new LinkedHashSet<>()).add(type);
            if (v.isJsonObject() && depth < 3) {
                collect(path + ".", v.getAsJsonObject(), types, depth + 1);
            }
        }
    }

    // ---- helpers ----

    private static String requireIdent(JsonObject a, String key) {
        String v = requireString(a, key);
        if (v.isBlank() || v.contains("\"") || v.contains("\0")) {
            throw new IllegalArgumentException(key + " contains illegal characters");
        }
        return v;
    }

    private static BsonDocument base(String command, JsonObject a) {
        BsonDocument cmd = new BsonDocument();
        cmd.put(command, new BsonString(requireIdent(a, "collection")));
        cmd.put("$db", new BsonString(requireIdent(a, "database")));
        return cmd;
    }

    private static BsonDocument run(MongoRunner mongo, String command, int value, JsonObject a) {
        BsonDocument cmd = new BsonDocument();
        cmd.put(command, new BsonInt32(value));
        cmd.put("$db", new BsonString(requireIdent(a, "database")));
        return check(mongo.run(cmd));
    }

    private static BsonDocument check(BsonDocument reply) {
        if (reply.containsKey("ok") && reply.getNumber("ok").doubleValue() == 0.0) {
            String msg = reply.containsKey("errmsg") ? reply.getString("errmsg").getValue() : "command failed";
            String code = reply.containsKey("codeName") ? reply.getString("codeName").getValue() : "MongoError";
            throw new MongoToolException(code + ": " + msg);
        }
        return reply;
    }

    private static String requireJson(JsonObject a, String key) {
        if (!a.has(key) || a.get(key).isJsonNull()) {
            throw new IllegalArgumentException("missing required argument: " + key);
        }
        JsonElement e = a.get(key);
        return e.isJsonPrimitive() ? e.getAsString() : e.toString();
    }

    private static String optJson(JsonObject a, String key, String alias) {
        String k = a.has(key) && !a.get(key).isJsonNull() ? key : (alias != null && a.has(alias) && !a.get(alias).isJsonNull() ? alias : null);
        return k == null ? null : requireJson(a, k);
    }

    private static String ejsonArray(BsonArray arr) {
        JsonArray out = new JsonArray();
        for (BsonValue v : arr) {
            out.add(JsonParser.parseString(v.asDocument().toJson(org.bson.json.JsonWriterSettings.builder()
                    .outputMode(org.bson.json.JsonMode.RELAXED).build())));
        }
        return out.toString();
    }

    private static Outcome sqlError(AdHocQueryRunner.Result r) {
        return Outcome.error("ERROR [" + r.sqlState() + "]: " + r.error());
    }

    private static final class MongoToolException extends RuntimeException {
        MongoToolException(String m) {
            super(m);
        }
    }
}
