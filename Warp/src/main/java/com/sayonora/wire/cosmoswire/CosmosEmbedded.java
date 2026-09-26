package com.sayonora.wire.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.cosmoswire.CosmosStore.Coll;
import com.sayonora.wire.cosmoswire.CosmosStore.DbRow;
import com.sayonora.wire.cosmoswire.CosmosStore.Doc;
import java.util.ArrayList;
import java.util.List;

/**
 * The in-process face of the Cosmos store for the MCP tools (and tests): the same tables, SQL engine and validation the REST frontend
 * uses, without a socket. Methods return JSON ready for a tool result and throw {@link IllegalArgumentException} for bad input.
 */
public final class CosmosEmbedded {

    private final CosmosStore store;
    private final CosmosService svc;

    public CosmosEmbedded(BackendRegistry registry) {
        this.store = new CosmosStore(registry);
        this.svc = new CosmosService(store, CosmosConfig.defaults());
    }

    private static <T> T wrap(java.util.function.Supplier<T> s) {
        try {
            return s.get();
        } catch (CosmosException e) {
            throw new IllegalArgumentException(e.code + ": " + e.getMessage());
        }
    }

    public JsonObject listDatabases() {
        return wrap(() -> {
            JsonArray a = new JsonArray();
            for (DbRow d : store.listDbs()) {
                JsonObject o = new JsonObject();
                o.addProperty("id", d.id());
                o.addProperty("containers", store.listColls(d.id()).size());
                a.add(o);
            }
            JsonObject out = new JsonObject();
            out.add("databases", a);
            return out;
        });
    }

    public JsonObject listContainers(String db) {
        return wrap(() -> {
            store.needDb(db);
            JsonArray a = new JsonArray();
            for (Coll c : store.listColls(db)) {
                JsonObject o = new JsonObject();
                o.addProperty("id", c.id());
                o.add("partitionKey", c.def().get("partitionKey"));
                if (c.defaultTtl() != null) {
                    o.addProperty("defaultTtl", c.defaultTtl());
                }
                long n = 0;
                for (String h : store.hosts()) {
                    n += store.count(h, c);
                }
                o.addProperty("items", n);
                a.add(o);
            }
            JsonObject out = new JsonObject();
            out.add("containers", a);
            return out;
        });
    }

    public JsonObject createDatabase(String id) {
        return wrap(() -> store.createDb(id, null).toJson());
    }

    public JsonObject createContainer(String db, String id, String partitionKeyPath) {
        return wrap(() -> {
            JsonObject b = new JsonObject();
            JsonObject pk = new JsonObject();
            JsonArray paths = new JsonArray();
            paths.add(partitionKeyPath == null ? "/id" : partitionKeyPath);
            pk.add("paths", paths);
            pk.addProperty("kind", "Hash");
            pk.addProperty("version", 2);
            b.add("partitionKey", pk);
            JsonObject def = new JsonObject();
            def.add("partitionKey", pk);
            def.add("indexingPolicy", com.google.gson.JsonParser.parseString("{\"indexingMode\":\"consistent\",\"automatic\":true,"
                    + "\"includedPaths\":[{\"path\":\"/*\"}],\"excludedPaths\":[{\"path\":\"/\\\"_etag\\\"/?\"}]}"));
            return store.createColl(db, id, def, null).toJson();
        });
    }

    private Coll coll(String db, String container) {
        return store.needColl(db, container);
    }

    /** Runs a SQL query. {@code partitionKey} (a JSON scalar or array of scalars) limits it to one logical partition. */
    public JsonObject query(String db, String container, String sql, JsonArray parameters, JsonElement partitionKey, int maxItems, String continuation) {
        return wrap(() -> {
            Coll c = coll(db, container);
            CosmosQuery q = CosmosQuery.compile(sql, parameters);
            CosmosService.QueryPage p = svc.query(c, q, header(partitionKey), Math.max(1, maxItems), continuation);
            JsonArray a = new JsonArray();
            p.rows().forEach(a::add);
            JsonObject out = new JsonObject();
            out.add("items", a);
            out.addProperty("count", a.size());
            if (p.continuation() != null) {
                out.addProperty("continuation", p.continuation());
            }
            out.addProperty("requestCharge", p.charge());
            return out;
        });
    }

    private static List<JsonElement> header(JsonElement pk) {
        if (pk == null) {
            return null;
        }
        List<JsonElement> out = new ArrayList<>();
        if (pk.isJsonArray()) {
            for (JsonElement e : pk.getAsJsonArray()) {
                out.add(e);
            }
        } else {
            out.add(pk);
        }
        return out.isEmpty() ? null : out;
    }

    public JsonObject getItem(String db, String container, String id, JsonElement partitionKey) {
        return wrap(() -> {
            Coll c = coll(db, container);
            Doc d = svc.read(c, header(partitionKey), id);
            if (d == null) {
                throw CosmosStore.notFound();
            }
            return d.toJson(c, false);
        });
    }

    public JsonObject upsertItem(String db, String container, JsonObject item, JsonElement partitionKey) {
        return wrap(() -> {
            Coll c = coll(db, container);
            CosmosService.ItemResult r = svc.create(c, header(partitionKey), item, true, null);
            JsonObject out = new JsonObject();
            out.addProperty("status", r.status());
            out.add("item", r.json());
            return out;
        });
    }

    public JsonObject deleteItem(String db, String container, String id, JsonElement partitionKey) {
        return wrap(() -> {
            Coll c = coll(db, container);
            svc.delete(c, header(partitionKey), id, null);
            JsonObject out = new JsonObject();
            out.addProperty("deleted", id);
            return out;
        });
    }
}
