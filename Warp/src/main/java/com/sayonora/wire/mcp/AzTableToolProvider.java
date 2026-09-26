package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.AzureToolSupport.*;
import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.azurewire.AzureEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Azure Table Storage vocabulary (aztable_* tools mirroring the table and entity operations of Azure's tooling: list, create
 * and delete tables; insert, get, query, upsert (replace or merge) and delete entities with OData {@code $filter}/{@code $select}/
 * {@code $top} and continuation tokens). Each tool sends the equivalent Table REST request (OData JSON, no metadata) to
 * azurewire's own {@code TableService} in process.
 */
final class AzTableToolProvider extends StoreToolProvider {

    private static final Map<String, String> JSON = Map.of("Accept", "application/json;odata=nometadata",
            "Content-Type", "application/json;odata=nometadata");

    AzTableToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.AZTABLE, describer, stores);
    }

    private AzureEmbedded az() {
        return stores.engine("aztable", AzureEmbedded::table);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject acct = str("Storage account (default: the only configured account)");
        JsonObject table = str("Table name");
        JsonObject pk = str("PartitionKey");
        JsonObject rk = str("RowKey");
        JsonObject entity = obj("Entity properties (plain JSON values; PartitionKey and RowKey may be given here or as arguments)");
        return List.of(
                new Tool("aztable_list_tables", "List the tables of a storage account.",
                        schema(List.of(), "account", acct, "top", num("Max tables (default 100, max 1000)"),
                                "nextTableName", str("nextTableName of a truncated listing")), false),
                new Tool("aztable_create_table", "Create a table.", schema(List.of("table"), "account", acct, "table", table), true),
                new Tool("aztable_delete_table", "Delete a table and its entities.", schema(List.of("table"), "account", acct, "table", table), true),
                new Tool("aztable_insert_entity", "Insert a new entity (fails when PartitionKey+RowKey already exist).",
                        schema(List.of("table", "entity"), "account", acct, "table", table, "partitionKey", pk, "rowKey", rk,
                                "entity", entity), true),
                new Tool("aztable_upsert_entity", "Insert or update an entity: mode replace (default) overwrites all properties, merge keeps the others.",
                        schema(List.of("table", "entity"), "account", acct, "table", table, "partitionKey", pk, "rowKey", rk,
                                "entity", entity, "mode", str("replace (default) or merge")), true),
                new Tool("aztable_get_entity", "Read one entity by PartitionKey and RowKey.",
                        schema(List.of("table", "partitionKey", "rowKey"), "account", acct, "table", table, "partitionKey", pk, "rowKey", rk), false),
                new Tool("aztable_query_entities", "Query entities with an OData filter (e.g. \"PartitionKey eq 'a' and Age gt 3\").",
                        schema(List.of("table"), "account", acct, "table", table, "filter", str("OData $filter"),
                                "select", str("Comma separated property names"), "top", num("Max entities (default 100, max 1000)"),
                                "nextPartitionKey", str("continuation.nextPartitionKey of the previous page"),
                                "nextRowKey", str("continuation.nextRowKey of the previous page")), false),
                new Tool("aztable_delete_entity", "Delete one entity.", schema(List.of("table", "partitionKey", "rowKey"),
                        "account", acct, "table", table, "partitionKey", pk, "rowKey", rk), true));
    }

    private static String key(String v) {
        return "'" + enc(v.replace("'", "''")) + "'";
    }

    private static String entityPath(String table, String pk, String rk) {
        return "/" + enc(table) + "(PartitionKey=" + key(pk) + ",RowKey=" + key(rk) + ")";
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        String account = account(a);
        switch (tool) {
            case "aztable_list_tables": {
                AzureEmbedded.Response r = az().request(account, "GET", "/Tables", queryOf(List.of(
                        q("$top", String.valueOf(limit(a, "top", 100, 1000))), q("NextTableName", optString(a, "nextTableName")))), JSON, null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.add("tables", JsonParser.parseString(r.text()).getAsJsonObject().get("value"));
                o.addProperty("nextTableName", r.headers().get("x-ms-continuation-NextTableName"));
                return json(o);
            }
            case "aztable_create_table": {
                JsonObject b = new JsonObject();
                b.addProperty("TableName", requireString(a, "table"));
                AzureEmbedded.Response r = az().request(account, "POST", "/Tables", null, JSON, b.toString().getBytes(StandardCharsets.UTF_8));
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            case "aztable_delete_table": {
                AzureEmbedded.Response r = az().request(account, "DELETE", "/Tables(" + key(requireString(a, "table")) + ")", null, JSON, null);
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            case "aztable_insert_entity": {
                JsonObject e = entityBody(a);
                AzureEmbedded.Response r = az().request(account, "POST", "/" + enc(requireString(a, "table")), null,
                        with(JSON, "Prefer", "return-no-content"), e.toString().getBytes(StandardCharsets.UTF_8));
                return r.status() >= 300 ? failure(r) : json(entityAnswer(e, r));
            }
            case "aztable_upsert_entity": {
                JsonObject e = entityBody(a);
                boolean merge = "merge".equalsIgnoreCase(optString(a, "mode"));
                if (optString(a, "mode") != null && !merge && !"replace".equalsIgnoreCase(optString(a, "mode"))) {
                    throw new IllegalArgumentException("mode must be replace or merge");
                }
                AzureEmbedded.Response r = az().request(account, merge ? "MERGE" : "PUT",
                        entityPath(requireString(a, "table"), e.get("PartitionKey").getAsString(), e.get("RowKey").getAsString()), null,
                        JSON, e.toString().getBytes(StandardCharsets.UTF_8));
                return r.status() >= 300 ? failure(r) : json(entityAnswer(e, r));
            }
            case "aztable_get_entity": {
                AzureEmbedded.Response r = az().request(account, "GET", entityPath(requireString(a, "table"), requireString(a, "partitionKey"),
                        requireString(a, "rowKey")), null, JSON, null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.add("entity", JsonParser.parseString(r.text()));
                return json(o);
            }
            case "aztable_query_entities": {
                AzureEmbedded.Response r = az().request(account, "GET", "/" + enc(requireString(a, "table")) + "()", queryOf(List.of(
                        q("$filter", optString(a, "filter")), q("$select", optString(a, "select")),
                        q("$top", String.valueOf(limit(a, "top", 100, 1000))),
                        q("NextPartitionKey", optString(a, "nextPartitionKey")), q("NextRowKey", optString(a, "nextRowKey")))), JSON, null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                JsonElement v = JsonParser.parseString(r.text()).getAsJsonObject().get("value");
                o.add("entities", v);
                o.addProperty("count", v.getAsJsonArray().size());
                String npk = r.headers().get("x-ms-continuation-NextPartitionKey");
                if (npk != null) {
                    JsonObject c = new JsonObject();
                    c.addProperty("nextPartitionKey", npk);
                    c.addProperty("nextRowKey", r.headers().get("x-ms-continuation-NextRowKey"));
                    o.add("continuation", c);
                }
                return json(o);
            }
            case "aztable_delete_entity": {
                Map<String, String> h = new LinkedHashMap<>(JSON);
                h.put("If-Match", "*");
                AzureEmbedded.Response r = az().request(account, "DELETE", entityPath(requireString(a, "table"),
                        requireString(a, "partitionKey"), requireString(a, "rowKey")), null, h, null);
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            default:
                return Outcome.error("unknown aztable tool: " + tool);
        }
    }

    private static Map<String, String> with(Map<String, String> base, String k, String v) {
        Map<String, String> m = new LinkedHashMap<>(base);
        m.put(k, v);
        return m;
    }

    private static JsonObject entityBody(JsonObject a) {
        JsonObject e = a.has("entity") && a.get("entity").isJsonObject() ? a.getAsJsonObject("entity").deepCopy() : null;
        if (e == null) {
            throw new IllegalArgumentException("entity must be an object");
        }
        if (optString(a, "partitionKey") != null) {
            e.addProperty("PartitionKey", optString(a, "partitionKey"));
        }
        if (optString(a, "rowKey") != null) {
            e.addProperty("RowKey", optString(a, "rowKey"));
        }
        if (!e.has("PartitionKey") || !e.has("RowKey")) {
            throw new IllegalArgumentException("PartitionKey and RowKey are required (arguments partitionKey/rowKey or inside entity)");
        }
        return e;
    }

    private static JsonObject entityAnswer(JsonObject e, AzureEmbedded.Response r) {
        JsonObject o = new JsonObject();
        o.addProperty("partitionKey", e.get("PartitionKey").getAsString());
        o.addProperty("rowKey", e.get("RowKey").getAsString());
        o.addProperty("etag", r.headers().get("ETag"));
        o.addProperty("status", r.status());
        return o;
    }

    private static JsonObject ok(AzureEmbedded.Response r) {
        JsonObject o = new JsonObject();
        o.addProperty("ok", true);
        o.addProperty("status", r.status());
        return o;
    }
}
