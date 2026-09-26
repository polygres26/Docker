package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.cosmoswire.CosmosEmbedded;
import java.util.List;

/**
 * Azure Cosmos DB for NoSQL vocabulary (cosmos_* tools: databases, containers, SQL queries, item get/upsert/delete) over the
 * Warp-hosted Cosmos store. Each tool uses the same tables, SQL engine and validation the cosmoswire REST frontend serves
 * ({@code CosmosEmbedded}). Writes (create database/container, upsert and delete item) are hidden under {@code WARP_MCP_READ_ONLY}.
 * Stored procedures, triggers and UDFs are not executed by cosmoswire and have no tools.
 */
final class CosmosToolProvider extends StoreToolProvider {

    private static final int MAX_ITEMS = 1000;

    CosmosToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.COSMOSSTORE, describer, stores);
    }

    private CosmosEmbedded cosmos() {
        return stores.engine("cosmos", CosmosEmbedded::new);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject db = str("Database id");
        JsonObject container = str("Container id");
        JsonObject pk = prop("string", "Partition key value: a JSON scalar or an array of scalars (hierarchical keys), e.g. \"alice\" or [\"a\",1]. "
                + "Omit for a cross-partition operation (queries only).");
        return List.of(
                new Tool("cosmos_list_databases", "List the databases with their container counts.", schema(List.of()), false),
                new Tool("cosmos_list_containers", "List the containers of a database with partition key, TTL and item count.",
                        schema(List.of("database"), "database", db), false),
                new Tool("cosmos_query", "Run a Cosmos DB SQL query (SELECT ... FROM c WHERE ... ORDER BY ...; parameters as [{name:\"@p\", value}]). "
                        + "Without partitionKey the query is cross-partition (scatter-gather over the backends).",
                        schema(List.of("database", "container", "query"), "database", db, "container", container, "query", str("SQL query text"),
                                "parameters", arr("[{name, value}]"), "partitionKey", pk, "maxItems", num("1-" + MAX_ITEMS + " (default 100)"),
                                "continuation", str("continuation of the previous page")), false),
                new Tool("cosmos_get_item", "Point-read an item by id and partition key.",
                        schema(List.of("database", "container", "id", "partitionKey"), "database", db, "container", container, "id", str("Item id"),
                                "partitionKey", pk), false),
                new Tool("cosmos_create_database", "Create a database.", schema(List.of("database"), "database", db), true),
                new Tool("cosmos_create_container", "Create a container with a partition key path (default /id).",
                        schema(List.of("database", "container"), "database", db, "container", container, "partitionKeyPath", str("e.g. /tenantId")), true),
                new Tool("cosmos_upsert_item", "Create or replace an item (a JSON object with an id).",
                        schema(List.of("database", "container", "item"), "database", db, "container", container, "item", obj("The item"),
                                "partitionKey", pk), true),
                new Tool("cosmos_delete_item", "Delete an item by id and partition key.",
                        schema(List.of("database", "container", "id", "partitionKey"), "database", db, "container", container, "id", str("Item id"),
                                "partitionKey", pk), true));
    }

    private static JsonElement pkOf(JsonObject a) {
        JsonElement e = a.get("partitionKey");
        if (e == null) {
            return null;
        }
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isString()) {
            String s = e.getAsString().trim();
            if (s.startsWith("[")) { // a JSON array given as text
                try {
                    return com.google.gson.JsonParser.parseString(s);
                } catch (RuntimeException ex) {
                    return e;
                }
            }
        }
        return e;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) {
        try {
            switch (tool) {
                case "cosmos_list_databases":
                    return json(cosmos().listDatabases());
                case "cosmos_list_containers":
                    return json(cosmos().listContainers(requireString(a, "database")));
                case "cosmos_query": {
                    JsonArray params = a.has("parameters") && a.get("parameters").isJsonArray() ? a.getAsJsonArray("parameters") : null;
                    return json(cosmos().query(requireString(a, "database"), requireString(a, "container"), requireString(a, "query"), params, pkOf(a),
                            limit(a, "maxItems", 100, MAX_ITEMS), optString(a, "continuation")));
                }
                case "cosmos_get_item":
                    return json(cosmos().getItem(requireString(a, "database"), requireString(a, "container"), requireString(a, "id"), pkOf(a)));
                case "cosmos_create_database":
                    return json(cosmos().createDatabase(requireString(a, "database")));
                case "cosmos_create_container":
                    return json(cosmos().createContainer(requireString(a, "database"), requireString(a, "container"), optString(a, "partitionKeyPath")));
                case "cosmos_upsert_item": {
                    if (!a.has("item") || !a.get("item").isJsonObject()) {
                        throw new IllegalArgumentException("missing required argument: item (a JSON object)");
                    }
                    return json(cosmos().upsertItem(requireString(a, "database"), requireString(a, "container"), a.getAsJsonObject("item"), pkOf(a)));
                }
                case "cosmos_delete_item":
                    return json(cosmos().deleteItem(requireString(a, "database"), requireString(a, "container"), requireString(a, "id"), pkOf(a)));
                default:
                    return Outcome.error("unknown cosmos tool: " + tool);
            }
        } catch (IllegalArgumentException e) {
            return Outcome.error(e.getMessage());
        }
    }
}
