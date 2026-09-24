package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.wire.dynamowire.DynamoException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DynamoDB vocabulary (tool and argument names follow the community DynamoDB MCP server,
 * imankamyabi/dynamodb-mcp-server -- the awslabs server is modelling-only and has no data-plane
 * tools). Every tool is translated into the real DynamoDB API request and run through dynamowire's
 * own {@code OperationHandlers} against the same Postgres item tables, so items written with boto3
 * are visible here and vice versa; results are DynamoDB's own typed-attribute JSON.
 */
final class DynamoDbToolProvider implements BackendToolProvider {

    private static final Gson GSON = new Gson();
    private static final Set<String> TYPE_TAGS = Set.of("S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS");
    private static final Map<String, String> UNSUPPORTED = Map.of(
            "create_gsi", "dynamowire has no global secondary indexes",
            "update_gsi", "dynamowire has no global secondary indexes",
            "create_lsi", "dynamowire has no local secondary indexes",
            "update_capacity", "dynamowire has no provisioned capacity (storage is Postgres)");

    private final EmulatedStores stores;
    private final ExternalClients clients;

    DynamoDbToolProvider(EmulatedStores stores, ExternalClients clients) {
        this.stores = stores;
        this.clients = clients;
    }

    @Override
    public BackendKind kind() {
        return BackendKind.DYNAMODB;
    }

    @Override
    public List<Tool> tools() {
        String tn = "Name of the DynamoDB table";
        JsonObject values = obj("Values for :placeholders in expressions (DynamoDB typed JSON such as {\":v\":{\"S\":\"x\"}}; plain JSON values are also accepted)");
        JsonObject names = obj("Substitutions for #name placeholders in expressions");
        return List.of(
                new Tool("list_tables", "List DynamoDB tables.",
                        schema(List.of(), "limit", num("Maximum number of table names"),
                                "exclusiveStartTableName", str("Table name to start after")), false),
                new Tool("describe_table", "Describe a table: key schema, attribute definitions, item count, status.",
                        schema(List.of("tableName"), "tableName", str(tn)), false),
                new Tool("create_table", "Create a table (S or N partition key, optional sort key).",
                        schema(List.of("tableName", "partitionKey", "partitionKeyType"),
                                "tableName", str(tn), "partitionKey", str("Partition key attribute name"),
                                "partitionKeyType", str("S or N"), "sortKey", str("Sort key attribute name"),
                                "sortKeyType", str("S or N"), "readCapacity", num("Ignored (on-demand)"),
                                "writeCapacity", num("Ignored (on-demand)")), true),
                new Tool("put_item", "Put an item into a table.",
                        schema(List.of("tableName", "item"), "tableName", str(tn),
                                "item", obj("Item as DynamoDB typed JSON ({\"id\":{\"S\":\"1\"}}) or plain JSON")), true),
                new Tool("get_item", "Get one item by primary key.",
                        schema(List.of("tableName", "key"), "tableName", str(tn),
                                "key", obj("Primary key as DynamoDB typed JSON or plain JSON")), false),
                new Tool("update_item", "Update an item with an UpdateExpression.",
                        schema(List.of("tableName", "key", "updateExpression"), "tableName", str(tn),
                                "key", obj("Primary key"), "updateExpression", str("e.g. SET #a = :v"),
                                "expressionAttributeNames", names, "expressionAttributeValues", values,
                                "conditionExpression", str("Optional condition"),
                                "returnValues", str("NONE, ALL_OLD, ALL_NEW, UPDATED_OLD or UPDATED_NEW")), true),
                new Tool("delete_item", "Delete an item by primary key (extension: not in the community server, "
                        + "but DynamoDB clients expect it).",
                        schema(List.of("tableName", "key"), "tableName", str(tn), "key", obj("Primary key"),
                                "conditionExpression", str("Optional condition")), true),
                new Tool("query_table", "Query items by key condition.",
                        schema(List.of("tableName", "keyConditionExpression", "expressionAttributeValues"),
                                "tableName", str(tn), "keyConditionExpression", str("e.g. pk = :pk AND sk > :s"),
                                "expressionAttributeValues", values, "expressionAttributeNames", names,
                                "filterExpression", str("Optional post-filter"), "limit", num("Max items")), false),
                new Tool("scan_table", "Scan a whole table, optionally filtered.",
                        schema(List.of("tableName"), "tableName", str(tn), "filterExpression", str("Optional filter"),
                                "expressionAttributeValues", values, "expressionAttributeNames", names,
                                "limit", num("Max items")), false));
    }

    /** The DynamoDB JSON-API surface a tool runs against: dynamowire's own handlers for the emulated
     * store, or (real backends) the AWS SDK client of the registered {@code dynamodb://} backend. */
    interface DynamoApi {
        JsonObject invoke(String operation, JsonObject request);
    }

    private DynamoApi apiFor(Ctx ctx) {
        if (ctx.backend().emulated()) {
            var server = stores.dynamo();
            if (server == null) {
                throw new IllegalStateException("dynamowire is not running on this gateway (it failed to start or "
                        + "is disabled), so there is no DynamoDB-shaped store to serve this tool");
            }
            return server::invoke;
        }
        return new DynamoSdkApi(clients.dynamo(ctx.backend().target()));
    }

    @Override
    public JsonObject describe(Ctx ctx) {
        DynamoApi api = apiFor(ctx);
        JsonObject out = new JsonObject();
        JsonArray tables = new JsonArray();
        List<String> names = new ArrayList<>();
        api.invoke("ListTables", new JsonObject()).getAsJsonArray("TableNames").forEach(e -> names.add(e.getAsString()));
        java.util.Collections.sort(names);
        for (String n : names.subList(0, Math.min(names.size(), 50))) {
            JsonObject t = new JsonObject();
            t.addProperty("name", n);
            try {
                JsonObject req = new JsonObject();
                req.addProperty("TableName", n);
                JsonObject d = api.invoke("DescribeTable", req).getAsJsonObject("Table");
                t.add("keySchema", d.get("KeySchema"));
                if (d.has("ItemCount")) {
                    t.add("itemCount", d.get("ItemCount"));
                }
            } catch (RuntimeException e) {
                t.addProperty("note", "describe failed: " + e.getMessage());
            }
            tables.add(t);
        }
        out.addProperty("tableCount", names.size());
        out.add("tables", tables);
        return out;
    }

    @Override
    public Outcome call(String tool, JsonObject a, Ctx ctx) {
        if (UNSUPPORTED.containsKey(tool)) {
            return Outcome.error("UnsupportedOperation: " + tool + " -- " + UNSUPPORTED.get(tool));
        }
        DynamoApi server;
        try {
            server = apiFor(ctx);
        } catch (IllegalStateException e) {
            return Outcome.error(e.getMessage());
        }
        try {
            JsonObject req = new JsonObject();
            String op;
            switch (tool) {
                case "list_tables" -> {
                    JsonObject resp = server.invoke("ListTables", new JsonObject());
                    List<String> names = new ArrayList<>();
                    resp.getAsJsonArray("TableNames").forEach(e -> names.add(e.getAsString()));
                    java.util.Collections.sort(names);
                    String after = optString(a, "exclusiveStartTableName");
                    Integer limit = optInt(a, "limit");
                    JsonObject out = new JsonObject();
                    JsonArray arr = new JsonArray();
                    String last = null;
                    boolean more = false;
                    for (String n : names) {
                        if (after != null && n.compareTo(after) <= 0) {
                            continue;
                        }
                        if (limit != null && arr.size() >= limit) {
                            more = true;
                            break;
                        }
                        arr.add(n);
                        last = n;
                    }
                    out.add("TableNames", arr);
                    if (more) {
                        out.addProperty("LastEvaluatedTableName", last);
                    }
                    return Outcome.ok(out.toString());
                }
                case "describe_table" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    return Outcome.ok(server.invoke("DescribeTable", req).toString());
                }
                case "create_table" -> {
                    String pk = requireString(a, "partitionKey");
                    String pkType = requireString(a, "partitionKeyType");
                    req.addProperty("TableName", requireString(a, "tableName"));
                    JsonArray defs = new JsonArray();
                    JsonArray ks = new JsonArray();
                    defs.add(attrDef(pk, pkType));
                    ks.add(keyEntry(pk, "HASH"));
                    String sk = optString(a, "sortKey");
                    if (sk != null) {
                        defs.add(attrDef(sk, optString(a, "sortKeyType") == null ? "S" : optString(a, "sortKeyType")));
                        ks.add(keyEntry(sk, "RANGE"));
                    }
                    req.add("AttributeDefinitions", defs);
                    req.add("KeySchema", ks);
                    req.addProperty("BillingMode", "PAY_PER_REQUEST");
                    return Outcome.ok(server.invoke("CreateTable", req).toString());
                }
                case "put_item" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    req.add("Item", typedMap(a.get("item"), "item"));
                    server.invoke("PutItem", req);
                    return Outcome.ok("{}");
                }
                case "get_item" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    req.add("Key", typedMap(a.get("key"), "key"));
                    return Outcome.ok(server.invoke("GetItem", req).toString());
                }
                case "update_item" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    req.add("Key", typedMap(a.get("key"), "key"));
                    req.addProperty("UpdateExpression", requireString(a, "updateExpression"));
                    expressionArgs(a, req);
                    copy(a, "conditionExpression", req, "ConditionExpression");
                    copy(a, "returnValues", req, "ReturnValues");
                    return Outcome.ok(server.invoke("UpdateItem", req).toString());
                }
                case "delete_item" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    req.add("Key", typedMap(a.get("key"), "key"));
                    copy(a, "conditionExpression", req, "ConditionExpression");
                    server.invoke("DeleteItem", req);
                    return Outcome.ok("{}");
                }
                case "query_table", "scan_table" -> {
                    req.addProperty("TableName", requireString(a, "tableName"));
                    if (tool.equals("query_table")) {
                        req.addProperty("KeyConditionExpression", requireString(a, "keyConditionExpression"));
                        op = "Query";
                    } else {
                        op = "Scan";
                    }
                    expressionArgs(a, req);
                    copy(a, "filterExpression", req, "FilterExpression");
                    Integer limit = optInt(a, "limit");
                    if (limit != null) {
                        req.addProperty("Limit", limit);
                    }
                    return Outcome.ok(server.invoke(op, req).toString());
                }
                default -> {
                    return Outcome.error("unknown DynamoDB tool: " + tool);
                }
            }
        } catch (DynamoException e) {
            return Outcome.error(e.dynamoErrorType + ": " + e.getMessage());
        } catch (IllegalArgumentException e) {
            return Outcome.error("ValidationException: " + e.getMessage());
        } catch (RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            return Outcome.error("InternalFailure: " + c.getMessage());
        }
    }

    private static void expressionArgs(JsonObject a, JsonObject req) {
        if (a.has("expressionAttributeNames") && a.get("expressionAttributeNames").isJsonObject()) {
            req.add("ExpressionAttributeNames", a.get("expressionAttributeNames"));
        }
        if (a.has("expressionAttributeValues") && a.get("expressionAttributeValues").isJsonObject()) {
            req.add("ExpressionAttributeValues", typedMap(a.get("expressionAttributeValues"), "expressionAttributeValues"));
        }
    }

    private static void copy(JsonObject a, String from, JsonObject req, String to) {
        String v = optString(a, from);
        if (v != null) {
            req.addProperty(to, v);
        }
    }

    private static JsonObject attrDef(String name, String type) {
        JsonObject o = new JsonObject();
        o.addProperty("AttributeName", name);
        o.addProperty("AttributeType", type);
        return o;
    }

    private static JsonObject keyEntry(String name, String keyType) {
        JsonObject o = new JsonObject();
        o.addProperty("AttributeName", name);
        o.addProperty("KeyType", keyType);
        return o;
    }

    private static JsonObject typedMap(JsonElement e, String argName) {
        if (e == null || !e.isJsonObject()) {
            throw new IllegalArgumentException("argument " + argName + " must be a JSON object");
        }
        JsonObject out = new JsonObject();
        for (Map.Entry<String, JsonElement> en : e.getAsJsonObject().entrySet()) {
            out.add(en.getKey(), typed(en.getValue()));
        }
        return out;
    }

    /** Already-typed DynamoDB attribute values pass through; plain JSON is converted. */
    static JsonElement typed(JsonElement v) {
        JsonObject o = new JsonObject();
        if (v.isJsonObject()) {
            JsonObject in = v.getAsJsonObject();
            if (in.size() == 1 && TYPE_TAGS.contains(in.keySet().iterator().next())) {
                return v;
            }
            JsonObject m = new JsonObject();
            in.entrySet().forEach(en -> m.add(en.getKey(), typed(en.getValue())));
            o.add("M", m);
        } else if (v.isJsonArray()) {
            JsonArray l = new JsonArray();
            v.getAsJsonArray().forEach(x -> l.add(typed(x)));
            o.add("L", l);
        } else if (v.isJsonNull()) {
            o.addProperty("NULL", true);
        } else if (v.getAsJsonPrimitive().isBoolean()) {
            o.addProperty("BOOL", v.getAsBoolean());
        } else if (v.getAsJsonPrimitive().isNumber()) {
            o.addProperty("N", v.getAsString());
        } else {
            o.addProperty("S", v.getAsString());
        }
        return o;
    }
}
