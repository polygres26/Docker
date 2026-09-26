package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.dynamowire.DynamoException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * The DynamoDB JSON API surface ({@link DynamoDbToolProvider.DynamoApi}) backed by a REAL DynamoDB
 * endpoint through the AWS SDK client the connector factory builds. Requests and results use
 * DynamoDB's own typed-attribute JSON, exactly like the emulated store, so the tool code is shared.
 */
final class DynamoSdkApi implements DynamoDbToolProvider.DynamoApi {

    private final DynamoDbClient client;

    DynamoSdkApi(DynamoDbClient client) {
        this.client = client;
    }

    @Override
    public JsonObject invoke(String operation, JsonObject req) {
        try {
            return switch (operation) {
                case "ListTables" -> listTables();
                case "DescribeTable" -> describe(str(req, "TableName"));
                case "CreateTable" -> createTable(req);
                case "PutItem" -> {
                    client.putItem(PutItemRequest.builder().tableName(str(req, "TableName"))
                            .item(attrMap(req.getAsJsonObject("Item"))).build());
                    yield new JsonObject();
                }
                case "GetItem" -> {
                    var r = client.getItem(GetItemRequest.builder().tableName(str(req, "TableName"))
                            .key(attrMap(req.getAsJsonObject("Key"))).build());
                    JsonObject out = new JsonObject();
                    if (r.hasItem() && !r.item().isEmpty()) {
                        out.add("Item", jsonMap(r.item()));
                    }
                    yield out;
                }
                case "UpdateItem" -> updateItem(req);
                case "DeleteItem" -> {
                    DeleteItemRequest.Builder b = DeleteItemRequest.builder().tableName(str(req, "TableName"))
                            .key(attrMap(req.getAsJsonObject("Key")));
                    if (req.has("ConditionExpression")) {
                        b.conditionExpression(str(req, "ConditionExpression"));
                    }
                    client.deleteItem(b.build());
                    yield new JsonObject();
                }
                case "Query" -> query(req);
                case "Scan" -> scan(req);
                default -> throw new DynamoException("UnknownOperationException", "unsupported operation " + operation);
            };
        } catch (DynamoDbException e) {
            String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "DynamoDbException";
            String msg = e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();
            throw new DynamoException(code, msg);
        } catch (SdkClientException e) {
            throw new DynamoException("ServiceUnavailable", "could not reach the DynamoDB endpoint: " + e.getMessage());
        }
    }

    private JsonObject listTables() {
        List<String> names = new ArrayList<>();
        String last = null;
        do {
            var r = client.listTables(ListTablesRequest.builder().exclusiveStartTableName(last).build());
            names.addAll(r.tableNames());
            last = r.lastEvaluatedTableName();
        } while (last != null);
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        names.forEach(arr::add);
        out.add("TableNames", arr);
        return out;
    }

    private JsonObject describe(String table) {
        TableDescription d = client.describeTable(DescribeTableRequest.builder().tableName(table).build()).table();
        return wrapTable("Table", d);
    }

    private static JsonObject wrapTable(String key, TableDescription d) {
        JsonObject t = new JsonObject();
        t.addProperty("TableName", d.tableName());
        JsonArray ks = new JsonArray();
        for (KeySchemaElement k : d.keySchema()) {
            JsonObject o = new JsonObject();
            o.addProperty("AttributeName", k.attributeName());
            o.addProperty("KeyType", k.keyTypeAsString());
            ks.add(o);
        }
        t.add("KeySchema", ks);
        JsonArray defs = new JsonArray();
        for (AttributeDefinition a : d.attributeDefinitions()) {
            JsonObject o = new JsonObject();
            o.addProperty("AttributeName", a.attributeName());
            o.addProperty("AttributeType", a.attributeTypeAsString());
            defs.add(o);
        }
        t.add("AttributeDefinitions", defs);
        if (d.itemCount() != null) {
            t.addProperty("ItemCount", d.itemCount());
        }
        t.addProperty("TableStatus", d.tableStatusAsString());
        JsonObject out = new JsonObject();
        out.add(key, t);
        return out;
    }

    private JsonObject createTable(JsonObject req) {
        List<AttributeDefinition> defs = new ArrayList<>();
        for (JsonElement e : req.getAsJsonArray("AttributeDefinitions")) {
            JsonObject o = e.getAsJsonObject();
            defs.add(AttributeDefinition.builder().attributeName(str(o, "AttributeName"))
                    .attributeType(str(o, "AttributeType")).build());
        }
        List<KeySchemaElement> ks = new ArrayList<>();
        for (JsonElement e : req.getAsJsonArray("KeySchema")) {
            JsonObject o = e.getAsJsonObject();
            ks.add(KeySchemaElement.builder().attributeName(str(o, "AttributeName")).keyType(str(o, "KeyType")).build());
        }
        var r = client.createTable(CreateTableRequest.builder().tableName(str(req, "TableName"))
                .attributeDefinitions(defs).keySchema(ks).billingMode(BillingMode.PAY_PER_REQUEST).build());
        return wrapTable("TableDescription", r.tableDescription());
    }

    private JsonObject updateItem(JsonObject req) {
        UpdateItemRequest.Builder b = UpdateItemRequest.builder().tableName(str(req, "TableName"))
                .key(attrMap(req.getAsJsonObject("Key"))).updateExpression(str(req, "UpdateExpression"));
        if (req.has("ExpressionAttributeNames")) {
            b.expressionAttributeNames(stringMap(req.getAsJsonObject("ExpressionAttributeNames")));
        }
        if (req.has("ExpressionAttributeValues")) {
            b.expressionAttributeValues(attrMap(req.getAsJsonObject("ExpressionAttributeValues")));
        }
        if (req.has("ConditionExpression")) {
            b.conditionExpression(str(req, "ConditionExpression"));
        }
        if (req.has("ReturnValues")) {
            b.returnValues(str(req, "ReturnValues"));
        }
        var r = client.updateItem(b.build());
        JsonObject out = new JsonObject();
        if (r.hasAttributes() && !r.attributes().isEmpty()) {
            out.add("Attributes", jsonMap(r.attributes()));
        }
        return out;
    }

    private JsonObject query(JsonObject req) {
        QueryRequest.Builder b = QueryRequest.builder().tableName(str(req, "TableName"))
                .keyConditionExpression(str(req, "KeyConditionExpression"));
        if (req.has("FilterExpression")) {
            b.filterExpression(str(req, "FilterExpression"));
        }
        if (req.has("ExpressionAttributeNames")) {
            b.expressionAttributeNames(stringMap(req.getAsJsonObject("ExpressionAttributeNames")));
        }
        if (req.has("ExpressionAttributeValues")) {
            b.expressionAttributeValues(attrMap(req.getAsJsonObject("ExpressionAttributeValues")));
        }
        if (req.has("Limit")) {
            b.limit(req.get("Limit").getAsInt());
        }
        var r = client.query(b.build());
        return items(r.items(), r.count(), r.scannedCount());
    }

    private JsonObject scan(JsonObject req) {
        ScanRequest.Builder b = ScanRequest.builder().tableName(str(req, "TableName"));
        if (req.has("FilterExpression")) {
            b.filterExpression(str(req, "FilterExpression"));
        }
        if (req.has("ExpressionAttributeNames")) {
            b.expressionAttributeNames(stringMap(req.getAsJsonObject("ExpressionAttributeNames")));
        }
        if (req.has("ExpressionAttributeValues")) {
            b.expressionAttributeValues(attrMap(req.getAsJsonObject("ExpressionAttributeValues")));
        }
        if (req.has("Limit")) {
            b.limit(req.get("Limit").getAsInt());
        }
        var r = client.scan(b.build());
        return items(r.items(), r.count(), r.scannedCount());
    }

    private static JsonObject items(List<Map<String, AttributeValue>> items, Integer count, Integer scanned) {
        JsonObject out = new JsonObject();
        JsonArray arr = new JsonArray();
        items.forEach(i -> arr.add(jsonMap(i)));
        out.add("Items", arr);
        out.addProperty("Count", count);
        out.addProperty("ScannedCount", scanned);
        return out;
    }

    // ---- typed-attribute JSON <-> SDK AttributeValue ----

    private static String str(JsonObject o, String k) {
        return o.get(k).getAsString();
    }

    private static Map<String, String> stringMap(JsonObject o) {
        Map<String, String> m = new HashMap<>();
        o.entrySet().forEach(e -> m.put(e.getKey(), e.getValue().getAsString()));
        return m;
    }

    static Map<String, AttributeValue> attrMap(JsonObject o) {
        Map<String, AttributeValue> m = new LinkedHashMap<>();
        o.entrySet().forEach(e -> m.put(e.getKey(), attr(e.getValue().getAsJsonObject())));
        return m;
    }

    static AttributeValue attr(JsonObject typed) {
        String tag = typed.keySet().iterator().next();
        JsonElement v = typed.get(tag);
        return switch (tag) {
            case "S" -> AttributeValue.fromS(v.getAsString());
            case "N" -> AttributeValue.fromN(v.getAsString());
            case "B" -> AttributeValue.fromB(SdkBytes.fromByteArray(Base64.getDecoder().decode(v.getAsString())));
            case "BOOL" -> AttributeValue.fromBool(v.getAsBoolean());
            case "NULL" -> AttributeValue.fromNul(true);
            case "SS" -> AttributeValue.fromSs(strings(v.getAsJsonArray()));
            case "NS" -> AttributeValue.fromNs(strings(v.getAsJsonArray()));
            case "BS" -> AttributeValue.fromBs(strings(v.getAsJsonArray()).stream()
                    .map(s -> SdkBytes.fromByteArray(Base64.getDecoder().decode(s))).toList());
            case "L" -> {
                List<AttributeValue> l = new ArrayList<>();
                v.getAsJsonArray().forEach(x -> l.add(attr(x.getAsJsonObject())));
                yield AttributeValue.fromL(l);
            }
            case "M" -> AttributeValue.fromM(attrMap(v.getAsJsonObject()));
            default -> throw new IllegalArgumentException("unknown DynamoDB attribute type tag " + tag);
        };
    }

    private static List<String> strings(JsonArray a) {
        List<String> out = new ArrayList<>();
        a.forEach(e -> out.add(e.getAsString()));
        return out;
    }

    static JsonObject jsonMap(Map<String, AttributeValue> m) {
        JsonObject o = new JsonObject();
        m.forEach((k, v) -> o.add(k, json(v)));
        return o;
    }

    static JsonObject json(AttributeValue v) {
        JsonObject o = new JsonObject();
        switch (v.type()) {
            case S -> o.addProperty("S", v.s());
            case N -> o.addProperty("N", v.n());
            case B -> o.addProperty("B", Base64.getEncoder().encodeToString(v.b().asByteArray()));
            case BOOL -> o.addProperty("BOOL", v.bool());
            case NUL -> o.addProperty("NULL", true);
            case SS -> o.add("SS", array(v.ss()));
            case NS -> o.add("NS", array(v.ns()));
            case BS -> o.add("BS", array(v.bs().stream().map(b -> Base64.getEncoder().encodeToString(b.asByteArray())).toList()));
            case L -> {
                JsonArray a = new JsonArray();
                v.l().forEach(x -> a.add(json(x)));
                o.add("L", a);
            }
            case M -> o.add("M", jsonMap(v.m()));
            default -> o.addProperty("S", String.valueOf(v));
        }
        return o;
    }

    private static JsonArray array(List<String> l) {
        JsonArray a = new JsonArray();
        l.forEach(a::add);
        return a;
    }
}
