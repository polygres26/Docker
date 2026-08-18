package com.polygres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements each DynamoDB operation's request/response JSON shape on top of {@link PgItemStore}.
 * {@code cache} (nullable — null when {@code POLYWIRE_DYNAMOWIRE_CACHE_ENABLED=false}) wraps
 * {@code GetItem} in cache-aside and invalidates the exact key on every write that touches it —
 * see {@link DynamoCache}'s class javadoc for scope (exact-key lookups only, not Query/Scan).
 */
final class OperationHandlers {

    private final PgItemStore store;
    private final DynamoCache cache;

    OperationHandlers(PgItemStore store, DynamoCache cache) {
        this.store = store;
        this.cache = cache;
    }

    private String cacheKeyFor(TableSchema schema, Map<String, AttributeValue> attrs) {
        String pk = attrs.get(schema.partitionKeyName()).scalar;
        String sk = schema.hasSortKey() ? attrs.get(schema.sortKeyName()).scalar : null;
        return DynamoCache.key(schema.tableName(), pk, sk);
    }

    JsonObject dispatch(String operation, JsonObject req) {
        return switch (operation) {
            case "CreateTable" -> createTable(req);
            case "DeleteTable" -> deleteTable(req);
            case "DescribeTable" -> describeTable(req);
            case "ListTables" -> listTables(req);
            case "PutItem" -> putItem(req);
            case "GetItem" -> getItem(req);
            case "DeleteItem" -> deleteItem(req);
            case "UpdateItem" -> updateItem(req);
            case "Query" -> query(req);
            case "Scan" -> scan(req);
            case "BatchGetItem" -> batchGetItem(req);
            case "BatchWriteItem" -> batchWriteItem(req);
            case "TransactGetItems" -> transactGetItems(req);
            case "TransactWriteItems" -> transactWriteItems(req);
            default -> throw new DynamoException("UnknownOperationException", "Operation not implemented by dynamowire: " + operation);
        };
    }

    // ---------------------------------------------------------------- table management

    private JsonObject createTable(JsonObject req) {
        String tableName = req.get("TableName").getAsString();
        JsonArray keySchema = req.getAsJsonArray("KeySchema");
        JsonArray attrDefs = req.getAsJsonArray("AttributeDefinitions");
        Map<String, String> attrTypes = new LinkedHashMap<>();
        for (JsonElement e : attrDefs) {
            JsonObject o = e.getAsJsonObject();
            attrTypes.put(o.get("AttributeName").getAsString(), o.get("AttributeType").getAsString());
        }
        String pkName = null, pkType = null, skName = null, skType = null;
        for (JsonElement e : keySchema) {
            JsonObject o = e.getAsJsonObject();
            String name = o.get("AttributeName").getAsString();
            String keyType = o.get("KeyType").getAsString();
            String type = attrTypes.get(name);
            if (type == null) throw new DynamoException("ValidationException", "KeySchema references undefined attribute: " + name);
            if (!"S".equals(type) && !"N".equals(type)) {
                throw new DynamoException("ValidationException", "Key attribute type must be S or N (B not supported by dynamowire)");
            }
            if ("HASH".equals(keyType)) { pkName = name; pkType = type; }
            else if ("RANGE".equals(keyType)) { skName = name; skType = type; }
        }
        if (pkName == null) throw new DynamoException("ValidationException", "KeySchema must include a HASH key");
        TableSchema schema = store.createTable(tableName, pkName, pkType, skName, skType);
        JsonObject resp = new JsonObject();
        resp.add("TableDescription", describeTableJson(schema, 0));
        return resp;
    }

    private JsonObject deleteTable(JsonObject req) {
        String tableName = req.get("TableName").getAsString();
        TableSchema schema = store.describeTable(tableName);
        store.deleteTable(tableName);
        JsonObject resp = new JsonObject();
        JsonObject desc = describeTableJson(schema, 0);
        desc.addProperty("TableStatus", "DELETING");
        resp.add("TableDescription", desc);
        return resp;
    }

    private JsonObject describeTable(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        long count = store.itemCount(schema);
        JsonObject resp = new JsonObject();
        resp.add("Table", describeTableJson(schema, count));
        return resp;
    }

    private JsonObject describeTableJson(TableSchema schema, long itemCount) {
        JsonObject t = new JsonObject();
        t.addProperty("TableName", schema.tableName());
        t.addProperty("TableStatus", schema.status());
        t.addProperty("CreationDateTime", schema.creationTimeEpochMillis() / 1000.0);
        t.addProperty("ItemCount", itemCount);
        t.addProperty("TableSizeBytes", 0);
        JsonArray keySchema = new JsonArray();
        keySchema.add(keySchemaEntry(schema.partitionKeyName(), "HASH"));
        if (schema.hasSortKey()) keySchema.add(keySchemaEntry(schema.sortKeyName(), "RANGE"));
        t.add("KeySchema", keySchema);
        JsonArray attrDefs = new JsonArray();
        attrDefs.add(attrDef(schema.partitionKeyName(), schema.partitionKeyType()));
        if (schema.hasSortKey()) attrDefs.add(attrDef(schema.sortKeyName(), schema.sortKeyType()));
        t.add("AttributeDefinitions", attrDefs);
        JsonObject arn = new JsonObject();
        t.addProperty("TableArn", "arn:aws:dynamodb:local:000000000000:table/" + schema.tableName());
        return t;
    }

    private JsonObject keySchemaEntry(String name, String type) {
        JsonObject o = new JsonObject();
        o.addProperty("AttributeName", name);
        o.addProperty("KeyType", type);
        return o;
    }

    private JsonObject attrDef(String name, String type) {
        JsonObject o = new JsonObject();
        o.addProperty("AttributeName", name);
        o.addProperty("AttributeType", type);
        return o;
    }

    private JsonObject listTables(JsonObject req) {
        List<String> names = store.listTables();
        JsonObject resp = new JsonObject();
        JsonArray arr = new JsonArray();
        for (String n : names) arr.add(n);
        resp.add("TableNames", arr);
        return resp;
    }

    // ---------------------------------------------------------------- item CRUD

    private JsonObject putItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> item = PgItemStore.jsonToItem(req.getAsJsonObject("Item"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        String cond = optString(req, "ConditionExpression");
        Map<String, AttributeValue> old = store.putItem(schema, item, cond, ctx);
        if (cache != null) {
            cache.invalidate(cacheKeyFor(schema, item));
        }
        JsonObject resp = new JsonObject();
        if ("ALL_OLD".equals(optString(req, "ReturnValues")) && old != null) {
            resp.add("Attributes", PgItemStore.itemToJson(old));
        }
        return resp;
    }

    private JsonObject getItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(req.getAsJsonObject("Key"));
        Map<String, AttributeValue> item;
        if (cache != null) {
            String cacheKey = cacheKeyFor(schema, key);
            String cachedJson = cache.get(cacheKey);
            if (cachedJson != null) {
                item = PgItemStore.jsonToItem(JsonParser.parseString(cachedJson).getAsJsonObject());
            } else {
                item = store.getItem(schema, key);
                if (item != null) {
                    cache.put(cacheKey, PgItemStore.itemToJson(item).toString());
                }
            }
        } else {
            item = store.getItem(schema, key);
        }
        JsonObject resp = new JsonObject();
        if (item != null) {
            item = applyProjection(item, optString(req, "ProjectionExpression"), ExpressionContext.parse(req));
            resp.add("Item", PgItemStore.itemToJson(item));
        }
        return resp;
    }

    private JsonObject deleteItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(req.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        Map<String, AttributeValue> old = store.deleteItem(schema, key, optString(req, "ConditionExpression"), ctx);
        if (cache != null) {
            cache.invalidate(cacheKeyFor(schema, key));
        }
        JsonObject resp = new JsonObject();
        if ("ALL_OLD".equals(optString(req, "ReturnValues")) && old != null) {
            resp.add("Attributes", PgItemStore.itemToJson(old));
        }
        return resp;
    }

    private JsonObject updateItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(req.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        String updateExpr = req.get("UpdateExpression").getAsString();
        Map<String, AttributeValue> newItem = store.updateItem(schema, key, updateExpr, optString(req, "ConditionExpression"), ctx);
        if (cache != null) {
            cache.invalidate(cacheKeyFor(schema, key));
        }
        JsonObject resp = new JsonObject();
        String rv = optString(req, "ReturnValues");
        if (rv != null && !"NONE".equals(rv)) {
            resp.add("Attributes", PgItemStore.itemToJson(newItem));
        }
        return resp;
    }

    // ---------------------------------------------------------------- Query / Scan

    private JsonObject query(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        ExpressionContext ctx = ExpressionContext.parse(req);
        KeyConditionParser kc = KeyConditionParser.parse(req.get("KeyConditionExpression").getAsString(), schema, ctx);
        Integer limit = req.has("Limit") ? req.get("Limit").getAsInt() : null;
        boolean forward = !req.has("ScanIndexForward") || req.get("ScanIndexForward").getAsBoolean();
        Map<String, AttributeValue> startKey = req.has("ExclusiveStartKey") ? PgItemStore.jsonToItem(req.getAsJsonObject("ExclusiveStartKey")) : null;
        PgItemStore.PageResult page = store.query(schema, kc, ctx, optString(req, "FilterExpression"), limit, startKey, forward);
        return pageResponse(page, optString(req, "ProjectionExpression"), ctx);
    }

    private JsonObject scan(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        ExpressionContext ctx = ExpressionContext.parse(req);
        Integer limit = req.has("Limit") ? req.get("Limit").getAsInt() : null;
        Map<String, AttributeValue> startKey = req.has("ExclusiveStartKey") ? PgItemStore.jsonToItem(req.getAsJsonObject("ExclusiveStartKey")) : null;
        PgItemStore.PageResult page = store.scan(schema, optString(req, "FilterExpression"), ctx, limit, startKey);
        return pageResponse(page, optString(req, "ProjectionExpression"), ctx);
    }

    private JsonObject pageResponse(PgItemStore.PageResult page, String projectionExpr, ExpressionContext ctx) {
        JsonObject resp = new JsonObject();
        JsonArray items = new JsonArray();
        for (Map<String, AttributeValue> item : page.items()) {
            Map<String, AttributeValue> projected = applyProjection(item, projectionExpr, ctx);
            items.add(PgItemStore.itemToJson(projected));
        }
        resp.add("Items", items);
        resp.addProperty("Count", page.items().size());
        resp.addProperty("ScannedCount", page.items().size());
        if (page.lastEvaluatedKey() != null) resp.add("LastEvaluatedKey", PgItemStore.itemToJson(page.lastEvaluatedKey()));
        return resp;
    }

    private Map<String, AttributeValue> applyProjection(Map<String, AttributeValue> item, String projectionExpr, ExpressionContext ctx) {
        if (projectionExpr == null || projectionExpr.isBlank()) return item;
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        for (String rawPath : projectionExpr.split(",")) {
            ItemPath p = ItemPath.parse(rawPath.trim(), ctx);
            AttributeValue v = p.get(item);
            if (v != null) p.set(out, v);
        }
        return out;
    }

    // ---------------------------------------------------------------- batch operations

    private JsonObject batchGetItem(JsonObject req) {
        JsonObject requestItems = req.getAsJsonObject("RequestItems");
        JsonObject responses = new JsonObject();
        for (var e : requestItems.entrySet()) {
            String tableName = e.getKey();
            TableSchema schema = store.describeTable(tableName);
            JsonObject spec = e.getValue().getAsJsonObject();
            String projection = spec.has("ProjectionExpression") ? spec.get("ProjectionExpression").getAsString() : null;
            ExpressionContext ctx = ExpressionContext.parse(spec);
            JsonArray items = new JsonArray();
            for (JsonElement keyEl : spec.getAsJsonArray("Keys")) {
                Map<String, AttributeValue> key = PgItemStore.jsonToItem(keyEl.getAsJsonObject());
                Map<String, AttributeValue> item = store.getItem(schema, key);
                if (item != null) items.add(PgItemStore.itemToJson(applyProjection(item, projection, ctx)));
            }
            responses.add(tableName, items);
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        resp.add("UnprocessedKeys", new JsonObject());
        return resp;
    }

    private JsonObject batchWriteItem(JsonObject req) {
        JsonObject requestItems = req.getAsJsonObject("RequestItems");
        for (var e : requestItems.entrySet()) {
            TableSchema schema = store.describeTable(e.getKey());
            for (JsonElement reqEl : e.getValue().getAsJsonArray()) {
                JsonObject writeReq = reqEl.getAsJsonObject();
                if (writeReq.has("PutRequest")) {
                    Map<String, AttributeValue> item = PgItemStore.jsonToItem(writeReq.getAsJsonObject("PutRequest").getAsJsonObject("Item"));
                    store.putItem(schema, item, null, new ExpressionContext());
                    if (cache != null) {
                        cache.invalidate(cacheKeyFor(schema, item));
                    }
                } else if (writeReq.has("DeleteRequest")) {
                    Map<String, AttributeValue> key = PgItemStore.jsonToItem(writeReq.getAsJsonObject("DeleteRequest").getAsJsonObject("Key"));
                    store.deleteItem(schema, key, null, new ExpressionContext());
                    if (cache != null) {
                        cache.invalidate(cacheKeyFor(schema, key));
                    }
                }
            }
        }
        JsonObject resp = new JsonObject();
        resp.add("UnprocessedItems", new JsonObject());
        return resp;
    }

    // ---------------------------------------------------------------- transactions (single real Postgres backend)

    private JsonObject transactGetItems(JsonObject req) {
        JsonArray transactItems = req.getAsJsonArray("TransactItems");
        JsonArray responses = new JsonArray();
        // A plain read snapshot suffices here — no cross-item write atomicity concern for reads.
        for (JsonElement e : transactItems) {
            JsonObject get = e.getAsJsonObject().getAsJsonObject("Get");
            TableSchema schema = store.describeTable(get.get("TableName").getAsString());
            Map<String, AttributeValue> key = PgItemStore.jsonToItem(get.getAsJsonObject("Key"));
            Map<String, AttributeValue> item = store.getItem(schema, key);
            JsonObject itemResp = new JsonObject();
            if (item != null) itemResp.add("Item", PgItemStore.itemToJson(item));
            responses.add(itemResp);
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        return resp;
    }

    private JsonObject transactWriteItems(JsonObject req) {
        JsonArray transactItems = req.getAsJsonArray("TransactItems");
        // Keys touched are collected as the ops run and only invalidated after the surrounding
        // Postgres transaction actually commits (runInTransaction throws + rolls back on failure,
        // in which case this list is simply discarded — invalidating a key for a write that never
        // committed would be wrong, not just wasteful).
        List<String> touchedCacheKeys = new ArrayList<>();
        // Real Postgres transaction (BEGIN/COMMIT) — see DynamoWireServer's javadoc on why this
        // doesn't need the project's existing XA/2PC coordinator: every dynamowire item lives on
        // the single Postgres backend this frontend talks to, so ordinary transactional isolation
        // is sufficient; XA exists in this project for cross-*backend* atomicity, which doesn't
        // apply here.
        store.runInTransaction(conn -> {
            for (JsonElement e : transactItems) {
                JsonObject op = e.getAsJsonObject();
                try {
                    if (op.has("Put")) applyTransactPut(conn, op.getAsJsonObject("Put"), touchedCacheKeys);
                    else if (op.has("Delete")) applyTransactDelete(conn, op.getAsJsonObject("Delete"), touchedCacheKeys);
                    else if (op.has("Update")) applyTransactUpdate(conn, op.getAsJsonObject("Update"), touchedCacheKeys);
                    else if (op.has("ConditionCheck")) applyTransactConditionCheck(conn, op.getAsJsonObject("ConditionCheck"));
                } catch (SQLException sqle) {
                    throw new RuntimeException(sqle);
                }
            }
            return null;
        });
        if (cache != null) {
            for (String cacheKey : touchedCacheKeys) {
                cache.invalidate(cacheKey);
            }
        }
        return new JsonObject();
    }

    private void applyTransactPut(Connection conn, JsonObject put, List<String> touchedCacheKeys) throws SQLException {
        TableSchema schema = store.describeTable(put.get("TableName").getAsString());
        Map<String, AttributeValue> item = PgItemStore.jsonToItem(put.getAsJsonObject("Item"));
        ExpressionContext ctx = ExpressionContext.parse(put);
        String cond = optString(put, "ConditionExpression");
        transactionalUpsert(conn, schema, item, cond, ctx);
        if (cache != null) {
            touchedCacheKeys.add(cacheKeyFor(schema, item));
        }
    }

    private void applyTransactUpdate(Connection conn, JsonObject update, List<String> touchedCacheKeys) throws SQLException {
        TableSchema schema = store.describeTable(update.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(update.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(update);
        Map<String, AttributeValue> existing = readWithinTxn(conn, schema, key);
        String cond = optString(update, "ConditionExpression");
        if (cond != null && !ConditionExpressionEvaluator.evaluate(cond, existing, ctx)) {
            throw new DynamoException("TransactionCanceledException", "ConditionalCheckFailed on Update");
        }
        Map<String, AttributeValue> item = existing != null ? new LinkedHashMap<>(existing) : new LinkedHashMap<>(key);
        item.putAll(key);
        UpdateExpressionParser.apply(update.get("UpdateExpression").getAsString(), item, ctx);
        item.putAll(key);
        transactionalUpsert(conn, schema, item, null, ctx);
        if (cache != null) {
            touchedCacheKeys.add(cacheKeyFor(schema, key));
        }
    }

    private void applyTransactDelete(Connection conn, JsonObject del, List<String> touchedCacheKeys) throws SQLException {
        TableSchema schema = store.describeTable(del.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(del.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(del);
        Map<String, AttributeValue> existing = readWithinTxn(conn, schema, key);
        String cond = optString(del, "ConditionExpression");
        if (cond != null && !ConditionExpressionEvaluator.evaluate(cond, existing, ctx)) {
            throw new DynamoException("TransactionCanceledException", "ConditionalCheckFailed on Delete");
        }
        String pg = store.tableToPgName(schema.tableName());
        String pk = key.get(schema.partitionKeyName()).scalar;
        String sk = schema.hasSortKey() ? key.get(schema.sortKeyName()).scalar : "";
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            ps.executeUpdate();
        }
        if (cache != null) {
            touchedCacheKeys.add(cacheKeyFor(schema, key));
        }
    }

    private void applyTransactConditionCheck(Connection conn, JsonObject check) throws SQLException {
        TableSchema schema = store.describeTable(check.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(check.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(check);
        Map<String, AttributeValue> existing = readWithinTxn(conn, schema, key);
        String cond = check.get("ConditionExpression").getAsString();
        if (!ConditionExpressionEvaluator.evaluate(cond, existing, ctx)) {
            throw new DynamoException("TransactionCanceledException", "ConditionalCheckFailed on ConditionCheck");
        }
    }

    private Map<String, AttributeValue> readWithinTxn(Connection conn, TableSchema schema, Map<String, AttributeValue> key) throws SQLException {
        String pg = store.tableToPgName(schema.tableName());
        String pk = key.get(schema.partitionKeyName()).scalar;
        String sk = schema.hasSortKey() ? key.get(schema.sortKeyName()).scalar : "";
        try (PreparedStatement ps = conn.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ? FOR UPDATE")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return PgItemStore.jsonToItem(com.google.gson.JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        }
    }

    private void transactionalUpsert(Connection conn, TableSchema schema, Map<String, AttributeValue> item, String cond, ExpressionContext ctx) throws SQLException {
        Map<String, AttributeValue> key = new LinkedHashMap<>();
        key.put(schema.partitionKeyName(), item.get(schema.partitionKeyName()));
        if (schema.hasSortKey()) key.put(schema.sortKeyName(), item.get(schema.sortKeyName()));
        Map<String, AttributeValue> existing = readWithinTxn(conn, schema, key);
        if (cond != null && !ConditionExpressionEvaluator.evaluate(cond, existing, ctx)) {
            throw new DynamoException("TransactionCanceledException", "ConditionalCheckFailed on Put");
        }
        String pg = store.tableToPgName(schema.tableName());
        String pk = item.get(schema.partitionKeyName()).scalar;
        String sk = schema.hasSortKey() ? item.get(schema.sortKeyName()).scalar : "";
        java.math.BigDecimal skNum = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? new java.math.BigDecimal(sk) : null;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + pg + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) " +
                "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            if (skNum != null) ps.setBigDecimal(3, skNum); else ps.setNull(3, java.sql.Types.NUMERIC);
            ps.setString(4, PgItemStore.itemToJson(item).toString());
            ps.executeUpdate();
        }
    }

    private static String optString(JsonObject obj, String key) {
        return obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsString() : null;
    }
}
