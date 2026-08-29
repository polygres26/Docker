package com.nexagres.wire.dynamowire;

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

final class OperationHandlers {

    private final PgItemStore store;
    private final com.nexagres.wire.cluster.RowCache cache;
    private final com.nexagres.wire.core.SqlMetricsCollector sqlMetrics;

    OperationHandlers(PgItemStore store, com.nexagres.wire.cluster.RowCache cache) {
        this(store, cache, null);
    }

    OperationHandlers(PgItemStore store, com.nexagres.wire.cluster.RowCache cache, com.nexagres.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.cache = cache;
        this.sqlMetrics = sqlMetrics;
    }

    /** Only getItem consults the cache today (Query/Scan/BatchGetItem don't), so this is the one
     * spot that needs to distinguish a cache hit from a real Postgres round trip. */
    private void recordRttOutcome(String outcome, long elapsedNanos) {
        if (sqlMetrics != null) {
            sqlMetrics.recordRttOutcome("dynamowire", outcome, elapsedNanos);
        }
    }

    // Keyed by the PHYSICAL Postgres table name (store.tableToPgName), not schema.tableName()
    // (dynamowire's own logical DynamoDB table name) -- this is what lets a SQL SELECT against
    // the same physical table (e.g. "SELECT item FROM dynamo_item_orders WHERE pk_value = ?")
    // compute the identical key and share this cache entry. See RowCache's own javadoc.
    private String cacheKeyFor(TableSchema schema, Map<String, AttributeValue> attrs) {
        String pk = attrs.get(schema.partitionKeyName()).scalar;
        String sk = schema.hasSortKey() ? attrs.get(schema.sortKeyName()).scalar : null;
        return com.nexagres.wire.cluster.RowCache.key(store.tableToPgName(schema.tableName()), pk, sk);
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
            case "ExecuteStatement" -> executeStatement(req);
            case "BatchExecuteStatement" -> batchExecuteStatement(req);
            case "ExecuteTransaction" -> throw new DynamoException("UnknownOperationException",
                    "dynamowire's PartiQL support covers ExecuteStatement and BatchExecuteStatement "
                            + "today, not ExecuteTransaction -- a disclosed gap, not a silent one.");
            default -> throw new DynamoException("UnknownOperationException", "Operation not implemented by dynamowire: " + operation);
        };
    }

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

    private JsonObject putItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> item = PgItemStore.jsonToItem(req.getAsJsonObject("Item"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        String cond = optString(req, "ConditionExpression");
        boolean needExisting = "ALL_OLD".equals(optString(req, "ReturnValues"));
        long writeStart = System.nanoTime();
        Map<String, AttributeValue> old = store.putItem(schema, item, cond, ctx, needExisting);
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
        String projectionExpr = optString(req, "ProjectionExpression");
        JsonObject resp = new JsonObject();
        if (cache != null) {
            String cacheKey = cacheKeyFor(schema, key);
            long cacheStart = System.nanoTime();
            String cachedJson = cache.get(cacheKey);
            if (cachedJson != null) {
                recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - cacheStart);
                JsonObject cachedItem = JsonParser.parseString(cachedJson).getAsJsonObject();
                // Fast path: no projection means the cached JSON is already exactly the response
                // shape -- skip the round-trip through Map<String,AttributeValue> and back that
                // the miss path (and the projected case below) still need. Found live while
                // chasing cache-hit latency: this was a real, avoidable second cost on top of
                // describeTable's uncached DB round trip (see PgItemStore's schemaCache).
                if (projectionExpr == null) {
                    resp.add("Item", cachedItem);
                } else {
                    Map<String, AttributeValue> projected = applyProjection(
                            PgItemStore.jsonToItem(cachedItem), projectionExpr, ExpressionContext.parse(req));
                    resp.add("Item", PgItemStore.itemToJson(projected));
                }
                return resp;
            }
            long readStart = System.nanoTime();
            Map<String, AttributeValue> item = store.getItem(schema, key);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
            if (item != null) {
                cache.put(cacheKey, PgItemStore.itemToJson(item).toString());
                item = applyProjection(item, projectionExpr, ExpressionContext.parse(req));
                resp.add("Item", PgItemStore.itemToJson(item));
            }
            return resp;
        }
        long readStart = System.nanoTime();
        Map<String, AttributeValue> item = store.getItem(schema, key);
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
        if (item != null) {
            item = applyProjection(item, projectionExpr, ExpressionContext.parse(req));
            resp.add("Item", PgItemStore.itemToJson(item));
        }
        return resp;
    }

    private JsonObject deleteItem(JsonObject req) {
        TableSchema schema = store.describeTable(req.get("TableName").getAsString());
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(req.getAsJsonObject("Key"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        boolean needExisting = "ALL_OLD".equals(optString(req, "ReturnValues"));
        long writeStart = System.nanoTime();
        Map<String, AttributeValue> old = store.deleteItem(schema, key, optString(req, "ConditionExpression"), ctx, needExisting);
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
        long writeStart = System.nanoTime();
        Map<String, AttributeValue> newItem = store.updateItem(schema, key, updateExpr, optString(req, "ConditionExpression"), ctx);
        recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
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
                    store.putItem(schema, item, null, new ExpressionContext(), false);
                    if (cache != null) {
                        cache.invalidate(cacheKeyFor(schema, item));
                    }
                } else if (writeReq.has("DeleteRequest")) {
                    Map<String, AttributeValue> key = PgItemStore.jsonToItem(writeReq.getAsJsonObject("DeleteRequest").getAsJsonObject("Key"));
                    store.deleteItem(schema, key, null, new ExpressionContext(), false);
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

    private JsonObject transactGetItems(JsonObject req) {
        JsonArray transactItems = req.getAsJsonArray("TransactItems");
        JsonArray responses = new JsonArray();
        
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

        List<String> touchedCacheKeys = new ArrayList<>();

        // A sharded store routes the whole transaction by one partition-key value -- real
        // DynamoDB requires every item in a transaction to resolve to partitions it can commit
        // atomically together too, so using the first item's own key is consistent with that,
        // not a shortcut specific to this implementation.
        String routingPartitionKey = firstTransactPartitionKey(transactItems);

        store.runInTransaction(routingPartitionKey, conn -> {
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

    private String firstTransactPartitionKey(JsonArray transactItems) {
        if (transactItems.isEmpty()) {
            return null;
        }
        JsonObject first = transactItems.get(0).getAsJsonObject();
        for (String opName : List.of("Put", "Delete", "Update", "ConditionCheck")) {
            if (!first.has(opName)) {
                continue;
            }
            JsonObject op = first.getAsJsonObject(opName);
            TableSchema schema = store.describeTable(op.get("TableName").getAsString());
            JsonObject keySource = "Put".equals(opName) ? op.getAsJsonObject("Item") : op.getAsJsonObject("Key");
            AttributeValue pk = PgItemStore.jsonToItem(keySource).get(schema.partitionKeyName());
            return pk == null ? null : pk.scalar;
        }
        return null;
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

    // -------------------------------------------------------------------------------------------
    // PartiQL: ExecuteStatement / BatchExecuteStatement -- see PartiQlParser's own javadoc for the
    // full design. Both funnel through executePartiQl, which does the actual parse -> resolve ->
    // execute -> reuses GetItem/Query/PutItem/UpdateItem/DeleteItem's own store methods unchanged.
    // -------------------------------------------------------------------------------------------

    private record PartiQlResult(java.util.List<Map<String, AttributeValue>> items) {}

    private JsonObject executeStatement(JsonObject req) {
        String statementText = req.get("Statement").getAsString();
        JsonArray paramsArr = req.has("Parameters") && !req.get("Parameters").isJsonNull()
                ? req.getAsJsonArray("Parameters") : new JsonArray();
        PartiQlResult result = executePartiQl(statementText, paramsArr);
        JsonObject resp = new JsonObject();
        JsonArray items = new JsonArray();
        if (result.items() != null) {
            for (Map<String, AttributeValue> item : result.items()) items.add(PgItemStore.itemToJson(item));
        }
        resp.add("Items", items);
        return resp;
    }

    private JsonObject batchExecuteStatement(JsonObject req) {
        JsonArray responses = new JsonArray();
        for (var e : req.getAsJsonArray("Statements")) {
            JsonObject s = e.getAsJsonObject();
            String statementText = s.get("Statement").getAsString();
            JsonArray paramsArr = s.has("Parameters") && !s.get("Parameters").isJsonNull()
                    ? s.getAsJsonArray("Parameters") : new JsonArray();
            JsonObject entry = new JsonObject();
            try {
                PartiQlResult result = executePartiQl(statementText, paramsArr);
                // Real BatchExecuteStatement returns at most one Item per statement (each
                // statement is expected to target a single item, same as ExecuteStatement's own
                // UPDATE/DELETE/INSERT constraint here) -- a SELECT matching more than one row
                // (a partition-key-only query on a table with a sort key) just reports the first.
                if (result.items() != null && !result.items().isEmpty()) {
                    entry.add("Item", PgItemStore.itemToJson(result.items().get(0)));
                }
            } catch (DynamoException de) {
                JsonObject err = new JsonObject();
                err.addProperty("Code", de.dynamoErrorType);
                err.addProperty("Message", de.getMessage());
                entry.add("Error", err);
            }
            responses.add(entry);
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        return resp;
    }

    private PartiQlResult executePartiQl(String statementText, JsonArray paramsArr) {
        String substituted = PartiQlParser.substitutePositionalParams(statementText);
        PartiQlParser.Statement stmt = PartiQlParser.parse(substituted);
        ExpressionContext ctx = new ExpressionContext();
        for (int i = 0; i < paramsArr.size(); i++) {
            ctx.values.put(":p" + i, AttributeValue.fromJson(paramsArr.get(i)));
        }
        if (stmt instanceof PartiQlParser.Select sel) {
            TableSchema schema = store.describeTable(sel.table());
            KeyConditionParser kc = KeyConditionParser.parse(sel.whereExpr(), schema, ctx);
            long start = System.nanoTime();
            PgItemStore.PageResult page = store.query(schema, kc, ctx, null, null, null, true);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - start);
            return new PartiQlResult(page.items());
        }
        if (stmt instanceof PartiQlParser.Insert ins) {
            TableSchema schema = store.describeTable(ins.table());
            Map<String, AttributeValue> item = resolvePartiQlItemValue(ins.valueToken(), ctx);
            long start = System.nanoTime();
            store.putItem(schema, item, null, ctx, false);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - start);
            if (cache != null) cache.invalidate(cacheKeyFor(schema, item));
            return new PartiQlResult(null);
        }
        if (stmt instanceof PartiQlParser.Update upd) {
            TableSchema schema = store.describeTable(upd.table());
            Map<String, AttributeValue> key = resolvePartiQlKey(schema, upd.keyTokens(), ctx);
            long start = System.nanoTime();
            store.updateItem(schema, key, "SET " + upd.setClause(), null, ctx);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - start);
            if (cache != null) cache.invalidate(cacheKeyFor(schema, key));
            return new PartiQlResult(null);
        }
        if (stmt instanceof PartiQlParser.Delete del) {
            TableSchema schema = store.describeTable(del.table());
            Map<String, AttributeValue> key = resolvePartiQlKey(schema, del.keyTokens(), ctx);
            long start = System.nanoTime();
            store.deleteItem(schema, key, null, ctx, false);
            recordRttOutcome(com.nexagres.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - start);
            if (cache != null) cache.invalidate(cacheKeyFor(schema, key));
            return new PartiQlResult(null);
        }
        throw new DynamoException("ValidationException", "Unsupported PartiQL statement");
    }

    /** Maps a WHERE clause's {attributeName -> valueToken} pairs onto the table's actual
     * partition/sort key names -- PartiQlParser only knows syntax, not schema, so this is where a
     * clause referencing something other than a real key column is caught. */
    private Map<String, AttributeValue> resolvePartiQlKey(TableSchema schema, Map<String, String> keyTokens, ExpressionContext ctx) {
        Map<String, AttributeValue> key = new LinkedHashMap<>();
        String pkToken = keyTokens.get(schema.partitionKeyName());
        if (pkToken == null) {
            throw new DynamoException("ValidationException",
                    "WHERE clause must include the partition key: " + schema.partitionKeyName());
        }
        key.put(schema.partitionKeyName(), resolvePartiQlTokenValue(pkToken, ctx));
        if (schema.hasSortKey()) {
            String skToken = keyTokens.get(schema.sortKeyName());
            if (skToken == null) {
                throw new DynamoException("ValidationException",
                        "WHERE clause must include the sort key: " + schema.sortKeyName());
            }
            key.put(schema.sortKeyName(), resolvePartiQlTokenValue(skToken, ctx));
        }
        if (keyTokens.size() > (schema.hasSortKey() ? 2 : 1)) {
            throw new DynamoException("ValidationException",
                    "WHERE clause references an attribute that isn't a key column on this table");
        }
        return key;
    }

    /** {@code INSERT ... VALUE} is either a single {@code ?}/{@code :pN} parameter resolving to a
     * whole item (an {@code AttributeValue.Type.M}, the overwhelmingly common real usage -- the
     * AWS SDK itself has no API for constructing a literal PartiQL tuple string, only for binding
     * a real Map as a parameter), or a literal {@code {'k': v, ...}} tuple for simple/manual use. */
    private Map<String, AttributeValue> resolvePartiQlItemValue(String valueToken, ExpressionContext ctx) {
        String t = valueToken.trim();
        if (t.startsWith(":")) {
            AttributeValue v = ctx.resolveValue(t);
            if (v.type != AttributeValue.Type.M) {
                throw new DynamoException("ValidationException", "INSERT ... VALUE parameter must be a map (a full item)");
            }
            return v.map;
        }
        if (t.startsWith("{") && t.endsWith("}")) {
            Map<String, AttributeValue> item = new LinkedHashMap<>();
            for (String pair : splitTopLevelPartiQl(t.substring(1, t.length() - 1))) {
                int colon = pair.indexOf(':');
                if (colon < 0) {
                    throw new DynamoException("ValidationException", "Malformed INSERT VALUE tuple entry: " + pair);
                }
                String key = stripPartiQlQuotes(pair.substring(0, colon).trim());
                item.put(key, resolvePartiQlTokenValue(pair.substring(colon + 1).trim(), ctx));
            }
            return item;
        }
        throw new DynamoException("ValidationException",
                "INSERT ... VALUE must be a ? parameter (a full item map) or a literal {...} tuple, not: " + valueToken);
    }

    private static AttributeValue resolvePartiQlTokenValue(String token, ExpressionContext ctx) {
        if (token.startsWith(":")) {
            return ctx.resolveValue(token);
        }
        String t = token.trim();
        if (t.equalsIgnoreCase("NULL")) return AttributeValue.ofNull();
        if (t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false")) return AttributeValue.ofBool(Boolean.parseBoolean(t));
        if (t.length() >= 2 && t.startsWith("'") && t.endsWith("'")) {
            return AttributeValue.ofS(t.substring(1, t.length() - 1).replace("''", "'"));
        }
        try {
            new java.math.BigDecimal(t);
            return AttributeValue.ofN(t);
        } catch (NumberFormatException e) {
            throw new DynamoException("ValidationException", "Could not interpret PartiQL literal: " + token);
        }
    }

    private static java.util.List<String> splitTopLevelPartiQl(String s) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean inStr = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                inStr = !inStr;
            } else if (!inStr && (c == '{' || c == '[')) {
                depth++;
            } else if (!inStr && (c == '}' || c == ']')) {
                depth--;
            } else if (!inStr && depth == 0 && c == ',') {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts;
    }

    private static String stripPartiQlQuotes(String s) {
        if (s.length() >= 2 && ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\"")))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String optString(JsonObject obj, String key) {
        return obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsString() : null;
    }
}
