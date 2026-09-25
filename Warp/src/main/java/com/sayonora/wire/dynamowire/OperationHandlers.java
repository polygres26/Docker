package com.sayonora.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The DynamoDB API surface of dynamowire: validates each request the way DynamoDB does (enum
 * checks before table lookup, expression / legacy-parameter rules, limits) and executes it against
 * {@link PgItemStore}. Table-level operations are in {@link TableOps}, PartiQL in {@link PartiQl}.
 */
final class OperationHandlers {

    private static final Set<String> RETURN_VALUES = Set.of("NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW");
    private static final int MAX_BATCH_GET = 100;
    private static final int MAX_BATCH_WRITE = 25;
    private static final int MAX_TRANSACT = 100;
    private static final long MAX_BATCH_GET_BYTES = 16L * 1024 * 1024;

    private final PgItemStore store;
    private final com.sayonora.wire.cluster.RowCache cache;
    private final com.sayonora.wire.core.SqlMetricsCollector sqlMetrics;
    final TableOps tables;
    final QueryEngine engine;
    private final PartiQl partiql;

    OperationHandlers(PgItemStore store, com.sayonora.wire.cluster.RowCache cache) {
        this(store, cache, null);
    }

    OperationHandlers(PgItemStore store, com.sayonora.wire.cluster.RowCache cache, com.sayonora.wire.core.SqlMetricsCollector sqlMetrics) {
        this.store = store;
        this.cache = cache;
        this.sqlMetrics = sqlMetrics;
        this.tables = new TableOps(store);
        this.engine = new QueryEngine(store);
        this.partiql = new PartiQl(this, store);
    }

    PgItemStore store() {
        return store;
    }

    private void recordRttOutcome(String outcome, long elapsedNanos) {
        if (sqlMetrics != null) {
            sqlMetrics.recordRttOutcome("dynamowire", outcome, elapsedNanos);
        }
    }

    // Keyed by the PHYSICAL Postgres table name so a SQL SELECT on the same physical table computes
    // the identical key and shares the cache entry. See RowCache's own javadoc.
    private String cacheKeyFor(TableSchema schema, Map<String, AttributeValue> attrs) {
        String pk = KeyCodec.partitionToken(schema, attrs);
        String sk = schema.hasSortKey() ? KeyCodec.sortToken(schema, attrs) : null;
        return com.sayonora.wire.cluster.RowCache.key(store.tableToPgName(schema.tableName()), pk, sk);
    }

    void invalidate(TableSchema schema, Map<String, AttributeValue> keyOrItem) {
        if (cache != null && keyOrItem != null) {
            cache.invalidate(cacheKeyFor(schema, keyOrItem));
        }
    }

    JsonObject dispatch(String operation, JsonObject req) {
        return switch (operation) {
            case "CreateTable" -> tables.createTable(req);
            case "DeleteTable" -> deleteTable(req);
            case "DescribeTable" -> tables.describeTable(schemaFor(req));
            case "UpdateTable" -> updateTable(req);
            case "ListTables" -> tables.listTables(req);
            case "PutItem" -> putItem(req);
            case "GetItem" -> getItem(req);
            case "DeleteItem" -> deleteItem(req);
            case "UpdateItem" -> updateItem(req);
            case "Query" -> queryOrScan(req, false);
            case "Scan" -> queryOrScan(req, true);
            case "BatchGetItem" -> batchGetItem(req);
            case "BatchWriteItem" -> batchWriteItem(req);
            case "TransactGetItems" -> transactGetItems(req);
            case "TransactWriteItems" -> transactWriteItems(req);
            case "ExecuteStatement" -> partiql.executeStatement(req);
            case "BatchExecuteStatement" -> partiql.batchExecuteStatement(req);
            case "ExecuteTransaction" -> partiql.executeTransaction(req);
            case "DescribeTimeToLive" -> tables.describeTimeToLive(schemaFor(req));
            case "UpdateTimeToLive" -> tables.updateTimeToLive(schemaFor(req), req);
            case "DescribeContinuousBackups" -> tables.describeContinuousBackups(schemaFor(req));
            case "UpdateContinuousBackups" -> tables.updateContinuousBackups(schemaFor(req), req);
            case "TagResource" -> tables.tagResource(req);
            case "UntagResource" -> tables.untagResource(req);
            case "ListTagsOfResource" -> tables.listTagsOfResource(req);
            case "DescribeLimits" -> tables.describeLimits();
            case "ListBackups" -> {
                JsonObject o = new JsonObject();
                o.add("BackupSummaries", new JsonArray());
                yield o;
            }
            case "ListGlobalTables" -> {
                JsonObject o = new JsonObject();
                o.add("GlobalTables", new JsonArray());
                yield o;
            }
            case "CreateBackup", "DescribeBackup", "DeleteBackup", "RestoreTableFromBackup", "RestoreTableToPointInTime",
                    "CreateGlobalTable", "DescribeGlobalTable", "UpdateGlobalTable", "ExportTableToPointInTime", "ImportTable",
                    "DescribeStream", "ListStreams", "GetShardIterator", "GetRecords" ->
                    throw new DynamoException("UnsupportedOperationException", "Operation not supported by dynamowire: " + operation
                            + " (backups, global tables, exports/imports and DynamoDB Streams are not implemented)");
            default -> throw new DynamoException("UnknownOperationException", "Operation not implemented by dynamowire: " + operation);
        };
    }

    // ------------------------------------------------------------------------------- common

    static String optString(JsonObject obj, String key) {
        return obj.has(key) && !obj.get(key).isJsonNull() ? obj.get(key).getAsString() : null;
    }

    private static DynamoException enumError(String value, String path, String allowed) {
        return DynamoException.validation("1 validation error detected: Value '" + value + "' at '" + path
                + "' failed to satisfy constraint: Member must satisfy enum value set: " + allowed);
    }

    /** TableName, which may also be a table ARN. */
    String tableName(JsonObject req) {
        String n = optString(req, "TableName");
        if (n != null && n.startsWith("arn:")) {
            String[] p = n.split(":", 6);
            if (p.length == 6 && p[5].startsWith("table/")) {
                n = p[5].substring("table/".length());
                if (n.contains("/")) {
                    throw DynamoException.validation("TableName does not accept index or stream ARNs: " + optString(req, "TableName"));
                }
            } else {
                throw DynamoException.validation("Invalid table ARN: " + n);
            }
        }
        ItemValidator.validateTableName(n);
        return n;
    }

    TableSchema schemaFor(JsonObject req) {
        return store.describeTable(tableName(req));
    }

    private static String checkEnum(JsonObject req, String field, String path, String defaultValue, String... allowed) {
        String v = optString(req, field);
        if (v == null) return defaultValue;
        for (String a : allowed) if (a.equals(v)) return v;
        throw enumError(v, path, "[" + String.join(", ", allowed) + "]");
    }

    private String consumedMode(JsonObject req) {
        return checkEnum(req, "ReturnConsumedCapacity", "returnConsumedCapacity", "NONE", "INDEXES", "TOTAL", "NONE");
    }

    private String metricsMode(JsonObject req) {
        return checkEnum(req, "ReturnItemCollectionMetrics", "returnItemCollectionMetrics", "NONE", "SIZE", "NONE");
    }

    private String onFailureMode(JsonObject req) {
        return checkEnum(req, "ReturnValuesOnConditionCheckFailure", "returnValuesOnConditionCheckFailure", "NONE", "ALL_OLD", "NONE");
    }

    private String returnValues(JsonObject req, String... allowed) {
        String v = optString(req, "ReturnValues");
        if (v == null) return "NONE";
        if (!RETURN_VALUES.contains(v)) {
            throw enumError(v, "returnValues", "[ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]");
        }
        for (String a : allowed) if (a.equals(v)) return v;
        throw DynamoException.validation("Return values set to invalid value");
    }

    /** The write's condition: ConditionExpression, or the legacy Expected / ConditionalOperator. */
    private Expr.Cond condition(JsonObject req, ExpressionContext ctx) {
        LegacyParams.rejectMixing(req, List.of("Expected", "ConditionalOperator"), List.of("ConditionExpression"));
        String op = optString(req, "ConditionalOperator");
        LegacyParams.checkConditionalOperator(op);
        String expr = optString(req, "ConditionExpression");
        if (expr != null) {
            return ExprParser.parseCondition(expr, "ConditionExpression", ctx);
        }
        if (op != null && !LegacyParams.has(req, "Expected")) {
            throw DynamoException.validation("One or more parameter values were invalid: ConditionalOperator can only be used when Expected is specified");
        }
        return LegacyParams.expected(req, op);
    }

    private static JsonObject itemJson(Map<String, AttributeValue> item) {
        return PgItemStore.itemToJson(item);
    }

    private Map<String, AttributeValue> parseItem(JsonObject req, String field) {
        if (!LegacyParams.has(req, field)) {
            throw DynamoException.validation("Item to put in PutItem cannot be null");
        }
        return PgItemStore.jsonToItem(req.getAsJsonObject(field));
    }

    private Map<String, AttributeValue> parseKey(TableSchema s, JsonObject req, String field) {
        if (!LegacyParams.has(req, field)) {
            throw DynamoException.validation("The provided key element does not match the schema");
        }
        Map<String, AttributeValue> key = PgItemStore.jsonToItem(req.getAsJsonObject(field));
        ItemValidator.validateKey(s, key);
        return key;
    }

    /** Rethrows a ConditionalCheckFailed with the offending item attached when the caller asked for it. */
    private static DynamoException conditionFailed(PgItemStore.ConditionalCheckFailed e, String onFailure) {
        if ("ALL_OLD".equals(onFailure) && e.item != null) {
            e.withExtra("Item", PgItemStore.itemToJson(e.item));
        }
        return e;
    }

    // ------------------------------------------------------------------------------- tables

    private JsonObject deleteTable(JsonObject req) {
        return tables.deleteTable(schemaFor(req));
    }

    private JsonObject updateTable(JsonObject req) {
        TableSchema s = schemaFor(req);
        return tables.updateTable(s, req);
    }

    // ------------------------------------------------------------------------------- write responses

    private void addWriteExtras(JsonObject resp, String consumed, String metrics, TableSchema s,
            Map<String, AttributeValue> old, Map<String, AttributeValue> neu, Map<String, AttributeValue> keyOrItem) {
        if (Capacity.requested(consumed)) {
            resp.add("ConsumedCapacity", Capacity.toJson(consumed, Capacity.writeUsage(s, old, neu, false)));
        }
        if ("SIZE".equals(metrics) && s.hasLocalIndex()) {
            resp.add("ItemCollectionMetrics", itemCollectionMetrics(s, keyOrItem));
        }
    }

    private JsonObject itemCollectionMetrics(TableSchema s, Map<String, AttributeValue> keyOrItem) {
        JsonObject m = new JsonObject();
        JsonObject key = new JsonObject();
        key.add(s.partitionKeyName(), keyOrItem.get(s.partitionKeyName()).toJson());
        m.add("ItemCollectionKey", key);
        JsonArray range = new JsonArray();
        range.add(0.0);
        range.add(1.0);
        m.add("SizeEstimateRangeGB", range);
        return m;
    }

    // ------------------------------------------------------------------------------- PutItem

    private JsonObject putItem(JsonObject req) {
        String rv = returnValues(req, "NONE", "ALL_OLD");
        String consumed = consumedMode(req), metrics = metricsMode(req), onFailure = onFailureMode(req);
        TableSchema schema = schemaFor(req);
        Map<String, AttributeValue> item = parseItem(req, "Item");
        ExpressionContext ctx = ExpressionContext.parse(req);
        Expr.Cond cond = condition(req, ctx);
        ctx.checkUnused("ConditionExpression");
        long writeStart = System.nanoTime();
        PgItemStore.WriteResult r;
        try {
            r = store.put(schema, item, cond, "ALL_OLD".equals(rv));
        } catch (PgItemStore.ConditionalCheckFailed e) {
            throw conditionFailed(e, onFailure);
        }
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        invalidate(schema, item);
        JsonObject resp = new JsonObject();
        if ("ALL_OLD".equals(rv) && r.old() != null) {
            resp.add("Attributes", itemJson(r.old()));
        }
        addWriteExtras(resp, consumed, metrics, schema, r.old(), item, item);
        return resp;
    }

    // ------------------------------------------------------------------------------- GetItem

    private List<Expr.Path> projection(JsonObject req, ExpressionContext ctx) {
        LegacyParams.rejectMixing(req, List.of("AttributesToGet"), List.of("ProjectionExpression"));
        String expr = optString(req, "ProjectionExpression");
        if (expr != null) {
            return ExprParser.parseProjection(expr, ctx);
        }
        return LegacyParams.attributesToGet(req);
    }

    private JsonObject getItem(JsonObject req) {
        String consumed = consumedMode(req);
        TableSchema schema = schemaFor(req);
        Map<String, AttributeValue> key = parseKey(schema, req, "Key");
        ExpressionContext ctx = ExpressionContext.parse(req);
        List<Expr.Path> proj = projection(req, ctx);
        ctx.checkUnused("ProjectionExpression");
        boolean consistent = LegacyParams.has(req, "ConsistentRead") && req.get("ConsistentRead").getAsBoolean();
        JsonObject resp = new JsonObject();
        if (cache != null) {
            String cacheKey = cacheKeyFor(schema, key);
            long cacheStart = System.nanoTime();
            String cachedJson = cache.get(cacheKey);
            if (cachedJson != null) {
                recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_CACHE_HIT, System.nanoTime() - cacheStart);
                JsonObject cachedItem = JsonParser.parseString(cachedJson).getAsJsonObject();
                if (proj == null && !Capacity.requested(consumed)) {
                    resp.add("Item", cachedItem);
                    return resp;
                }
                return getItemResponse(resp, schema, PgItemStore.jsonToItem(cachedItem), proj, consumed, consistent);
            }
            long readStart = System.nanoTime();
            Map<String, AttributeValue> item = store.getItem(schema, key);
            recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
            if (item != null) {
                cache.put(cacheKey, itemJson(item).toString());
            }
            return getItemResponse(resp, schema, item, proj, consumed, consistent);
        }
        long readStart = System.nanoTime();
        Map<String, AttributeValue> item = store.getItem(schema, key);
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - readStart);
        return getItemResponse(resp, schema, item, proj, consumed, consistent);
    }

    private JsonObject getItemResponse(JsonObject resp, TableSchema schema, Map<String, AttributeValue> item,
            List<Expr.Path> proj, String consumed, boolean consistent) {
        if (item != null) {
            resp.add("Item", itemJson(ExprEval.project(item, proj)));
        }
        if (Capacity.requested(consumed)) {
            Capacity.Usage u = new Capacity.Usage(schema.tableName());
            u.tableUnits = Capacity.readUnits(item == null ? 0 : AttributeValue.itemSize(item), consistent, false);
            resp.add("ConsumedCapacity", Capacity.toJson(consumed, u));
        }
        return resp;
    }

    // ------------------------------------------------------------------------------- DeleteItem

    private JsonObject deleteItem(JsonObject req) {
        String rv = returnValues(req, "NONE", "ALL_OLD");
        String consumed = consumedMode(req), metrics = metricsMode(req), onFailure = onFailureMode(req);
        TableSchema schema = schemaFor(req);
        Map<String, AttributeValue> key = parseKey(schema, req, "Key");
        ExpressionContext ctx = ExpressionContext.parse(req);
        Expr.Cond cond = condition(req, ctx);
        ctx.checkUnused("ConditionExpression");
        long writeStart = System.nanoTime();
        PgItemStore.WriteResult r;
        try {
            r = store.delete(schema, key, cond, "ALL_OLD".equals(rv) || Capacity.requested(consumed));
        } catch (PgItemStore.ConditionalCheckFailed e) {
            throw conditionFailed(e, onFailure);
        }
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        invalidate(schema, key);
        JsonObject resp = new JsonObject();
        if ("ALL_OLD".equals(rv) && r.old() != null) {
            resp.add("Attributes", itemJson(r.old()));
        }
        addWriteExtras(resp, consumed, metrics, schema, r.old(), null, key);
        return resp;
    }

    // ------------------------------------------------------------------------------- UpdateItem

    private JsonObject updateItem(JsonObject req) {
        String rv = returnValues(req, "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW");
        String consumed = consumedMode(req), metrics = metricsMode(req), onFailure = onFailureMode(req);
        TableSchema schema = schemaFor(req);
        Map<String, AttributeValue> key = parseKey(schema, req, "Key");
        LegacyParams.rejectMixing(req, List.of("AttributeUpdates"), List.of("UpdateExpression"));
        ExpressionContext ctx = ExpressionContext.parse(req);
        Expr.UpdatePlan plan;
        String updateExpr = optString(req, "UpdateExpression");
        if (updateExpr != null) {
            plan = ExprParser.parseUpdate(updateExpr, ctx);
        } else if (LegacyParams.has(req, "AttributeUpdates")) {
            plan = LegacyParams.attributeUpdates(req);
        } else {
            plan = new Expr.UpdatePlan(List.of());
        }
        Expr.Cond cond = condition(req, ctx);
        ctx.checkUnused("UpdateExpression");
        long writeStart = System.nanoTime();
        PgItemStore.WriteResult r;
        try {
            r = store.update(schema, key, plan, cond);
        } catch (PgItemStore.ConditionalCheckFailed e) {
            throw conditionFailed(e, onFailure);
        }
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_WRITE, System.nanoTime() - writeStart);
        invalidate(schema, key);
        JsonObject resp = new JsonObject();
        switch (rv) {
            case "ALL_OLD" -> {
                if (r.old() != null) resp.add("Attributes", itemJson(r.old()));
            }
            case "ALL_NEW" -> resp.add("Attributes", itemJson(r.neu()));
            case "UPDATED_OLD" -> {
                Map<String, AttributeValue> v = ExprEval.valuesAt(r.old(), r.touched());
                if (!v.isEmpty()) resp.add("Attributes", itemJson(v));
            }
            case "UPDATED_NEW" -> {
                Map<String, AttributeValue> v = ExprEval.valuesAt(r.neu(), r.touched());
                if (!v.isEmpty()) resp.add("Attributes", itemJson(v));
            }
            default -> { }
        }
        addWriteExtras(resp, consumed, metrics, schema, r.old(), r.neu(), key);
        return resp;
    }

    // ------------------------------------------------------------------------------- Query / Scan

    private JsonObject queryOrScan(JsonObject req, boolean scan) {
        String select = checkEnum(req, "Select", "select", null, "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT");
        String consumed = consumedMode(req);
        LegacyParams.checkConditionalOperator(optString(req, "ConditionalOperator"));
        TableSchema s = schemaFor(req);
        String indexName = optString(req, "IndexName");
        TableSchema.IndexDef idx = null;
        if (indexName != null) {
            idx = s.index(indexName);
            if (idx == null) {
                throw DynamoException.validation("The table does not have the specified index: " + indexName);
            }
        }
        if (scan) {
            LegacyParams.rejectMixing(req, List.of("ScanFilter", "AttributesToGet", "ConditionalOperator"),
                    List.of("FilterExpression", "ProjectionExpression"));
        } else {
            LegacyParams.rejectMixing(req, List.of("KeyConditions", "QueryFilter", "AttributesToGet", "ConditionalOperator"),
                    List.of("KeyConditionExpression", "FilterExpression", "ProjectionExpression"));
        }
        boolean consistent = LegacyParams.has(req, "ConsistentRead") && req.get("ConsistentRead").getAsBoolean();
        if (consistent && idx != null && !idx.local()) {
            throw DynamoException.validation("Consistent reads are not supported on global secondary indexes");
        }
        Integer limit = null;
        if (LegacyParams.has(req, "Limit")) {
            limit = req.get("Limit").getAsInt();
            if (limit < 1) throw DynamoException.validation("Limit must be greater than or equal to 1");
        }
        int segment = 0, totalSegments = 0;
        if (scan) {
            boolean hasSeg = LegacyParams.has(req, "Segment"), hasTotal = LegacyParams.has(req, "TotalSegments");
            if (hasSeg && !hasTotal) {
                throw DynamoException.validation("The TotalSegments parameter is required but was not present in the request when parameter Segment is present");
            }
            if (hasTotal && !hasSeg) {
                throw DynamoException.validation("The Segment parameter is required but was not present in the request when parameter TotalSegments is present");
            }
            if (hasTotal) {
                totalSegments = req.get("TotalSegments").getAsInt();
                segment = req.get("Segment").getAsInt();
                if (totalSegments < 1 || totalSegments > 1_000_000) {
                    throw DynamoException.validation("1 validation error detected: Value '" + totalSegments + "' at 'totalSegments' failed to satisfy constraint: Member must have value "
                            + (totalSegments < 1 ? "greater than or equal to 1" : "less than or equal to 1000000"));
                }
                if (segment < 0) {
                    throw DynamoException.validation("1 validation error detected: Value '" + segment + "' at 'segment' failed to satisfy constraint: Member must have value greater than or equal to 0");
                }
                if (segment >= totalSegments) {
                    throw DynamoException.validation("The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: "
                            + segment + " is not less than TotalSegments: " + totalSegments);
                }
            }
        }

        ExpressionContext ctx = ExpressionContext.parse(req);
        KeyPlanner.Plan keyPlan = null;
        if (!scan) {
            List<Expr.KeyCond> terms;
            String kce = optString(req, "KeyConditionExpression");
            if (kce != null) {
                terms = KeyPlanner.termsFromCondition(ExprParser.parseCondition(kce, "KeyConditionExpression", ctx));
            } else if (LegacyParams.has(req, "KeyConditions")) {
                terms = LegacyParams.keyConditions(req);
            } else {
                throw DynamoException.validation("Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.");
            }
            keyPlan = KeyPlanner.plan(s, idx, terms);
        }
        Expr.Cond filter;
        String filterExpr = optString(req, "FilterExpression");
        if (filterExpr != null) {
            filter = ExprParser.parseCondition(filterExpr, "FilterExpression", ctx);
        } else {
            filter = LegacyParams.filterMap(req, scan ? "ScanFilter" : "QueryFilter", optString(req, "ConditionalOperator"));
        }
        List<Expr.Path> proj = projection(req, ctx);
        ctx.checkUnused(scan ? "FilterExpression" : "KeyConditionExpression");
        if (!scan) KeyPlanner.checkFilterHasNoKeys(filter, s, idx);

        // Select / projection rules
        boolean hasProj = proj != null;
        String sel = select != null ? select : hasProj ? "SPECIFIC_ATTRIBUTES" : idx != null ? "ALL_PROJECTED_ATTRIBUTES" : "ALL_ATTRIBUTES";
        switch (sel) {
            case "COUNT" -> {
                if (hasProj) throw DynamoException.validation("Cannot specify the ProjectionExpression when choosing to get only the Count");
            }
            case "ALL_ATTRIBUTES" -> {
                if (hasProj) throw DynamoException.validation("Cannot specify the " + (optString(req, "ProjectionExpression") != null
                        ? "ProjectionExpression" : "AttributesToGet") + " when choosing to get ALL_ATTRIBUTES");
                if (idx != null && !idx.local() && !"ALL".equals(idx.projectionType())) {
                    throw DynamoException.validation("One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index "
                            + idx.name() + " because its projection type is not ALL");
                }
            }
            case "ALL_PROJECTED_ATTRIBUTES" -> {
                if (idx == null) throw DynamoException.validation("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName");
                if (hasProj) throw DynamoException.validation("Cannot specify the " + (optString(req, "ProjectionExpression") != null
                        ? "ProjectionExpression" : "AttributesToGet") + " when choosing to get ALL_PROJECTED_ATTRIBUTES");
            }
            default -> {
                if (!hasProj) throw DynamoException.validation("Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES");
            }
        }
        if (hasProj && idx != null && !idx.local() && !"ALL".equals(idx.projectionType())) {
            Set<String> projected = new LinkedHashSet<>();
            projected.add(s.partitionKeyName());
            if (s.hasSortKey()) projected.add(s.sortKeyName());
            for (TableSchema.KeyAttr k : idx.allKeys()) projected.add(k.name());
            projected.addAll(idx.nonKeyAttributes());
            List<String> missing = new ArrayList<>();
            for (Expr.Path p : proj) if (!projected.contains(p.top())) missing.add(p.top());
            if (!missing.isEmpty()) {
                throw DynamoException.validation("One or more parameter values were invalid: Global secondary index " + idx.name()
                        + " does not project " + missing);
            }
        }
        Map<String, AttributeValue> startKey = null;
        if (LegacyParams.has(req, "ExclusiveStartKey")) {
            startKey = PgItemStore.jsonToItem(req.getAsJsonObject("ExclusiveStartKey"));
            KeyPlanner.checkStartKey(s, idx, startKey);
            if (!scan) {
                for (int i = 0; i < keyPlan.hashAttrs().size(); i++) {
                    AttributeValue v = startKey.get(keyPlan.hashAttrs().get(i).name());
                    if (v == null || !v.deepEquals(keyPlan.hashValues().get(i))) {
                        throw DynamoException.validation("The provided starting key is invalid");
                    }
                }
            }
        }
        boolean forward = !LegacyParams.has(req, "ScanIndexForward") || req.get("ScanIndexForward").getAsBoolean();

        long start = System.nanoTime();
        QueryEngine.Result res = engine.run(new QueryEngine.Spec(s, idx, keyPlan, filter, limit, startKey, forward, segment,
                totalSegments, !"COUNT".equals(sel)));
        recordRttOutcome(com.sayonora.wire.core.SqlMetricsCollector.OUTCOME_PG_READ, System.nanoTime() - start);

        JsonObject resp = new JsonObject();
        if (!"COUNT".equals(sel)) {
            JsonArray items = new JsonArray();
            for (Map<String, AttributeValue> item : res.items()) {
                Map<String, AttributeValue> view = item;
                if (idx != null && idx.local() && sel.equals("ALL_PROJECTED_ATTRIBUTES")) {
                    view = QueryEngine.projectForIndex(s, idx, item);
                }
                items.add(itemJson(ExprEval.project(view, proj)));
            }
            resp.add("Items", items);
        }
        resp.addProperty("Count", res.matched());
        resp.addProperty("ScannedCount", res.scannedCount());
        if (res.lastEvaluatedKey() != null) resp.add("LastEvaluatedKey", itemJson(res.lastEvaluatedKey()));
        if (Capacity.requested(consumed)) {
            Capacity.Usage u = new Capacity.Usage(s.tableName());
            double units = Capacity.readUnits(res.scannedBytes(), consistent, false);
            if (idx != null) u.addIndex(idx, units);
            else u.tableUnits = units;
            resp.add("ConsumedCapacity", Capacity.toJson(consumed, u));
        }
        return resp;
    }

    // ------------------------------------------------------------------------------- BatchGetItem

    private JsonObject batchGetItem(JsonObject req) {
        String consumed = consumedMode(req);
        if (!LegacyParams.has(req, "RequestItems") || req.getAsJsonObject("RequestItems").entrySet().isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonObject requestItems = req.getAsJsonObject("RequestItems");
        int total = 0;
        for (var e : requestItems.entrySet()) {
            JsonObject spec = e.getValue().getAsJsonObject();
            if (!LegacyParams.has(spec, "Keys") || spec.getAsJsonArray("Keys").isEmpty()) {
                throw DynamoException.validation("The list of keys in RequestItems for BatchGetItem is required: " + e.getKey() + " has empty list");
            }
            total += spec.getAsJsonArray("Keys").size();
        }
        if (total > MAX_BATCH_GET) {
            throw DynamoException.validation("Too many items requested for the BatchGetItem call");
        }
        JsonObject responses = new JsonObject();
        JsonObject unprocessed = new JsonObject();
        JsonArray capacity = new JsonArray();
        long bytes = 0;
        boolean cutoff = false;
        for (var e : requestItems.entrySet()) {
            ItemValidator.validateTableName(e.getKey());
            TableSchema schema = store.describeTable(e.getKey());
            JsonObject spec = e.getValue().getAsJsonObject();
            ExpressionContext ctx = ExpressionContext.parse(spec);
            List<Expr.Path> proj = projection(spec, ctx);
            ctx.checkUnused("ProjectionExpression");
            boolean consistent = LegacyParams.has(spec, "ConsistentRead") && spec.get("ConsistentRead").getAsBoolean();
            JsonArray items = new JsonArray();
            Set<String> seen = new LinkedHashSet<>();
            Capacity.Usage usage = new Capacity.Usage(schema.tableName());
            JsonArray remaining = new JsonArray();
            for (JsonElement keyEl : spec.getAsJsonArray("Keys")) {
                Map<String, AttributeValue> key = PgItemStore.jsonToItem(keyEl.getAsJsonObject());
                ItemValidator.validateKey(schema, key);
                if (!seen.add(KeyCodec.partitionToken(schema, key) + "\0" + KeyCodec.sortToken(schema, key))) {
                    throw DynamoException.validation("Provided list of item keys contains duplicates");
                }
                if (cutoff) {
                    remaining.add(keyEl);
                    continue;
                }
                Map<String, AttributeValue> item = store.getItem(schema, key);
                if (item != null) {
                    int size = AttributeValue.itemSize(item);
                    bytes += size;
                    usage.tableUnits += Capacity.readUnits(size, consistent, false);
                    items.add(itemJson(ExprEval.project(item, proj)));
                    if (bytes > MAX_BATCH_GET_BYTES) cutoff = true;
                } else {
                    usage.tableUnits += Capacity.readUnits(0, consistent, false);
                }
            }
            responses.add(e.getKey(), items);
            if (!remaining.isEmpty()) {
                JsonObject un = spec.deepCopy();
                un.add("Keys", remaining);
                unprocessed.add(e.getKey(), un);
            }
            if (Capacity.requested(consumed)) capacity.add(Capacity.toJson(consumed, usage));
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        resp.add("UnprocessedKeys", unprocessed);
        if (Capacity.requested(consumed)) resp.add("ConsumedCapacity", capacity);
        return resp;
    }

    // ------------------------------------------------------------------------------- BatchWriteItem

    /**
     * Not atomic (same as real DynamoDB): every write is applied on its own shard. When the store is
     * spread over several backends a write that fails on its shard (backend down, ...) is reported
     * back in {@code UnprocessedItems} -- the client retries just those.
     */
    private JsonObject batchWriteItem(JsonObject req) {
        String consumed = consumedMode(req), metrics = metricsMode(req);
        if (!LegacyParams.has(req, "RequestItems") || req.getAsJsonObject("RequestItems").entrySet().isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonObject requestItems = req.getAsJsonObject("RequestItems");
        int total = 0;
        for (var e : requestItems.entrySet()) {
            JsonArray writes = e.getValue().getAsJsonArray();
            if (writes.isEmpty()) {
                throw DynamoException.validation("The batch write request list for a table cannot be null or empty: " + e.getKey());
            }
            total += writes.size();
        }
        if (total > MAX_BATCH_WRITE) {
            throw DynamoException.validation("Too many items requested for the BatchWriteItem call");
        }
        // validate everything (and resolve tables) before writing anything
        Map<String, TableSchema> schemas = new LinkedHashMap<>();
        Map<String, List<Object[]>> plan = new LinkedHashMap<>();
        for (var e : requestItems.entrySet()) {
            ItemValidator.validateTableName(e.getKey());
            TableSchema schema = store.describeTable(e.getKey());
            schemas.put(e.getKey(), schema);
            Set<String> seen = new LinkedHashSet<>();
            List<Object[]> ops = new ArrayList<>();
            for (JsonElement reqEl : e.getValue().getAsJsonArray()) {
                JsonObject w = reqEl.getAsJsonObject();
                boolean put = LegacyParams.has(w, "PutRequest"), del = LegacyParams.has(w, "DeleteRequest");
                if (put == del) {
                    throw DynamoException.validation("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes");
                }
                Map<String, AttributeValue> keyOrItem;
                if (put) {
                    keyOrItem = PgItemStore.jsonToItem(w.getAsJsonObject("PutRequest").getAsJsonObject("Item"));
                    ItemValidator.validateItem(schema, keyOrItem, false);
                } else {
                    keyOrItem = PgItemStore.jsonToItem(w.getAsJsonObject("DeleteRequest").getAsJsonObject("Key"));
                    ItemValidator.validateKey(schema, keyOrItem);
                }
                if (!seen.add(KeyCodec.partitionToken(schema, keyOrItem) + "\0" + KeyCodec.sortToken(schema, keyOrItem))) {
                    throw DynamoException.validation("Provided list of item keys contains duplicates");
                }
                ops.add(new Object[] {put, keyOrItem, w});
            }
            plan.put(e.getKey(), ops);
        }
        JsonObject unprocessed = new JsonObject();
        JsonArray capacity = new JsonArray();
        JsonObject metricsOut = new JsonObject();
        for (var e : plan.entrySet()) {
            TableSchema schema = schemas.get(e.getKey());
            Capacity.Usage usage = new Capacity.Usage(schema.tableName());
            JsonArray metricsList = new JsonArray();
            for (Object[] op : e.getValue()) {
                boolean put = (Boolean) op[0];
                @SuppressWarnings("unchecked")
                Map<String, AttributeValue> keyOrItem = (Map<String, AttributeValue>) op[1];
                JsonObject writeReq = (JsonObject) op[2];
                try {
                    boolean needOld = Capacity.requested(consumed);
                    PgItemStore.WriteResult r = put ? store.put(schema, keyOrItem, null, needOld)
                            : store.delete(schema, keyOrItem, null, needOld);
                    invalidate(schema, keyOrItem);
                    if (Capacity.requested(consumed)) usage.add(Capacity.writeUsage(schema, r.old(), put ? keyOrItem : null, false));
                    if ("SIZE".equals(metrics) && schema.hasLocalIndex()) metricsList.add(itemCollectionMetrics(schema, keyOrItem));
                } catch (DynamoException de) {
                    throw de;
                } catch (RuntimeException re) {
                    if (!store.isSharded()) {
                        throw re;
                    }
                    if (!unprocessed.has(e.getKey())) {
                        unprocessed.add(e.getKey(), new JsonArray());
                    }
                    unprocessed.getAsJsonArray(e.getKey()).add(writeReq);
                }
            }
            if (Capacity.requested(consumed)) capacity.add(Capacity.toJson(consumed, usage));
            if (!metricsList.isEmpty()) metricsOut.add(e.getKey(), metricsList);
        }
        JsonObject resp = new JsonObject();
        resp.add("UnprocessedItems", unprocessed);
        if (Capacity.requested(consumed)) resp.add("ConsumedCapacity", capacity);
        if (metricsOut.size() > 0) resp.add("ItemCollectionMetrics", metricsOut);
        return resp;
    }

    // ------------------------------------------------------------------------------- TransactGetItems

    /** One read of a TransactGetItems / read-only ExecuteTransaction. */
    record TxGet(TableSchema schema, Map<String, AttributeValue> key, List<Expr.Path> projection) {}

    private JsonObject transactGetItems(JsonObject req) {
        String consumed = consumedMode(req);
        if (!LegacyParams.has(req, "TransactItems") || req.getAsJsonArray("TransactItems").isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonArray transactItems = req.getAsJsonArray("TransactItems");
        if (transactItems.size() > MAX_TRANSACT) {
            throw DynamoException.validation("1 validation error detected: Value '" + transactItems + "' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to " + MAX_TRANSACT);
        }
        List<TxGet> gets = new ArrayList<>();
        for (JsonElement e : transactItems) {
            JsonObject get = e.getAsJsonObject().getAsJsonObject("Get");
            if (get == null) {
                throw DynamoException.validation("TransactItems can only contain Get requests in TransactGetItems");
            }
            TableSchema schema = schemaFor(get);
            Map<String, AttributeValue> key = parseKey(schema, get, "Key");
            ExpressionContext ctx = ExpressionContext.parse(get);
            List<Expr.Path> proj = projection(get, ctx);
            ctx.checkUnused("ProjectionExpression");
            gets.add(new TxGet(schema, key, proj));
        }
        List<Map<String, AttributeValue>> items = runTransactGets(gets);
        JsonArray responses = new JsonArray();
        Map<String, Capacity.Usage> usage = new LinkedHashMap<>();
        for (int i = 0; i < gets.size(); i++) {
            JsonObject itemResp = new JsonObject();
            Map<String, AttributeValue> item = items.get(i);
            if (item != null) itemResp.add("Item", itemJson(ExprEval.project(item, gets.get(i).projection())));
            responses.add(itemResp);
            usage.computeIfAbsent(gets.get(i).schema().tableName(), Capacity.Usage::new).tableUnits +=
                    Capacity.readUnits(item == null ? 0 : AttributeValue.itemSize(item), true, true);
        }
        JsonObject resp = new JsonObject();
        resp.add("Responses", responses);
        if (Capacity.requested(consumed)) {
            JsonArray cc = new JsonArray();
            for (Capacity.Usage u : usage.values()) cc.add(Capacity.toJson(consumed, u));
            resp.add("ConsumedCapacity", cc);
        }
        return resp;
    }

    /** Reads in one snapshot per shard. */
    List<Map<String, AttributeValue>> runTransactGets(List<TxGet> gets) {
        try (PgItemStore.TxnScope scope = store.beginTransaction(true)) {
            List<Map<String, AttributeValue>> out = new ArrayList<>();
            for (TxGet g : gets) {
                java.sql.Connection c = scope.forPartition(KeyCodec.partitionToken(g.schema(), g.key()));
                out.add(store.readUnlocked(c, g.schema(), g.key()));
            }
            scope.rollback();
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("TransactGetItems failed", e);
        }
    }

    // ------------------------------------------------------------------------------- TransactWriteItems

    /** One write of a TransactWriteItems / ExecuteTransaction. */
    record TxOp(String kind, TableSchema schema, Map<String, AttributeValue> keyOrItem, Expr.Cond cond,
            Expr.UpdatePlan plan, boolean returnOldOnFail, String failCode) {}

    private JsonObject transactWriteItems(JsonObject req) {
        String consumed = consumedMode(req), metrics = metricsMode(req);
        if (!LegacyParams.has(req, "TransactItems") || req.getAsJsonArray("TransactItems").isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonArray transactItems = req.getAsJsonArray("TransactItems");
        if (transactItems.size() > MAX_TRANSACT) {
            throw DynamoException.validation("1 validation error detected: Value '" + transactItems + "' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to " + MAX_TRANSACT);
        }
        List<TxOp> ops = new ArrayList<>();
        for (JsonElement e : transactItems) {
            JsonObject item = e.getAsJsonObject();
            int kinds = 0;
            for (String k : List.of("Put", "Update", "Delete", "ConditionCheck")) if (LegacyParams.has(item, k)) kinds++;
            if (kinds != 1) {
                throw DynamoException.validation("TransactItems can only contain one of Check, Put, Update or Delete");
            }
            if (item.has("Put")) ops.add(parseTxPut(item.getAsJsonObject("Put")));
            else if (item.has("Update")) ops.add(parseTxUpdate(item.getAsJsonObject("Update")));
            else if (item.has("Delete")) ops.add(parseTxDelete(item.getAsJsonObject("Delete")));
            else ops.add(parseTxCheck(item.getAsJsonObject("ConditionCheck")));
        }
        String token = optString(req, "ClientRequestToken");
        if (token != null && (token.isEmpty() || token.length() > 36)) {
            throw DynamoException.validation("1 validation error detected: Value '" + token
                    + "' at 'clientRequestToken' failed to satisfy constraint: Member must have length less than or equal to 36 and greater than or equal to 1");
        }
        executeTxWrites(ops, token, token == null ? null : requestHash(transactItems));
        JsonObject resp = new JsonObject();
        if (Capacity.requested(consumed)) {
            Map<String, Capacity.Usage> usage = new LinkedHashMap<>();
            for (TxOp op : ops) {
                if (op.kind().equals("Check")) continue;
                usage.computeIfAbsent(op.schema().tableName(), Capacity.Usage::new)
                        .add(Capacity.writeUsage(op.schema(), null, op.keyOrItem(), true));
            }
            JsonArray cc = new JsonArray();
            for (Capacity.Usage u : usage.values()) cc.add(Capacity.toJson(consumed, u));
            resp.add("ConsumedCapacity", cc);
        }
        if ("SIZE".equals(metrics)) {
            JsonObject m = new JsonObject();
            for (TxOp op : ops) {
                if (op.kind().equals("Check") || !op.schema().hasLocalIndex()) continue;
                if (!m.has(op.schema().tableName())) m.add(op.schema().tableName(), new JsonArray());
                m.getAsJsonArray(op.schema().tableName()).add(itemCollectionMetrics(op.schema(), op.keyOrItem()));
            }
            if (m.size() > 0) resp.add("ItemCollectionMetrics", m);
        }
        return resp;
    }

    private static String requestHash(JsonElement payload) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(payload.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private TxOp parseTxPut(JsonObject o) {
        TableSchema s = schemaFor(o);
        Map<String, AttributeValue> item = parseItem(o, "Item");
        ItemValidator.validateItem(s, item, false);
        ExpressionContext ctx = ExpressionContext.parse(o);
        Expr.Cond cond = optString(o, "ConditionExpression") == null ? null
                : ExprParser.parseCondition(o.get("ConditionExpression").getAsString(), "ConditionExpression", ctx);
        ctx.checkUnused("ConditionExpression");
        return new TxOp("Put", s, item, cond, null, "ALL_OLD".equals(onFailureMode(o)), "ConditionalCheckFailed");
    }

    private TxOp parseTxDelete(JsonObject o) {
        TableSchema s = schemaFor(o);
        Map<String, AttributeValue> key = parseKey(s, o, "Key");
        ExpressionContext ctx = ExpressionContext.parse(o);
        Expr.Cond cond = optString(o, "ConditionExpression") == null ? null
                : ExprParser.parseCondition(o.get("ConditionExpression").getAsString(), "ConditionExpression", ctx);
        ctx.checkUnused("ConditionExpression");
        return new TxOp("Delete", s, key, cond, null, "ALL_OLD".equals(onFailureMode(o)), "ConditionalCheckFailed");
    }

    private TxOp parseTxCheck(JsonObject o) {
        TableSchema s = schemaFor(o);
        Map<String, AttributeValue> key = parseKey(s, o, "Key");
        ExpressionContext ctx = ExpressionContext.parse(o);
        if (optString(o, "ConditionExpression") == null) {
            throw DynamoException.validation("Member must not be null: ConditionExpression is required for ConditionCheck");
        }
        Expr.Cond cond = ExprParser.parseCondition(o.get("ConditionExpression").getAsString(), "ConditionExpression", ctx);
        ctx.checkUnused("ConditionExpression");
        return new TxOp("Check", s, key, cond, null, "ALL_OLD".equals(onFailureMode(o)), "ConditionalCheckFailed");
    }

    private TxOp parseTxUpdate(JsonObject o) {
        TableSchema s = schemaFor(o);
        Map<String, AttributeValue> key = parseKey(s, o, "Key");
        ExpressionContext ctx = ExpressionContext.parse(o);
        if (optString(o, "UpdateExpression") == null) {
            throw DynamoException.validation("Member must not be null: UpdateExpression is required for Update");
        }
        Expr.UpdatePlan plan = ExprParser.parseUpdate(o.get("UpdateExpression").getAsString(), ctx);
        Expr.Cond cond = optString(o, "ConditionExpression") == null ? null
                : ExprParser.parseCondition(o.get("ConditionExpression").getAsString(), "ConditionExpression", ctx);
        ctx.checkUnused("UpdateExpression");
        return new TxOp("Update", s, key, cond, plan, "ALL_OLD".equals(onFailureMode(o)), "ConditionalCheckFailed");
    }

    /**
     * Executes a list of transactional writes atomically: every row is locked and every condition
     * evaluated before anything is written; any failed condition cancels the whole transaction with
     * a per-item reason list. Items may live on different shards -- each shard gets one database
     * transaction and all of them are prepared before the first commit.
     */
    void executeTxWrites(List<TxOp> ops, String token, String requestHash) {
        Set<String> seen = new LinkedHashSet<>();
        for (TxOp op : ops) {
            String id = op.schema().tableName() + "\0" + KeyCodec.partitionToken(op.schema(), op.keyOrItem())
                    + "\0" + KeyCodec.sortToken(op.schema(), op.keyOrItem());
            if (!seen.add(id)) {
                throw DynamoException.validation("Transaction request cannot include multiple operations on one item");
            }
        }
        if (token != null) {
            String prev = store.findTransactionToken(token);
            if (prev != null) {
                if (prev.equals(requestHash)) return; // idempotent replay of a completed request
                throw new DynamoException("IdempotentParameterMismatchException",
                        "The request uses a ClientRequestToken that was already used with different parameters");
            }
        }
        int n = ops.size();
        String[] reasons = new String[n];
        Map<String, AttributeValue>[] failedItems = newArray(n);
        try (PgItemStore.TxnScope scope = store.beginTransaction(false)) {
            java.sql.Connection[] conns = new java.sql.Connection[n];
            Map<String, AttributeValue>[] toWrite = newArray(n);
            boolean failed = false;
            for (int i = 0; i < n; i++) {
                TxOp op = ops.get(i);
                conns[i] = scope.forPartition(KeyCodec.partitionToken(op.schema(), op.keyOrItem()));
                Map<String, AttributeValue> existing = store.readLocked(conns[i], op.schema(), op.keyOrItem());
                if (op.cond() != null && !ExprEval.test(op.cond(), existing)) {
                    reasons[i] = op.failCode();
                    if (op.returnOldOnFail()) failedItems[i] = existing;
                    failed = true;
                    continue;
                }
                switch (op.kind()) {
                    case "Put" -> toWrite[i] = op.keyOrItem();
                    case "Update" -> toWrite[i] = store.computeUpdate(op.schema(), op.keyOrItem(), existing, op.plan()).neu();
                    default -> { }
                }
            }
            if (failed) {
                scope.rollback();
                throw canceled(reasons, failedItems);
            }
            for (int i = 0; i < n; i++) {
                TxOp op = ops.get(i);
                switch (op.kind()) {
                    case "Put", "Update" -> store.writeRow(conns[i], op.schema(), toWrite[i]);
                    case "Delete" -> store.deleteRow(conns[i], op.schema(), op.keyOrItem());
                    default -> { }
                }
            }
            scope.commit();
        } catch (SQLException e) {
            if ("40001".equals(e.getSQLState()) || "40P01".equals(e.getSQLState())) {
                String[] all = new String[n];
                java.util.Arrays.fill(all, "TransactionConflict");
                throw canceled(all, newArray(n));
            }
            throw new RuntimeException("TransactWriteItems failed", e);
        }
        for (TxOp op : ops) {
            if (!op.kind().equals("Check")) invalidate(op.schema(), op.keyOrItem());
        }
        if (token != null) {
            store.recordTransactionToken(token, requestHash);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, AttributeValue>[] newArray(int n) {
        return (Map<String, AttributeValue>[]) new Map[n];
    }

    private static DynamoException canceled(String[] reasons, Map<String, AttributeValue>[] items) {
        JsonArray arr = new JsonArray();
        List<String> codes = new ArrayList<>();
        for (int i = 0; i < reasons.length; i++) {
            JsonObject r = new JsonObject();
            String code = reasons[i] == null ? "None" : reasons[i];
            codes.add(code);
            r.addProperty("Code", code);
            if (reasons[i] != null) {
                r.addProperty("Message", reasons[i].equals("ConditionalCheckFailed") ? "The conditional request failed"
                        : reasons[i].equals("TransactionConflict") ? "Transaction is ongoing for the item"
                        : reasons[i].equals("DuplicateItem") ? "Duplicate primary key exists in table" : reasons[i]);
                if (items[i] != null) r.add("Item", PgItemStore.itemToJson(items[i]));
            }
            arr.add(r);
        }
        DynamoException ex = new DynamoException("TransactionCanceledException",
                "Transaction cancelled, please refer cancellation reasons for specific reasons [" + String.join(", ", codes) + "]");
        ex.withExtra("CancellationReasons", arr);
        return ex;
    }

    // ------------------------------------------------------------------------------- helpers for PartiQL

    PgItemStore.WriteResult putForPartiQl(TableSchema s, Map<String, AttributeValue> item, Expr.Cond cond, boolean needOld) {
        PgItemStore.WriteResult r = store.put(s, item, cond, needOld);
        invalidate(s, item);
        return r;
    }

    PgItemStore.WriteResult deleteForPartiQl(TableSchema s, Map<String, AttributeValue> key, Expr.Cond cond, boolean needOld) {
        PgItemStore.WriteResult r = store.delete(s, key, cond, needOld);
        invalidate(s, key);
        return r;
    }

    PgItemStore.WriteResult updateForPartiQl(TableSchema s, Map<String, AttributeValue> key, Expr.UpdatePlan plan, Expr.Cond cond) {
        PgItemStore.WriteResult r = store.update(s, key, plan, cond);
        invalidate(s, key);
        return r;
    }
}
