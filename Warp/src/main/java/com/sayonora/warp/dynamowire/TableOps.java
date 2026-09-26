package com.sayonora.warp.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Table-level control-plane operations of dynamowire: CreateTable / UpdateTable / DeleteTable /
 * DescribeTable / ListTables, TTL, continuous backups, tags and limits. Secondary indexes are
 * created as Postgres expression indexes on every shard (see {@link PgItemStore}), so
 * CreateTable / UpdateTable index changes apply to all shards before the metadata is committed.
 */
final class TableOps {

    private static final int MAX_LSI = 5;
    private static final int MAX_GSI = 20;

    private final PgItemStore store;
    final String region = System.getProperty("warp.dynamowire.region", envOr("WARP_DYNAMOWIRE_REGION", "us-east-1"));
    final String account = System.getProperty("warp.dynamowire.account", envOr("WARP_DYNAMOWIRE_ACCOUNT_ID", "000000000000"));

    TableOps(PgItemStore store) {
        this.store = store;
    }

    private static String envOr(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }

    String tableArn(String name) {
        return "arn:aws:dynamodb:" + region + ":" + account + ":table/" + name;
    }

    // ------------------------------------------------------------------------------- helpers

    static String opt(JsonObject o, String k) {
        return LegacyParams.has(o, k) ? o.get(k).getAsString() : null;
    }

    private static DynamoException enumError(String value, String path, String allowed) {
        return DynamoException.validation("1 validation error detected: Value '" + value + "' at '" + path
                + "' failed to satisfy constraint: Member must satisfy enum value set: " + allowed);
    }

    private static Map<String, String> parseAttributeDefinitions(JsonObject req) {
        Map<String, String> types = new LinkedHashMap<>();
        if (!LegacyParams.has(req, "AttributeDefinitions") || req.getAsJsonArray("AttributeDefinitions").isEmpty()) {
            throw DynamoException.validation("No Attribute Schema Defined");
        }
        int i = 0;
        for (JsonElement e : req.getAsJsonArray("AttributeDefinitions")) {
            i++;
            JsonObject o = e.getAsJsonObject();
            String type = opt(o, "AttributeType");
            if (!"S".equals(type) && !"N".equals(type) && !"B".equals(type)) {
                throw enumError(type, "attributeDefinitions." + i + ".member.attributeType", "[B, N, S]");
            }
            if (types.put(opt(o, "AttributeName"), type) != null) {
                throw DynamoException.validation("One or more parameter values were invalid: Duplicate attribute name in AttributeDefinitions: "
                        + opt(o, "AttributeName"));
            }
        }
        return types;
    }

    private static List<TableSchema.KeyAttr> keyElements(JsonArray ks, Map<String, String> attrTypes, String what) {
        List<TableSchema.KeyAttr> out = new ArrayList<>();
        for (JsonElement e : ks) {
            String name = opt(e.getAsJsonObject(), "AttributeName");
            String type = attrTypes.get(name);
            if (type == null) {
                throw DynamoException.validation("One or more parameter values were invalid: Some index key attributes are not defined in "
                        + "AttributeDefinitions. Keys: [" + name + "], AttributeDefinitions: " + attrTypes.keySet());
            }
            out.add(new TableSchema.KeyAttr(name, type));
        }
        return out;
    }

    private static JsonArray one(JsonElement e) {
        JsonArray a = new JsonArray();
        a.add(e);
        return a;
    }

    private static String keyType(JsonElement e) {
        String kt = opt(e.getAsJsonObject(), "KeyType");
        if (!"HASH".equals(kt) && !"RANGE".equals(kt)) {
            throw enumError(kt, "keySchema.member.keyType", "[HASH, RANGE]");
        }
        return kt;
    }

    static String pgIndexName(String table, String index) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-1").digest((table + "\0" + index).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("dxi_");
            for (int i = 0; i < 10; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private TableSchema.IndexDef parseIndex(String table, JsonObject o, boolean local, Map<String, String> attrTypes,
            TableSchema.KeyAttr tableHash, TableSchema.KeyAttr tableRange, boolean provisioned) {
        String name = opt(o, "IndexName");
        ItemValidator.validateIndexName(name);
        JsonArray ks = o.getAsJsonArray("KeySchema");
        if (ks == null || ks.isEmpty()) {
            throw DynamoException.validation("One or more parameter values were invalid: Index KeySchema does not have a range key or hash key for index: " + name);
        }
        List<TableSchema.KeyAttr> hash = new ArrayList<>(), range = new ArrayList<>();
        boolean seenRange = false;
        Set<String> names = new LinkedHashSet<>();
        for (JsonElement e : ks) {
            String kt = keyType(e);
            TableSchema.KeyAttr a = keyElements(one(e), attrTypes, name).get(0);
            if (!names.add(a.name())) {
                throw DynamoException.validation("One or more parameter values were invalid: Duplicate key attribute in index KeySchema: " + a.name());
            }
            if (kt.equals("HASH")) {
                if (seenRange) throw DynamoException.validation("One or more parameter values were invalid: Index KeySchema: HASH keys must come before RANGE keys for index: " + name);
                hash.add(a);
            } else {
                seenRange = true;
                range.add(a);
            }
        }
        if (hash.isEmpty()) {
            throw DynamoException.validation("One or more parameter values were invalid: Index KeySchema does not have a hash key for index: " + name);
        }
        if (local) {
            if (hash.size() != 1 || !hash.get(0).name().equals(tableHash.name()) || range.size() != 1) {
                throw DynamoException.validation("One or more parameter values were invalid: Index KeySchema does not have a range key for index: "
                        + name + " (a local secondary index has the table's hash key and one range key)");
            }
        } else if (hash.size() > 4 || range.size() > 4) {
            throw DynamoException.validation("One or more parameter values were invalid: A global secondary index key schema can have at most 4 HASH and 4 RANGE attributes");
        }
        JsonObject proj = o.getAsJsonObject("Projection");
        if (proj == null) {
            throw DynamoException.validation("One or more parameter values were invalid: Unknown Projection for index: " + name);
        }
        String ptype = opt(proj, "ProjectionType");
        if (!"ALL".equals(ptype) && !"KEYS_ONLY".equals(ptype) && !"INCLUDE".equals(ptype)) {
            throw enumError(ptype, "globalSecondaryIndexes.1.member.projection.projectionType", "[ALL, INCLUDE, KEYS_ONLY]");
        }
        List<String> nonKey = new ArrayList<>();
        if (LegacyParams.has(proj, "NonKeyAttributes")) {
            for (JsonElement e : proj.getAsJsonArray("NonKeyAttributes")) nonKey.add(e.getAsString());
        }
        if ("INCLUDE".equals(ptype) && nonKey.isEmpty()) {
            throw DynamoException.validation("One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not specified");
        }
        if (!"INCLUDE".equals(ptype) && !nonKey.isEmpty()) {
            throw DynamoException.validation("One or more parameter values were invalid: ProjectionType is " + ptype
                    + ", but NonKeyAttributes is specified");
        }
        if (nonKey.size() > 100) {
            throw DynamoException.validation("One or more parameter values were invalid: Number of NonKeyAttributes in index projection must not exceed 100");
        }
        long rcu = 0, wcu = 0;
        if (!local && LegacyParams.has(o, "ProvisionedThroughput")) {
            rcu = o.getAsJsonObject("ProvisionedThroughput").get("ReadCapacityUnits").getAsLong();
            wcu = o.getAsJsonObject("ProvisionedThroughput").get("WriteCapacityUnits").getAsLong();
        }
        return new TableSchema.IndexDef(name, local, hash, range, ptype, nonKey, "ACTIVE", pgIndexName(table, name), rcu, wcu);
    }

    // ------------------------------------------------------------------------------- CreateTable

    JsonObject createTable(JsonObject req) {
        String name = opt(req, "TableName");
        ItemValidator.validateTableName(name);
        if (name.startsWith("arn:")) {
            throw DynamoException.validation("TableName must be a short name, not an ARN: " + name);
        }
        if (!LegacyParams.has(req, "KeySchema")) {
            throw DynamoException.validation("No Hash Key specified in schema.  All Dynamo DB tables must have exactly one hash key");
        }
        Map<String, String> attrTypes = parseAttributeDefinitions(req);
        JsonArray ks = req.getAsJsonArray("KeySchema");
        if (ks.isEmpty() || ks.size() > 2) {
            throw DynamoException.validation("1 validation error detected: Value '" + ks + "' at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2");
        }
        if (!"HASH".equals(keyType(ks.get(0)))) {
            throw DynamoException.validation("No Hash Key specified in schema.  All Dynamo DB tables must have exactly one hash key");
        }
        TableSchema.KeyAttr hash = keyElements(one(ks.get(0)), attrTypes, name).get(0);
        TableSchema.KeyAttr range = null;
        if (ks.size() == 2) {
            if (!"RANGE".equals(keyType(ks.get(1)))) {
                throw DynamoException.validation("Invalid KeySchema: The second KeySchemaElement is not a RANGE key");
            }
            range = keyElements(one(ks.get(1)), attrTypes, name).get(0);
            if (range.name().equals(hash.name())) {
                throw DynamoException.validation("Invalid KeySchema: Some index key attribute have no definition");
            }
        }

        String billing = opt(req, "BillingMode");
        if (billing != null && !billing.equals("PROVISIONED") && !billing.equals("PAY_PER_REQUEST")) {
            throw enumError(billing, "billingMode", "[PROVISIONED, PAY_PER_REQUEST]");
        }
        long rcu = 0, wcu = 0;
        boolean hasPt = LegacyParams.has(req, "ProvisionedThroughput");
        if ("PAY_PER_REQUEST".equals(billing)) {
            if (hasPt) {
                throw DynamoException.validation("One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST");
            }
        } else {
            if (!hasPt) {
                throw DynamoException.validation(billing == null ? "No provisioned throughput specified for the table"
                        : "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is PROVISIONED");
            }
            long[] pt = throughput(req.getAsJsonObject("ProvisionedThroughput"), "provisionedThroughput");
            rcu = pt[0];
            wcu = pt[1];
            billing = "PROVISIONED";
        }

        List<TableSchema.IndexDef> indexes = new ArrayList<>();
        Set<String> indexNames = new LinkedHashSet<>();
        if (LegacyParams.has(req, "LocalSecondaryIndexes")) {
            if (range == null) {
                throw DynamoException.validation("One or more parameter values were invalid: Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex");
            }
            JsonArray l = req.getAsJsonArray("LocalSecondaryIndexes");
            if (l.size() > MAX_LSI) {
                throw DynamoException.validation("One or more parameter values were invalid: Number of local secondary indexes exceeds per-table limit of " + MAX_LSI);
            }
            for (JsonElement e : l) {
                TableSchema.IndexDef d = parseIndex(name, e.getAsJsonObject(), true, attrTypes, hash, range, "PROVISIONED".equals(billing));
                if (!indexNames.add(d.name())) {
                    throw DynamoException.validation("One or more parameter values were invalid: Duplicate index name: " + d.name());
                }
                indexes.add(d);
            }
        }
        if (LegacyParams.has(req, "GlobalSecondaryIndexes")) {
            JsonArray g = req.getAsJsonArray("GlobalSecondaryIndexes");
            if (g.size() > MAX_GSI) {
                throw DynamoException.validation("One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of " + MAX_GSI);
            }
            for (JsonElement e : g) {
                TableSchema.IndexDef d = parseIndex(name, e.getAsJsonObject(), false, attrTypes, hash, range, "PROVISIONED".equals(billing));
                if (!indexNames.add(d.name())) {
                    throw DynamoException.validation("One or more parameter values were invalid: Duplicate index name: " + d.name());
                }
                indexes.add(d);
            }
        }
        // every AttributeDefinition must be used by a key
        Set<String> used = new LinkedHashSet<>();
        used.add(hash.name());
        if (range != null) used.add(range.name());
        for (TableSchema.IndexDef d : indexes) for (TableSchema.KeyAttr k : d.allKeys()) used.add(k.name());
        if (!used.equals(attrTypes.keySet())) {
            throw DynamoException.validation("One or more parameter values were invalid: Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions");
        }
        if (LegacyParams.has(req, "StreamSpecification") && req.getAsJsonObject("StreamSpecification").has("StreamEnabled")
                && req.getAsJsonObject("StreamSpecification").get("StreamEnabled").getAsBoolean()) {
            throw DynamoException.validation("DynamoDB Streams are not supported by Warp's dynamowire (StreamSpecification.StreamEnabled must be false or omitted)");
        }
        String tableClass = opt(req, "TableClass");
        if (tableClass != null && !tableClass.equals("STANDARD") && !tableClass.equals("STANDARD_INFREQUENT_ACCESS")) {
            throw enumError(tableClass, "tableClass", "[STANDARD, STANDARD_INFREQUENT_ACCESS]");
        }
        Map<String, String> tags = new LinkedHashMap<>();
        if (LegacyParams.has(req, "Tags")) tags.putAll(parseTags(req.getAsJsonArray("Tags")));

        long now = System.currentTimeMillis();
        TableSchema.Meta meta = new TableSchema.Meta(attrTypes, billing, rcu, wcu,
                LegacyParams.has(req, "DeletionProtectionEnabled") && req.get("DeletionProtectionEnabled").getAsBoolean(),
                tableClass == null ? "STANDARD" : tableClass, null, false, false, tags, indexes, UUID.randomUUID().toString(),
                "PAY_PER_REQUEST".equals(billing) ? now : 0);
        TableSchema schema = new TableSchema(name, hash.name(), hash.type(), range == null ? null : range.name(),
                range == null ? null : range.type(), "ACTIVE", now, meta);
        store.createTable(schema);
        JsonObject resp = new JsonObject();
        resp.add("TableDescription", describeJson(schema, new long[1 + indexes.size()], 0));
        return resp;
    }

    private static long[] throughput(JsonObject pt, String path) {
        long[] out = new long[2];
        String[] fields = {"ReadCapacityUnits", "WriteCapacityUnits"};
        for (int i = 0; i < 2; i++) {
            if (!pt.has(fields[i]) || pt.get(fields[i]).isJsonNull()) {
                throw DynamoException.validation("One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified");
            }
            long v = pt.get(fields[i]).getAsLong();
            if (v < 1) {
                throw DynamoException.validation("1 validation error detected: Value '" + v + "' at '" + path + "."
                        + Character.toLowerCase(fields[i].charAt(0)) + fields[i].substring(1)
                        + "' failed to satisfy constraint: Member must have value greater than or equal to 1");
            }
            out[i] = v;
        }
        return out;
    }

    private static Map<String, String> parseTags(JsonArray tags) {
        Map<String, String> out = new LinkedHashMap<>();
        for (JsonElement e : tags) {
            JsonObject t = e.getAsJsonObject();
            String k = opt(t, "Key"), v = opt(t, "Value");
            if (k == null || k.isEmpty() || k.length() > 128) {
                throw DynamoException.validation("1 validation error detected: Value '" + k + "' at 'tags.1.member.key' failed to satisfy constraint: Member must have length less than or equal to 128 and greater than or equal to 1");
            }
            if (v == null || v.length() > 256) {
                throw DynamoException.validation("1 validation error detected: Value '" + v + "' at 'tags.1.member.value' failed to satisfy constraint: Member must have length less than or equal to 256");
            }
            out.put(k, v);
        }
        return out;
    }

    // ------------------------------------------------------------------------------- Describe

    JsonObject describeJson(TableSchema s, long[] counts, long sizeBytes) {
        TableSchema.Meta m = s.meta();
        JsonObject t = new JsonObject();
        t.addProperty("TableName", s.tableName());
        t.addProperty("TableStatus", s.status());
        t.addProperty("CreationDateTime", s.creationTimeEpochMillis() / 1000.0);
        t.addProperty("ItemCount", counts[0]);
        t.addProperty("TableSizeBytes", sizeBytes);
        JsonArray keySchema = new JsonArray();
        keySchema.add(keySchemaEntry(s.partitionKeyName(), "HASH"));
        if (s.hasSortKey()) keySchema.add(keySchemaEntry(s.sortKeyName(), "RANGE"));
        t.add("KeySchema", keySchema);
        JsonArray attrDefs = new JsonArray();
        for (var e : m.attributeTypes().entrySet()) {
            JsonObject o = new JsonObject();
            o.addProperty("AttributeName", e.getKey());
            o.addProperty("AttributeType", e.getValue());
            attrDefs.add(o);
        }
        t.add("AttributeDefinitions", attrDefs);
        t.addProperty("TableArn", tableArn(s.tableName()));
        t.addProperty("TableId", m.tableId());
        JsonObject pt = new JsonObject();
        pt.addProperty("NumberOfDecreasesToday", 0);
        pt.addProperty("ReadCapacityUnits", m.readUnits());
        pt.addProperty("WriteCapacityUnits", m.writeUnits());
        t.add("ProvisionedThroughput", pt);
        if ("PAY_PER_REQUEST".equals(m.billingMode())) {
            JsonObject b = new JsonObject();
            b.addProperty("BillingMode", "PAY_PER_REQUEST");
            b.addProperty("LastUpdateToPayPerRequestDateTime", m.lastPayPerRequestMillis() / 1000.0);
            t.add("BillingModeSummary", b);
        }
        JsonArray gsi = new JsonArray(), lsi = new JsonArray();
        List<TableSchema.IndexDef> indexes = m.indexes();
        for (int i = 0; i < indexes.size(); i++) {
            TableSchema.IndexDef d = indexes.get(i);
            JsonObject o = new JsonObject();
            o.addProperty("IndexName", d.name());
            JsonArray iks = new JsonArray();
            for (TableSchema.KeyAttr k : d.hash()) iks.add(keySchemaEntry(k.name(), "HASH"));
            for (TableSchema.KeyAttr k : d.range()) iks.add(keySchemaEntry(k.name(), "RANGE"));
            o.add("KeySchema", iks);
            JsonObject proj = new JsonObject();
            proj.addProperty("ProjectionType", d.projectionType());
            if ("INCLUDE".equals(d.projectionType())) {
                JsonArray nk = new JsonArray();
                d.nonKeyAttributes().forEach(nk::add);
                proj.add("NonKeyAttributes", nk);
            }
            o.add("Projection", proj);
            if (!d.local()) {
                o.addProperty("IndexStatus", d.status());
                o.addProperty("Backfilling", false);
                JsonObject ipt = new JsonObject();
                ipt.addProperty("NumberOfDecreasesToday", 0);
                ipt.addProperty("ReadCapacityUnits", d.readUnits());
                ipt.addProperty("WriteCapacityUnits", d.writeUnits());
                o.add("ProvisionedThroughput", ipt);
            }
            o.addProperty("IndexSizeBytes", 0);
            o.addProperty("ItemCount", i + 1 < counts.length ? counts[i + 1] : 0);
            o.addProperty("IndexArn", tableArn(s.tableName()) + "/index/" + d.name());
            (d.local() ? lsi : gsi).add(o);
        }
        if (!gsi.isEmpty()) t.add("GlobalSecondaryIndexes", gsi);
        if (!lsi.isEmpty()) t.add("LocalSecondaryIndexes", lsi);
        t.addProperty("DeletionProtectionEnabled", m.deletionProtection());
        JsonObject tc = new JsonObject();
        tc.addProperty("TableClass", m.tableClass());
        t.add("TableClassSummary", tc);
        return t;
    }

    private static JsonObject keySchemaEntry(String name, String type) {
        JsonObject o = new JsonObject();
        o.addProperty("AttributeName", name);
        o.addProperty("KeyType", type);
        return o;
    }

    JsonObject describeTable(TableSchema s) {
        long[] counts = store.counts(s);
        JsonObject resp = new JsonObject();
        resp.add("Table", describeJson(s, counts, store.sizeBytes(s)));
        return resp;
    }

    JsonObject deleteTable(TableSchema s) {
        if (s.meta().deletionProtection()) {
            throw DynamoException.validation("Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.");
        }
        store.deleteTable(s.tableName());
        JsonObject desc = describeJson(s, new long[1 + s.meta().indexes().size()], 0);
        desc.addProperty("TableStatus", "DELETING");
        JsonObject resp = new JsonObject();
        resp.add("TableDescription", desc);
        return resp;
    }

    // ------------------------------------------------------------------------------- UpdateTable

    JsonObject updateTable(TableSchema s, JsonObject req) {
        TableSchema.Meta m = s.meta();
        boolean anything = false;
        if (LegacyParams.has(req, "ReplicaUpdates")) {
            throw DynamoException.validation("Global tables / replicas (ReplicaUpdates) are not supported by Warp's dynamowire");
        }
        if (LegacyParams.has(req, "StreamSpecification") && req.getAsJsonObject("StreamSpecification").has("StreamEnabled")
                && req.getAsJsonObject("StreamSpecification").get("StreamEnabled").getAsBoolean()) {
            throw DynamoException.validation("DynamoDB Streams are not supported by Warp's dynamowire");
        }
        // billing / throughput
        String billing = opt(req, "BillingMode");
        if (billing != null && !billing.equals("PROVISIONED") && !billing.equals("PAY_PER_REQUEST")) {
            throw enumError(billing, "billingMode", "[PROVISIONED, PAY_PER_REQUEST]");
        }
        if (billing != null || LegacyParams.has(req, "ProvisionedThroughput")) {
            anything = true;
            String target = billing != null ? billing : m.billingMode();
            if ("PAY_PER_REQUEST".equals(target)) {
                if (LegacyParams.has(req, "ProvisionedThroughput")) {
                    throw DynamoException.validation("One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST");
                }
                m = m.withBilling("PAY_PER_REQUEST", 0, 0, m.billingMode().equals("PAY_PER_REQUEST") ? m.lastPayPerRequestMillis() : System.currentTimeMillis());
            } else {
                if (!LegacyParams.has(req, "ProvisionedThroughput")) {
                    throw DynamoException.validation("One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is PROVISIONED");
                }
                long[] pt = throughput(req.getAsJsonObject("ProvisionedThroughput"), "provisionedThroughput");
                m = m.withBilling("PROVISIONED", pt[0], pt[1], m.lastPayPerRequestMillis());
            }
        }
        if (LegacyParams.has(req, "DeletionProtectionEnabled")) {
            anything = true;
            m = m.withDeletionProtection(req.get("DeletionProtectionEnabled").getAsBoolean());
        }
        if (LegacyParams.has(req, "TableClass")) {
            anything = true;
            m = m.withTableClass(req.get("TableClass").getAsString());
        }
        if (LegacyParams.has(req, "SSESpecification") || LegacyParams.has(req, "StreamSpecification")
                || LegacyParams.has(req, "OnDemandThroughput") || LegacyParams.has(req, "WarmThroughput")) {
            anything = true; // accepted, no effect
        }
        // secondary indexes
        List<TableSchema.IndexDef> indexes = new ArrayList<>(m.indexes());
        List<TableSchema.IndexDef> created = new ArrayList<>(), dropped = new ArrayList<>();
        if (LegacyParams.has(req, "GlobalSecondaryIndexUpdates")) {
            anything = true;
            Map<String, String> attrTypes = new LinkedHashMap<>(m.attributeTypes());
            if (LegacyParams.has(req, "AttributeDefinitions")) {
                for (var e : parseAttributeDefinitions(req).entrySet()) {
                    String prev = attrTypes.put(e.getKey(), e.getValue());
                    if (prev != null && !prev.equals(e.getValue())) {
                        throw DynamoException.validation("One or more parameter values were invalid: Attribute " + e.getKey()
                                + " is already defined with type " + prev);
                    }
                }
            }
            TableSchema.KeyAttr hash = new TableSchema.KeyAttr(s.partitionKeyName(), s.partitionKeyType());
            TableSchema.KeyAttr range = s.hasSortKey() ? new TableSchema.KeyAttr(s.sortKeyName(), s.sortKeyType()) : null;
            for (JsonElement e : req.getAsJsonArray("GlobalSecondaryIndexUpdates")) {
                JsonObject u = e.getAsJsonObject();
                if (u.has("Create")) {
                    JsonObject c = u.getAsJsonObject("Create");
                    TableSchema.IndexDef d = parseIndex(s.tableName(), c, false, attrTypes, hash, range, "PROVISIONED".equals(m.billingMode()));
                    for (TableSchema.IndexDef x : indexes) {
                        if (x.name().equals(d.name())) {
                            throw DynamoException.validation("One or more parameter values were invalid: Duplicate index name: " + d.name());
                        }
                    }
                    if (indexes.stream().filter(x -> !x.local()).count() >= MAX_GSI) {
                        throw new DynamoException("LimitExceededException", "GlobalSecondaryIndex count exceeds the per-table limit of " + MAX_GSI);
                    }
                    indexes.add(d);
                    created.add(d);
                } else if (u.has("Delete")) {
                    String n = opt(u.getAsJsonObject("Delete"), "IndexName");
                    TableSchema.IndexDef found = null;
                    for (TableSchema.IndexDef x : indexes) if (x.name().equals(n) && !x.local()) found = x;
                    if (found == null) {
                        throw new DynamoException("ResourceNotFoundException", "Requested resource not found");
                    }
                    indexes.remove(found);
                    dropped.add(found);
                } else if (u.has("Update")) {
                    String n = opt(u.getAsJsonObject("Update"), "IndexName");
                    TableSchema.IndexDef found = null;
                    for (TableSchema.IndexDef x : indexes) if (x.name().equals(n) && !x.local()) found = x;
                    if (found == null) {
                        throw new DynamoException("ResourceNotFoundException", "Requested resource not found");
                    }
                    if (LegacyParams.has(u.getAsJsonObject("Update"), "ProvisionedThroughput")) {
                        long[] pt = throughput(u.getAsJsonObject("Update").getAsJsonObject("ProvisionedThroughput"), "provisionedThroughput");
                        int at = indexes.indexOf(found);
                        indexes.set(at, new TableSchema.IndexDef(found.name(), false, found.hash(), found.range(), found.projectionType(),
                                found.nonKeyAttributes(), found.status(), found.pgName(), pt[0], pt[1]));
                    }
                } else {
                    throw DynamoException.validation("One or more parameter values were invalid: One of Create, Update or Delete must be specified in a GlobalSecondaryIndexUpdate");
                }
            }
            // drop attribute definitions no longer used, then verify all are used
            Set<String> used = new LinkedHashSet<>();
            used.add(s.partitionKeyName());
            if (s.hasSortKey()) used.add(s.sortKeyName());
            for (TableSchema.IndexDef d : indexes) for (TableSchema.KeyAttr k : d.allKeys()) used.add(k.name());
            Map<String, String> newTypes = new LinkedHashMap<>();
            for (var en : attrTypes.entrySet()) {
                if (used.contains(en.getKey())) newTypes.put(en.getKey(), en.getValue());
                else if (LegacyParams.has(req, "AttributeDefinitions") && !m.attributeTypes().containsKey(en.getKey())) {
                    throw DynamoException.validation("One or more parameter values were invalid: Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions");
                }
            }
            m = m.withAttributeTypes(newTypes).withIndexes(indexes);
        } else if (LegacyParams.has(req, "AttributeDefinitions")) {
            anything = true;
        }
        if (!anything) {
            throw DynamoException.validation("At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SseSpecification or ReplicaUpdates is required");
        }
        // DDL on every shard before the metadata makes the change visible
        if (!created.isEmpty() || !dropped.isEmpty()) {
            TableSchema withNew = s.withMeta(m);
            store.onAllShards(c -> {
                for (TableSchema.IndexDef d : dropped) store.dropIndexDdl(c, d);
                for (TableSchema.IndexDef d : created) store.createIndexDdl(c, withNew, d);
            });
        }
        TableSchema updated = store.updateMeta(s, m);
        JsonObject resp = new JsonObject();
        resp.add("TableDescription", describeJson(updated, store.counts(updated), store.sizeBytes(updated)));
        return resp;
    }

    // ------------------------------------------------------------------------------- ListTables

    JsonObject listTables(JsonObject req) {
        int limit = 100;
        if (LegacyParams.has(req, "Limit")) {
            limit = req.get("Limit").getAsInt();
            if (limit < 1) {
                throw DynamoException.validation("1 validation error detected: Value '" + limit
                        + "' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1");
            }
            if (limit > 100) {
                throw DynamoException.validation("1 validation error detected: Value '" + limit
                        + "' at 'limit' failed to satisfy constraint: Member must have value less than or equal to 100");
            }
        }
        String start = opt(req, "ExclusiveStartTableName");
        List<String> names = store.listTables(start, limit + 1);
        JsonObject resp = new JsonObject();
        JsonArray arr = new JsonArray();
        for (int i = 0; i < Math.min(limit, names.size()); i++) arr.add(names.get(i));
        resp.add("TableNames", arr);
        if (names.size() > limit) {
            resp.addProperty("LastEvaluatedTableName", names.get(limit - 1));
        }
        return resp;
    }

    // ------------------------------------------------------------------------------- TTL

    JsonObject describeTimeToLive(TableSchema s) {
        JsonObject d = new JsonObject();
        d.addProperty("TimeToLiveStatus", s.meta().ttlEnabled() ? "ENABLED" : "DISABLED");
        if (s.meta().ttlEnabled()) d.addProperty("AttributeName", s.meta().ttlAttribute());
        JsonObject resp = new JsonObject();
        resp.add("TimeToLiveDescription", d);
        return resp;
    }

    JsonObject updateTimeToLive(TableSchema s, JsonObject req) {
        JsonObject spec = LegacyParams.has(req, "TimeToLiveSpecification") ? req.getAsJsonObject("TimeToLiveSpecification") : null;
        if (spec == null || !spec.has("Enabled") || !LegacyParams.has(spec, "AttributeName")) {
            throw DynamoException.validation("1 validation error detected: Value null at 'timeToLiveSpecification' failed to satisfy constraint: Member must not be null");
        }
        boolean enable = spec.get("Enabled").getAsBoolean();
        String attr = spec.get("AttributeName").getAsString();
        if (attr.isEmpty() || attr.length() > 255) {
            throw DynamoException.validation("1 validation error detected: Value '" + attr
                    + "' at 'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must have length less than or equal to 255 and greater than or equal to 1");
        }
        if (enable && s.meta().ttlEnabled()) {
            throw DynamoException.validation("TimeToLive is already enabled");
        }
        if (!enable && !s.meta().ttlEnabled()) {
            throw DynamoException.validation("TimeToLive is already disabled");
        }
        TableSchema next = s.withMeta(s.meta().withTtl(enable ? attr : null, enable));
        store.onAllShards(c -> {
            if (!PgItemStore.isPostgres(c)) return;
            if (enable) store.createTtlIndexDdl(c, next);
            else store.dropTtlIndexDdl(c, s);
        });
        store.updateMeta(s, next.meta());
        JsonObject out = new JsonObject();
        JsonObject echo = new JsonObject();
        echo.addProperty("Enabled", enable);
        echo.addProperty("AttributeName", attr);
        out.add("TimeToLiveSpecification", echo);
        return out;
    }

    // ------------------------------------------------------------------------------- PITR

    JsonObject describeContinuousBackups(TableSchema s) {
        JsonObject d = new JsonObject();
        d.addProperty("ContinuousBackupsStatus", "ENABLED");
        JsonObject p = new JsonObject();
        boolean on = s.meta().pitrEnabled();
        p.addProperty("PointInTimeRecoveryStatus", on ? "ENABLED" : "DISABLED");
        if (on) {
            p.addProperty("RecoveryPeriodInDays", 35);
            p.addProperty("EarliestRestorableDateTime", System.currentTimeMillis() / 1000.0 - 60);
            p.addProperty("LatestRestorableDateTime", System.currentTimeMillis() / 1000.0);
        }
        d.add("PointInTimeRecoveryDescription", p);
        JsonObject resp = new JsonObject();
        resp.add("ContinuousBackupsDescription", d);
        return resp;
    }

    JsonObject updateContinuousBackups(TableSchema s, JsonObject req) {
        JsonObject spec = LegacyParams.has(req, "PointInTimeRecoverySpecification") ? req.getAsJsonObject("PointInTimeRecoverySpecification") : null;
        if (spec == null || !spec.has("PointInTimeRecoveryEnabled")) {
            throw DynamoException.validation("1 validation error detected: Value null at 'pointInTimeRecoverySpecification' failed to satisfy constraint: Member must not be null");
        }
        boolean on = spec.get("PointInTimeRecoveryEnabled").getAsBoolean();
        TableSchema updated = store.updateMeta(s, s.meta().withPitr(on));
        return describeContinuousBackups(updated);
    }

    // ------------------------------------------------------------------------------- tags

    /** The table an ARN names; ValidationException for a malformed ARN, AccessDenied for a missing table. */
    TableSchema tableForArn(String arn, String operation) {
        if (arn == null) {
            throw DynamoException.validation("1 validation error detected: Value null at 'resourceArn' failed to satisfy constraint: Member must not be null");
        }
        String[] p = arn.split(":", 6);
        if (p.length != 6 || !p[0].equals("arn") || !p[2].equals("dynamodb") || !p[5].startsWith("table/")
                || !p[4].matches("\\d{12}") || p[3].isEmpty()) {
            throw DynamoException.validation("Invalid TableArn: Invalid ResourceArn provided as input " + arn);
        }
        String rest = p[5].substring("table/".length());
        String name = rest.contains("/") ? rest.substring(0, rest.indexOf('/')) : rest;
        try {
            return store.describeTable(name);
        } catch (DynamoException e) {
            if (e.dynamoErrorType.equals("ResourceNotFoundException")) {
                throw new DynamoException("AccessDeniedException", "User: arn:aws:iam::" + p[4] + ":root is not authorized to perform: dynamodb:"
                        + operation + " on resource: " + arn + " because no resource-based policy allows the dynamodb:" + operation + " action");
            }
            throw e;
        }
    }

    JsonObject tagResource(JsonObject req) {
        TableSchema s = tableForArn(opt(req, "ResourceArn"), "TagResource");
        if (!LegacyParams.has(req, "Tags") || req.getAsJsonArray("Tags").isEmpty()) {
            throw DynamoException.validation("1 validation error detected: Value '[]' at 'tags' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        Map<String, String> tags = new LinkedHashMap<>(s.meta().tags());
        tags.putAll(parseTags(req.getAsJsonArray("Tags")));
        if (tags.size() > 50) {
            throw DynamoException.validation("Too many tags: a table can have at most 50 tags");
        }
        store.updateMeta(s, s.meta().withTags(tags));
        return new JsonObject();
    }

    JsonObject untagResource(JsonObject req) {
        TableSchema s = tableForArn(opt(req, "ResourceArn"), "UntagResource");
        Map<String, String> tags = new LinkedHashMap<>(s.meta().tags());
        if (LegacyParams.has(req, "TagKeys")) {
            for (JsonElement e : req.getAsJsonArray("TagKeys")) tags.remove(e.getAsString());
        }
        store.updateMeta(s, s.meta().withTags(tags));
        return new JsonObject();
    }

    JsonObject listTagsOfResource(JsonObject req) {
        TableSchema s = tableForArn(opt(req, "ResourceArn"), "ListTagsOfResource");
        JsonArray arr = new JsonArray();
        s.meta().tags().forEach((k, v) -> {
            JsonObject t = new JsonObject();
            t.addProperty("Key", k);
            t.addProperty("Value", v);
            arr.add(t);
        });
        JsonObject resp = new JsonObject();
        resp.add("Tags", arr);
        return resp;
    }

    JsonObject describeLimits() {
        JsonObject o = new JsonObject();
        o.addProperty("AccountMaxReadCapacityUnits", 80000);
        o.addProperty("AccountMaxWriteCapacityUnits", 80000);
        o.addProperty("TableMaxReadCapacityUnits", 40000);
        o.addProperty("TableMaxWriteCapacityUnits", 40000);
        return o;
    }
}
