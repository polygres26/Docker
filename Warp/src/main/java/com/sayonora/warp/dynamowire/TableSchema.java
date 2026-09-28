package com.sayonora.warp.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A DynamoDB table as dynamowire knows it: the primary key (the only part the item storage layout
 * depends on) plus {@link Meta} (attribute definitions, secondary indexes, billing, TTL, tags, ...),
 * which is persisted as one JSON document alongside the catalog row.
 */
public record TableSchema(
        String tableName,
        String partitionKeyName,
        String partitionKeyType,
        String sortKeyName,
        String sortKeyType,
        String status,
        long creationTimeEpochMillis,
        Meta meta) {

    public TableSchema(String tableName, String partitionKeyName, String partitionKeyType, String sortKeyName,
            String sortKeyType, String status, long creationTimeEpochMillis) {
        this(tableName, partitionKeyName, partitionKeyType, sortKeyName, sortKeyType, status, creationTimeEpochMillis,
                Meta.defaults(tableName, partitionKeyName, partitionKeyType, sortKeyName, sortKeyType, creationTimeEpochMillis));
    }

    public boolean hasSortKey() {
        return sortKeyName != null;
    }

    public TableSchema withMeta(Meta m) {
        return new TableSchema(tableName, partitionKeyName, partitionKeyType, sortKeyName, sortKeyType, status,
                creationTimeEpochMillis, m);
    }

    public IndexDef index(String name) {
        for (IndexDef i : meta.indexes()) if (i.name().equals(name)) return i;
        return null;
    }

    public boolean hasLocalIndex() {
        for (IndexDef i : meta.indexes()) if (i.local()) return true;
        return false;
    }

    /** Type (S/N/B) of an attribute that is declared in AttributeDefinitions, else null. */
    public String attributeType(String attr) {
        return meta.attributeTypes().get(attr);
    }

    // ------------------------------------------------------------------------------- pieces

    public record KeyAttr(String name, String type) {}

    /**
     * A secondary index. GSIs may have several HASH and RANGE attributes (multi-attribute keys); an
     * LSI has the table's partition key plus one RANGE attribute.
     */
    public record IndexDef(String name, boolean local, List<KeyAttr> hash, List<KeyAttr> range,
            String projectionType, List<String> nonKeyAttributes, String status, String pgName,
            long readUnits, long writeUnits) {

        /** Attributes an item must carry to be part of the index (an LSI's partition key is always there). */
        public List<KeyAttr> presenceKeys() {
            return local ? range : allKeys();
        }

        public List<KeyAttr> allKeys() {
            List<KeyAttr> l = new ArrayList<>(hash);
            l.addAll(range);
            return l;
        }

        public JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("name", name);
            o.addProperty("local", local);
            o.add("hash", keyArr(hash));
            o.add("range", keyArr(range));
            o.addProperty("projection", projectionType);
            JsonArray nk = new JsonArray();
            for (String s : nonKeyAttributes) nk.add(s);
            o.add("nonKey", nk);
            o.addProperty("status", status);
            o.addProperty("pg", pgName);
            o.addProperty("rcu", readUnits);
            o.addProperty("wcu", writeUnits);
            return o;
        }

        static IndexDef fromJson(JsonObject o) {
            List<String> nk = new ArrayList<>();
            for (JsonElement e : o.getAsJsonArray("nonKey")) nk.add(e.getAsString());
            return new IndexDef(o.get("name").getAsString(), o.get("local").getAsBoolean(),
                    keyList(o.getAsJsonArray("hash")), keyList(o.getAsJsonArray("range")),
                    o.get("projection").getAsString(), nk, o.get("status").getAsString(), o.get("pg").getAsString(),
                    o.has("rcu") ? o.get("rcu").getAsLong() : 0, o.has("wcu") ? o.get("wcu").getAsLong() : 0);
        }

        private static JsonArray keyArr(List<KeyAttr> l) {
            JsonArray a = new JsonArray();
            for (KeyAttr k : l) {
                JsonObject o = new JsonObject();
                o.addProperty("n", k.name());
                o.addProperty("t", k.type());
                a.add(o);
            }
            return a;
        }

        private static List<KeyAttr> keyList(JsonArray a) {
            List<KeyAttr> l = new ArrayList<>();
            for (JsonElement e : a) l.add(new KeyAttr(e.getAsJsonObject().get("n").getAsString(),
                    e.getAsJsonObject().get("t").getAsString()));
            return l;
        }

        public IndexDef withStatus(String s) {
            return new IndexDef(name, local, hash, range, projectionType, nonKeyAttributes, s, pgName, readUnits, writeUnits);
        }
    }

    public record Meta(
            Map<String, String> attributeTypes,
            String billingMode,
            long readUnits,
            long writeUnits,
            boolean deletionProtection,
            String tableClass,
            String ttlAttribute,
            boolean ttlEnabled,
            boolean pitrEnabled,
            Map<String, String> tags,
            List<IndexDef> indexes,
            String tableId,
            long lastPayPerRequestMillis) {

        /** Metadata of a table with only a primary key (what a catalog row written by an older version means). */
        public static Meta defaults(String tableName, String pkName, String pkType, String skName, String skType, long createdMillis) {
            Map<String, String> at = new LinkedHashMap<>();
            at.put(pkName, pkType);
            if (skName != null) at.put(skName, skType);
            return new Meta(at, "PAY_PER_REQUEST", 0, 0, false, "STANDARD", null, false, false, new LinkedHashMap<>(),
                    List.of(), UUID.nameUUIDFromBytes(tableName.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString(), createdMillis);
        }

        /** Test convenience. */
        public static Meta defaults(String pkName, String pkType, String skName, String skType) {
            return defaults("table", pkName, pkType, skName, skType, 0);
        }

        public Meta withIndexes(List<IndexDef> l) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, deletionProtection, tableClass,
                    ttlAttribute, ttlEnabled, pitrEnabled, tags, Collections.unmodifiableList(l), tableId, lastPayPerRequestMillis);
        }

        public Meta withAttributeTypes(Map<String, String> at) {
            return new Meta(at, billingMode, readUnits, writeUnits, deletionProtection, tableClass, ttlAttribute,
                    ttlEnabled, pitrEnabled, tags, indexes, tableId, lastPayPerRequestMillis);
        }

        public Meta withBilling(String mode, long rcu, long wcu, long lastPayPerRequest) {
            return new Meta(attributeTypes, mode, rcu, wcu, deletionProtection, tableClass, ttlAttribute, ttlEnabled,
                    pitrEnabled, tags, indexes, tableId, lastPayPerRequest);
        }

        public Meta withDeletionProtection(boolean b) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, b, tableClass, ttlAttribute, ttlEnabled,
                    pitrEnabled, tags, indexes, tableId, lastPayPerRequestMillis);
        }

        public Meta withTableClass(String c) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, deletionProtection, c, ttlAttribute,
                    ttlEnabled, pitrEnabled, tags, indexes, tableId, lastPayPerRequestMillis);
        }

        public Meta withTtl(String attr, boolean enabled) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, deletionProtection, tableClass, attr,
                    enabled, pitrEnabled, tags, indexes, tableId, lastPayPerRequestMillis);
        }

        public Meta withPitr(boolean b) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, deletionProtection, tableClass,
                    ttlAttribute, ttlEnabled, b, tags, indexes, tableId, lastPayPerRequestMillis);
        }

        public Meta withTags(Map<String, String> t) {
            return new Meta(attributeTypes, billingMode, readUnits, writeUnits, deletionProtection, tableClass,
                    ttlAttribute, ttlEnabled, pitrEnabled, t, indexes, tableId, lastPayPerRequestMillis);
        }

        public JsonObject toJson() {
            JsonObject o = new JsonObject();
            JsonObject at = new JsonObject();
            attributeTypes.forEach(at::addProperty);
            o.add("attrs", at);
            o.addProperty("billing", billingMode);
            o.addProperty("rcu", readUnits);
            o.addProperty("wcu", writeUnits);
            o.addProperty("delProt", deletionProtection);
            o.addProperty("class", tableClass);
            if (ttlAttribute != null) o.addProperty("ttlAttr", ttlAttribute);
            o.addProperty("ttlOn", ttlEnabled);
            o.addProperty("pitr", pitrEnabled);
            JsonObject tg = new JsonObject();
            tags.forEach(tg::addProperty);
            o.add("tags", tg);
            JsonArray ix = new JsonArray();
            for (IndexDef i : indexes) ix.add(i.toJson());
            o.add("indexes", ix);
            o.addProperty("id", tableId);
            o.addProperty("ppr", lastPayPerRequestMillis);
            return o;
        }

        public static Meta fromJson(JsonObject o, Meta fallback) {
            Map<String, String> at = new LinkedHashMap<>();
            o.getAsJsonObject("attrs").entrySet().forEach(e -> at.put(e.getKey(), e.getValue().getAsString()));
            Map<String, String> tg = new LinkedHashMap<>();
            o.getAsJsonObject("tags").entrySet().forEach(e -> tg.put(e.getKey(), e.getValue().getAsString()));
            List<IndexDef> ix = new ArrayList<>();
            for (JsonElement e : o.getAsJsonArray("indexes")) ix.add(IndexDef.fromJson(e.getAsJsonObject()));
            return new Meta(at, o.get("billing").getAsString(), o.get("rcu").getAsLong(), o.get("wcu").getAsLong(),
                    o.get("delProt").getAsBoolean(), o.get("class").getAsString(),
                    o.has("ttlAttr") ? o.get("ttlAttr").getAsString() : null, o.get("ttlOn").getAsBoolean(),
                    o.get("pitr").getAsBoolean(), tg, ix, o.get("id").getAsString(), o.get("ppr").getAsLong());
        }
    }
}
