package com.sayonora.wire.dynamowire;

import com.google.gson.JsonObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ConsumedCapacity accounting with DynamoDB's unit rules: reads are charged per 4 KB (half a unit
 * for an eventually consistent read, double for transactions), writes per 1 KB. Warp has no
 * provisioned throughput to throttle against; the numbers exist so SDKs and tooling that look at
 * ConsumedCapacity behave as against the real service.
 */
final class Capacity {

    private Capacity() {}

    static double readUnits(long bytes, boolean consistent, boolean transactional) {
        double units = Math.max(1, Math.ceil(bytes / 4096.0));
        if (transactional) return units * 2;
        return consistent ? units : units / 2;
    }

    static double writeUnits(long bytes, boolean transactional) {
        double units = Math.max(1, Math.ceil(bytes / 1024.0));
        return transactional ? units * 2 : units;
    }

    /** Per-table accumulator for one request. */
    static final class Usage {
        final String table;
        double tableUnits;
        final Map<String, Double> gsi = new LinkedHashMap<>();
        final Map<String, Double> lsi = new LinkedHashMap<>();

        Usage(String table) {
            this.table = table;
        }

        double total() {
            double t = tableUnits;
            for (double d : gsi.values()) t += d;
            for (double d : lsi.values()) t += d;
            return t;
        }

        void addIndex(TableSchema.IndexDef idx, double units) {
            (idx.local() ? lsi : gsi).merge(idx.name(), units, Double::sum);
        }

        void add(Usage other) {
            tableUnits += other.tableUnits;
            other.gsi.forEach((k, v) -> gsi.merge(k, v, Double::sum));
            other.lsi.forEach((k, v) -> lsi.merge(k, v, Double::sum));
        }
    }

    static boolean requested(String mode) {
        return "TOTAL".equals(mode) || "INDEXES".equals(mode);
    }

    static JsonObject toJson(String mode, Usage u) {
        JsonObject o = new JsonObject();
        o.addProperty("TableName", u.table);
        o.addProperty("CapacityUnits", u.total());
        if ("INDEXES".equals(mode)) {
            JsonObject t = new JsonObject();
            t.addProperty("CapacityUnits", u.tableUnits);
            o.add("Table", t);
            if (!u.gsi.isEmpty()) o.add("GlobalSecondaryIndexes", units(u.gsi));
            if (!u.lsi.isEmpty()) o.add("LocalSecondaryIndexes", units(u.lsi));
        }
        return o;
    }

    private static JsonObject units(Map<String, Double> m) {
        JsonObject o = new JsonObject();
        m.forEach((k, v) -> {
            JsonObject e = new JsonObject();
            e.addProperty("CapacityUnits", v);
            o.add(k, e);
        });
        return o;
    }

    /** Write cost of replacing {@code old} with {@code neu} (either may be null) in {@code s}. */
    static Usage writeUsage(TableSchema s, Map<String, AttributeValue> old, Map<String, AttributeValue> neu, boolean tx) {
        Usage u = new Usage(s.tableName());
        long tableBytes = Math.max(old == null ? 0 : AttributeValue.itemSize(old), neu == null ? 0 : AttributeValue.itemSize(neu));
        u.tableUnits = writeUnits(tableBytes, tx);
        for (TableSchema.IndexDef idx : s.meta().indexes()) {
            boolean inOld = old != null && inIndex(idx, old), inNew = neu != null && inIndex(idx, neu);
            if (!inOld && !inNew) continue;
            long bytes = 0;
            if (inOld) bytes = Math.max(bytes, AttributeValue.itemSize(QueryEngine.projectForIndex(s, idx, old)));
            if (inNew) bytes = Math.max(bytes, AttributeValue.itemSize(QueryEngine.projectForIndex(s, idx, neu)));
            u.addIndex(idx, writeUnits(bytes, tx));
        }
        return u;
    }

    static boolean inIndex(TableSchema.IndexDef idx, Map<String, AttributeValue> item) {
        for (TableSchema.KeyAttr k : idx.presenceKeys()) if (!item.containsKey(k.name())) return false;
        return true;
    }
}
