package com.sayonora.wire.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.core.BackendRegistry;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure unit coverage (no Postgres, no Ignite) of the notify-payload-to-invalidation mapping in
 * {@link CacheInvalidationListener}: what a trigger sends is parsed into exactly the {@code
 * (schema, table)} / {@code (physicalTable, keys)} calls the real {@link CacheStage}/{@link
 * RowCache} would receive. The listener's socket/reconnect behaviour is covered end-to-end by
 * {@code OutOfBandCacheInvalidationIntegrationTest} instead.
 */
class CacheInvalidationPayloadTest {

    /** Records every call, and answers {@code rowCacheKeyFor} the way CacheStage would for a
     * dynamowire table (bare lower-case name) or a mongowire table (case-preserved db.collection). */
    private static final class FakeTarget implements CacheInvalidationListener.Target {
        final List<String> calls = new ArrayList<>();
        String dynamoTable;
        String mongoTable;

        @Override
        public void invalidateTable(String schema, String table) {
            calls.add("table:" + schema + "." + table);
        }

        @Override
        public String rowCacheKeyFor(String schema, String table) {
            if (dynamoTable != null && dynamoTable.equalsIgnoreCase(table)) {
                return table.toLowerCase(java.util.Locale.ROOT);
            }
            if (mongoTable != null && schema != null && mongoTable.equalsIgnoreCase(schema + "." + table)) {
                return schema + "." + table;
            }
            return null;
        }

        @Override
        public void invalidateRows(String physicalTable, List<String[]> keys) {
            StringBuilder sb = new StringBuilder("rows:" + physicalTable + ":");
            for (String[] k : keys) {
                sb.append(RowCache.key(physicalTable, k[0], k[1])).append(';');
            }
            calls.add(sb.toString());
        }

        @Override
        public void clearAll(String reason) {
            calls.add("clearAll");
        }
    }

    private static CacheInvalidationListener listener(FakeTarget target) {
        return new CacheInvalidationListener(BackendRegistry.fromConfig(null, null, null, null, null, java.util.Map.of()), target);
    }

    @Test
    void parsesTableLevelRowsLevelAndTruncatePayloads() {
        var table = CacheInvalidationListener.Payload.parse("{\"v\" : 1, \"schema\" : \"public\", \"table\" : \"orders\", \"op\" : \"UPDATE\"}");
        assertNotNull(table);
        assertEquals("public", table.schema());
        assertEquals("orders", table.table());
        assertEquals("UPDATE", table.op());
        assertNull(table.keys());

        var rows = CacheInvalidationListener.Payload.parse(
                "{\"v\":1,\"schema\":\"public\",\"table\":\"dynamo_item_orders\",\"op\":\"UPDATE\",\"keys\":[[\"pk1\",\"sk1\"],[\"pk2\",null]]}");
        assertNotNull(rows);
        assertEquals(2, rows.keys().size());
        assertEquals("pk1", rows.keys().get(0)[0]);
        assertEquals("sk1", rows.keys().get(0)[1]);
        assertEquals("pk2", rows.keys().get(1)[0]);
        assertNull(rows.keys().get(1)[1]);

        var degraded = CacheInvalidationListener.Payload.parse(
                "{\"v\":1,\"schema\":\"public\",\"table\":\"dynamo_item_orders\",\"op\":\"UPDATE\",\"keys\":null}");
        assertNull(degraded.keys(), "keys:null is the degraded whole-table form");

        var truncate = CacheInvalidationListener.Payload.parse("{\"v\":1,\"schema\":\"public\",\"table\":\"t\",\"op\":\"TRUNCATE\"}");
        assertEquals("TRUNCATE", truncate.op());

        assertNull(CacheInvalidationListener.Payload.parse("not json at all"));
        assertNull(CacheInvalidationListener.Payload.parse("[1,2,3]"));
        assertNull(CacheInvalidationListener.Payload.parse("{\"v\":1,\"schema\":\"public\"}"), "no table -> unparseable");
        assertNull(CacheInvalidationListener.Payload.parse(""));
    }

    @Test
    void tableLevelNotifyInvalidatesTheResultCacheForBothSpellings() {
        FakeTarget target = new FakeTarget();
        try (CacheInvalidationListener l = listener(target)) {
            l.apply("default", "{\"v\":1,\"schema\":\"public\",\"table\":\"Orders\",\"op\":\"INSERT\"}");
        }
        // The listener hands schema+table over untouched; CacheStage#invalidateTable is what
        // removes both the bare and the schema-qualified (lower-cased) index entries.
        assertEquals(List.of("table:public.Orders"), target.calls);
    }

    @Test
    void malformedPayloadFallsBackToTableOrFullClear() {
        FakeTarget target = new FakeTarget();
        try (CacheInvalidationListener l = listener(target)) {
            // Truncated JSON with a readable table -> that table only.
            l.apply("default", "{\"v\":1,\"schema\":\"public\",\"table\":\"orders\",\"op\":\"UPD");
            // Nothing readable at all -> the conservative full clear.
            l.apply("default", "garbage");
            l.apply("default", null);
        }
        assertEquals(List.of("table:null.orders", "clearAll", "clearAll"), target.calls);
    }

    @Test
    void rowsLevelNotifyOnADynamowireTableInvalidatesExactRowCacheKeys() {
        FakeTarget target = new FakeTarget();
        target.dynamoTable = "dynamo_item_orders";
        try (CacheInvalidationListener l = listener(target)) {
            l.apply("default", "{\"v\":1,\"schema\":\"public\",\"table\":\"dynamo_item_orders\",\"op\":\"UPDATE\","
                    + "\"keys\":[[\"item-42\",\"\"],[\"item-43\",\"s1\"]]}");
        }
        assertEquals(List.of(
                "table:public.dynamo_item_orders",
                // dynamowire's own key shape: bare physical table, sk "" for a no-sort-key row --
                // exactly what OperationHandlers#cacheKeyFor / CacheStage#tryRowCacheLookup compute.
                "rows:dynamo_item_orders:dynamo_item_orders|item-42|;dynamo_item_orders|item-43|s1;"),
                target.calls);
    }

    @Test
    void rowsLevelNotifyOnAMongowireTableInvalidatesBothCaseSpellings() {
        FakeTarget target = new FakeTarget();
        target.mongoTable = "Test.Orders";
        try (CacheInvalidationListener l = listener(target)) {
            l.apply("default", "{\"v\":1,\"schema\":\"Test\",\"table\":\"Orders\",\"op\":\"DELETE\",\"keys\":[[\"\\\"item-42\\\"\",null]]}");
        }
        assertEquals(List.of(
                "table:Test.Orders",
                // MongoCommandDispatcher keys db.collection case-preserved ...
                "rows:Test.Orders:Test.Orders|\"item-42\"|;",
                // ... while CacheStage's SQL fast path lower-cases the same pair.
                "rows:test.orders:test.orders|\"item-42\"|;"),
                target.calls);
    }

    @Test
    void keylessNotifyOnARowCachedTableClearsTheRowCache() {
        FakeTarget target = new FakeTarget();
        target.dynamoTable = "dynamo_item_orders";
        try (CacheInvalidationListener l = listener(target)) {
            l.apply("default", "{\"v\":1,\"schema\":\"public\",\"table\":\"dynamo_item_orders\",\"op\":\"TRUNCATE\"}");
        }
        assertEquals(List.of("table:public.dynamo_item_orders", "clearAll"), target.calls);
    }

    @Test
    void aTableNoRowCacheKnowsAboutOnlyTouchesTheResultCache() {
        FakeTarget target = new FakeTarget();
        target.dynamoTable = "dynamo_item_orders";
        try (CacheInvalidationListener l = listener(target)) {
            l.apply("default", "{\"v\":1,\"schema\":\"public\",\"table\":\"orders\",\"op\":\"UPDATE\",\"keys\":[[\"1\",null]]}");
        }
        assertEquals(List.of("table:public.orders"), target.calls);
        assertTrue(target.calls.stream().noneMatch(c -> c.startsWith("rows:")));
    }
}
