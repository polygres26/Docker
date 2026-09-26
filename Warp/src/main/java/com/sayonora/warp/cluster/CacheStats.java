package com.sayonora.warp.cluster;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Real hit/miss/invalidation counters for {@link CacheStage}, node-local and in-memory (they reset on
 * restart and are not aggregated across cluster nodes). Two tiers are counted here: the query-result
 * cache and the generic primary-key row cache. The shared {@link RowCache} (dynamowire/mongowire
 * point lookups) counts its own hits and misses. Recorded at the existing hit/miss/invalidate sites, so
 * the counters cost one {@link LongAdder} increment per cache lookup.
 */
public final class CacheStats {

    /** Distinct tables tracked; further tables fold into {@link #OTHER_TABLES}. */
    static final int TABLE_CAP = 200;
    static final String OTHER_TABLES = "(other tables)";
    static final int RECENT_CAP = 50;

    public static final String TIER_RESULT = "result";
    public static final String TIER_PK = "pk";

    public record TableStat(String table, long hits, long misses) {
    }

    /** One invalidation that dropped something, or a full clear. {@code entries} is the number of result-cache entries removed. */
    public record Invalidation(Instant at, String kind, String target, long entries, String source) {
    }

    public record Snapshot(long resultHits, long resultMisses, long pkHits, long pkMisses, long invalidatedEntries,
            long invalidationEvents, long fullClears, List<TableStat> byTable, List<Invalidation> recent) {
    }

    private static final class TableEntry {
        final LongAdder hits = new LongAdder();
        final LongAdder misses = new LongAdder();
    }

    private final LongAdder rowServed = new LongAdder();
    private final LongAdder resultHits = new LongAdder();
    private final LongAdder resultMisses = new LongAdder();
    private final LongAdder pkHits = new LongAdder();
    private final LongAdder pkMisses = new LongAdder();
    private final LongAdder invalidatedEntries = new LongAdder();
    private final LongAdder invalidationEvents = new LongAdder();
    private final LongAdder fullClears = new LongAdder();
    private final ConcurrentHashMap<String, TableEntry> byTable = new ConcurrentHashMap<>();
    private final Deque<Invalidation> recent = new ArrayDeque<>();

    /** A statement answered from the shared row cache by CacheStage's SQL fast path. */
    public void rowServed() {
        rowServed.increment();
    }

    /** Statements CacheStage answered without executing them (they never reach the statistics stage). */
    public long servedFromCache() {
        return resultHits.sum() + pkHits.sum() + rowServed.sum();
    }

    public void hit(String tier, String table) {
        (TIER_PK.equals(tier) ? pkHits : resultHits).increment();
        tableEntry(table).hits.increment();
    }

    public void miss(String tier, String table) {
        (TIER_PK.equals(tier) ? pkMisses : resultMisses).increment();
        tableEntry(table).misses.increment();
    }

    /** A table-level invalidation; only recorded when it actually removed result-cache entries. */
    public void tableInvalidated(String table, long removed, String source) {
        if (removed <= 0) {
            return;
        }
        invalidatedEntries.add(removed);
        invalidationEvents.increment();
        push(new Invalidation(Instant.now(), "table", table, removed, source));
    }

    public void cleared(long removed, String reason) {
        fullClears.increment();
        invalidatedEntries.add(Math.max(0, removed));
        push(new Invalidation(Instant.now(), "clear", "all tables", Math.max(0, removed), reason));
    }

    private synchronized void push(Invalidation event) {
        recent.addFirst(event);
        while (recent.size() > RECENT_CAP) {
            recent.removeLast();
        }
    }

    private TableEntry tableEntry(String table) {
        String key = table == null || table.isBlank() ? OTHER_TABLES : table.toLowerCase(java.util.Locale.ROOT);
        TableEntry e = byTable.get(key);
        if (e == null) {
            if (byTable.size() >= TABLE_CAP) {
                key = OTHER_TABLES;
            }
            e = byTable.computeIfAbsent(key, k -> new TableEntry());
        }
        return e;
    }

    public synchronized Snapshot snapshot() {
        List<TableStat> tables = new ArrayList<>();
        for (Map.Entry<String, TableEntry> e : byTable.entrySet()) {
            tables.add(new TableStat(e.getKey(), e.getValue().hits.sum(), e.getValue().misses.sum()));
        }
        tables.sort(Comparator.comparingLong((TableStat t) -> t.hits() + t.misses()).reversed());
        return new Snapshot(resultHits.sum(), resultMisses.sum(), pkHits.sum(), pkMisses.sum(),
                invalidatedEntries.sum(), invalidationEvents.sum(), fullClears.sum(), tables, List.copyOf(recent));
    }
}
