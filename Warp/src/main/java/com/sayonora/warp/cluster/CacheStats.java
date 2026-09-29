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

    /** Why a SELECT that reached {@link CacheStage#handle} never attempted ANY of its three cache
     * tiers at all -- distinct from a real {@link #miss}, which means a tier WAS tried and the key
     * wasn't there. Previously there was no signal at all for this case (the statement just fell
     * through to {@code next.proceed}); this is the real "why wasn't this cached?" answer for the
     * single largest, previously-invisible gap in cache transparency. */
    public static final String BYPASS_NOT_CONFIGURED = "not_configured";
    public static final String BYPASS_TABLE_NOT_CACHED = "table_not_cached";

    public record TableStat(String table, long hits, long misses) {
    }

    public record BypassReasonStat(String reason, long count) {
    }

    public record BypassTableStat(String table, long count) {
    }

    /** One invalidation that dropped something, or a full clear. {@code entries} is the number of result-cache entries removed. */
    public record Invalidation(Instant at, String kind, String target, long entries, String source) {
    }

    public record Snapshot(long resultHits, long resultMisses, long pkHits, long pkMisses, long invalidatedEntries,
            long invalidationEvents, long fullClears, List<TableStat> byTable, List<Invalidation> recent,
            long bypassedTotal, List<BypassReasonStat> bypassByReason, List<BypassTableStat> bypassByTable) {
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
    private final ConcurrentHashMap<String, LongAdder> bypassByReason = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> bypassByTable = new ConcurrentHashMap<>();

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

    /** A SELECT that reached {@link CacheStage#handle} but never attempted any cache tier --
     * {@code reason} is one of the {@code BYPASS_*} constants above, {@code table} the SQL's own
     * FROM target (or {@code null}/blank, folded into {@link #OTHER_TABLES} the same way {@link
     * #hit}/{@link #miss} already do). */
    public void bypass(String reason, String table) {
        bypassByReason.computeIfAbsent(reason, k -> new LongAdder()).increment();
        String key = table == null || table.isBlank() ? OTHER_TABLES : table.toLowerCase(java.util.Locale.ROOT);
        if (!bypassByTable.containsKey(key) && bypassByTable.size() >= TABLE_CAP) {
            key = OTHER_TABLES;
        }
        bypassByTable.computeIfAbsent(key, k -> new LongAdder()).increment();
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

        long bypassedTotal = 0;
        List<BypassReasonStat> byReason = new ArrayList<>();
        for (Map.Entry<String, LongAdder> e : bypassByReason.entrySet()) {
            long count = e.getValue().sum();
            bypassedTotal += count;
            byReason.add(new BypassReasonStat(e.getKey(), count));
        }
        byReason.sort(Comparator.comparingLong(BypassReasonStat::count).reversed());

        List<BypassTableStat> bypassTables = new ArrayList<>();
        for (Map.Entry<String, LongAdder> e : bypassByTable.entrySet()) {
            bypassTables.add(new BypassTableStat(e.getKey(), e.getValue().sum()));
        }
        bypassTables.sort(Comparator.comparingLong(BypassTableStat::count).reversed());

        return new Snapshot(resultHits.sum(), resultMisses.sum(), pkHits.sum(), pkMisses.sum(),
                invalidatedEntries.sum(), invalidationEvents.sum(), fullClears.sum(), tables, List.copyOf(recent),
                bypassedTotal, byReason, bypassTables);
    }
}
