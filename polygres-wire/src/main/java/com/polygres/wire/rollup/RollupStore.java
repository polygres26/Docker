package com.polygres.wire.rollup;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Live-reloadable holder of the current rollup definitions plus each rollup's freshness state —
 * the two things {@code RollupStage} needs on every candidate query, kept together since a reload
 * (a definition's {@code source_table} changing, for instance) and a refresh (a new "last
 * successfully materialized at" timestamp) are independent events but both mutate what "is this
 * rollup safe to use right now" means.
 *
 * <p>Same {@code volatile}-single-reference-swap shape as {@code
 * com.polygres.wire.core.AccessControlStage#reloadPolicy}: a reload never leaves a reader seeing half the
 * old definitions and half the new ones.
 */
public final class RollupStore {

    private static final Logger log = LoggerFactory.getLogger(RollupStore.class);

    /** Precompiled alongside its {@link RollupDefinition} so {@link #matchingDefinition} never recompiles a pattern per call. Same {@code \bschema\.table\b} convention {@code RouterStage}/{@code CacheStage} already use. */
    private record Entry(RollupDefinition definition, Pattern sourceTablePattern) {
    }

    private volatile List<Entry> entries;
    // Per-rollup name -> last successful refresh instant. A rollup with no entry here has never
    // been successfully materialized -- treated as maximally stale (see isFresh), never "fresh by
    // default", matching this feature's fail-open-to-the-real-table philosophy in the one direction
    // that actually matters: never serve a rollup that was never proven to have real data in it.
    private final Map<String, Instant> lastRefreshed = new ConcurrentHashMap<>();
    // Real query-acceleration counts, for the admin dashboard's "did this rollup actually get used"
    // view (ARCHITECTURE.md's UI entry, following the AtScale "Autonomous Aggregates" comparison).
    // Deliberately in-memory only, not persisted -- a restart resets counts to zero, same "real but
    // scoped" honesty as StatisticsStore's row-count collection. Durable, cross-restart hit history
    // would need a real audit-event type (mirroring NL2SQL_QUERY_EXECUTED) -- real future work, not
    // attempted here to keep this addition small. Two parallel maps (not one combined record) since
    // a hit's count and its timestamp are updated together but read independently and at different
    // granularities (the count via LongAdder for low-contention concurrent increments; the
    // timestamp as a plain volatile-per-entry map, same shape lastRefreshed already uses).
    private final Map<String, LongAdder> hitCounts = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastHit = new ConcurrentHashMap<>();

    public RollupStore(List<RollupDefinition> definitions) {
        this.entries = toEntries(definitions);
    }

    public static RollupStore empty() {
        return new RollupStore(List.of());
    }

    private static List<Entry> toEntries(List<RollupDefinition> definitions) {
        return definitions.stream()
                .map(d -> new Entry(d, Pattern.compile(
                        "\\b" + Pattern.quote(d.sourceTable()) + "\\b", Pattern.CASE_INSENSITIVE)))
                .toList();
    }

    public List<RollupDefinition> definitions() {
        return entries.stream().map(Entry::definition).toList();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public void reload(List<RollupDefinition> newDefinitions) {
        this.entries = toEntries(newDefinitions);
        // Definitions that no longer exist keep a stale freshness entry around harmlessly (never
        // consulted again unless the same name is redefined, in which case a fresh refresh is
        // required before it's usable again anyway -- correct either way).
        log.info("rollup: definitions reloaded ({} rollup(s))", newDefinitions.size());
    }

    /** @return the first configured rollup whose {@code source_table} regex matches {@code sql}, or {@code null}. First-match, not best-match — matching {@code CacheStage}'s own "first pattern wins" convention; an operator defining two overlapping rollups on the same table is expected to order them deliberately or keep them non-overlapping. */
    public RollupDefinition matchingDefinition(String sql) {
        for (Entry entry : entries) {
            if (entry.sourceTablePattern().matcher(sql).find()) {
                return entry.definition();
            }
        }
        return null;
    }

    public void markRefreshed(String rollupName) {
        lastRefreshed.put(rollupName, Instant.now());
    }

    /** {@code false} for a rollup that has never been successfully refreshed at all -- see this class's field javadoc. */
    public boolean isFresh(RollupDefinition definition) {
        Instant last = lastRefreshed.get(definition.name());
        if (last == null) {
            return false;
        }
        return Duration.between(last, Instant.now()).toMinutes() <= definition.maxStalenessMinutes();
    }

    public Instant lastRefreshedAt(String rollupName) {
        return lastRefreshed.get(rollupName);
    }

    /** Called by {@code RollupStage} right after a successful acceleration — never a correctness signal (nothing reads this to decide whether a rollup may be used, unlike {@link #isFresh}), purely for admin visibility. */
    public void recordHit(String rollupName) {
        hitCounts.computeIfAbsent(rollupName, k -> new LongAdder()).increment();
        lastHit.put(rollupName, Instant.now());
    }

    /** {@code 0} for a rollup that has never accelerated a query (including one that's never been queried against at all) — not {@code null}, since "zero hits" is itself the meaningful, common state, not an absence of data. */
    public long hitCount(String rollupName) {
        LongAdder adder = hitCounts.get(rollupName);
        return adder == null ? 0 : adder.sum();
    }

    /** {@code null} for a rollup that has never accelerated a query. */
    public Instant lastHitAt(String rollupName) {
        return lastHit.get(rollupName);
    }
}
