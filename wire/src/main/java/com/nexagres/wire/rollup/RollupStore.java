package com.nexagres.wire.rollup;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RollupStore {

    private static final Logger log = LoggerFactory.getLogger(RollupStore.class);

    private record Entry(RollupDefinition definition, Pattern sourceTablePattern) {
    }

    private volatile List<Entry> entries;
    
    private final Map<String, Instant> lastRefreshed = new ConcurrentHashMap<>();
    
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
        
        log.info("rollup: definitions reloaded ({} rollup(s))", newDefinitions.size());
    }

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

    public void recordHit(String rollupName) {
        hitCounts.computeIfAbsent(rollupName, k -> new LongAdder()).increment();
        lastHit.put(rollupName, Instant.now());
    }

    public long hitCount(String rollupName) {
        LongAdder adder = hitCounts.get(rollupName);
        return adder == null ? 0 : adder.sum();
    }

    public Instant lastHitAt(String rollupName) {
        return lastHit.get(rollupName);
    }
}
