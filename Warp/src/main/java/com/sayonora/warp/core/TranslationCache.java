package com.sayonora.warp.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class TranslationCache {

    private static final int DEFAULT_MAX_ENTRIES = 250;

    private final Map<CacheKey, String> cache;
    private final int maxEntries;
    private final java.util.concurrent.atomic.LongAdder hits = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder misses = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder evictions = new java.util.concurrent.atomic.LongAdder();

    public TranslationCache() {
        this(intEnv("WARP_TRANSLATION_CACHE_SIZE", DEFAULT_MAX_ENTRIES));
    }

    public TranslationCache(int maxEntries) {
        int capped = Math.max(1, maxEntries);
        this.maxEntries = capped;
        this.cache = java.util.Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<CacheKey, String> eldest) {
                boolean evict = size() > capped;
                if (evict) {
                    evictions.increment();
                }
                return evict;
            }
        });
    }

    public String get(String sqlText, SourceDialect from, SourceDialect to) {
        String value = cache.get(new CacheKey(from, to, normalize(sqlText)));
        (value == null ? misses : hits).increment();
        return value;
    }

    public void put(String sqlText, SourceDialect from, SourceDialect to, String translatedSqlText) {
        cache.put(new CacheKey(from, to, normalize(sqlText)), translatedSqlText);
    }

    public int size() {
        return cache.size();
    }

    public int maxEntries() {
        return maxEntries;
    }

    public long hits() {
        return hits.sum();
    }

    public long misses() {
        return misses.sum();
    }

    /** Entries pushed out by the size bound (least recently used first). */
    public long evictions() {
        return evictions.sum();
    }

    private static String normalize(String sqlText) {
        return sqlText == null ? "" : sqlText.strip().replaceAll("\\s+", " ");
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private record CacheKey(SourceDialect from, SourceDialect to, String normalizedSqlText) {
        private CacheKey {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(normalizedSqlText, "normalizedSqlText");
        }
    }
}
