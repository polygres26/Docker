package com.nexagres.wire.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class TranslationCache {

    private static final int DEFAULT_MAX_ENTRIES = 250;

    private final Map<CacheKey, String> cache;

    public TranslationCache() {
        this(intEnv("WARP_TRANSLATION_CACHE_SIZE", DEFAULT_MAX_ENTRIES));
    }

    public TranslationCache(int maxEntries) {
        int capped = Math.max(1, maxEntries);
        this.cache = java.util.Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<CacheKey, String> eldest) {
                return size() > capped;
            }
        });
    }

    public String get(String sqlText, SourceDialect from, SourceDialect to) {
        return cache.get(new CacheKey(from, to, normalize(sqlText)));
    }

    public void put(String sqlText, SourceDialect from, SourceDialect to, String translatedSqlText) {
        cache.put(new CacheKey(from, to, normalize(sqlText)), translatedSqlText);
    }

    public int size() {
        return cache.size();
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
