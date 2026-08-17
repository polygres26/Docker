package com.polygres.wire.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thin in-memory cache in front of {@link DialectTranslations}/{@link TranslationLlmClient} so a
 * repeat query (proxied connections see the same handful of statement shapes over and over — same
 * observation {@link com.polygres.wire.core.BackendConnectionPools#applyStatementCacheProperties}
 * javadoc makes about pgJDBC's own statement cache) skips re-translation entirely, deterministic
 * rule or LLM fallback either way.
 *
 * <p>Keyed on {@code (sourceDialect, targetDialect, normalizedSqlText)} — normalization is just
 * whitespace-collapse + trim, not a real SQL parser/canonicalizer (deliberately simple: two
 * cosmetically-different-but-semantically-identical queries missing each other's cache entry costs
 * one extra translation, not a correctness bug, so this doesn't need to be exhaustive).
 *
 * <p>Hand-rolled {@link LinkedHashMap}-based LRU rather than a Caffeine (or similar) dependency —
 * Caffeine isn't already a transitive dependency of this module, and the task this cache does
 * (bounded map, evict oldest on overflow) doesn't need Caffeine's window-TinyLFU sophistication.
 * Synchronized rather than a {@code ConcurrentHashMap} because {@link LinkedHashMap}'s
 * access-order eviction isn't thread-safe on its own — contention here is low (translation itself,
 * the miss path, is far more expensive than a synchronized map lookup).
 */
public final class TranslationCache {

    /** Matches {@code POLYWIRE_STMT_CACHE_SIZE}'s default (see {@code BackendConnectionPools}) — no
     * particular reason the two need to match, just a reasonable, consistent default. */
    private static final int DEFAULT_MAX_ENTRIES = 250;

    private final Map<CacheKey, String> cache;

    public TranslationCache() {
        this(intEnv("POLYWIRE_TRANSLATION_CACHE_SIZE", DEFAULT_MAX_ENTRIES));
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
