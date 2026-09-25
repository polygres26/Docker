package com.sayonora.wire.core;

import java.util.List;
import java.util.Map;

/**
 * A TTL-refreshed cache in front of {@link BackendCatalogDiscovery#discoverAll} -- required before
 * schema auto-discovery can safely sit in the shared statement pipeline every wire-protocol
 * frontend uses. MCP tool calls are comparatively rare, so {@code WarpMcpServer}'s original
 * per-call discovery was tolerable there; a real Postgres/Oracle/MySQL/SQL Server client can send
 * thousands of statements a second, and re-running real JDBC {@code DatabaseMetaData} introspection
 * against every registered backend on every single one would be a real, unacceptable latency
 * regression for ALL protocol traffic, not just federated queries.
 *
 * <p>Deliberately simple for a first implementation: on-demand, TTL-based refresh (default {@value
 * #DEFAULT_TTL_MILLIS}ms, {@code WARP_SCHEMA_DISCOVERY_CACHE_TTL_MS} to override) triggered by
 * whatever statement happens to ask for the catalog after it goes stale -- not a background
 * scheduler thread. A real periodic background refresh (matching {@code StatisticsScheduler}'s own
 * pattern) is genuine follow-up work, not built here; the on-demand shape means the FIRST statement
 * after a TTL expiry pays the real discovery cost, every one after it within the TTL window doesn't.
 * {@link #invalidate()} forces the next call to refresh regardless of TTL -- called on {@link
 * BackendRegistry#reload}, so a backend topology change is picked up promptly rather than waiting
 * out a stale TTL window.
 */
public final class BackendCatalogCache {

    private static final long DEFAULT_TTL_MILLIS = 30_000;

    private final BackendRegistry registry;
    private final long ttlMillis;
    private volatile List<BackendCatalogDiscovery.DiscoveredTable> cachedTables = List.of();
    private volatile Map<String, List<BackendCatalogDiscovery.DiscoveredTable>> cachedByName = Map.of();
    // Real bug, found live: using Long.MIN_VALUE as a "never refreshed yet" sentinel for
    // lastRefreshNanos and comparing via plain subtraction overflows on the very first check
    // (System.nanoTime() - Long.MIN_VALUE wraps around to a NEGATIVE result in two's-complement
    // arithmetic for any realistic nanoTime() value), which made the very first staleness check
    // silently evaluate false -- the cache never populated at all, forever, and every caller saw
    // an empty catalog. A separate boolean avoids relying on sentinel arithmetic entirely.
    private volatile boolean everRefreshed = false;
    private volatile long lastRefreshNanos;

    public BackendCatalogCache(BackendRegistry registry) {
        this(registry, ttlFromEnvOrDefault());
    }

    BackendCatalogCache(BackendRegistry registry, long ttlMillis) {
        this.registry = registry;
        this.ttlMillis = ttlMillis;
    }

    private static long ttlFromEnvOrDefault() {
        String spec = System.getenv("WARP_SCHEMA_DISCOVERY_CACHE_TTL_MS");
        if (spec == null || spec.isBlank()) {
            return DEFAULT_TTL_MILLIS;
        }
        try {
            return Long.parseLong(spec.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_TTL_MILLIS;
        }
    }

    /** Case-insensitive-keyed table name -> every backend that has it, refreshed if stale. Callers
     * should treat the returned map as a point-in-time snapshot -- it may be replaced by a
     * concurrent refresh immediately after this call returns, same as any other TTL cache. */
    public Map<String, List<BackendCatalogDiscovery.DiscoveredTable>> byTableNameLowercase() {
        if (isStale()) {
            synchronized (this) {
                if (isStale()) {
                    cachedTables = BackendCatalogDiscovery.discoverAll(registry);
                    cachedByName = BackendCatalogDiscovery.byTableNameLowercase(cachedTables);
                    lastRefreshNanos = System.nanoTime();
                    everRefreshed = true;
                }
            }
        }
        return cachedByName;
    }

    private boolean isStale() {
        return !everRefreshed || (System.nanoTime() - lastRefreshNanos > ttlMillis * 1_000_000L);
    }

    public void invalidate() {
        everRefreshed = false;
    }
}
