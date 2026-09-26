package com.sayonora.warp.cluster;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The shared, cross-protocol row-identity cache: one exact-primary-key lookup, one cache entry,
 * reachable from any wire protocol that can express "give me the row identified by this key" --
 * today that's dynamowire's GetItem and CacheStage's own new single-table/primary-key-equality
 * SELECT fast path (see {@link CacheStage#tryRowCacheLookup}). mongowire's exact-{@code _id}
 * lookup is the natural next protocol to move onto this, not done yet.
 *
 * <p>This intentionally does NOT replace {@link CacheStage}'s own {@code resultCache} -- that one
 * caches arbitrary query results (joins, ranges, aggregates, whatever shape a SELECT has) and
 * stays exactly as it is. A row cache and a result cache are different granularities; there's no
 * single structure that's both. What DynamoDB's GetItem and a SQL SELECT-by-primary-key have in
 * common is that they're both point lookups, so a point-lookup cache is the actual shared surface.
 *
 * <p>The cache key MUST be built from the physical Postgres table name (the string a SQL
 * FROM/WHERE clause actually contains), never a protocol-specific logical name (e.g. dynamowire's
 * own DynamoDB table name, which is transformed into a different physical table via
 * {@code PgItemStore.tableToPgName}) -- otherwise a SQL query and the protocol that populated the
 * entry would never compute the same key.
 */
public final class RowCache {

    private static final Logger log = LoggerFactory.getLogger(RowCache.class);

    private static final String CACHE_NAME = "warp-row-cache";

    private final IgniteCache<String, String> cache;

    private RowCache(IgniteCache<String, String> cache) {
        this.cache = cache;
    }

    public static RowCache create(WarpCluster cluster, String ttlMillisSpec) {
        long ttl = ttlMillisSpec == null || ttlMillisSpec.isBlank() ? 30_000 : Long.parseLong(ttlMillisSpec);
        return new RowCache(cluster.getOrCreateCache(CACHE_NAME, ttl));
    }

    public static String key(String physicalTable, String pk, String sk) {
        return physicalTable + "|" + pk + "|" + (sk == null ? "" : sk);
    }

    public String get(String key) {
        return cache.get(key);
    }

    public void put(String key, String valueJson) {
        cache.put(key, valueJson);
    }

    public void invalidate(String key) {
        cache.remove(key);
        log.debug("row cache invalidated: {}", key);
    }

    /**
     * Batch form of {@link #invalidate} for {@link CacheInvalidationListener}'s rows-mode payloads:
     * each entry of {@code keys} is {@code [pk]} or {@code [pk, sk]} (a {@code null}/absent sk
     * becomes the same empty-string sk {@link #key} already produces), all against one physical
     * table, removed in a single Ignite {@code removeAll} round trip rather than one per key.
     */
    public void invalidateRows(String physicalTable, java.util.List<String[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        java.util.Set<String> cacheKeys = new java.util.HashSet<>();
        for (String[] k : keys) {
            if (k == null || k.length == 0 || k[0] == null) {
                continue;
            }
            cacheKeys.add(key(physicalTable, k[0], k.length > 1 ? k[1] : null));
        }
        if (cacheKeys.isEmpty()) {
            return;
        }
        cache.removeAll(cacheKeys);
        log.debug("row cache invalidated {} key(s) on {}", cacheKeys.size(), physicalTable);
    }

    /** As {@link CacheStage#clearAll} -- the full clear a LISTEN (re)connect has to fall back
     * to, since a notify dropped while disconnected can't be replayed. */
    public void clearAll(String reason) {
        int before = cache.size();
        cache.clear();
        log.info("row cache: cleared all entries ({}) -- {}", before, reason);
    }
}
