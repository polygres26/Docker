package com.polygres.wire.dynamowire;

import com.polygres.wire.cluster.PolyWireCluster;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exact-key cache-aside cache for {@code GetItem}, built directly on {@link
 * PolyWireCluster#getOrCreateCache} rather than forced through {@code CacheStage}/
 * {@code StatementPipeline} — see {@link DynamoWireServer}'s class javadoc for why those
 * SQL-text-shaped abstractions don't fit DynamoDB's typed key-value operations.
 *
 * <p><b>Scope: exact single-item {@code GetItem} lookups only</b>, keyed by {@code
 * (tableName, pk-value, sk-value-or-none)}. {@code Query}/{@code Scan} results are never cached
 * here: correctly invalidating a cached filtered/range result when an unrelated write happens
 * elsewhere in the table is a materially harder correctness problem (which items would have
 * matched the filter after a write whose new values you haven't seen) than invalidating a single
 * exact-key entry — deliberately left out of scope for this pass rather than risking a filtered
 * read silently serving stale data, same "reject/limit clearly rather than guess" posture as
 * {@code DialectTranslationStage}.
 *
 * <p><b>Default ON</b> — unlike the four SQL frontends' {@code CacheStage}, which is opt-in via
 * {@code POLYWIRE_CACHE_TABLES}. For an arbitrary SQL SELECT, "cache it" risks silently serving
 * stale joined/aggregated data unless an operator explicitly vets which tables are safe. Here
 * every cache entry is scoped to exactly the one item it was read for by its own primary key —
 * there's no equivalent join/aggregation staleness surface, so "cache every GetItem, invalidate
 * the exact key on every write to it" is a safe default for a key-value store the way it would
 * not be for arbitrary SQL. Opt out with {@code POLYWIRE_DYNAMOWIRE_CACHE_ENABLED=false}.
 */
public final class DynamoCache {

    private static final Logger log = LoggerFactory.getLogger(DynamoCache.class);

    // Value is the item's DynamoDB-typed-attribute-value JSON text (PgItemStore.itemToJson().toString()),
    // the same round-trip-exact shape PgItemStore already stores in Postgres's jsonb column.
    private final IgniteCache<String, String> cache;

    private DynamoCache(IgniteCache<String, String> cache) {
        this.cache = cache;
    }

    /** {@code ttlMillisSpec}: {@code POLYWIRE_DYNAMOWIRE_CACHE_TTL_MS}, default 30000. */
    public static DynamoCache create(PolyWireCluster cluster, String ttlMillisSpec) {
        long ttl = ttlMillisSpec == null || ttlMillisSpec.isBlank() ? 30_000 : Long.parseLong(ttlMillisSpec);
        return new DynamoCache(cluster.getOrCreateCache("polywire-dynamowire-item-cache", ttl));
    }

    static String key(String tableName, String pk, String sk) {
        return tableName + "|" + pk + "|" + (sk == null ? "" : sk);
    }

    String get(String key) {
        return cache.get(key);
    }

    void put(String key, String itemJson) {
        cache.put(key, itemJson);
    }

    void invalidate(String key) {
        cache.remove(key);
        log.debug("dynamowire cache invalidated: {}", key);
    }
}
