package com.nexagres.wire.dynamowire;

import com.nexagres.wire.cluster.PolyWireCluster;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DynamoCache {

    private static final Logger log = LoggerFactory.getLogger(DynamoCache.class);

    private final IgniteCache<String, String> cache;

    private DynamoCache(IgniteCache<String, String> cache) {
        this.cache = cache;
    }

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
