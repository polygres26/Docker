package com.nexagres.wire.mongowire;

import com.nexagres.wire.cluster.PolyWireCluster;
import org.apache.ignite.IgniteCache;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MongoCache {

    private static final Logger log = LoggerFactory.getLogger(MongoCache.class);

    private final IgniteCache<String, String> cache;

    private MongoCache(IgniteCache<String, String> cache) {
        this.cache = cache;
    }

    public static MongoCache create(PolyWireCluster cluster, String ttlMillisSpec) {
        long ttl = ttlMillisSpec == null || ttlMillisSpec.isBlank() ? 30_000 : Long.parseLong(ttlMillisSpec);
        return new MongoCache(cluster.getOrCreateCache("polywire-mongowire-doc-cache", ttl));
    }

    static String key(String db, String collection, String idJson) {
        return db + "|" + collection + "|" + idJson;
    }

    Document get(String key) {
        String json = cache.get(key);
        return json == null ? null : BsonJson.fromJson(json);
    }

    void put(String key, Document doc) {
        cache.put(key, BsonJson.toJson(doc));
    }

    void invalidate(String key) {
        cache.remove(key);
        log.debug("mongowire cache invalidated: {}", key);
    }
}
