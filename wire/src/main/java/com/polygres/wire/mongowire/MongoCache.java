package com.polygres.wire.mongowire;

import com.polygres.wire.cluster.PolyWireCluster;
import org.apache.ignite.IgniteCache;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exact-{@code _id} cache-aside cache for {@code find}/{@code find_one}, built directly on
 * {@link PolyWireCluster#getOrCreateCache} rather than forced through {@code CacheStage}/
 * {@code StatementPipeline} — see {@link MongoWireSessionHandler}'s class javadoc for why those
 * SQL-text-shaped abstractions don't fit MongoDB's document-filter semantics.
 *
 * <p><b>Scope: filters that are an exact single-field equality match on {@code _id} only</b> (see
 * {@link MongoQueryTranslator#exactIdEquality}) — keyed on {@code (database, collection,
 * _id-value)}. A {@code find} with any other filter shape (other fields, ranges, {@code $in},
 * multi-field) is never cached here: correctly invalidating a cached filtered-result when an
 * unrelated write happens elsewhere in the collection is a materially harder correctness problem
 * than invalidating a single exact-{@code _id} entry, deliberately out of scope for this pass —
 * same "reject/limit clearly rather than guess" posture as {@code DialectTranslationStage}.
 *
 * <p><b>Invalidation is keyed off the actual {@code _id}s a write touched</b> (returned by
 * {@link PostgresDocumentStore#updateMany}/{@link PostgresDocumentStore#deleteMany} as {@code
 * WriteResult.ids()}), not off whether the write's own filter happened to be an exact-{@code _id}
 * filter — an {@code update_one}/{@code delete_one} filtered on some other field still correctly
 * invalidates the cache entry for whichever {@code _id} it actually matched.
 *
 * <p><b>Default ON</b> — unlike the four SQL frontends' {@code CacheStage}, which is opt-in via
 * {@code POLYWIRE_CACHE_TABLES}. Every cache entry here is scoped to exactly the one document it
 * was read for by its own {@code _id}, with none of arbitrary-SQL {@code CacheStage}'s
 * stale-join/aggregation risk — so caching every exact-{@code _id} lookup by default is safe the
 * way "cache every arbitrary SELECT" would not be. Opt out with
 * {@code POLYWIRE_MONGOWIRE_CACHE_ENABLED=false}.
 */
public final class MongoCache {

    private static final Logger log = LoggerFactory.getLogger(MongoCache.class);

    // Value is the document's extended-JSON text (BsonJson.toJson), the same shape stored in
    // Postgres's jsonb "doc" column.
    private final IgniteCache<String, String> cache;

    private MongoCache(IgniteCache<String, String> cache) {
        this.cache = cache;
    }

    /** {@code ttlMillisSpec}: {@code POLYWIRE_MONGOWIRE_CACHE_TTL_MS}, default 30000. */
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
