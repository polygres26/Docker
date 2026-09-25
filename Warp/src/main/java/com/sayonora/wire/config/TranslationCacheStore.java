package com.sayonora.wire.config;

import com.sayonora.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TranslationCacheStore {

    private static final Logger log = LoggerFactory.getLogger(TranslationCacheStore.class);

    // recordAccess is pure hit-count/last-hit-at analytics -- nothing about whether a query can
    // be served depends on it (that's TranslationCache, the in-memory map, already answered by
    // the time this is called). It used to run synchronously in the request path, a real Postgres
    // round trip on every translation-cache hit -- found live while chasing why orawire's
    // cache-hit RTT was ~1ms against pgwire's <0.3ms despite sharing the exact same pipeline
    // stages: DialectTranslationStage only does real work when source and target dialects differ,
    // which for orawire (ORACLE -> POSTGRES backend) is every single statement, unlike pgwire
    // (POSTGRES -> POSTGRES, an immediate no-op). This was the actual cost, not TTC encoding or
    // per-request object allocation. Fire-and-forget on a background thread instead: a write
    // landing a few milliseconds late (or being dropped under extreme backpressure, given the
    // bounded queue) only makes hit_count/last_hit_at slightly stale, never wrong in a way
    // anything else reads for correctness.
    private static final ExecutorService RECORD_ACCESS_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "translation-cache-recorder");
        t.setDaemon(true);
        return t;
    });

    private final com.sayonora.wire.server.ServerOptions options;

    public TranslationCacheStore(com.sayonora.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() {
        try (Connection conn = com.sayonora.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS warp_translation_cache ("
                    + "id bigserial PRIMARY KEY, "
                    + "source_dialect text NOT NULL, "
                    + "target_dialect text NOT NULL, "
                    + "original_sql text NOT NULL, "
                    + "original_sql_hash text NOT NULL, "
                    + "translated_sql text NOT NULL, "
                    + "first_cached_at timestamptz NOT NULL DEFAULT now(), "
                    + "hit_count bigint NOT NULL DEFAULT 1, "
                    + "last_hit_at timestamptz NOT NULL DEFAULT now())");
            st.execute("CREATE UNIQUE INDEX IF NOT EXISTS warp_translation_cache_key "
                    + "ON warp_translation_cache (source_dialect, target_dialect, original_sql_hash)");
        } catch (SQLException e) {
            log.warn("translation cache store: could not ensure warp_translation_cache schema exists"
                    + " -- write-through recording will keep failing best-effort until this is fixed", e);
        }
    }

    /**
     * Queues the write and returns immediately -- see the class javadoc for why this is safe to
     * do off the request path. Never throws back to the caller; a queueing or DB failure is
     * logged and dropped, exactly as a failed synchronous attempt used to be swallowed here too.
     */
    public void recordAccess(SourceDialect sourceDialect, SourceDialect targetDialect,
            String originalSql, String translatedSql) {
        try {
            RECORD_ACCESS_EXECUTOR.submit(() -> recordAccessNow(sourceDialect, targetDialect, originalSql, translatedSql));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            log.warn("translation cache store: recorder queue rejected a write for {}->{} -- dropped", sourceDialect, targetDialect);
        }
    }

    private void recordAccessNow(SourceDialect sourceDialect, SourceDialect targetDialect,
            String originalSql, String translatedSql) {
        try (Connection conn = com.sayonora.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO warp_translation_cache "
                                + "(source_dialect, target_dialect, original_sql, original_sql_hash, translated_sql) "
                                + "VALUES (?, ?, ?, md5(?), ?) "
                                + "ON CONFLICT (source_dialect, target_dialect, original_sql_hash) "
                                + "DO UPDATE SET hit_count = warp_translation_cache.hit_count + 1, "
                                + "last_hit_at = now()")) {
            ps.setString(1, sourceDialect == null ? null : sourceDialect.name());
            ps.setString(2, targetDialect == null ? null : targetDialect.name());
            ps.setString(3, originalSql);
            ps.setString(4, originalSql);
            ps.setString(5, translatedSql);
            ps.executeUpdate();
        } catch (Exception e) {

            log.warn("translation cache store: could not record access for {}->{}: {}",
                    sourceDialect, targetDialect, e.getMessage());
        }
    }

}
