package com.polygres.wire.config;

import com.polygres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TranslationCacheStore {

    private static final Logger log = LoggerFactory.getLogger(TranslationCacheStore.class);

    private final com.polygres.wire.server.ServerOptions options;

    public TranslationCacheStore(com.polygres.wire.server.ServerOptions options) {
        this.options = options;
    }

    public void ensureSchema() {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS polywire_translation_cache ("
                    + "id bigserial PRIMARY KEY, "
                    + "source_dialect text NOT NULL, "
                    + "target_dialect text NOT NULL, "
                    + "original_sql text NOT NULL, "
                    + "original_sql_hash text NOT NULL, "
                    + "translated_sql text NOT NULL, "
                    + "first_cached_at timestamptz NOT NULL DEFAULT now(), "
                    + "hit_count bigint NOT NULL DEFAULT 1, "
                    + "last_hit_at timestamptz NOT NULL DEFAULT now())");
            st.execute("CREATE UNIQUE INDEX IF NOT EXISTS polywire_translation_cache_key "
                    + "ON polywire_translation_cache (source_dialect, target_dialect, original_sql_hash)");
        } catch (SQLException e) {
            log.warn("translation cache store: could not ensure polywire_translation_cache schema exists"
                    + " -- write-through recording will keep failing best-effort until this is fixed", e);
        }
    }

    public void recordAccess(SourceDialect sourceDialect, SourceDialect targetDialect,
            String originalSql, String translatedSql) {
        try (Connection conn = com.polygres.wire.pgwire.PgConnections.open(options);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO polywire_translation_cache "
                                + "(source_dialect, target_dialect, original_sql, original_sql_hash, translated_sql) "
                                + "VALUES (?, ?, ?, md5(?), ?) "
                                + "ON CONFLICT (source_dialect, target_dialect, original_sql_hash) "
                                + "DO UPDATE SET hit_count = polywire_translation_cache.hit_count + 1, "
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
