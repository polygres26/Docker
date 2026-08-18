package com.polygres.wire.config;

import com.polygres.wire.core.SourceDialect;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable, plain-SQL-queryable record of every successfully dialect-translated statement
 * {@code DialectTranslationStage}/{@code TranslationCache} handles — the on-disk counterpart of
 * that in-memory, node-local, restart-losing cache. Before this class the only way to see what
 * PolyWire has translated (and how often a given translation gets reused) was to grep node logs
 * for the "translation cache HIT/MISS" lines {@code DialectTranslationStage} already emits.
 *
 * <p>Reuses the same Postgres backend PolyWire already treats as real infrastructure for
 * {@link ConfigStore}'s {@code polywire_config} and {@link FailedStatementLog}'s
 * {@code polywire_failed_statements} tables, rather than standing up a separate store — same
 * "reuse what's already real" reasoning as those classes' javadoc. This mirrors the precedent of
 * Omnigate's {@code SqlPlanViewStage}/{@code SqlPlanStore} (in-memory store answering a magic
 * {@code SELECT * FROM omnigate_sql_plan} table name) only in spirit ("make an in-memory cache
 * inspectable via SQL") — the mechanism here is deliberately different: a real, durable Postgres
 * table any client can query with plain SQL (including joins, filters, {@code ORDER BY hit_count
 * DESC}, etc.), not an interception trick tied to one magic table name recognized only by this
 * process.
 *
 * <h2>Schema</h2>
 * <pre>{@code
 * CREATE TABLE polywire_translation_cache (
 *     id                 bigserial PRIMARY KEY,
 *     source_dialect     text NOT NULL,
 *     target_dialect     text NOT NULL,
 *     original_sql       text NOT NULL,
 *     original_sql_hash  text NOT NULL,
 *     translated_sql     text NOT NULL,
 *     first_cached_at    timestamptz NOT NULL DEFAULT now(),
 *     hit_count          bigint NOT NULL DEFAULT 1,
 *     last_hit_at        timestamptz NOT NULL DEFAULT now()
 * );
 * CREATE UNIQUE INDEX polywire_translation_cache_key
 *     ON polywire_translation_cache (source_dialect, target_dialect, original_sql_hash);
 * }</pre>
 *
 * <p><b>Why {@code original_sql_hash} carries the uniqueness constraint instead of
 * {@code original_sql} itself</b>: Postgres's btree index has a hard per-entry size limit
 * (roughly 1/3 of the 8KB page size, ~2712 bytes) — a long statement (a bulk {@code INSERT ...
 * VALUES (...), (...), ...}, a generated report query, etc.) could exceed that and make the
 * unique index itself impossible to build, not just slow. {@code original_sql_hash} is
 * {@code md5(original_sql)} computed in Postgres at insert time — a fixed-width 32-character
 * value, so the unique index stays small and bounded regardless of statement length.
 * {@code original_sql} itself is kept as a plain, unindexed (for uniqueness purposes) text
 * column purely for operator readability — {@code SELECT * FROM polywire_translation_cache} still
 * shows the real SQL text, nothing is lost, only the constraint moves onto the hash.
 *
 * <h2>One method, both write-through directions</h2>
 * {@link #recordAccess} is a single {@code INSERT ... ON CONFLICT ... DO UPDATE} upsert, called
 * from both {@code DialectTranslationStage.translateWithFallback}'s cache-miss-then-translated
 * path and its cache-hit path:
 * <ul>
 *   <li>Miss (new statement shape, just translated): no existing row for
 *       {@code (source_dialect, target_dialect, original_sql_hash)} — the {@code INSERT} branch
 *       fires, creating a fresh row with {@code hit_count = 1}.
 *   <li>Hit (in-memory {@code TranslationCache} lookup already succeeded): a row already exists
 *       — the {@code ON CONFLICT DO UPDATE} branch fires, incrementing {@code hit_count} and
 *       bumping {@code last_hit_at}, leaving {@code translated_sql}/{@code first_cached_at}
 *       untouched.
 * </ul>
 * This also self-heals the one edge case a simpler "insert on miss, separate update on hit"
 * split would get wrong: a node restart empties the in-memory {@code TranslationCache} (it is
 * explicitly node-local and non-durable — see that class's javadoc) but not this table, so the
 * very next request for a statement this node already translated before restart looks like a
 * miss to {@code TranslationCache} yet correctly lands on the {@code DO UPDATE} branch here
 * (row already exists from before the restart) rather than raising a duplicate-key error or
 * creating a second row for the same statement.
 *
 * <h2>Synchronous, best-effort, off the hot path's correctness — same posture as
 * {@link FailedStatementLog}</h2>
 * Every cache hit is a real write against this table, not just every miss — that is a genuine
 * extra round-trip on what can be a very hot path (a repeatedly-issued proxied query). This class
 * deliberately does that write synchronously and best-effort anyway, matching
 * {@link FailedStatementLog}'s already-established precedent in this codebase (see its javadoc),
 * rather than introducing a batching/async queue:
 * <ul>
 *   <li>{@link #recordAccess} catches and locally logs any failure to write the record — a
 *       hit-counting problem must never fail, delay-retry, or otherwise affect the real client
 *       query that triggered it.
 *   <li>The write is a single-row upsert against a primary-key/unique-index lookup — cheap
 *       relative to the round-trip most of these proxied protocols already pay talking to their
 *       real backend, and each call borrows a pooled connection via {@link
 *       com.polygres.wire.pgwire.PgConnections#open} (same pool real query traffic shares, and
 *       the same {@code POLYWIRE_PG_STANDBY_HOST} primary/standby failover -- if the config-primary
 *       Postgres this table lives on fails over, this store's writes follow automatically).
 *   <li>If a live load test surfaces this as a real bottleneck, the natural next step is
 *       batching hit-count increments in memory and flushing periodically — deliberately not
 *       built here since nothing in this pass's live testing showed it was needed, and building
 *       unneeded batching logic risks the exact kind of complexity this codebase's other
 *       Postgres-backed stores (see {@link ConfigStore}, {@link FailedStatementLog}) avoid.
 * </ul>
 */
public final class TranslationCacheStore {

    private static final Logger log = LoggerFactory.getLogger(TranslationCacheStore.class);

    private final com.polygres.wire.server.ServerOptions options;

    public TranslationCacheStore(com.polygres.wire.server.ServerOptions options) {
        this.options = options;
    }

    /** Idempotent -- safe to call on every node's startup. */
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

    /**
     * Records one cache miss-then-translated statement, or one cache hit, for
     * {@code (sourceDialect, targetDialect, originalSql)} -- see class javadoc for how the single
     * upsert serves both directions. Never throws -- any problem writing the record itself is
     * caught and logged locally, never propagated, so this can never fail (or slow down, beyond
     * the write itself) the real client query that triggered it.
     *
     * @param translatedSql only used on the insert branch (a brand-new row) -- ignored by the
     *     {@code DO UPDATE} branch, so passing the same freshly-recomputed translation on a hit
     *     is harmless and keeps this one call site for both directions.
     */
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
            // Best-effort: a recording failure must never cascade into (or mask/slow) the real
            // client-facing query response that triggered this call.
            log.warn("translation cache store: could not record access for {}->{}: {}",
                    sourceDialect, targetDialect, e.getMessage());
        }
    }

}
