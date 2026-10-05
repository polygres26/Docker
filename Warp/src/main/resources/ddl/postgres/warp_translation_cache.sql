-- Write-through cache of LLM dialect translations (config.TranslationCacheStore). Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_translation_cache (
    id                bigserial PRIMARY KEY,
    source_dialect    text NOT NULL,
    target_dialect    text NOT NULL,
    original_sql      text NOT NULL,
    original_sql_hash text NOT NULL,
    translated_sql    text NOT NULL,
    first_cached_at   timestamptz NOT NULL DEFAULT now(),
    hit_count         bigint NOT NULL DEFAULT 1,
    last_hit_at       timestamptz NOT NULL DEFAULT now()
)
-- ### unique key
CREATE UNIQUE INDEX IF NOT EXISTS warp_translation_cache_key
ON warp_translation_cache (source_dialect, target_dialect, original_sql_hash)
