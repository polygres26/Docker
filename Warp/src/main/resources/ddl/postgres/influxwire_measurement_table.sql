-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    time TIMESTAMPTZ NOT NULL,
    tags JSONB NOT NULL DEFAULT '{}',
    fields JSONB NOT NULL DEFAULT '{}'
)
-- ### col_db
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS db TEXT NOT NULL DEFAULT ''
-- ### col_rp
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS rp TEXT NOT NULL DEFAULT 'autogen'
-- ### col_ns
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ns SMALLINT NOT NULL DEFAULT 0
-- ### index_time
CREATE INDEX IF NOT EXISTS ${table}_time_idx ON ${table} (time DESC)
-- ### index_tags
CREATE INDEX IF NOT EXISTS ${table}_tags_idx ON ${table} USING GIN (tags)
-- ### index_db
CREATE INDEX IF NOT EXISTS ${table}_db_idx ON ${table} (db, rp, time DESC)
