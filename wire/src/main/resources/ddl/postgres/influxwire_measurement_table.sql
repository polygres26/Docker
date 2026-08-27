-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    time TIMESTAMPTZ NOT NULL,
    tags JSONB NOT NULL DEFAULT '{}',
    fields JSONB NOT NULL DEFAULT '{}'
)
-- ### index_time
CREATE INDEX IF NOT EXISTS ${table}_time_idx ON ${table} (time DESC)
-- ### index_tags
CREATE INDEX IF NOT EXISTS ${table}_tags_idx ON ${table} USING GIN (tags)
