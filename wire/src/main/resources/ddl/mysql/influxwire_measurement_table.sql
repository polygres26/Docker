-- Real per-engine differences from the Postgres original (ddl/postgres/influxwire_measurement_table.sql):
--   TIMESTAMPTZ -> DATETIME(6) (MySQL has no true timezone-aware timestamp type; DATETIME(6) keeps
--     the same microsecond precision real InfluxDB line-protocol timestamps need -- this project's
--     own established convention, matching how PgTimeSeriesStore already normalizes every
--     timestamp to UTC before ever writing it, is required here too, at the Java call-site level)
--   JSONB -> JSON (MySQL's own real native JSON type; default '{}' needs an expression default,
--     not a literal, on MySQL 8.0.13+)
-- Real, disclosed gap: no GIN-equivalent index exists for MySQL's JSON type -- a tag-lookup query
-- against this table falls back to a full scan rather than an index-accelerated one; a real,
-- narrower functional index on one specific, known tag key is possible but not generic the way
-- Postgres's own GIN index over the whole tags column is. Not created here.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    time DATETIME(6) NOT NULL,
    tags JSON NOT NULL,
    fields JSON NOT NULL
)
-- ### index_time
CREATE INDEX ${table}_time_idx ON ${table} (time DESC)
