-- Real per-engine differences from the Postgres original (ddl/postgres/influxwire_measurement_table.sql):
--   TIMESTAMPTZ -> DATETIMEOFFSET (SQL Server's own real timezone-aware timestamp type)
--   JSONB -> NVARCHAR(MAX) with a real ISJSON() check constraint (same real reasoning as
--     ddl/sqlserver/dynamowire_item_table.sql's own comment)
-- Real, disclosed gap: same as the MySQL/Oracle variants -- no GIN-equivalent generic tag index
-- (SQL Server's own real answer, a computed column + index per known JSON key, needs a fixed key
-- set, not a generic whole-column index).
-- ### table
CREATE TABLE ${table} (
    time DATETIMEOFFSET NOT NULL,
    tags NVARCHAR(MAX) NOT NULL DEFAULT '{}' CHECK (ISJSON(tags) = 1),
    fields NVARCHAR(MAX) NOT NULL DEFAULT '{}' CHECK (ISJSON(fields) = 1)
)
-- ### index_time
CREATE INDEX ${table}_time_idx ON ${table} (time DESC)
