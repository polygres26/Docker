-- Real per-engine differences from the Postgres original (ddl/postgres/influxwire_measurement_table.sql):
--   TIMESTAMPTZ -> TIMESTAMP WITH TIME ZONE (Oracle's own real equivalent)
--   JSONB -> CLOB with a real "IS JSON" check constraint (same real reasoning as
--     ddl/oracle/dynamowire_item_table.sql's own comment)
-- Real, disclosed gap: same as the MySQL variant -- no GIN-equivalent generic tag index here
-- either (Oracle's own real JSON search index needs a fixed, known set of extracted keys, not a
-- generic whole-column index the way Postgres's GIN is).
-- ### table
CREATE TABLE ${table} (
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    tags CLOB DEFAULT '{}' NOT NULL,
    fields CLOB DEFAULT '{}' NOT NULL,
    CONSTRAINT ${table}_tags_json CHECK (tags IS JSON),
    CONSTRAINT ${table}_fields_json CHECK (fields IS JSON)
)
-- ### index_time
CREATE INDEX ${table}_time_idx ON ${table} (time DESC)
