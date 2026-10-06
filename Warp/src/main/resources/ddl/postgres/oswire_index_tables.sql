-- oswire: the table and sequence behind one OpenSearch index, on every shard (PostgresSearchStore). ${table} is the generated table name.
-- The ALTERs bring tables created by older versions up to date.
-- ### sequence
CREATE SEQUENCE IF NOT EXISTS ${table}_seq MINVALUE 0 START 0
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    doc_id     TEXT PRIMARY KEY,
    source     JSONB NOT NULL,
    embedding  JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    seq_no     BIGINT NOT NULL DEFAULT 0,
    version    BIGINT NOT NULL DEFAULT 1
)
-- ### seq_no column
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS seq_no BIGINT NOT NULL DEFAULT 0
-- ### version column
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1
-- ### ins_seq column
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ins_seq BIGINT
