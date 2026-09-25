-- Postgres's own original query shape, unchanged -- a real single-statement UPDATE ... WHERE
-- msg_id = (SELECT ... FOR UPDATE SKIP LOCKED) ... RETURNING. Oracle/SQL Server/MySQL now have
-- their own real DDL AND query support too (ddl/<engine>/sqswire_queue_table.sql,
-- com.sayonora.wire.sqswire.SqswireDialect) -- see that class's own javadoc for the real,
-- deliberate two-statement claim pattern those three engines share instead of this one.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    msg_id BIGSERIAL PRIMARY KEY,
    receipt_handle TEXT,
    vt TIMESTAMPTZ NOT NULL DEFAULT now(),
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_ct INT NOT NULL DEFAULT 0,
    body TEXT NOT NULL,
    message_group_id TEXT,
    dedup_id TEXT
)
-- ### index_vt
CREATE INDEX IF NOT EXISTS ${table}_vt_idx ON ${table} (vt)
-- ### index_dedup
CREATE INDEX IF NOT EXISTS ${table}_dedup_idx ON ${table} (dedup_id, enqueued_at)
-- ### col_message_id
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS message_id TEXT
-- ### col_attrs
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS attrs TEXT
-- ### col_trace_header
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS trace_header TEXT
-- ### col_first_receive_at
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS first_receive_at TIMESTAMPTZ
-- ### col_dlq_source_arn
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS dlq_source_arn TEXT
-- ### col_attempt_id
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS attempt_id TEXT
-- ### col_attempt_at
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS attempt_at TIMESTAMPTZ
-- ### index_group
CREATE INDEX IF NOT EXISTS ${table}_grp_idx ON ${table} (message_group_id, vt)
-- ### dedup_table
CREATE TABLE IF NOT EXISTS ${table}_dd (
    dedup_key TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
