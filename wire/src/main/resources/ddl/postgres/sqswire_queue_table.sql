-- Postgres's own original query shape, unchanged -- a real single-statement UPDATE ... WHERE
-- msg_id = (SELECT ... FOR UPDATE SKIP LOCKED) ... RETURNING. Oracle/SQL Server/MySQL now have
-- their own real DDL AND query support too (ddl/<engine>/sqswire_queue_table.sql,
-- com.nexagres.wire.sqswire.SqswireDialect) -- see that class's own javadoc for the real,
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
