-- Postgres-only today, unlike ddl/*/dynamowire_item_table.sql and ddl/*/influxwire_measurement_
-- table.sql -- see DdlTemplates' own javadoc: this queue's own QUERY code (RETURNING on
-- INSERT/UPDATE, FOR UPDATE SKIP LOCKED inside an UPDATE subselect, ON CONFLICT, FILTER (WHERE
-- ...)) is real Postgres-only SQL too, not just this table's DDL, so a non-Postgres variant of
-- just this file wouldn't make sqswire actually work against another engine yet -- real,
-- tracked follow-up (see docs/POLYWIRE_GUIDE.md's own backend-engine prerequisites section).
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
