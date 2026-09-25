-- Real per-engine differences from ddl/postgres/sqswire_queue_table.sql:
--   BIGSERIAL -> BIGINT AUTO_INCREMENT (query logic uses JDBC's standard getGeneratedKeys(),
--     not RETURNING -- MySQL has no RETURNING at all, see SqswireDialect's own javadoc)
--   TEXT -> VARCHAR(255) for receipt_handle/message_group_id/dedup_id (real, bounded key/lookup
--     columns), TEXT stays TEXT for body (real message payloads can be large)
--   TIMESTAMPTZ -> DATETIME(6) (MySQL has no true timezone-aware timestamp type; this project's
--     own established convention -- see ddl/mysql/influxwire_measurement_table.sql's own
--     comment -- is UTC-normalization at the Java call-site, not the column type)
-- Real, disclosed gap: MySQL's CREATE INDEX has no IF NOT EXISTS, same real mitigating factor as
-- ddl/mysql/dynamowire_item_table.sql's own comment.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    msg_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    receipt_handle VARCHAR(255),
    vt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    enqueued_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    read_ct INT NOT NULL DEFAULT 0,
    body TEXT NOT NULL,
    message_group_id VARCHAR(255),
    dedup_id VARCHAR(255)
)
-- ### index_vt
CREATE INDEX ${table}_vt_idx ON ${table} (vt)
-- ### index_dedup
CREATE INDEX ${table}_dedup_idx ON ${table} (dedup_id, enqueued_at)
