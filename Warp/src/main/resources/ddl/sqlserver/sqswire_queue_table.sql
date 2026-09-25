-- Real per-engine differences from ddl/postgres/sqswire_queue_table.sql:
--   BIGSERIAL -> BIGINT IDENTITY(1,1) (SQL Server's own real identity-column syntax; query logic
--     uses JDBC's standard getGeneratedKeys(), not the OUTPUT clause, for this specific
--     single-column insert-id case -- see SqswireDialect's own javadoc)
--   TEXT -> NVARCHAR(450) for receipt_handle/message_group_id/dedup_id (real, bounded key/lookup
--     columns, index-eligible), NVARCHAR(MAX) for body (real message payloads can be large)
--   TIMESTAMPTZ -> DATETIMEOFFSET
-- ### table
CREATE TABLE ${table} (
    msg_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    receipt_handle NVARCHAR(450),
    vt DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    enqueued_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    read_ct INT NOT NULL DEFAULT 0,
    body NVARCHAR(MAX) NOT NULL,
    message_group_id NVARCHAR(450),
    dedup_id NVARCHAR(450)
)
-- ### index_vt
CREATE INDEX ${table}_vt_idx ON ${table} (vt)
-- ### index_dedup
CREATE INDEX ${table}_dedup_idx ON ${table} (dedup_id, enqueued_at)
