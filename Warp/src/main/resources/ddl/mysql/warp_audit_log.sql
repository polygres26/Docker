-- Durable, hash-chained audit log (audit.AuditLogStore). WARP_AUDIT_LOG_DB can name any JDBC database, so this has a file per engine
-- (see DdlTemplates.engineDirFor). Only Postgres and MySQL have one: the same statement the store always ran, whose TIMESTAMP and TEXT
-- columns mean something different on SQL Server and do not exist on Oracle.
-- ### table
CREATE TABLE IF NOT EXISTS warp_audit_log (
    seq_num    BIGINT PRIMARY KEY,
    ts         TIMESTAMP,
    event_type VARCHAR(64),
    user_id    VARCHAR(255),
    summary    TEXT,
    details    TEXT,
    prev_hash  VARCHAR(64),
    row_hash   VARCHAR(64) NOT NULL
)
