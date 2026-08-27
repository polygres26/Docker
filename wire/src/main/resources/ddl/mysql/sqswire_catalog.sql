-- Real per-engine differences from ddl/postgres/sqswire_catalog.sql:
--   TEXT -> VARCHAR(255) (a PRIMARY KEY column needs a bounded length under InnoDB)
--   BOOLEAN -> TINYINT(1) (MySQL's own real BOOLEAN is an alias for this; JDBC's setInt(1/0),
--     not setBoolean, is what SqswireDialect uses here -- see its own javadoc)
-- ### table
CREATE TABLE IF NOT EXISTS _sqs_queues (
    queue_name VARCHAR(255) PRIMARY KEY,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo TINYINT(1) NOT NULL DEFAULT 0,
    dlq_queue_name VARCHAR(255),
    max_receive_count INT
)
