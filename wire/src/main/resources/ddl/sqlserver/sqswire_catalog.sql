-- Real per-engine differences from ddl/postgres/sqswire_catalog.sql:
--   TEXT -> NVARCHAR(450) (SQL Server caps a PRIMARY KEY column at 900 bytes)
--   BOOLEAN -> BIT (SQL Server's own real boolean-ish type; JDBC's setBoolean works fine against
--     it, unlike Oracle's NUMBER(1) -- see SqswireDialect's own javadoc)
-- Real, disclosed gap: no IF NOT EXISTS on CREATE TABLE either, same real mitigating factor as
-- ddl/sqlserver/dynamowire_item_table.sql's own comment.
-- ### table
CREATE TABLE _sqs_queues (
    queue_name NVARCHAR(450) NOT NULL,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo BIT NOT NULL DEFAULT 0,
    dlq_queue_name NVARCHAR(450),
    max_receive_count INT,
    CONSTRAINT _sqs_queues_pk PRIMARY KEY (queue_name)
)
