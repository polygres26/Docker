-- Real per-engine differences from ddl/postgres/sqswire_catalog.sql:
--   TEXT -> NVARCHAR(450) (SQL Server caps a PRIMARY KEY column at 900 bytes)
--   BOOLEAN -> BIT (SQL Server's own real boolean-ish type; JDBC's setBoolean works fine against
--     it, unlike Oracle's NUMBER(1) -- see SqswireDialect's own javadoc)
-- Real, previously-latent bug, found and fixed the same way ddl/sqlserver/dynamowire_catalog.sql's
-- own comment documents: sqs_queues_catalog is a single, permanent, shared table (unlike a
-- per-queue table, which PgQueueStore's own Java-side catalog check already protects from a
-- second CREATE), so it needs to survive being re-created on every Warp restart against a real
-- SQL Server backend -- PgQueueStore's own catalogEnsured guard is per-process, not persistent.
-- SQL Server has no CREATE TABLE IF NOT EXISTS at all -- the standard, documented T-SQL idiom is
-- IF OBJECT_ID(...) IS NULL guarding the whole statement, real, valid T-SQL as one batch.
-- ### table
IF OBJECT_ID('sqs_queues_catalog', 'U') IS NULL
CREATE TABLE sqs_queues_catalog (
    queue_name NVARCHAR(450) NOT NULL,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo BIT NOT NULL DEFAULT 0,
    dlq_queue_name NVARCHAR(450),
    max_receive_count INT,
    CONSTRAINT sqs_queues_catalog_pk PRIMARY KEY (queue_name)
)
