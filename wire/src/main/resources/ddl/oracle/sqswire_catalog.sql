-- Real per-engine differences from ddl/postgres/sqswire_catalog.sql:
--   TEXT -> VARCHAR2(255)
--   BOOLEAN -> NUMBER(1) (Oracle has no native BOOLEAN in SQL, only in PL/SQL -- NUMBER(1) with
--     0/1 is the standard real idiom; JDBC's setInt(1/0), not setBoolean, is what SqswireDialect
--     uses here -- see its own javadoc)
--   INT -> NUMBER
-- Real, previously-latent bug, found and fixed the same way ddl/oracle/dynamowire_catalog.sql's
-- own comment documents: sqs_queues_catalog is a single, permanent, shared table (unlike a
-- per-queue table, which PgQueueStore's own Java-side catalog check already protects from a
-- second CREATE), so it needs to survive being re-created on every Warp restart against a real
-- Oracle backend -- PgQueueStore's own catalogEnsured guard is per-process, not persistent.
-- Oracle has no CREATE TABLE IF NOT EXISTS at all, on any version (see
-- ddl/oracle/dynamowire_catalog.sql's own comment for the live-confirmed ORA-00911) -- the real,
-- standard idiom instead: a PL/SQL block that attempts the CREATE and swallows exactly ORA-00955
-- ("name is already used by an existing object"), re-raising anything else.
-- ### table
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE sqs_queues_catalog (
        queue_name VARCHAR2(255) NOT NULL,
        visibility_timeout NUMBER DEFAULT 30 NOT NULL,
        is_fifo NUMBER(1) DEFAULT 0 NOT NULL,
        dlq_queue_name VARCHAR2(255),
        max_receive_count NUMBER,
        CONSTRAINT sqs_queues_catalog_pk PRIMARY KEY (queue_name)
    )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;
