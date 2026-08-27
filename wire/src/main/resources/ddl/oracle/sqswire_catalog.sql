-- Real per-engine differences from ddl/postgres/sqswire_catalog.sql:
--   TEXT -> VARCHAR2(255)
--   BOOLEAN -> NUMBER(1) (Oracle has no native BOOLEAN in SQL, only in PL/SQL -- NUMBER(1) with
--     0/1 is the standard real idiom; JDBC's setInt(1/0), not setBoolean, is what SqswireDialect
--     uses here -- see its own javadoc)
--   INT -> NUMBER
-- Real, disclosed gap: no IF NOT EXISTS at all pre-23c, same real mitigating factor as
-- ddl/oracle/dynamowire_item_table.sql's own comment.
-- ### table
CREATE TABLE sqs_queues_catalog (
    queue_name VARCHAR2(255) NOT NULL,
    visibility_timeout NUMBER DEFAULT 30 NOT NULL,
    is_fifo NUMBER(1) DEFAULT 0 NOT NULL,
    dlq_queue_name VARCHAR2(255),
    max_receive_count NUMBER,
    CONSTRAINT sqs_queues_catalog_pk PRIMARY KEY (queue_name)
)
