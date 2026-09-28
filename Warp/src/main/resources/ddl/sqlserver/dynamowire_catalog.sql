-- Real per-engine differences from the Postgres original (ddl/postgres/dynamowire_catalog.sql):
--   text -> NVARCHAR(450)/NVARCHAR(128) (SQL Server caps a PRIMARY KEY column at 900 bytes)
--   bigint stays BIGINT -- SQL Server's own real 64-bit integer type, same name as Postgres's.
-- Unlike dynamowire_item_table.sql (whose own idempotency need is already covered by
-- PgItemStore's Java-side catalog check before a second CreateTable call), _dynamo_tables is a
-- single, permanent, shared table that must survive being re-created on every Warp restart
-- (PgItemStore's own in-memory catalogEnsured guard is per-process, not persistent). SQL Server
-- has no CREATE TABLE IF NOT EXISTS at all (unlike Postgres/MySQL/Oracle 23c+) -- the standard,
-- documented T-SQL idiom is IF OBJECT_ID(...) IS NULL guarding the whole statement, which is real,
-- valid T-SQL as one batch (confirmed against mssql-jdbc's own single-statement execute()).
-- ### table
IF OBJECT_ID('_dynamo_tables', 'U') IS NULL
CREATE TABLE _dynamo_tables (
    table_name NVARCHAR(450) NOT NULL PRIMARY KEY,
    pg_table NVARCHAR(450) NOT NULL,
    pk_name NVARCHAR(450) NOT NULL,
    pk_type NVARCHAR(128) NOT NULL,
    sk_name NVARCHAR(450),
    sk_type NVARCHAR(128),
    status NVARCHAR(128) NOT NULL,
    creation_time_millis BIGINT NOT NULL
)
