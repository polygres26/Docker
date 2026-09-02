-- Real per-engine differences from the Postgres original (ddl/postgres/dynamowire_catalog.sql):
--   text -> VARCHAR2(255)/VARCHAR2(64) (same real key-size reasoning as the item-table DDL's own
--     comment -- an unbounded VARCHAR2/CLOB primary key isn't a real Oracle option at all)
--   bigint -> NUMBER(19) (Oracle's own real 64-bit-integer-equivalent precision)
-- Named dynamo_tables_catalog here, NOT _dynamo_tables like every other engine's own DDL --
-- confirmed live, a real ORA-00911 ("invalid character after TABLE") the first time this ran
-- against a real Oracle instance: Oracle rejects an unquoted identifier starting with `_`
-- outright (the same class of bug this project already found and fixed once for
-- ScatterGatherAggregateMerge's own synthetic aliases). Quoting it instead was considered and
-- rejected: MySQL treats a double-quoted string as a STRING LITERAL by default (ANSI_QUOTES mode
-- is off by default), so a single quoted form can't work identically across every engine here --
-- see PgItemStore#catalogTableName's own javadoc for the full reasoning. Every one of
-- PgItemStore's own catalog queries resolves this name per engine, not just this DDL file.
--
-- Unlike dynamowire_item_table.sql (whose own idempotency need is already covered by
-- PgItemStore's Java-side catalog check before a second CreateTable call), this is a
-- single, permanent, shared table that must survive being re-created on every Warp restart
-- (PgItemStore's own in-memory catalogEnsured guard is per-process, not persistent) -- real
-- idempotency is required here, not optional. Real, confirmed live against a real Oracle Free 23
-- instance: plain `CREATE TABLE IF NOT EXISTS` is NOT valid Oracle syntax at all (a real
-- ORA-00911 "invalid character after EXISTS" -- this project's own initial assumption that
-- Oracle 23c added it was wrong, corrected by that live failure). The real, standard Oracle
-- idiom instead: a PL/SQL block that attempts the CREATE and swallows exactly ORA-00955
-- ("name is already used by an existing object"), re-raising anything else.
-- ### table
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE dynamo_tables_catalog (
        table_name VARCHAR2(255) NOT NULL PRIMARY KEY,
        pg_table VARCHAR2(255) NOT NULL,
        pk_name VARCHAR2(255) NOT NULL,
        pk_type VARCHAR2(64) NOT NULL,
        sk_name VARCHAR2(255),
        sk_type VARCHAR2(64),
        status VARCHAR2(64) NOT NULL,
        creation_time_millis NUMBER(19) NOT NULL
    )';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;
