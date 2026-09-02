-- Real per-engine difference from the Postgres original (ddl/postgres/dynamowire_catalog.sql):
--   text -> VARCHAR(255) -- a PRIMARY KEY column needs a bounded length under InnoDB's own index
--   key-size limit; MySQL's own real error confirmed live, found the first time this catalog
--   table was ever created against a real MySQL backend: "BLOB/TEXT column 'table_name' used in
--   key specification without a key length" (ER_BLOB_KEY_WITHOUT_LENGTH). Every other column
--   here is metadata about a dynamowire table (names, types, a status word), never approaching
--   255 characters in practice.
--   bigint stays bigint -- MySQL's own real 64-bit integer type, same name as Postgres's.
-- ### table
CREATE TABLE IF NOT EXISTS _dynamo_tables (
    table_name VARCHAR(255) NOT NULL PRIMARY KEY,
    pg_table VARCHAR(255) NOT NULL,
    pk_name VARCHAR(255) NOT NULL,
    pk_type VARCHAR(64) NOT NULL,
    sk_name VARCHAR(255),
    sk_type VARCHAR(64),
    status VARCHAR(64) NOT NULL,
    creation_time_millis BIGINT NOT NULL
)
