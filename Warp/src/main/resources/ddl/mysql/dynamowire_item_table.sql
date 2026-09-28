-- Real per-engine differences from the Postgres original (ddl/postgres/dynamowire_item_table.sql):
--   text -> VARCHAR(255) (a PRIMARY KEY column needs a bounded length under InnoDB's own index
--     key-size limit; 255 comfortably covers real DynamoDB partition/sort key values, which are
--     themselves capped at 2048/1024 bytes by AWS but almost never approach that in practice)
--   numeric -> DECIMAL(38,10) (MySQL has no unbounded-precision numeric type; 38 digits matches
--     DynamoDB's own real numeric-precision limit)
--   jsonb -> JSON (MySQL's real native JSON column type since 5.7; no jsonb-style GIN/operator
--     support needed here since this store only ever reads/writes the whole column as text)
-- Real, disclosed gap: MySQL's CREATE INDEX has no IF NOT EXISTS -- harmless in practice, since
-- PgItemStore's own catalog check already prevents a second CreateTable call for the same table
-- name, but a real limitation if this DDL is ever re-run by hand.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    pk_value VARCHAR(255) NOT NULL,
    sk_value VARCHAR(255) NOT NULL DEFAULT '',
    sk_num DECIMAL(38,10),
    item JSON NOT NULL,
    PRIMARY KEY (pk_value, sk_value)
)
-- ### index_pk_sknum
CREATE INDEX ${table}_pk_sknum_idx ON ${table} (pk_value, sk_num)
