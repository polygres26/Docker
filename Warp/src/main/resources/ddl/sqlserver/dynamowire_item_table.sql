-- Real per-engine differences from the Postgres original (ddl/postgres/dynamowire_item_table.sql):
--   text -> NVARCHAR(450) (SQL Server caps a PRIMARY KEY/index column at 900 bytes; 450 unicode
--     chars comfortably covers real DynamoDB partition/sort key values while leaving room for
--     the composite (pk_value, sk_value) key to fit under that limit)
--   numeric -> DECIMAL(38,10) (SQL Server's own real max-precision numeric type)
--   jsonb -> NVARCHAR(MAX) with a real ISJSON() check constraint (SQL Server has no native JSON
--     column type at all -- this is the standard, documented Microsoft pattern for storing and
--     validating JSON in a plain text column)
-- Real, disclosed gap: SQL Server's CREATE INDEX has no IF NOT EXISTS either -- same real
-- mitigating factor as the MySQL/Oracle variants (PgItemStore's own catalog check already
-- prevents a second CreateTable call), but a real limitation if this DDL is ever re-run by hand.
-- ### table
CREATE TABLE ${table} (
    pk_value NVARCHAR(450) NOT NULL,
    sk_value NVARCHAR(450) NOT NULL DEFAULT '',
    sk_num DECIMAL(38,10),
    item NVARCHAR(MAX) NOT NULL CHECK (ISJSON(item) = 1),
    CONSTRAINT ${table}_pk PRIMARY KEY (pk_value, sk_value)
)
-- ### index_pk_sknum
CREATE INDEX ${table}_pk_sknum_idx ON ${table} (pk_value, sk_num)
