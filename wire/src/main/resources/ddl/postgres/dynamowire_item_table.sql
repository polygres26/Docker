-- pk_value/sk_value use COLLATE "C" (byte order) so the primary-key btree sorts keys the way DynamoDB
-- orders them (UTF-8 byte order) whatever the database's default collation is.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    pk_value text COLLATE "C" NOT NULL,
    sk_value text COLLATE "C" NOT NULL DEFAULT '',
    sk_num numeric,
    item jsonb NOT NULL,
    PRIMARY KEY (pk_value, sk_value)
)
-- ### index_pk_sknum
CREATE INDEX IF NOT EXISTS ${table}_pk_sknum_idx ON ${table} (pk_value, sk_num)
