-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    pk_value text NOT NULL,
    sk_value text NOT NULL DEFAULT '',
    sk_num numeric,
    item jsonb NOT NULL,
    PRIMARY KEY (pk_value, sk_value)
)
-- ### index_pk_sknum
CREATE INDEX IF NOT EXISTS ${table}_pk_sknum_idx ON ${table} (pk_value, sk_num)
