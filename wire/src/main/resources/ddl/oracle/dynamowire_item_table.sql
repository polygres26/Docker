-- Real per-engine differences from the Postgres original (ddl/postgres/dynamowire_item_table.sql):
--   text -> VARCHAR2(255) (same real key-size reasoning as the MySQL variant's own comment)
--   numeric -> NUMBER (Oracle's own real unbounded-precision numeric type, a closer match to
--     Postgres's own "numeric" than MySQL's bounded DECIMAL)
--   jsonb -> CLOB with a real "IS JSON" check constraint (Oracle 12c+ real, native JSON validation
--     -- 21c+ has an actual native JSON type, but CLOB+check is the broadly-compatible real choice
--     here, matching this project's own BackendDriverRegistry Oracle-version-agnostic stance)
-- Real, disclosed gap: Oracle DDL (pre-23c) has no IF NOT EXISTS at all -- same real mitigating
-- factor as the MySQL variant (PgItemStore's own catalog check already prevents a second
-- CreateTable call), but a real limitation if this DDL is ever re-run by hand.
-- Second real, disclosed gap, found live: Oracle treats an empty string ('') as NULL (a real,
-- well-known Oracle-specific behavior, unlike every other engine here) -- an item with no sort
-- key, which PgItemStore's own Java code writes as sk_value = '' (matching this table's own
-- Postgres DEFAULT ''), would violate the PRIMARY KEY's real NOT NULL requirement on sk_value the
-- instant it reaches Oracle. Confirmed live: CREATE TABLE/CREATE INDEX both succeed and a real
-- non-empty sk_value inserts and reads back correctly; only an EMPTY sk_value actually fails
-- (ORA-01400). A real fix needs a non-empty sentinel value for "no sort key" specifically on
-- Oracle, not attempted here -- this file's own DDL is otherwise real and correct.
-- ### table
CREATE TABLE ${table} (
    pk_value VARCHAR2(255) NOT NULL,
    sk_value VARCHAR2(255) DEFAULT '' NOT NULL,
    sk_num NUMBER,
    item CLOB NOT NULL,
    CONSTRAINT ${table}_pk PRIMARY KEY (pk_value, sk_value),
    CONSTRAINT ${table}_item_json CHECK (item IS JSON)
)
-- ### index_pk_sknum
CREATE INDEX ${table}_pk_sknum_idx ON ${table} (pk_value, sk_num)
