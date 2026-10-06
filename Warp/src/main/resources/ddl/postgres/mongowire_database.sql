-- mongowire: one Postgres schema per MongoDB database, with its collection catalog and the unique-key table (PostgresDocumentStore).
-- ${schema}, ${catalog}, ${unique_keys} and ${index} are already-quoted identifiers built from the database name.
-- ### schema
CREATE SCHEMA IF NOT EXISTS ${schema}
-- ### collection catalog
CREATE TABLE IF NOT EXISTS ${catalog} (name text PRIMARY KEY, entry bytea NOT NULL)
-- ### unique keys
CREATE TABLE IF NOT EXISTS ${unique_keys} (
    coll text NOT NULL,
    idx  text NOT NULL,
    k    text NOT NULL,
    id   text NOT NULL,
    PRIMARY KEY (coll, idx, k)
)
-- ### unique keys by document id
CREATE INDEX IF NOT EXISTS ${index} ON ${unique_keys} (coll, id)
