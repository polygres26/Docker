-- mongowire: the table behind one MongoDB collection (PostgresDocumentStore). ${table} and ${index} are already-quoted identifiers.
-- The two ALTERs bring tables created by older versions up to date.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (id text PRIMARY KEY, doc jsonb NOT NULL, bson bytea, seq bigint GENERATED ALWAYS AS IDENTITY)
-- ### bson column
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS bson bytea
-- ### seq column
ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS seq bigint GENERATED ALWAYS AS IDENTITY
-- ### seq index
CREATE INDEX IF NOT EXISTS ${index} ON ${table} (seq)
