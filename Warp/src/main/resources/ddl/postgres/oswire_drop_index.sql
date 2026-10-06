-- oswire: deleting an OpenSearch index drops its table and sequence on every shard (PostgresSearchStore).
-- ### drop table
DROP TABLE IF EXISTS ${table}
-- ### drop sequence
DROP SEQUENCE IF EXISTS ${table}_seq
