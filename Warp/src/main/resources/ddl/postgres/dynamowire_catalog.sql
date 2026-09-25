-- ### table
CREATE TABLE IF NOT EXISTS _dynamo_tables (
    table_name text PRIMARY KEY,
    pg_table text NOT NULL,
    pk_name text NOT NULL,
    pk_type text NOT NULL,
    sk_name text,
    sk_type text,
    status text NOT NULL,
    creation_time_millis bigint NOT NULL
)
