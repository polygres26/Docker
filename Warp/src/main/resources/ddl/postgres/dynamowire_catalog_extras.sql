-- dynamowire catalog additions that exist on Postgres only (PgItemStore). They are added in place so catalogs created by older versions keep working.
-- ### table metadata (attribute definitions, secondary indexes, billing, TTL, tags) as a JSON document
ALTER TABLE _dynamo_tables ADD COLUMN IF NOT EXISTS meta text
-- ### transact-write idempotency tokens
CREATE TABLE IF NOT EXISTS _dynamo_txn_tokens (
    token          text PRIMARY KEY,
    request_hash   text NOT NULL,
    created_millis bigint NOT NULL
)
