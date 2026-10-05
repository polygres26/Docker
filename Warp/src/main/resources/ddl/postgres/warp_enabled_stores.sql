-- Which store protocols are enabled on a backend host (core.StoreBootstrap). Postgres only: the stores themselves live in Postgres.
-- ### table
CREATE TABLE IF NOT EXISTS ${table} (
    store      text PRIMARY KEY,
    enabled_at timestamptz NOT NULL DEFAULT now()
)
