-- ACME certificate state (tls.acme.AcmeDb). Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_acme_state (
    name       text PRIMARY KEY,
    value      text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
)
