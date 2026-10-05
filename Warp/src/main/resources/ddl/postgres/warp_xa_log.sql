-- Recovery log for in-doubt XA transactions (xa.XaRecoveryLog). Postgres only (control plane).
-- The three backend_* columns were added later: ADD COLUMN IF NOT EXISTS lets an already deployed table pick them up on the next restart.
-- They stay nullable because a row from before they existed has none, and recovery then falls back to resolving the backend by name.
-- ### table
CREATE TABLE IF NOT EXISTS warp_xa_log (
    gtrid_hex    text NOT NULL,
    branch_index integer NOT NULL,
    backend_name text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz,
    PRIMARY KEY (gtrid_hex, branch_index)
)
-- ### backend url
ALTER TABLE warp_xa_log ADD COLUMN IF NOT EXISTS backend_jdbc_url text
-- ### backend user
ALTER TABLE warp_xa_log ADD COLUMN IF NOT EXISTS backend_user text
-- ### backend password
ALTER TABLE warp_xa_log ADD COLUMN IF NOT EXISTS backend_password text
