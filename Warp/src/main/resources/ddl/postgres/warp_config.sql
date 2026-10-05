-- Warp's own control plane: the versioned configuration every instance applies (config.ConfigStore). This is Postgres by design: the
-- default backend is Warp's control-plane connection, and the change notification below is Postgres LISTEN/NOTIFY.
-- ### table
CREATE TABLE IF NOT EXISTS warp_config (
    version    bigserial PRIMARY KEY,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
)
-- ### notify function
CREATE OR REPLACE FUNCTION warp_config_notify() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('${channel}', NEW.version::text); RETURN NEW; END;
$$ LANGUAGE plpgsql
-- ### notify trigger (dropped first so the function above is always the one wired in)
DROP TRIGGER IF EXISTS warp_config_notify_trigger ON warp_config
-- ### notify trigger create
CREATE TRIGGER warp_config_notify_trigger AFTER INSERT ON warp_config
FOR EACH ROW EXECUTE FUNCTION warp_config_notify()
