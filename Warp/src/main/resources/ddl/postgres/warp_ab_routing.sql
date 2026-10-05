-- A/B routing configuration versions (ab.AbStore); every new version notifies the instances. Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_ab_routing (
    version    bigserial PRIMARY KEY,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
)
-- ### notify function
CREATE OR REPLACE FUNCTION warp_ab_routing_notify() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('${channel}', NEW.version::text); RETURN NEW; END; $$ LANGUAGE plpgsql
-- ### notify trigger drop
DROP TRIGGER IF EXISTS warp_ab_routing_notify_trigger ON warp_ab_routing
-- ### notify trigger create
CREATE TRIGGER warp_ab_routing_notify_trigger AFTER INSERT ON warp_ab_routing
FOR EACH ROW EXECUTE FUNCTION warp_ab_routing_notify()
