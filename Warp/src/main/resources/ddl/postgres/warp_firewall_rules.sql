-- SQL firewall rules (config.FirewallRuleStore); every change notifies the instances so they reload. Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_firewall_rules (
    id             bigserial PRIMARY KEY,
    priority       integer NOT NULL DEFAULT 100,
    action         text NOT NULL CHECK (action IN ('allow', 'deny')),
    statement_type text,
    table_pattern  text,
    sql_pattern    text,
    enabled        boolean NOT NULL DEFAULT true,
    description    text,
    created_at     timestamptz NOT NULL DEFAULT now()
)
-- ### notify function
CREATE OR REPLACE FUNCTION warp_firewall_rules_notify() RETURNS trigger AS $notify$
BEGIN PERFORM pg_notify('${channel}', ''); RETURN NULL; END;
$notify$ LANGUAGE plpgsql
-- ### notify trigger drop
DROP TRIGGER IF EXISTS warp_firewall_rules_notify_trigger ON warp_firewall_rules
-- ### notify trigger create
CREATE TRIGGER warp_firewall_rules_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON warp_firewall_rules
FOR EACH STATEMENT EXECUTE FUNCTION warp_firewall_rules_notify()
