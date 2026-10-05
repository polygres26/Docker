-- Warp instances heartbeating into the shared config database (config.NodeRegistry): the electorate for the failover majority rule and the
-- peer list for remote partition joins. Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_nodes (
    node_id        uuid PRIMARY KEY,
    host           text NOT NULL,
    admin_port     int NOT NULL,
    zone           text,
    version        text,
    started_at     timestamptz NOT NULL,
    last_heartbeat timestamptz NOT NULL
)
-- ### peer port (additive migration for remote-partition-join peer discovery; IF NOT EXISTS keeps it idempotent on existing deployments)
ALTER TABLE warp_nodes ADD COLUMN IF NOT EXISTS peer_grpc_port int NOT NULL DEFAULT 0
