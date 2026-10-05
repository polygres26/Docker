-- Coordination between Warp instances for automatic promotion (config.PgFailoverCoordination): one lease per backend (only the holder
-- promotes) and each instance's latest observation of whether the primary is down (the majority vote). Postgres only (control plane).
-- ### lease
CREATE TABLE IF NOT EXISTS warp_failover_lease (
    backend    text PRIMARY KEY,
    holder     text NOT NULL,
    term       bigint NOT NULL,
    expires_at timestamptz NOT NULL
)
-- ### observation
CREATE TABLE IF NOT EXISTS warp_failover_observation (
    backend     text NOT NULL,
    instance    text NOT NULL,
    primary_url text NOT NULL,
    down        boolean NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (backend, instance)
)
