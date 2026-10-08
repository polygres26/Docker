-- Cluster-wide hold used while slots of a sharded table move between shards (config.PgReshardCoordination): one gate row per table,
-- written by the instance doing the move, and one acknowledgement row per (instance, table) written by every Warp instance once it has
-- applied the gate. Postgres only (control plane).
-- ### gate
CREATE TABLE IF NOT EXISTS warp_reshard_gate (
    table_name   text PRIMARY KEY,
    epoch        bigint NOT NULL,
    phase        text NOT NULL,
    slots        text NOT NULL DEFAULT '',
    block_scatter boolean NOT NULL DEFAULT false,
    flip_version bigint NOT NULL DEFAULT 0,
    holder       text NOT NULL,
    lease_until  timestamptz NOT NULL
)
-- ### ack
CREATE TABLE IF NOT EXISTS warp_reshard_ack (
    table_name text NOT NULL,
    instance   text NOT NULL,
    epoch      bigint NOT NULL,
    acked_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, instance)
)
