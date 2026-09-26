-- amqpwire store (StoreType.AMQP). Every enabled Postgres host gets all tables. The topology (exchanges, bindings, node
-- heartbeats, the cross-host publish outbox) is only WRITTEN on the first host of the set (the "home"). A queue (its definition
-- row and every message) lives wholly on the one host that owns hash(vhost, queue name); publishing to an exchange copies the
-- message into the queue of every matching binding, over several hosts through the outbox (idempotent per (vhost, queue, msg_id)).
-- ### exchanges
CREATE TABLE IF NOT EXISTS warp_amqp_exchanges (
    vhost       TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    durable     BOOLEAN NOT NULL,
    auto_delete BOOLEAN NOT NULL,
    internal    BOOLEAN NOT NULL,
    args        BYTEA NOT NULL,
    had_binding BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vhost, name)
);
-- ### bindings
CREATE TABLE IF NOT EXISTS warp_amqp_bindings (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vhost     TEXT NOT NULL,
    source    TEXT NOT NULL,
    dest      TEXT NOT NULL,
    dest_type CHAR(1) NOT NULL,
    rkey      TEXT NOT NULL,
    args      BYTEA NOT NULL,
    args_key  TEXT NOT NULL,
    UNIQUE (vhost, source, dest, dest_type, rkey, args_key)
);
-- ### bindings_dest_idx
CREATE INDEX IF NOT EXISTS warp_amqp_bindings_dest_idx ON warp_amqp_bindings (vhost, dest, dest_type);
-- ### nodes
CREATE TABLE IF NOT EXISTS warp_amqp_nodes (
    node TEXT PRIMARY KEY,
    beat TIMESTAMPTZ NOT NULL
);
-- ### outbox
CREATE TABLE IF NOT EXISTS warp_amqp_outbox (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### queues
CREATE TABLE IF NOT EXISTS warp_amqp_queues (
    vhost      TEXT NOT NULL,
    name       TEXT NOT NULL,
    durable    BOOLEAN NOT NULL,
    excl_owner TEXT,
    auto_delete BOOLEAN NOT NULL,
    args       BYTEA NOT NULL,
    max_prio   INT NOT NULL DEFAULT 0,
    ttl_ms     BIGINT,
    max_len    BIGINT,
    max_bytes  BIGINT,
    overflow   TEXT NOT NULL DEFAULT 'drop-head',
    dlx        TEXT,
    dlx_rkey   TEXT,
    expires_ms BIGINT,
    last_used  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vhost, name)
);
-- ### msgs
CREATE TABLE IF NOT EXISTS warp_amqp_msgs (
    seq        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vhost      TEXT NOT NULL,
    queue      TEXT NOT NULL,
    msg_id     TEXT NOT NULL,
    exchange   TEXT NOT NULL,
    rkey       TEXT NOT NULL,
    props      BYTEA NOT NULL,
    body       BYTEA NOT NULL,
    priority   INT NOT NULL DEFAULT 0,
    persistent BOOLEAN NOT NULL DEFAULT FALSE,
    enq_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    redelivered BOOLEAN NOT NULL DEFAULT FALSE,
    holder     TEXT,
    holder_at  TIMESTAMPTZ,
    deaths     BYTEA,
    raw10      BYTEA,
    dcount     INT NOT NULL DEFAULT 0
);
-- ### msgs_raw10
ALTER TABLE warp_amqp_msgs ADD COLUMN IF NOT EXISTS raw10 BYTEA;
-- ### msgs_dcount
ALTER TABLE warp_amqp_msgs ADD COLUMN IF NOT EXISTS dcount INT NOT NULL DEFAULT 0;
-- ### msgs_id_idx
CREATE UNIQUE INDEX IF NOT EXISTS warp_amqp_msgs_id_idx ON warp_amqp_msgs (vhost, queue, msg_id);
-- ### msgs_ready_idx
CREATE INDEX IF NOT EXISTS warp_amqp_msgs_ready_idx ON warp_amqp_msgs (vhost, queue, priority DESC, seq) WHERE holder IS NULL;
-- ### msgs_expiry_idx
CREATE INDEX IF NOT EXISTS warp_amqp_msgs_expiry_idx ON warp_amqp_msgs (expires_at) WHERE expires_at IS NOT NULL;
-- ### msgs_holder_idx
CREATE INDEX IF NOT EXISTS warp_amqp_msgs_holder_idx ON warp_amqp_msgs (holder) WHERE holder IS NOT NULL;
