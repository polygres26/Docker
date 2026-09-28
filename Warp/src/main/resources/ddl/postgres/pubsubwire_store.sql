-- pubsubwire store (StoreType.PUBSUB). Every enabled Postgres host gets all tables. The catalogs (topics, subscriptions,
-- snapshots, schemas, IAM policies, the publish outbox) are only WRITTEN on the first host of the set (the "home"). A
-- subscription's message queue (warp_pubsub_msgs rows) lives wholly on the one host that owns hash(subscription name), the
-- same rule sqswire uses for a queue; Publish copies each message into the queue of every subscription of the topic.
-- ### topics
CREATE TABLE IF NOT EXISTS warp_pubsub_topics (
    name       TEXT PRIMARY KEY,
    project    TEXT NOT NULL,
    doc        BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### topics_project_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_topics_project_idx ON warp_pubsub_topics (project, name);
-- ### subs
CREATE TABLE IF NOT EXISTS warp_pubsub_subs (
    name       TEXT PRIMARY KEY,
    project    TEXT NOT NULL,
    topic      TEXT NOT NULL,
    doc        BYTEA NOT NULL,
    filter     TEXT NOT NULL DEFAULT '',
    push       BOOLEAN NOT NULL DEFAULT FALSE,
    detached   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### subs_topic_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_subs_topic_idx ON warp_pubsub_subs (topic, name);
-- ### subs_project_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_subs_project_idx ON warp_pubsub_subs (project, name);
-- ### snapshots
CREATE TABLE IF NOT EXISTS warp_pubsub_snapshots (
    name       TEXT PRIMARY KEY,
    project    TEXT NOT NULL,
    topic      TEXT NOT NULL,
    origin_sub TEXT NOT NULL,
    doc        BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expire_at  TIMESTAMPTZ NOT NULL
);
-- ### snapshots_project_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_snapshots_project_idx ON warp_pubsub_snapshots (project, name);
-- ### schemas
CREATE TABLE IF NOT EXISTS warp_pubsub_schemas (
    name       TEXT PRIMARY KEY,
    project    TEXT NOT NULL,
    doc        BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### iam
CREATE TABLE IF NOT EXISTS warp_pubsub_iam (
    resource TEXT PRIMARY KEY,
    policy   BYTEA NOT NULL
);
-- ### msgs
CREATE TABLE IF NOT EXISTS warp_pubsub_msgs (
    sub              TEXT NOT NULL,
    seq              BIGINT GENERATED ALWAYS AS IDENTITY,
    msg_id           TEXT NOT NULL,
    data             BYTEA NOT NULL,
    attrs            TEXT NOT NULL DEFAULT '{}',
    ordering_key     TEXT NOT NULL DEFAULT '',
    publish_time     TIMESTAMPTZ NOT NULL,
    visible_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_attempt INT NOT NULL DEFAULT 0,
    ack_token        TEXT,
    acked            BOOLEAN NOT NULL DEFAULT FALSE,
    acked_at         TIMESTAMPTZ,
    PRIMARY KEY (sub, seq)
);
-- ### msgs_id_idx
CREATE UNIQUE INDEX IF NOT EXISTS warp_pubsub_msgs_id_idx ON warp_pubsub_msgs (sub, msg_id);
-- ### msgs_visible_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_msgs_visible_idx ON warp_pubsub_msgs (sub, visible_at) WHERE NOT acked;
-- ### msgs_order_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_msgs_order_idx ON warp_pubsub_msgs (sub, ordering_key, seq) WHERE NOT acked AND ordering_key <> '';
-- ### msgs_time_idx
CREATE INDEX IF NOT EXISTS warp_pubsub_msgs_time_idx ON warp_pubsub_msgs (sub, publish_time);
-- ### snapmsgs
CREATE TABLE IF NOT EXISTS warp_pubsub_snapmsgs (
    snapshot TEXT NOT NULL,
    sub      TEXT NOT NULL,
    msg_id   TEXT NOT NULL,
    PRIMARY KEY (snapshot, msg_id)
);
-- ### hold
CREATE TABLE IF NOT EXISTS warp_pubsub_hold (
    sub TEXT PRIMARY KEY
);
-- ### outbox
CREATE TABLE IF NOT EXISTS warp_pubsub_outbox (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      TEXT NOT NULL,
    payload    BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
