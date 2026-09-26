-- kafkawire store (StoreType.KAFKA): the Apache Kafka wire protocol on Postgres. Every enabled Postgres host gets all tables.
-- Topic metadata, the broker registry, producer ids, consumer group state and committed offsets are only WRITTEN on the first host of the
-- set (the "home"). A partition's log lives on ONE host: hash(topic + '-' + partition). Batches are stored exactly as produced (record
-- batch v2, opaque: compression, headers and idempotence fields are preserved) with the base offset assigned by the broker.
-- ### meta
CREATE TABLE IF NOT EXISTS warp_kafka_meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
-- ### brokers
CREATE TABLE IF NOT EXISTS warp_kafka_brokers (
    node_id INT PRIMARY KEY,
    host    TEXT NOT NULL,
    port    INT NOT NULL,
    beat    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (host, port)
);
-- ### topics
CREATE TABLE IF NOT EXISTS warp_kafka_topics (
    name       TEXT COLLATE "C" PRIMARY KEY,
    topic_id   UUID NOT NULL,
    partitions INT NOT NULL,
    config     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_ms BIGINT NOT NULL
);
-- ### parts
CREATE TABLE IF NOT EXISTS warp_kafka_parts (
    topic     TEXT COLLATE "C" NOT NULL,
    part      INT NOT NULL,
    log_start BIGINT NOT NULL DEFAULT 0,
    next_off  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, part)
);
-- ### log
CREATE TABLE IF NOT EXISTS warp_kafka_log (
    topic    TEXT COLLATE "C" NOT NULL,
    part     INT NOT NULL,
    base_off BIGINT NOT NULL,
    last_off BIGINT NOT NULL,
    nbytes   INT NOT NULL,
    first_ts BIGINT NOT NULL,
    max_ts   BIGINT NOT NULL,
    batch    BYTEA NOT NULL,
    PRIMARY KEY (topic, part, base_off)
);
-- ### log_ts
CREATE INDEX IF NOT EXISTS warp_kafka_log_ts_idx ON warp_kafka_log (topic, part, max_ts);
-- ### pid_seq
CREATE SEQUENCE IF NOT EXISTS warp_kafka_pid_seq START 1000;
-- ### producers
CREATE TABLE IF NOT EXISTS warp_kafka_producers (
    pid        BIGINT PRIMARY KEY,
    epoch      INT NOT NULL,
    created_ms BIGINT NOT NULL
);
-- ### pseq
CREATE TABLE IF NOT EXISTS warp_kafka_pseq (
    topic     TEXT COLLATE "C" NOT NULL,
    part      INT NOT NULL,
    pid       BIGINT NOT NULL,
    epoch     INT NOT NULL,
    first_seq INT NOT NULL,
    last_seq  INT NOT NULL,
    base_off  BIGINT NOT NULL,
    PRIMARY KEY (topic, part, pid)
);
-- ### groups
CREATE TABLE IF NOT EXISTS warp_kafka_groups (
    grp           TEXT COLLATE "C" PRIMARY KEY,
    state         TEXT NOT NULL,
    protocol_type TEXT NOT NULL DEFAULT '',
    protocol_name TEXT NOT NULL DEFAULT '',
    generation    INT NOT NULL DEFAULT 0,
    leader        TEXT NOT NULL DEFAULT '',
    members       JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_ms    BIGINT NOT NULL
);
-- ### offsets
CREATE TABLE IF NOT EXISTS warp_kafka_offsets (
    grp          TEXT COLLATE "C" NOT NULL,
    topic        TEXT COLLATE "C" NOT NULL,
    part         INT NOT NULL,
    committed    BIGINT NOT NULL,
    leader_epoch INT NOT NULL DEFAULT -1,
    metadata     TEXT,
    commit_ms    BIGINT NOT NULL,
    PRIMARY KEY (grp, topic, part)
);
