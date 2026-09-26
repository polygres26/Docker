-- kinesiswire store (StoreType.KINESIS). A stream (catalog row, shards, records, consumers) lives wholly on the host that owns
-- hash(stream name); ListStreams fans out over all hosts. Sequence numbers come from a per-shard counter row that
-- every writer increments under the row lock, which makes them strictly increasing per shard in commit order.
-- ### streams
CREATE TABLE IF NOT EXISTS warp_kinesis_streams (
    name             TEXT COLLATE "C" PRIMARY KEY,
    arn              TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ACTIVE',
    mode             TEXT NOT NULL DEFAULT 'PROVISIONED',
    retention_hours  INT NOT NULL DEFAULT 24,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags             JSONB NOT NULL DEFAULT '{}',
    encryption_type  TEXT NOT NULL DEFAULT 'NONE',
    key_id           TEXT,
    metrics          JSONB NOT NULL DEFAULT '[]',
    policy           TEXT,
    next_shard_index INT NOT NULL DEFAULT 0
);
-- ### shards
CREATE TABLE IF NOT EXISTS warp_kinesis_shards (
    stream          TEXT COLLATE "C" NOT NULL,
    shard_id        TEXT COLLATE "C" NOT NULL,
    idx             INT NOT NULL,
    hash_start      NUMERIC(39,0) NOT NULL,
    hash_end        NUMERIC(39,0) NOT NULL,
    parent          TEXT,
    adjacent_parent TEXT,
    next_seq        BIGINT NOT NULL DEFAULT 0,
    closed          BOOLEAN NOT NULL DEFAULT false,
    end_seq         BIGINT,
    PRIMARY KEY (stream, shard_id)
);
-- ### records
CREATE TABLE IF NOT EXISTS warp_kinesis_records (
    stream        TEXT COLLATE "C" NOT NULL,
    shard_id      TEXT COLLATE "C" NOT NULL,
    seq           BIGINT NOT NULL,
    partition_key TEXT NOT NULL,
    data          BYTEA NOT NULL,
    arrived       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stream, shard_id, seq)
);
-- ### records_arrived_idx
CREATE INDEX IF NOT EXISTS warp_kinesis_records_arrived_idx ON warp_kinesis_records (stream, arrived);
-- ### consumers
CREATE TABLE IF NOT EXISTS warp_kinesis_consumers (
    stream     TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    arn        TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, name)
);
