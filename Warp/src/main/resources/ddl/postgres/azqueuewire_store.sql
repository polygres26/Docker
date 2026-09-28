-- azqueuewire store (StoreType.AZQUEUE). The queue catalog (with metadata and stored access policies) is only WRITTEN on the
-- first host of the set (the "home") so ListQueues is one query; a queue's messages live wholly on the shard that owns
-- hash(account + "/" + queue name).
-- ### queues
CREATE TABLE IF NOT EXISTS warp_azqueue_queues (
    account    TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata   JSONB NOT NULL DEFAULT '{}',
    acl        JSONB NOT NULL DEFAULT '[]',
    PRIMARY KEY (account, name)
);
-- ### service
CREATE TABLE IF NOT EXISTS warp_azqueue_service (
    account    TEXT COLLATE "C" PRIMARY KEY,
    properties JSONB NOT NULL DEFAULT '{}'
);
-- ### messages
CREATE TABLE IF NOT EXISTS warp_azqueue_messages (
    seq           BIGSERIAL,
    account       TEXT COLLATE "C" NOT NULL,
    queue         TEXT COLLATE "C" NOT NULL,
    message_id    UUID NOT NULL,
    inserted_at   TIMESTAMPTZ NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    visible_at    TIMESTAMPTZ NOT NULL,
    dequeue_count INT NOT NULL DEFAULT 0,
    pop_receipt   TEXT,
    body          TEXT NOT NULL,
    PRIMARY KEY (account, queue, message_id)
);
-- ### messages_visible_idx
CREATE INDEX IF NOT EXISTS warp_azqueue_messages_visible_idx ON warp_azqueue_messages (account, queue, visible_at, seq);
-- ### messages_expiry_idx
CREATE INDEX IF NOT EXISTS warp_azqueue_messages_expiry_idx ON warp_azqueue_messages (expires_at);
