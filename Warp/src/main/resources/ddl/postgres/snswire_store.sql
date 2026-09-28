-- snswire store (StoreType.SNS). A topic and everything hanging off it (subscriptions, FIFO dedup window, recorded
-- deliveries) lives wholly on the host that owns hash(topic name); platform applications, endpoints and SMS settings live on
-- the first host of the set (the "home").
-- ### topics
CREATE TABLE IF NOT EXISTS warp_sns_topics (
    name       TEXT COLLATE "C" PRIMARY KEY,
    arn        TEXT NOT NULL,
    fifo       BOOLEAN NOT NULL DEFAULT false,
    attributes JSONB NOT NULL DEFAULT '{}',
    tags       JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### subscriptions
CREATE TABLE IF NOT EXISTS warp_sns_subscriptions (
    arn        TEXT COLLATE "C" PRIMARY KEY,
    topic      TEXT COLLATE "C" NOT NULL,
    topic_arn  TEXT NOT NULL,
    protocol   TEXT NOT NULL,
    endpoint   TEXT NOT NULL,
    owner      TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    confirmed  BOOLEAN NOT NULL DEFAULT true,
    token      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### subscriptions_topic_idx
CREATE INDEX IF NOT EXISTS warp_sns_subscriptions_topic_idx ON warp_sns_subscriptions (topic);
-- ### dedup
CREATE TABLE IF NOT EXISTS warp_sns_dedup (
    topic      TEXT COLLATE "C" NOT NULL,
    dedup_id   TEXT COLLATE "C" NOT NULL,
    message_id TEXT NOT NULL,
    sequence   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, dedup_id)
);
-- ### deliveries
CREATE TABLE IF NOT EXISTS warp_sns_deliveries (
    id               BIGSERIAL PRIMARY KEY,
    topic_arn        TEXT NOT NULL,
    subscription_arn TEXT,
    protocol         TEXT NOT NULL,
    endpoint         TEXT NOT NULL,
    message_id       TEXT NOT NULL,
    payload          TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### deliveries_time_idx
CREATE INDEX IF NOT EXISTS warp_sns_deliveries_time_idx ON warp_sns_deliveries (created_at);
-- ### platform_apps
CREATE TABLE IF NOT EXISTS warp_sns_platform_apps (
    arn        TEXT COLLATE "C" PRIMARY KEY,
    name       TEXT NOT NULL,
    platform   TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'
);
-- ### platform_endpoints
CREATE TABLE IF NOT EXISTS warp_sns_platform_endpoints (
    arn        TEXT COLLATE "C" PRIMARY KEY,
    app_arn    TEXT NOT NULL,
    token      TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'
);
-- ### sms
CREATE TABLE IF NOT EXISTS warp_sns_sms (
    k TEXT COLLATE "C" PRIMARY KEY,
    v TEXT NOT NULL
);
-- ### optouts
CREATE TABLE IF NOT EXISTS warp_sns_optouts (
    phone TEXT COLLATE "C" PRIMARY KEY,
    opted_out BOOLEAN NOT NULL DEFAULT true
);
