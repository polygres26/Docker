-- azblobwire store (StoreType.AZBLOB). Every enabled Postgres host gets all tables. The container catalog and the
-- per-account service properties are only WRITTEN on the first host of the set (the "home"); blobs, their
-- snapshots and their data chunks live on the shard that owns hash(account + "/" + container + "/" + blob name).
-- Key columns use COLLATE "C" so btree order == Azure's UTF-8 byte order and prefix listings are range scans.
-- ### containers
CREATE TABLE IF NOT EXISTS warp_azblob_containers (
    account       TEXT COLLATE "C" NOT NULL,
    name          TEXT COLLATE "C" NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT now(),
    etag          TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}',
    public_access TEXT NOT NULL DEFAULT '',
    acl           JSONB NOT NULL DEFAULT '[]',
    lease_state   TEXT NOT NULL DEFAULT 'available',
    lease_id      TEXT,
    lease_duration INT NOT NULL DEFAULT 0,
    lease_expiry  TIMESTAMPTZ,
    lease_break   TIMESTAMPTZ,
    PRIMARY KEY (account, name)
);
-- ### service
CREATE TABLE IF NOT EXISTS warp_azblob_service (
    account    TEXT COLLATE "C" PRIMARY KEY,
    properties JSONB NOT NULL DEFAULT '{}'
);
-- ### blobs
CREATE TABLE IF NOT EXISTS warp_azblob_blobs (
    account             TEXT COLLATE "C" NOT NULL,
    container           TEXT COLLATE "C" NOT NULL,
    name                TEXT COLLATE "C" NOT NULL,
    snapshot            TEXT COLLATE "C" NOT NULL DEFAULT '',
    blob_type           TEXT NOT NULL,
    size                BIGINT NOT NULL DEFAULT 0,
    etag                TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_modified       TIMESTAMPTZ NOT NULL DEFAULT now(),
    content_type        TEXT,
    content_encoding    TEXT,
    content_language    TEXT,
    content_md5         TEXT,
    cache_control       TEXT,
    content_disposition TEXT,
    metadata            JSONB NOT NULL DEFAULT '{}',
    tags                JSONB NOT NULL DEFAULT '{}',
    tier                TEXT,
    tier_inferred       BOOLEAN NOT NULL DEFAULT TRUE,
    tier_changed        TIMESTAMPTZ,
    segments            JSONB NOT NULL DEFAULT '[]',
    sealed              BOOLEAN NOT NULL DEFAULT FALSE,
    seq                 BIGINT NOT NULL DEFAULT 0,
    lease_state         TEXT NOT NULL DEFAULT 'available',
    lease_id            TEXT,
    lease_duration      INT NOT NULL DEFAULT 0,
    lease_expiry        TIMESTAMPTZ,
    lease_break         TIMESTAMPTZ,
    copy_id             TEXT,
    copy_source         TEXT,
    copy_status         TEXT,
    copy_completion     TIMESTAMPTZ,
    PRIMARY KEY (account, container, name, snapshot)
);
-- ### blobs_tags_idx
CREATE INDEX IF NOT EXISTS warp_azblob_blobs_tags_idx ON warp_azblob_blobs (account) WHERE tags <> '{}'::jsonb;
-- ### data
CREATE TABLE IF NOT EXISTS warp_azblob_data (
    data_id UUID NOT NULL,
    seq     INT NOT NULL,
    data    BYTEA NOT NULL,
    PRIMARY KEY (data_id, seq)
);
-- ### data_storage
ALTER TABLE warp_azblob_data ALTER COLUMN data SET STORAGE EXTERNAL;
-- ### data_owner
CREATE TABLE IF NOT EXISTS warp_azblob_data_owner (
    data_id    UUID PRIMARY KEY,
    account    TEXT COLLATE "C" NOT NULL,
    container  TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    snapshot   TEXT COLLATE "C" NOT NULL DEFAULT '',
    kind       TEXT NOT NULL,
    block_id   TEXT,
    size       BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### data_owner_idx
CREATE INDEX IF NOT EXISTS warp_azblob_data_owner_idx ON warp_azblob_data_owner (account, container, name, kind);
