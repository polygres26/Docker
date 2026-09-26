-- gcswire store (StoreType.GCS). Every enabled Postgres host gets all tables. The bucket catalog and the HMAC keys are only
-- WRITTEN on the first host of the set (the "home"); objects (all generations), their data chunks and the upload sessions
-- live on the shard that owns hash(bucket + "/" + object name). Separate warp_gcs_ tables (not the s3 store's): a GCS
-- generation is an int64 with live/noncurrent state and a metageneration, an object has no S3 version-id/delete-marker
-- model, and sharing tables with the s3 store would make one bucket namespace answer to two services.
-- Key columns use COLLATE "C" so btree order == GCS's UTF-8 byte order and prefix listings are range scans.
-- ### buckets
CREATE TABLE IF NOT EXISTS warp_gcs_buckets (
    name           TEXT COLLATE "C" PRIMARY KEY,
    project        TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    metageneration BIGINT NOT NULL DEFAULT 1,
    versioning     BOOLEAN NOT NULL DEFAULT FALSE,
    doc            JSONB NOT NULL DEFAULT '{}'
);
-- ### hmac
CREATE TABLE IF NOT EXISTS warp_gcs_hmac (
    access_id  TEXT PRIMARY KEY,
    secret     TEXT NOT NULL,
    project    TEXT NOT NULL,
    sa_email   TEXT NOT NULL,
    state      TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
-- ### objects
CREATE TABLE IF NOT EXISTS warp_gcs_objects (
    bucket              TEXT COLLATE "C" NOT NULL,
    name                TEXT COLLATE "C" NOT NULL,
    generation          BIGINT NOT NULL,
    metageneration      BIGINT NOT NULL DEFAULT 1,
    size                BIGINT NOT NULL,
    md5                 TEXT,
    crc32c              TEXT NOT NULL,
    component_count     INT,
    content_type        TEXT,
    content_encoding    TEXT,
    content_disposition TEXT,
    cache_control       TEXT,
    content_language    TEXT,
    storage_class       TEXT NOT NULL,
    custom_time         TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    class_updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at          TIMESTAMPTZ,
    metadata            JSONB NOT NULL DEFAULT '{}',
    doc                 JSONB NOT NULL DEFAULT '{}',
    segments            JSONB NOT NULL DEFAULT '[]',
    PRIMARY KEY (bucket, name, generation)
);
-- ### objects_live
CREATE UNIQUE INDEX IF NOT EXISTS warp_gcs_objects_live ON warp_gcs_objects (bucket, name) WHERE deleted_at IS NULL;
-- ### data
CREATE TABLE IF NOT EXISTS warp_gcs_data (
    data_id UUID NOT NULL,
    seq     INT NOT NULL,
    data    BYTEA NOT NULL,
    PRIMARY KEY (data_id, seq)
);
-- ### data_storage
ALTER TABLE warp_gcs_data ALTER COLUMN data SET STORAGE EXTERNAL;
-- ### data_owner
CREATE TABLE IF NOT EXISTS warp_gcs_data_owner (
    data_id    UUID PRIMARY KEY,
    bucket     TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0,
    kind       TEXT NOT NULL,
    ref        TEXT NOT NULL DEFAULT '',
    part       INT NOT NULL DEFAULT 0,
    etag       TEXT,
    size       BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### data_owner_idx
CREATE INDEX IF NOT EXISTS warp_gcs_data_owner_idx ON warp_gcs_data_owner (bucket, name, generation);
-- ### data_owner_ref
CREATE INDEX IF NOT EXISTS warp_gcs_data_owner_ref ON warp_gcs_data_owner (ref) WHERE ref <> '';
-- ### sessions
CREATE TABLE IF NOT EXISTS warp_gcs_sessions (
    upload_id  TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    bucket     TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    request    JSONB NOT NULL DEFAULT '{}',
    segments   JSONB NOT NULL DEFAULT '[]',
    persisted  BIGINT NOT NULL DEFAULT 0,
    state      TEXT NOT NULL DEFAULT 'open',
    result     JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
