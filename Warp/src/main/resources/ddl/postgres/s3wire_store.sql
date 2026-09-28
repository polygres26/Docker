-- s3wire object store (StoreType.S3). Every enabled Postgres host gets all tables; the bucket catalog
-- (warp_s3_buckets) is only WRITTEN on the first host of the set, objects/chunks/uploads live on the
-- shard that owns hash(bucket + "/" + key). Key columns use COLLATE "C" so btree order == S3's UTF-8
-- byte order and prefix listings are index range scans.
-- ### buckets
CREATE TABLE IF NOT EXISTS warp_s3_buckets (
    name       TEXT COLLATE "C" PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### objects
CREATE TABLE IF NOT EXISTS warp_s3_objects (
    bucket              TEXT COLLATE "C" NOT NULL,
    key                 TEXT COLLATE "C" NOT NULL,
    version_id          TEXT,
    object_id           UUID NOT NULL,
    size                BIGINT NOT NULL,
    etag                TEXT NOT NULL,
    chunk_size          INT NOT NULL,
    segments            JSONB,
    content_type        TEXT,
    cache_control       TEXT,
    content_disposition TEXT,
    content_encoding    TEXT,
    content_language    TEXT,
    expires             TEXT,
    user_metadata       JSONB,
    last_modified       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, key)
);
-- ### blobs
CREATE TABLE IF NOT EXISTS warp_s3_blobs (
    object_id  UUID PRIMARY KEY,
    state      TEXT NOT NULL,
    size       BIGINT NOT NULL DEFAULT 0,
    state_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### blobs_state_idx
CREATE INDEX IF NOT EXISTS warp_s3_blobs_state_idx ON warp_s3_blobs (state, state_at);
-- ### chunks
CREATE TABLE IF NOT EXISTS warp_s3_chunks (
    object_id UUID NOT NULL,
    seq       INT NOT NULL,
    data      BYTEA NOT NULL,
    PRIMARY KEY (object_id, seq)
);
-- ### chunks_storage
ALTER TABLE warp_s3_chunks ALTER COLUMN data SET STORAGE EXTERNAL;
-- ### multipart_uploads
CREATE TABLE IF NOT EXISTS warp_s3_multipart_uploads (
    upload_id UUID PRIMARY KEY,
    bucket    TEXT COLLATE "C" NOT NULL,
    key       TEXT COLLATE "C" NOT NULL,
    initiated TIMESTAMPTZ NOT NULL DEFAULT now(),
    attrs     JSONB
);
-- ### parts
CREATE TABLE IF NOT EXISTS warp_s3_parts (
    upload_id   UUID NOT NULL,
    part_number INT NOT NULL,
    object_id   UUID NOT NULL,
    size        BIGINT NOT NULL,
    etag        TEXT NOT NULL,
    chunk_size  INT NOT NULL,
    PRIMARY KEY (upload_id, part_number)
);
-- ### buckets_region
ALTER TABLE warp_s3_buckets ADD COLUMN IF NOT EXISTS region TEXT;
-- ### buckets_versioning
ALTER TABLE warp_s3_buckets ADD COLUMN IF NOT EXISTS versioning TEXT;
-- ### buckets_object_lock
ALTER TABLE warp_s3_buckets ADD COLUMN IF NOT EXISTS object_lock BOOLEAN NOT NULL DEFAULT false;
-- ### buckets_ownership
ALTER TABLE warp_s3_buckets ADD COLUMN IF NOT EXISTS ownership TEXT;
-- ### bucket_config
CREATE TABLE IF NOT EXISTS warp_s3_bucket_config (
    bucket     TEXT COLLATE "C" NOT NULL,
    kind       TEXT NOT NULL,
    body       TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, kind)
);
-- ### objects_checksums
ALTER TABLE warp_s3_objects ADD COLUMN IF NOT EXISTS checksums JSONB;
-- ### objects_checksum_type
ALTER TABLE warp_s3_objects ADD COLUMN IF NOT EXISTS checksum_type TEXT;
-- ### objects_extra
ALTER TABLE warp_s3_objects ADD COLUMN IF NOT EXISTS extra JSONB;
-- ### versions
CREATE TABLE IF NOT EXISTS warp_s3_versions (
    seq                 BIGINT GENERATED ALWAYS AS IDENTITY,
    bucket              TEXT COLLATE "C" NOT NULL,
    key                 TEXT COLLATE "C" NOT NULL,
    version_id          TEXT NOT NULL,
    delete_marker       BOOLEAN NOT NULL DEFAULT false,
    object_id           UUID NOT NULL,
    size                BIGINT NOT NULL,
    etag                TEXT NOT NULL,
    chunk_size          INT NOT NULL,
    segments            JSONB,
    content_type        TEXT,
    cache_control       TEXT,
    content_disposition TEXT,
    content_encoding    TEXT,
    content_language    TEXT,
    expires             TEXT,
    user_metadata       JSONB,
    last_modified       TIMESTAMPTZ NOT NULL DEFAULT now(),
    checksums           JSONB,
    checksum_type       TEXT,
    extra               JSONB,
    PRIMARY KEY (bucket, key, version_id)
);
-- ### versions_seq_idx
CREATE INDEX IF NOT EXISTS warp_s3_versions_seq_idx ON warp_s3_versions (bucket, key, seq);
-- ### parts_checksums
ALTER TABLE warp_s3_parts ADD COLUMN IF NOT EXISTS checksums JSONB;
-- ### parts_modified
ALTER TABLE warp_s3_parts ADD COLUMN IF NOT EXISTS modified TIMESTAMPTZ NOT NULL DEFAULT now();
-- ### annotations
CREATE TABLE IF NOT EXISTS warp_s3_annotations (
    bucket      TEXT COLLATE "C" NOT NULL,
    key         TEXT COLLATE "C" NOT NULL,
    version_id  TEXT NOT NULL,
    name        TEXT COLLATE "C" NOT NULL,
    payload     BYTEA NOT NULL,
    etag        TEXT NOT NULL,
    checksums   JSONB,
    checksum_type TEXT,
    content_type TEXT,
    modified    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, key, version_id, name)
);
