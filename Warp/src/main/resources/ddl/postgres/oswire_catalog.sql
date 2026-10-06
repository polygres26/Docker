-- oswire (OpenSearch frontend): the index catalog and the index / component templates (PostgresSearchStore). Postgres only: the store uses JSONB
-- operators throughout.
-- ### index catalog
CREATE TABLE IF NOT EXISTS warp_os_catalog (
    name       TEXT PRIMARY KEY,
    uuid       TEXT NOT NULL,
    table_name TEXT NOT NULL,
    settings   JSONB NOT NULL,
    mappings   JSONB NOT NULL,
    aliases    JSONB NOT NULL DEFAULT '{}',
    created    BIGINT NOT NULL,
    closed     BOOLEAN NOT NULL DEFAULT FALSE
)
-- ### refresh state (added later)
ALTER TABLE warp_os_catalog ADD COLUMN IF NOT EXISTS refresh_state JSONB NOT NULL DEFAULT '{}'
-- ### templates
CREATE TABLE IF NOT EXISTS warp_os_templates (
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    body JSONB NOT NULL,
    PRIMARY KEY (kind, name)
)
