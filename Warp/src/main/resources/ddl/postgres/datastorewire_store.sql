-- datastorewire store (StoreType.DATASTORE). Separate tables from firestorewire (a Datastore-mode database and a Firestore
-- native-mode database are different, mutually exclusive products with different key and value models).
-- An entity lives on the host that owns hash(namespace-qualified ROOT ancestor key), so an entity group (and every ancestor query)
-- is on one host; kind / kindless queries scatter-gather. `data` is the serialized google.datastore.v1.Entity, `key_bytes` the
-- order-preserving encoding of the key path (kind/id-or-name pairs; the partition is in `ns`), `root_key` its first element, `version` the
-- entity version (microsecond commit time of the last write) and create_us / update_us its timestamps.
-- ### entities
CREATE TABLE IF NOT EXISTS warp_datastore_entities (
    ns        TEXT NOT NULL,
    key_bytes BYTEA NOT NULL,
    root_key  BYTEA NOT NULL,
    kind      TEXT COLLATE "C" NOT NULL,
    data      BYTEA NOT NULL,
    version   BIGINT NOT NULL,
    create_us BIGINT NOT NULL,
    update_us BIGINT NOT NULL,
    PRIMARY KEY (ns, key_bytes)
)
-- ### entities_kind
CREATE INDEX IF NOT EXISTS warp_datastore_entities_kind ON warp_datastore_entities (ns, kind, key_bytes)
-- ### ids
CREATE TABLE IF NOT EXISTS warp_datastore_ids (
    scope   TEXT PRIMARY KEY,
    next_id BIGINT NOT NULL
)
