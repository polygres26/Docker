-- firestorewire store (StoreType.FIRESTORE). Every enabled Postgres host gets all tables. A document lives on the host that
-- owns hash(database + "/" + document path); collection scans and queries scatter-gather over all hosts.
--   warp_firestore_docs : the current state, one row per existing document. `data` is the serialized
--                         google.firestore.v1.Document (fields only) so every Firestore value type survives exactly.
--                         name_key is the document path as UTF-8 segments joined by 0x00, so bytea order == Firestore's
--                         __name__ order (segment-wise). coll_path / coll_id are the parent collection's full path / last id.
--   warp_firestore_log  : append-only version log (one row per write incl. deletes) used for read_time / read-only
--                         transactions, Listen resume tokens and the Listen change feed. Pruned to the retention window,
--                         always keeping the newest row at or before the horizon of every document.
-- ### docs
CREATE TABLE IF NOT EXISTS warp_firestore_docs (
    db        TEXT NOT NULL,
    name_key  BYTEA NOT NULL,
    path      TEXT COLLATE "C" NOT NULL,
    coll_path TEXT COLLATE "C" NOT NULL,
    coll_id   TEXT COLLATE "C" NOT NULL,
    data      BYTEA NOT NULL,
    create_us BIGINT NOT NULL,
    update_us BIGINT NOT NULL,
    PRIMARY KEY (db, name_key)
)
-- ### docs_coll
CREATE INDEX IF NOT EXISTS warp_firestore_docs_coll ON warp_firestore_docs (db, coll_path, name_key)
-- ### docs_group
CREATE INDEX IF NOT EXISTS warp_firestore_docs_group ON warp_firestore_docs (db, coll_id, name_key)
-- ### log
CREATE TABLE IF NOT EXISTS warp_firestore_log (
    seq       BIGSERIAL PRIMARY KEY,
    db        TEXT NOT NULL,
    name_key  BYTEA NOT NULL,
    path      TEXT COLLATE "C" NOT NULL,
    coll_path TEXT COLLATE "C" NOT NULL,
    coll_id   TEXT COLLATE "C" NOT NULL,
    commit_us BIGINT NOT NULL,
    deleted   BOOLEAN NOT NULL,
    data      BYTEA,
    create_us BIGINT NOT NULL
)
-- ### log_doc
CREATE INDEX IF NOT EXISTS warp_firestore_log_doc ON warp_firestore_log (db, name_key, commit_us)
-- ### log_time
CREATE INDEX IF NOT EXISTS warp_firestore_log_time ON warp_firestore_log (db, commit_us)
