-- cosmoswire store (StoreType.COSMOS): the Azure Cosmos DB for NoSQL REST API on Postgres. Every enabled Postgres host gets all tables.
-- The database / container / stored-procedure catalog is only WRITTEN on the first host of the set (the "home"). A document lives on
-- ONE host: hash(database + '/' + container + '/' + first partition key value). Its exact JSON text is kept in body (property order and
-- number formats are preserved); _rid/_etag/_ts/_lsn are columns. _lsn is a per-container, per-host sequence assigned under a row lock,
-- so on each host commit order == _lsn order (the change feed reads by it).
-- ### dbs
CREATE TABLE IF NOT EXISTS warp_cosmos_dbs (
    id         TEXT COLLATE "C" PRIMARY KEY,
    rid        TEXT NOT NULL,
    etag       TEXT NOT NULL,
    ts         BIGINT NOT NULL,
    throughput INT
);
-- ### colls
CREATE TABLE IF NOT EXISTS warp_cosmos_colls (
    db         TEXT COLLATE "C" NOT NULL,
    id         TEXT COLLATE "C" NOT NULL,
    rid        TEXT NOT NULL,
    etag       TEXT NOT NULL,
    ts         BIGINT NOT NULL,
    def        TEXT NOT NULL,
    throughput INT,
    PRIMARY KEY (db, id)
);
-- ### scripts
CREATE TABLE IF NOT EXISTS warp_cosmos_scripts (
    db    TEXT COLLATE "C" NOT NULL,
    coll  TEXT COLLATE "C" NOT NULL,
    kind  TEXT COLLATE "C" NOT NULL,
    id    TEXT COLLATE "C" NOT NULL,
    rid   TEXT NOT NULL,
    etag  TEXT NOT NULL,
    ts    BIGINT NOT NULL,
    def   TEXT NOT NULL,
    PRIMARY KEY (db, coll, kind, id)
);
-- ### docs
CREATE TABLE IF NOT EXISTS warp_cosmos_docs (
    db    TEXT COLLATE "C" NOT NULL,
    coll  TEXT COLLATE "C" NOT NULL,
    pkh   TEXT COLLATE "C" NOT NULL,
    id    TEXT COLLATE "C" NOT NULL,
    body  TEXT NOT NULL,
    etag  TEXT NOT NULL,
    ts    BIGINT NOT NULL,
    lsn   BIGINT NOT NULL,
    rid   TEXT NOT NULL,
    ttl   INT,
    PRIMARY KEY (db, coll, pkh, id)
);
-- ### docs_lsn
CREATE INDEX IF NOT EXISTS warp_cosmos_docs_lsn ON warp_cosmos_docs (db, coll, lsn);
-- ### docs_rid
CREATE INDEX IF NOT EXISTS warp_cosmos_docs_rid ON warp_cosmos_docs (db, coll, rid);
-- ### seq
CREATE TABLE IF NOT EXISTS warp_cosmos_seq (
    db   TEXT COLLATE "C" NOT NULL,
    coll TEXT COLLATE "C" NOT NULL,
    n    BIGINT NOT NULL,
    PRIMARY KEY (db, coll)
);
-- ### uniq
CREATE TABLE IF NOT EXISTS warp_cosmos_uniq (
    db   TEXT COLLATE "C" NOT NULL,
    coll TEXT COLLATE "C" NOT NULL,
    pkh  TEXT COLLATE "C" NOT NULL,
    ux   INT NOT NULL,
    k    TEXT COLLATE "C" NOT NULL,
    id   TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (db, coll, pkh, ux, k)
);
