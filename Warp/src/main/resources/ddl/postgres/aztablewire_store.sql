-- aztablewire store (StoreType.AZTABLE). The table catalog (with stored access policies) is only WRITTEN on the first
-- host of the set (the "home"); an entity lives on the shard that owns hash(account + "/" + table + "/" + PartitionKey).
-- Property values are kept in a JSONB map name -> {"t": <Edm type>, "v": <value>}.
-- ### tables
CREATE TABLE IF NOT EXISTS warp_aztable_tables (
    account    TEXT COLLATE "C" NOT NULL,
    name       TEXT COLLATE "C" NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acl        JSONB NOT NULL DEFAULT '[]',
    PRIMARY KEY (account, name)
);
-- ### service
CREATE TABLE IF NOT EXISTS warp_aztable_service (
    account    TEXT COLLATE "C" PRIMARY KEY,
    properties JSONB NOT NULL DEFAULT '{}'
);
-- ### entities
CREATE TABLE IF NOT EXISTS warp_aztable_entities (
    account       TEXT COLLATE "C" NOT NULL,
    tbl           TEXT COLLATE "C" NOT NULL,
    pk            TEXT COLLATE "C" NOT NULL,
    rk            TEXT COLLATE "C" NOT NULL,
    ts            TEXT NOT NULL,
    props         JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (account, tbl, pk, rk)
);
