-- cqlwire store (StoreType.CQL): Apache Cassandra CQL native protocol on Postgres. Every enabled Postgres host gets all tables.
-- The schema catalog (warp_cql_schema: keyspaces, tables, user types, indexes as JSON specs) is only WRITTEN on the first host of
-- the set (the "home"). Data lives in ONE cell table (like Cassandra's own storage model): a partition -- all its cells -- lives on the
-- host owning hash(partition key), so a single-partition write, LWT or batch is one local transaction.
--   tid   table id;  token  Murmur3 token of the partition key (full scans return partitions in Cassandra's order)
--   pk    serialized partition key;   ck  order-preserving encoding of the clustering key (X'' = static row, X'01' = the one row of
--   a table without clustering columns);  ckv  the clustering values, serialized;
--   col   column name ('' = the row marker written by INSERT);  path  collection element (set member, map key, list position)
--   val   serialized value;  ts  write timestamp in microseconds;  expires_at  TTL expiry (swept, and hidden from reads once past)
-- ### schema
CREATE TABLE IF NOT EXISTS warp_cql_schema (
    seq  BIGSERIAL,
    kind TEXT COLLATE "C" NOT NULL,
    ks   TEXT COLLATE "C" NOT NULL,
    name TEXT COLLATE "C" NOT NULL,
    spec TEXT NOT NULL,
    PRIMARY KEY (kind, ks, name)
);
-- ### cells
CREATE TABLE IF NOT EXISTS warp_cql_cells (
    tid        UUID NOT NULL,
    token      BIGINT NOT NULL,
    pk         BYTEA NOT NULL,
    ck         BYTEA NOT NULL,
    ckv        BYTEA NOT NULL DEFAULT ''::bytea,
    col        TEXT COLLATE "C" NOT NULL,
    path       BYTEA NOT NULL DEFAULT ''::bytea,
    val        BYTEA,
    ts         BIGINT NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (tid, token, pk, ck, col, path)
);
-- ### cells_expiry
CREATE INDEX IF NOT EXISTS warp_cql_cells_expiry_idx ON warp_cql_cells (expires_at) WHERE expires_at IS NOT NULL;
