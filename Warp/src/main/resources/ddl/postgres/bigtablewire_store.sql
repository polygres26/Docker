-- bigtablewire store (StoreType.BIGTABLE). Every enabled Postgres host gets all tables. The table catalog (warp_bt_tables: name,
-- serialized admin Table proto with column families and their GC rules) is only WRITTEN on the first host of the set (the
-- "home"). Cells live on the host that owns hash(table name, row key): a row and all its cells are on one host, so a
-- single-row mutation, CheckAndMutateRow or ReadModifyWriteRow is one local transaction (serialised per row by an advisory lock).
-- Byte columns sort bytewise (bytea), the family name uses COLLATE "C": the primary key order is Bigtable's row/column order.
-- ### tables
CREATE TABLE IF NOT EXISTS warp_bt_tables (
    name       TEXT COLLATE "C" PRIMARY KEY,
    parent     TEXT COLLATE "C" NOT NULL,
    spec       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### tables_parent_idx
CREATE INDEX IF NOT EXISTS warp_bt_tables_parent_idx ON warp_bt_tables (parent, name);
-- ### cells
CREATE TABLE IF NOT EXISTS warp_bt_cells (
    tbl     TEXT COLLATE "C" NOT NULL,
    row_key BYTEA NOT NULL,
    family  TEXT COLLATE "C" NOT NULL,
    qual    BYTEA NOT NULL,
    ts      BIGINT NOT NULL,
    val     BYTEA NOT NULL,
    PRIMARY KEY (tbl, row_key, family, qual, ts)
);
