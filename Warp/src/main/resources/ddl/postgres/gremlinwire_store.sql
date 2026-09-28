-- gremlinwire store (StoreType.GREMLIN): Apache TinkerPop Gremlin Server protocol (also the Cosmos DB Gremlin API surface) on Postgres.
-- A property graph of its own: NOT the Neo4j/boltwire tables (warp_graph_nodes/warp_graph_edges), because a Gremlin graph needs
-- arbitrary element ids (Long, String, UUID), multi-valued vertex properties (with meta-properties) and typed values, none of which the
-- BIGSERIAL/flat-JSONB Bolt schema has; and because this store shards over several hosts while the Bolt graph cannot.
-- Every enabled Postgres host gets all tables:
--   vertices  vkey = 'l:<long>' | 's:<string>' | 'u:<uuid>'; the row lives on the host owning hash(vkey).
--             props = {"name":[{"v":<GraphSON3 value>,"m":{"metakey":<GraphSON3 value>}}, ...]}  (one entry per property value)
--   edges     ekey likewise; the row lives with its OUT vertex (host of hash(out_key)); in_key may name a vertex of another host,
--             so in-edge lookups scatter over all hosts.  props = {"weight":<GraphSON3 value>}
-- The element id sequence is only used on the first host of the set (the "home").
-- ### vertices
CREATE TABLE IF NOT EXISTS warp_gremlin_vertices (
    vkey  TEXT COLLATE "C" PRIMARY KEY,
    seq   BIGSERIAL,
    kind  SMALLINT GENERATED ALWAYS AS (CASE WHEN vkey LIKE 'l:%' THEN 0 ELSE 1 END) STORED,
    lid   BIGINT GENERATED ALWAYS AS (CASE WHEN vkey LIKE 'l:%' THEN substr(vkey, 3)::bigint ELSE 0 END) STORED,
    label TEXT NOT NULL,
    props JSONB NOT NULL DEFAULT '{}'::jsonb
);
-- ### vertices_order_index
-- full scans return elements in id order (numeric ids first): the order does not depend on how many hosts the graph is spread over
CREATE INDEX IF NOT EXISTS warp_gremlin_vertices_order_idx ON warp_gremlin_vertices (kind, lid, vkey);
-- ### vertices_label_index
CREATE INDEX IF NOT EXISTS warp_gremlin_vertices_label_idx ON warp_gremlin_vertices (label, seq);
-- ### vertices_props_index
CREATE INDEX IF NOT EXISTS warp_gremlin_vertices_props_idx ON warp_gremlin_vertices USING GIN (props jsonb_path_ops);
-- ### edges
CREATE TABLE IF NOT EXISTS warp_gremlin_edges (
    ekey      TEXT COLLATE "C" PRIMARY KEY,
    seq       BIGSERIAL,
    kind      SMALLINT GENERATED ALWAYS AS (CASE WHEN ekey LIKE 'l:%' THEN 0 ELSE 1 END) STORED,
    lid       BIGINT GENERATED ALWAYS AS (CASE WHEN ekey LIKE 'l:%' THEN substr(ekey, 3)::bigint ELSE 0 END) STORED,
    label     TEXT NOT NULL,
    out_key   TEXT COLLATE "C" NOT NULL,
    in_key    TEXT COLLATE "C" NOT NULL,
    out_label TEXT NOT NULL,
    in_label  TEXT NOT NULL,
    props     JSONB NOT NULL DEFAULT '{}'::jsonb
);
-- ### edges_order_index
CREATE INDEX IF NOT EXISTS warp_gremlin_edges_order_idx ON warp_gremlin_edges (kind, lid, ekey);
-- ### edges_out_index
CREATE INDEX IF NOT EXISTS warp_gremlin_edges_out_idx ON warp_gremlin_edges (out_key, label, seq);
-- ### edges_in_index
CREATE INDEX IF NOT EXISTS warp_gremlin_edges_in_idx ON warp_gremlin_edges (in_key, label, seq);
-- ### id_sequence
CREATE SEQUENCE IF NOT EXISTS warp_gremlin_id_seq START 1;
