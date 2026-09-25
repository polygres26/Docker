-- Postgres-only today, unlike ddl/*/dynamowire_item_table.sql and ddl/*/influxwire_measurement_
-- table.sql -- see DdlTemplates' own javadoc: the "labels TEXT[]" array column has no direct
-- cross-engine equivalent at all (none of Oracle, SQL Server, or MySQL have a native array column
-- type) -- a real port needs a schema redesign (a JSON array column, or a normalized
-- node_labels(node_id, label) join table), not just a syntax swap, so there's no honest
-- "ddl/oracle/boltwire_graph_schema.sql" to write yet. Real, tracked follow-up (see
-- docs/WARP_GUIDE.md's own backend-engine prerequisites section).
-- ### nodes_table
CREATE TABLE IF NOT EXISTS warp_graph_nodes (
    id BIGSERIAL PRIMARY KEY,
    labels TEXT[] NOT NULL DEFAULT '{}',
    properties JSONB NOT NULL DEFAULT '{}'
)
-- ### nodes_labels_index
CREATE INDEX IF NOT EXISTS warp_graph_nodes_labels_idx ON warp_graph_nodes USING GIN (labels)
-- ### nodes_properties_index
CREATE INDEX IF NOT EXISTS warp_graph_nodes_props_idx ON warp_graph_nodes USING GIN (properties)
-- ### edges_table
CREATE TABLE IF NOT EXISTS warp_graph_edges (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    from_id BIGINT NOT NULL REFERENCES warp_graph_nodes(id),
    to_id BIGINT NOT NULL REFERENCES warp_graph_nodes(id),
    properties JSONB NOT NULL DEFAULT '{}'
)
-- ### edges_from_index
CREATE INDEX IF NOT EXISTS warp_graph_edges_from_idx ON warp_graph_edges (from_id)
-- ### edges_to_index
CREATE INDEX IF NOT EXISTS warp_graph_edges_to_idx ON warp_graph_edges (to_id)
-- ### edges_type_index
CREATE INDEX IF NOT EXISTS warp_graph_edges_type_idx ON warp_graph_edges (type)
