-- slot rebalancing (SlotRebalancer): removes the staging table; a table that is already gone raises the engine's own error, which the caller ignores.
-- ### drop staging table
DROP TABLE ${staging}
