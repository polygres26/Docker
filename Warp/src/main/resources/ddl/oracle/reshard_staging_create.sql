-- slot rebalancing (SlotRebalancer): an empty copy of the sharded table, same columns in the same order, no keys or indexes, where the moving rows are
-- gathered while writes to their slots are held, then published into the live table with one local INSERT ... SELECT.
-- ### create staging table
CREATE TABLE ${staging} AS SELECT * FROM ${table} WHERE 1 = 0
