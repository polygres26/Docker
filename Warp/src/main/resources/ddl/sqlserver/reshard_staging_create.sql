-- slot rebalancing (SlotRebalancer): an empty copy of the sharded table, same columns in the same order, no keys or indexes, where the moving rows are
-- gathered while writes to their slots are held, then published into the live table with one local INSERT ... SELECT.
-- SQL Server has no CREATE TABLE ... AS; SELECT ... INTO is its form (an identity column keeps its property there, so explicit values need IDENTITY_INSERT).
-- ### create staging table
SELECT * INTO ${staging} FROM ${table} WHERE 1 = 0
