-- A DynamoDB secondary index as a partial expression index on the item table (PgItemStore). The names and the column / presence expressions are
-- generated from the table's schema, never taken from a request verbatim.
-- ### index
CREATE INDEX IF NOT EXISTS ${index} ON ${table} (${columns}) WHERE ${where}
