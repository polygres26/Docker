-- Expression index that lets the TTL sweep find expired items (PgItemStore). ${attr} is the TTL attribute name as a quoted SQL literal.
-- ### index
CREATE INDEX IF NOT EXISTS ${index} ON ${table} (((item->${attr}->>'N')::numeric)) WHERE item->${attr}->>'N' IS NOT NULL
