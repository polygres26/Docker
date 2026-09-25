-- One point per (database, retention policy, series, timestamp): a repeated write of the same series+time
-- merges its fields (InfluxDB semantics). md5(tags::text) keeps the index entry small for wide tag sets;
-- jsonb::text is canonical so equal tag sets always hash equally. Includes `time` so TimescaleDB accepts it.
-- ### unique
CREATE UNIQUE INDEX IF NOT EXISTS ${table}_uq ON ${table} (db, rp, time, ns, (md5(tags::text)))
