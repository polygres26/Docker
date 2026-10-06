-- influxwire catalog (PgTimeSeriesStore): databases, retention policies, measurements and their field / tag keys. Postgres only.
-- ### databases
CREATE TABLE IF NOT EXISTS _warp_influx_dbs (name TEXT PRIMARY KEY)
-- ### retention policies
CREATE TABLE IF NOT EXISTS _warp_influx_rps (
    db             TEXT NOT NULL,
    name           TEXT NOT NULL,
    duration       BIGINT NOT NULL,
    shard_duration BIGINT NOT NULL,
    replication    INT NOT NULL,
    is_default     BOOLEAN NOT NULL,
    PRIMARY KEY (db, name)
)
-- ### measurements
CREATE TABLE IF NOT EXISTS _warp_influx_meas (
    db   TEXT NOT NULL,
    rp   TEXT NOT NULL,
    meas TEXT NOT NULL,
    PRIMARY KEY (db, rp, meas)
)
-- ### field keys
CREATE TABLE IF NOT EXISTS _warp_influx_fields (
    db   TEXT NOT NULL,
    rp   TEXT NOT NULL,
    meas TEXT NOT NULL,
    key  TEXT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY (db, rp, meas, key)
)
-- ### tag keys
CREATE TABLE IF NOT EXISTS _warp_influx_tagkeys (
    db   TEXT NOT NULL,
    rp   TEXT NOT NULL,
    meas TEXT NOT NULL,
    key  TEXT NOT NULL,
    PRIMARY KEY (db, rp, meas, key)
)
