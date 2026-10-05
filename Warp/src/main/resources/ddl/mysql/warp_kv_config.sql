-- Legacy key/value config table (core.ConfigStore; the live config is config.ConfigStore's versioned warp_config). It takes any JDBC URL, so
-- it has a file per engine; only Postgres and MySQL have one, as before.
-- ### table
CREATE TABLE IF NOT EXISTS warp_config (
    config_key   VARCHAR(255) PRIMARY KEY,
    config_value TEXT,
    updated_at   TIMESTAMP,
    updated_by   VARCHAR(255)
)
