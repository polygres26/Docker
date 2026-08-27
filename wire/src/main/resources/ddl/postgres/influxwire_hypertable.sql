-- Only ever run after ddl/postgres/influxwire_measurement_table.sql, and only when
-- PgTimeSeriesStore's own timescaleAvailable() check found the extension installed -- see that
-- class's own javadoc. Real, TimescaleDB-only optimization: no equivalent DDL exists for this in
-- ddl/oracle, ddl/sqlserver, or ddl/mysql at all (see this project's own POLYWIRE_GUIDE.md §4.4/
-- prerequisites section for the full reasoning) -- those three engines simply never call this file.
-- ### hypertable
SELECT create_hypertable('${table}', 'time', if_not_exists => true, migrate_data => true)
