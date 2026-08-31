package com.nexagres.dms.core;

/**
 * Source database vendors Advisor can profile and score. Deliberately narrower than Omnigate's
 * {@code com.omnigate.core.SourceDialect} (which also covers routing-only warehouse dialects like
 * Snowflake/BigQuery/Databricks) -- Advisor's job is "assess + migrate to Postgres", scoped to the
 * project's stated sequence: Oracle first, then MariaDB/MySQL, then SQL Server. POSTGRES is
 * included as the migration *target* dialect, not a source Advisor profiles.
 */
public enum SourceDialect {
    ORACLE, MYSQL, MARIADB, SQL_SERVER, POSTGRES
}
