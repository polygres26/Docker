package com.polygres.advisor.catalog;

import com.polygres.advisor.core.BackendTarget;
import com.polygres.advisor.core.SourceDialect;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalog + source-text profiler for SQL Server -- phase 3, after Oracle and MySQL/MariaDB.
 * Queries {@code sys.*} catalog views, scoped to the connecting database (no {@code msdb}/
 * instance-wide access required for the core scan) -- same low-privilege posture as the other two
 * profilers.
 *
 * <p>SQL Server Agent jobs ({@link CatalogSnapshot#scheduledJobCount}) live in {@code msdb}, a
 * separate database the connecting login may not have access to -- queried defensively; a
 * permissions failure there degrades to "job count unavailable" rather than failing the whole
 * scan (see {@link #profileScheduledJobs}).
 */
public class SqlServerCatalogProfiler implements CatalogProfiler {

    @Override
    public CatalogSnapshot profile(BackendTarget target) throws SQLException {
        CatalogSnapshot snapshot = new CatalogSnapshot();
        snapshot.dialect = SourceDialect.SQL_SERVER;

        try (Connection connection = target.open(); Statement statement = connection.createStatement()) {
            profileVersion(statement, snapshot);
            snapshot.tableCount = count(statement, "SELECT COUNT(*) FROM sys.tables");
            snapshot.viewCount = count(statement, "SELECT COUNT(*) FROM sys.views");
            snapshot.simpleTriggerCount = count(statement, "SELECT COUNT(*) FROM sys.triggers");
            snapshot.standaloneProcedureCount = count(statement, "SELECT COUNT(*) FROM sys.procedures");
            snapshot.standaloneFunctionCount = count(statement,
                "SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN','TF','IF')"); // scalar / table-valued / inline table-valued functions
            snapshot.synonymCount = count(statement, "SELECT COUNT(*) FROM sys.synonyms");
            snapshot.partitionedTableCount = count(statement,
                "SELECT COUNT(DISTINCT object_id) FROM sys.partitions WHERE partition_number > 1");
            snapshot.sequenceCount = count(statement, "SELECT COUNT(*) FROM sys.sequences");
            snapshot.dbLinkCount = count(statement, "SELECT COUNT(*) FROM sys.servers WHERE is_linked = 1");

            profileScheduledJobs(connection, statement, snapshot);
            profileSourceText(statement, snapshot);
        }

        return snapshot;
    }

    private void profileVersion(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT @@VERSION")) {
            if (rs.next()) {
                snapshot.sourceVersion = rs.getString(1);
            }
        }
    }

    /** SQL Server Agent jobs live in msdb, a separate database -- the connecting login may not have access to it. */
    private void profileScheduledJobs(Connection connection, Statement statement, CatalogSnapshot snapshot) {
        try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM msdb.dbo.sysjobs")) {
            if (rs.next()) {
                snapshot.scheduledJobCount = rs.getInt(1);
                if (snapshot.scheduledJobCount > 0) {
                    snapshot.warnings.add(snapshot.scheduledJobCount + " SQL Server Agent job(s) found -- "
                        + "Postgres has no built-in job scheduler (pg_cron covers a subset); flagged as a known gap.");
                }
            }
        } catch (SQLException e) {
            snapshot.warnings.add("Could not read msdb.dbo.sysjobs (needs access to msdb) -- scheduled-job count unavailable.");
        }
    }

    /** Scans every routine/trigger definition once for portability-risk T-SQL constructs -- same deterministic, non-LLM approach as the other two profilers. */
    private void profileSourceText(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        Map<String, Integer> syntaxCounts = new LinkedHashMap<>();
        syntaxCounts.put("MERGE statement", 0);
        syntaxCounts.put("OUTER/CROSS APPLY", 0);
        syntaxCounts.put("OPENQUERY/OPENROWSET", 0);
        syntaxCounts.put("WITH (NOLOCK)", 0);

        try (ResultSet rs = statement.executeQuery("SELECT definition FROM sys.sql_modules")) {
            while (rs.next()) {
                String body = rs.getString("definition");
                if (body == null) continue;
                String upper = body.toUpperCase();
                if (upper.contains("MERGE ")) syntaxCounts.merge("MERGE statement", 1, Integer::sum);
                if (upper.contains("OUTER APPLY") || upper.contains("CROSS APPLY")) syntaxCounts.merge("OUTER/CROSS APPLY", 1, Integer::sum);
                if (upper.contains("OPENQUERY") || upper.contains("OPENROWSET")) syntaxCounts.merge("OPENQUERY/OPENROWSET", 1, Integer::sum);
                if (upper.contains("NOLOCK")) syntaxCounts.merge("WITH (NOLOCK)", 1, Integer::sum);
            }
        }

        syntaxCounts.forEach((k, v) -> { if (v > 0) snapshot.syntaxConstructUsage.put(k, v); });
    }

    private int count(Statement statement, String sql) throws SQLException {
        try (ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }
}
