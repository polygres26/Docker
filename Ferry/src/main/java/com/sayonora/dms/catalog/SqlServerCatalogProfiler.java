package com.sayonora.dms.catalog;

import com.sayonora.dms.core.BackendTarget;
import com.sayonora.dms.core.SourceDialect;
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
            profileBuiltinFeatureUsage(statement, snapshot);
            profileSchemaSize(statement, snapshot);
        }

        return snapshot;
    }

    /**
     * Catalog-metadata-based portability-risk features -- exact counts from {@code sys.*} views,
     * not a text-scan heuristic, for the same reason {@link #tableCount}/{@link #viewCount} etc.
     * aren't text-scanned either: these are structural properties of columns/tables/objects, not
     * something that shows up in a routine body. Surfaced through {@link CatalogSnapshot#builtinPackageUsage}
     * (same generic "count by category" shape {@link MySqlCatalogProfiler}'s engine-mix scan
     * already uses) so {@code MigrationScorer} can weight them without a SQL-Server-specific field
     * for each one -- previously this map was populated by nothing at all for SQL Server, so
     * {@code scoreSqlServer} had no rubric to score it against; see {@code MYSQL_ENGINE_WEIGHT}'s
     * sibling table in {@code MigrationScorer} for the corresponding weights now that this exists.
     */
    private void profileBuiltinFeatureUsage(Statement statement, CatalogSnapshot snapshot) {
        profileOneFeature(statement, snapshot, "CLR assembly",
            "SELECT COUNT(*) FROM sys.assemblies WHERE is_user_defined = 1");
        profileOneFeature(statement, snapshot, "Service Broker queue",
            "SELECT COUNT(*) FROM sys.service_queues WHERE is_ms_shipped = 0");
        // 'timestamp' is the internal type name for both TIMESTAMP and ROWVERSION.
        profileOneFeature(statement, snapshot, "ROWVERSION/TIMESTAMP column",
            "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.system_type_id = t.system_type_id "
          + "WHERE t.name = 'timestamp'");
        // 2 = system-versioned temporal table.
        profileOneFeature(statement, snapshot, "Temporal table (SYSTEM_VERSIONING)",
            "SELECT COUNT(*) FROM sys.tables WHERE temporal_type = 2");
        profileOneFeature(statement, snapshot, "HIERARCHYID column",
            "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id "
          + "WHERE t.name = 'hierarchyid'");
        profileOneFeature(statement, snapshot, "Spatial column (geography/geometry)",
            "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id "
          + "WHERE t.name IN ('geography', 'geometry')");
    }

    /** Each feature is its own defensively-queried statement -- a permission gap or an
     * edition that lacks one sys.* view (e.g. Service Broker isn't in every edition) must not
     * take out every other feature's count along with it. */
    private void profileOneFeature(Statement statement, CatalogSnapshot snapshot, String feature, String sql) {
        try (ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                int count = rs.getInt(1);
                if (count > 0) snapshot.builtinPackageUsage.put(feature, count);
            }
        } catch (SQLException e) {
            snapshot.warnings.add("Could not check for \"" + feature + "\" usage: " + e.getMessage());
        }
    }

    private void profileVersion(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT @@VERSION")) {
            if (rs.next()) {
                snapshot.sourceVersion = rs.getString(1);
            }
        }
    }

    /** SQL Server Agent jobs live in msdb, a separate database -- the connecting login may not have access to it. */
    /** sys.dm_db_partition_stats covers the current database's own allocated pages -- no cross-database/instance-wide grant needed, unlike sp_spaceused's server-wide variants. */
    private void profileSchemaSize(Statement statement, CatalogSnapshot snapshot) {
        try (ResultSet rs = statement.executeQuery(
                "SELECT SUM(used_page_count) * 8 * 1024 FROM sys.dm_db_partition_stats")) {
            if (rs.next()) snapshot.schemaSizeBytes = rs.getLong(1);
        } catch (SQLException e) {
            snapshot.warnings.add("Could not read sys.dm_db_partition_stats -- schema size unavailable for sizing.");
        }
    }

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
        syntaxCounts.put("xp_cmdshell", 0);
        syntaxCounts.put("PIVOT/UNPIVOT", 0);
        syntaxCounts.put("sp_executesql (dynamic SQL)", 0);

        try (ResultSet rs = statement.executeQuery("SELECT definition FROM sys.sql_modules")) {
            while (rs.next()) {
                String body = rs.getString("definition");
                if (body == null) continue;
                String upper = body.toUpperCase();
                if (upper.contains("MERGE ")) syntaxCounts.merge("MERGE statement", 1, Integer::sum);
                if (upper.contains("OUTER APPLY") || upper.contains("CROSS APPLY")) syntaxCounts.merge("OUTER/CROSS APPLY", 1, Integer::sum);
                if (upper.contains("OPENQUERY") || upper.contains("OPENROWSET")) syntaxCounts.merge("OPENQUERY/OPENROWSET", 1, Integer::sum);
                if (upper.contains("NOLOCK")) syntaxCounts.merge("WITH (NOLOCK)", 1, Integer::sum);
                if (upper.contains("XP_CMDSHELL")) syntaxCounts.merge("xp_cmdshell", 1, Integer::sum);
                if (upper.contains(" PIVOT") || upper.contains(" UNPIVOT")) syntaxCounts.merge("PIVOT/UNPIVOT", 1, Integer::sum);
                if (upper.contains("SP_EXECUTESQL")) syntaxCounts.merge("sp_executesql (dynamic SQL)", 1, Integer::sum);
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
