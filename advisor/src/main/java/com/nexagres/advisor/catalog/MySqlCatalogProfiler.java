package com.nexagres.advisor.catalog;

import com.nexagres.advisor.core.BackendTarget;
import com.nexagres.advisor.core.SourceDialect;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalog + source-text profiler for MySQL and MariaDB -- the project's phase-2 dialect. One
 * class covers both (they share {@code information_schema}'s shape almost entirely); the handful
 * of places they diverge (native sequences, JSON functions) are branched on {@link #dialect}.
 *
 * <p>Queries {@code information_schema}, scoped to {@code TABLE_SCHEMA = DATABASE()} -- the
 * connecting user's current schema, same low-privilege posture as {@link OracleCatalogProfiler}'s
 * {@code USER_*} views (no {@code information_schema.GLOBAL_*} or {@code mysql.*} system-table
 * access required).
 *
 * <p>Unlike Oracle, MySQL/MariaDB have no PACKAGE construct and no native scheduler comparable to
 * {@code DBMS_SCHEDULER} (MariaDB's {@code EVENT}s are the closest analog, tracked separately);
 * {@link CatalogSnapshot#packageCount} and {@link CatalogSnapshot#scheduledJobCount} are populated
 * from what actually maps (events -> scheduledJobCount), not left meaningless.
 */
public class MySqlCatalogProfiler implements CatalogProfiler {

    /** Storage-engine and syntax constructs that carry real Postgres-migration risk, ranked by frequency/impact. */
    private static final String[] TRACKED_ENGINES = { "MyISAM", "MEMORY", "ARCHIVE", "FEDERATED" };

    @Override
    public CatalogSnapshot profile(BackendTarget target) throws SQLException {
        CatalogSnapshot snapshot = new CatalogSnapshot();
        snapshot.dialect = target.dialect();

        try (Connection connection = target.open(); Statement statement = connection.createStatement()) {
            profileVersion(statement, snapshot);
            snapshot.tableCount = count(statement,
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'");
            snapshot.viewCount = count(statement,
                "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = DATABASE()");
            snapshot.simpleTriggerCount = count(statement,
                "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema = DATABASE()");
            snapshot.standaloneProcedureCount = count(statement,
                "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE'");
            snapshot.standaloneFunctionCount = count(statement,
                "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'FUNCTION'");
            snapshot.partitionedTableCount = count(statement,
                "SELECT COUNT(DISTINCT table_name) FROM information_schema.partitions "
              + "WHERE table_schema = DATABASE() AND partition_name IS NOT NULL");

            if (snapshot.dialect == SourceDialect.MARIADB) {
                // Native SEQUENCE objects (MariaDB 10.3+) surface as table_type = 'SEQUENCE'.
                snapshot.sequenceCount = count(statement,
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'SEQUENCE'");
                snapshot.scheduledJobCount = count(statement,
                    "SELECT COUNT(*) FROM information_schema.events WHERE event_schema = DATABASE()");
            } else {
                // Plain MySQL has no native sequence object (AUTO_INCREMENT covers the common case)
                // and its EVENT scheduler entries are tracked the same way MariaDB's are.
                snapshot.scheduledJobCount = count(statement,
                    "SELECT COUNT(*) FROM information_schema.events WHERE event_schema = DATABASE()");
            }

            profileEngineMix(statement, snapshot);
            profileSourceText(statement, snapshot);
            profileSchemaSize(statement, snapshot);
        }

        if (snapshot.scheduledJobCount > 0) {
            snapshot.warnings.add(snapshot.scheduledJobCount + " scheduled EVENT(s) found -- "
                + "Postgres has no built-in event scheduler (pg_cron covers a subset); flagged as a known gap.");
        }

        return snapshot;
    }

    private void profileVersion(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                snapshot.sourceVersion = rs.getString(1);
            }
        }
    }

    /** MyISAM/MEMORY/ARCHIVE/FEDERATED tables carry real migration risk (no transactions, no FKs, or no direct Postgres equivalent at all) -- surfaced via builtinPackageUsage's generic "count by category" shape so MigrationScorer can weight them without a MySQL-specific field. */
    private void profileSchemaSize(Statement statement, CatalogSnapshot snapshot) {
        try (ResultSet rs = statement.executeQuery(
                "SELECT COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables WHERE table_schema = DATABASE()")) {
            if (rs.next()) snapshot.schemaSizeBytes = rs.getLong(1);
        } catch (SQLException e) {
            snapshot.warnings.add("Could not read information_schema.tables size columns -- schema size unavailable for sizing.");
        }
    }

    private void profileEngineMix(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        try (ResultSet rs = statement.executeQuery(
                "SELECT engine, COUNT(*) AS cnt FROM information_schema.tables "
              + "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' GROUP BY engine")) {
            while (rs.next()) {
                String engine = rs.getString("engine");
                int cnt = rs.getInt("cnt");
                for (String tracked : TRACKED_ENGINES) {
                    if (tracked.equalsIgnoreCase(engine) && cnt > 0) {
                        snapshot.builtinPackageUsage.put("ENGINE=" + tracked, cnt);
                    }
                }
            }
        }
    }

    /** Scans every routine/trigger body once for portability-risk syntax -- same non-LLM, deterministic approach as Oracle's source-text scan. */
    private void profileSourceText(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        Map<String, Integer> syntaxCounts = new LinkedHashMap<>();
        syntaxCounts.put("ON DUPLICATE KEY UPDATE", 0);
        syntaxCounts.put("GROUP_CONCAT", 0);
        syntaxCounts.put("STRAIGHT_JOIN", 0);
        syntaxCounts.put("GET_LOCK/RELEASE_LOCK", 0);
        syntaxCounts.put("FOUND_ROWS/SQL_CALC_FOUND_ROWS", 0);
        syntaxCounts.put("MATCH ... AGAINST (fulltext)", 0);
        syntaxCounts.put("REPLACE INTO", 0);
        syntaxCounts.put("LOAD_FILE/INTO OUTFILE", 0);
        syntaxCounts.put("JSON_* functions", 0);

        try (ResultSet rs = statement.executeQuery(
                "SELECT routine_definition AS body FROM information_schema.routines WHERE routine_schema = DATABASE() "
              + "UNION ALL "
              + "SELECT action_statement AS body FROM information_schema.triggers WHERE trigger_schema = DATABASE()")) {
            while (rs.next()) {
                String body = rs.getString("body");
                if (body == null) continue;
                String upper = body.toUpperCase();
                if (upper.contains("ON DUPLICATE KEY UPDATE")) syntaxCounts.merge("ON DUPLICATE KEY UPDATE", 1, Integer::sum);
                if (upper.contains("GROUP_CONCAT")) syntaxCounts.merge("GROUP_CONCAT", 1, Integer::sum);
                if (upper.contains("STRAIGHT_JOIN")) syntaxCounts.merge("STRAIGHT_JOIN", 1, Integer::sum);
                if (upper.contains("GET_LOCK") || upper.contains("RELEASE_LOCK")) syntaxCounts.merge("GET_LOCK/RELEASE_LOCK", 1, Integer::sum);
                if (upper.contains("FOUND_ROWS") || upper.contains("SQL_CALC_FOUND_ROWS")) syntaxCounts.merge("FOUND_ROWS/SQL_CALC_FOUND_ROWS", 1, Integer::sum);
                if (upper.contains("MATCH") && upper.contains("AGAINST")) syntaxCounts.merge("MATCH ... AGAINST (fulltext)", 1, Integer::sum);
                if (upper.contains("REPLACE INTO")) syntaxCounts.merge("REPLACE INTO", 1, Integer::sum);
                if (upper.contains("LOAD_FILE") || upper.contains("INTO OUTFILE")) syntaxCounts.merge("LOAD_FILE/INTO OUTFILE", 1, Integer::sum);
                if (upper.contains("JSON_")) syntaxCounts.merge("JSON_* functions", 1, Integer::sum);
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
