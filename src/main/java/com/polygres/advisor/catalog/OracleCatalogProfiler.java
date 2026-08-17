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
 * Catalog + source-text profiler for Oracle, scoped to Oracle Database 19c (the project's chosen
 * compatibility baseline -- see plan discussion: 19c is Oracle's LTS release and the de facto
 * enterprise floor, so the feature checklist below is built against it rather than trying to
 * cover every version's surface).
 *
 * <p>Deliberately queries {@code USER_*} data-dictionary views, not {@code DBA_*} or {@code ALL_*}
 * -- Advisor should run under the same low-privilege schema owner account a customer would
 * actually hand out for a read-only scan, not require DBA grants. A future enhancement can add an
 * optional {@code ALL_*}-scoped mode for a DBA running Advisor across multiple schemas at once.
 *
 * <p>Built-in package usage and syntax-construct detection are done by scanning {@code
 * USER_SOURCE.TEXT} with substring/regex matches -- deliberately simple and deterministic (see
 * {@link CatalogSnapshot}'s javadoc on why scoring inputs stay non-LLM). This will over-count
 * (e.g. a commented-out call) and under-count (e.g. dynamic SQL built as a string) versus true
 * usage; it's a first-pass signal, refined by the workload-capture layer (real V$SQL / audit
 * log) once that's built.
 */
public class OracleCatalogProfiler implements CatalogProfiler {

    /** Ranked by real-world frequency per the 19c feasibility plan -- highest-value packages first. */
    private static final String[] TRACKED_BUILTIN_PACKAGES = {
        "DBMS_OUTPUT", "UTL_FILE", "UTL_HTTP", "DBMS_LOB", "DBMS_SCHEDULER",
        "DBMS_LOCK", "DBMS_ALERT", "DBMS_CRYPTO", "DBMS_AQ", "DBMS_CLOUD",
        "DBMS_JOB", "DBMS_SQL", "DBMS_UTILITY", "DBMS_XMLGEN", "DBMS_METADATA"
    };

    @Override
    public CatalogSnapshot profile(BackendTarget target) throws SQLException {
        CatalogSnapshot snapshot = new CatalogSnapshot();
        snapshot.dialect = SourceDialect.ORACLE;

        try (Connection connection = target.open(); Statement statement = connection.createStatement()) {
            profileVersion(statement, snapshot);
            snapshot.tableCount = count(statement, "SELECT COUNT(*) FROM USER_TABLES");
            snapshot.viewCount = count(statement, "SELECT COUNT(*) FROM USER_VIEWS");
            snapshot.materializedViewCount = count(statement, "SELECT COUNT(*) FROM USER_MVIEWS");
            snapshot.sequenceCount = count(statement, "SELECT COUNT(*) FROM USER_SEQUENCES");
            snapshot.dbLinkCount = count(statement, "SELECT COUNT(*) FROM USER_DB_LINKS");
            snapshot.synonymCount = count(statement, "SELECT COUNT(*) FROM USER_SYNONYMS");
            snapshot.partitionedTableCount = count(statement,
                "SELECT COUNT(DISTINCT TABLE_NAME) FROM USER_TAB_PARTITIONS");
            snapshot.packageCount = count(statement,
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE'");
            snapshot.standaloneProcedureCount = count(statement,
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE'");
            snapshot.standaloneFunctionCount = count(statement,
                "SELECT COUNT(*) FROM USER_OBJECTS WHERE OBJECT_TYPE = 'FUNCTION'");
            profileTriggers(statement, snapshot);
            profileScheduledJobs(statement, snapshot);
            profileSourceText(statement, snapshot);
        }

        return snapshot;
    }

    private void profileVersion(Statement statement, CatalogSnapshot snapshot) {
        try (ResultSet rs = statement.executeQuery(
                "SELECT BANNER_FULL FROM V$VERSION WHERE BANNER_FULL LIKE 'Oracle Database%'")) {
            if (rs.next()) {
                snapshot.sourceVersion = rs.getString(1);
            }
        } catch (SQLException e) {
            // V$VERSION needs a grant some schema-owner accounts won't have; fall back and warn
            // rather than fail the whole scan over a cosmetic field.
            snapshot.warnings.add("Could not read V$VERSION (needs SELECT grant) -- version fingerprint unavailable.");
        }
        if (snapshot.sourceVersion == null || !snapshot.sourceVersion.contains("19.")) {
            snapshot.versionWarning = "This rubric is validated against Oracle 19c. "
                + (snapshot.sourceVersion == null ? "Source version could not be determined."
                    : "Detected: " + snapshot.sourceVersion)
                + " -- scores may be inaccurate for other versions.";
            snapshot.warnings.add(snapshot.versionWarning);
        }
    }

    private void profileTriggers(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        // Rough complexity proxy: trigger body line count. >20 lines is treated as "complex" --
        // a placeholder threshold to refine once real customer trigger bodies are on hand.
        try (ResultSet rs = statement.executeQuery(
                "SELECT TRIGGER_NAME, COUNT(*) AS LINE_COUNT FROM USER_TRIGGERS ut "
              + "JOIN USER_SOURCE us ON us.NAME = ut.TRIGGER_NAME AND us.TYPE = 'TRIGGER' "
              + "GROUP BY TRIGGER_NAME")) {
            while (rs.next()) {
                if (rs.getInt("LINE_COUNT") > 20) {
                    snapshot.complexTriggerCount++;
                } else {
                    snapshot.simpleTriggerCount++;
                }
            }
        } catch (SQLException e) {
            // USER_SOURCE has no TRIGGER rows on some Oracle configs (trigger body lives only in
            // USER_TRIGGERS.TRIGGER_BODY) -- fall back to a flat count without the complexity split.
            snapshot.simpleTriggerCount = count(statement, "SELECT COUNT(*) FROM USER_TRIGGERS");
        }
    }

    private void profileScheduledJobs(Statement statement, CatalogSnapshot snapshot) {
        try {
            snapshot.scheduledJobCount = count(statement, "SELECT COUNT(*) FROM USER_SCHEDULER_JOBS");
            if (snapshot.scheduledJobCount > 0) {
                snapshot.warnings.add(snapshot.scheduledJobCount + " DBMS_SCHEDULER job(s) found -- "
                    + "Postgres has no direct equivalent (pg_cron covers a subset); flagged as a known hard gap.");
            }
        } catch (SQLException e) {
            snapshot.warnings.add("Could not read USER_SCHEDULER_JOBS -- scheduled-job count unavailable.");
        }
    }

    /** Scans every USER_SOURCE line once for both builtin-package references and portability-risk syntax. */
    private void profileSourceText(Statement statement, CatalogSnapshot snapshot) throws SQLException {
        Map<String, Integer> builtinCounts = new LinkedHashMap<>();
        Map<String, Integer> syntaxCounts = new LinkedHashMap<>();
        syntaxCounts.put("CONNECT BY", 0);
        syntaxCounts.put("(+) outer join", 0);
        syntaxCounts.put("autonomous transaction", 0);

        try (ResultSet rs = statement.executeQuery("SELECT TEXT FROM USER_SOURCE")) {
            while (rs.next()) {
                String line = rs.getString("TEXT");
                if (line == null) continue;
                String upper = line.toUpperCase();

                for (String pkg : TRACKED_BUILTIN_PACKAGES) {
                    if (upper.contains(pkg)) {
                        builtinCounts.merge(pkg, 1, Integer::sum);
                    }
                }
                if (upper.contains("CONNECT BY")) {
                    syntaxCounts.merge("CONNECT BY", 1, Integer::sum);
                }
                if (line.contains("(+)")) {
                    syntaxCounts.merge("(+) outer join", 1, Integer::sum);
                }
                if (upper.contains("AUTONOMOUS_TRANSACTION")) {
                    syntaxCounts.merge("autonomous transaction", 1, Integer::sum);
                }
            }
        }

        builtinCounts.forEach((k, v) -> { if (v > 0) snapshot.builtinPackageUsage.put(k, v); });
        syntaxCounts.forEach((k, v) -> { if (v > 0) snapshot.syntaxConstructUsage.put(k, v); });
    }

    private int count(Statement statement, String sql) throws SQLException {
        try (ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    /**
     * Full source text for one PACKAGE/PACKAGE BODY/PROCEDURE/FUNCTION, ordered by line number --
     * the input {@link com.polygres.advisor.llm.PlsqlSummarizer} sends to the model. Separate from
     * {@link #profileSourceText}'s scan (which reads every object's source in bulk for the
     * deterministic builtin/syntax counts): this is an on-demand, single-object fetch, called only
     * when a summary is actually requested for that object, not baked into every scan.
     */
    public String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException {
        StringBuilder source = new StringBuilder();
        try (Connection connection = target.open();
             java.sql.PreparedStatement ps = connection.prepareStatement(
                 "SELECT TEXT FROM USER_SOURCE WHERE NAME = ? AND TYPE = ? ORDER BY LINE")) {
            ps.setString(1, objectName);
            ps.setString(2, objectType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    source.append(rs.getString("TEXT"));
                }
            }
        }
        return source.toString();
    }
}
