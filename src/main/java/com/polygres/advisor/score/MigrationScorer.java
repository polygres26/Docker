package com.polygres.advisor.score;

import com.polygres.advisor.catalog.CatalogSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Deterministic, rules-based migration-difficulty scorer -- weights over the {@link
 * CatalogSnapshot} feature inventory, per the project decision that the score itself must be
 * reproducible/auditable and NOT produced by an LLM (the LLM layer sits downstream: explaining
 * findings, proposing rewrites, drafting compatibility packages).
 *
 * <p>Weights are a first-pass starting rubric, not a tuned model -- expect to revise these once
 * real customer scans and actual migration outcomes are on hand to calibrate against. Dispatches
 * on {@link CatalogSnapshot#dialect} to the right weight table: Oracle's PL/SQL-shaped rubric and
 * MySQL/MariaDB's storage-engine/syntax-shaped one are different enough (packages vs. no packages,
 * DBMS_* builtins vs. engine choice) that sharing one table would mean either table having
 * meaningless entries for the other dialect.
 */
public class MigrationScorer {

    // -- Oracle weight tables --------------------------------------------------------------

    /** Per-call-site weight for tracked builtin packages -- higher for packages with no clean Postgres equivalent. */
    private static final Map<String, Integer> ORACLE_BUILTIN_WEIGHT = Map.ofEntries(
        Map.entry("DBMS_OUTPUT", 1),   // trivial -- RAISE NOTICE covers it
        Map.entry("UTL_FILE", 3),      // needs a filesystem-access rewrite (pg has no direct analog)
        Map.entry("UTL_HTTP", 3),      // needs an extension (e.g. http) or external call rewrite
        Map.entry("DBMS_LOB", 2),      // mostly mappable to bytea/text ops
        Map.entry("DBMS_SCHEDULER", 8),// known hard gap -- no direct Postgres equivalent (pg_cron covers a subset)
        Map.entry("DBMS_LOCK", 4),     // advisory locks exist but semantics differ
        Map.entry("DBMS_ALERT", 5),    // LISTEN/NOTIFY is a real but different model
        Map.entry("DBMS_CRYPTO", 3),   // pgcrypto covers most of this
        Map.entry("DBMS_AQ", 12),      // Advanced Queuing -- hard, no native equivalent
        Map.entry("DBMS_CLOUD", 10),   // edition-gated on Oracle side too; needs a bespoke package (see Engine plan)
        Map.entry("DBMS_JOB", 6),      // legacy scheduler, similar gap to DBMS_SCHEDULER
        Map.entry("DBMS_SQL", 5),      // dynamic SQL -- EXECUTE covers common cases, edge cases remain
        Map.entry("DBMS_UTILITY", 2),
        Map.entry("DBMS_XMLGEN", 4),
        Map.entry("DBMS_METADATA", 3)
    );

    private static final Map<String, Integer> ORACLE_SYNTAX_WEIGHT = Map.of(
        "CONNECT BY", 3,                 // needs rewrite to recursive CTE
        "(+) outer join", 2,             // needs rewrite to ANSI JOIN syntax
        "autonomous transaction", 6      // no direct equivalent; needs dblink/background-worker style rework
    );

    // -- MySQL/MariaDB weight tables --------------------------------------------------------

    /** Storage-engine flags, surfaced via CatalogSnapshot.builtinPackageUsage under "ENGINE=X" keys (see MySqlCatalogProfiler). */
    private static final Map<String, Integer> MYSQL_ENGINE_WEIGHT = Map.of(
        "ENGINE=MyISAM", 6,     // no transactions/FKs -- real behavioral gap, not just syntax
        "ENGINE=MEMORY", 4,     // volatile-storage semantics have no direct Postgres analog (UNLOGGED is the closest)
        "ENGINE=ARCHIVE", 5,    // compressed append-only -- no equivalent, needs a redesign
        "ENGINE=FEDERATED", 10  // remote-table proxying -- needs postgres_fdw reconfiguration, hard
    );

    private static final Map<String, Integer> MYSQL_SYNTAX_WEIGHT = Map.of(
        "ON DUPLICATE KEY UPDATE", 3,  // rewrite to INSERT ... ON CONFLICT
        "GROUP_CONCAT", 2,             // rewrite to STRING_AGG
        "STRAIGHT_JOIN", 2             // planner hint with no direct Postgres equivalent -- drop or restructure
    );

    // -- SQL Server weight table --------------------------------------------------------------

    private static final Map<String, Integer> SQLSERVER_SYNTAX_WEIGHT = Map.of(
        "MERGE statement", 5,          // Postgres 15+ has MERGE too, but semantics/locking differ enough to need review
        "OUTER/CROSS APPLY", 4,        // rewrite to LATERAL join
        "OPENQUERY/OPENROWSET", 8,     // needs postgres_fdw reconfiguration, hard
        "WITH (NOLOCK)", 2             // no direct equivalent; usually just dropped, but changes isolation semantics
    );

    public MigrationScoreReport score(CatalogSnapshot s) {
        return switch (s.dialect) {
            case ORACLE -> scoreOracle(s);
            case MYSQL, MARIADB -> scoreMySql(s);
            case SQL_SERVER -> scoreSqlServer(s);
            default -> throw new IllegalArgumentException("No scoring rubric for dialect: " + s.dialect);
        };
    }

    private MigrationScoreReport scoreOracle(CatalogSnapshot s) {
        MigrationScoreReport report = newReport(s);

        add(report, "Tables", s.tableCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Views", s.viewCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Materialized views", s.materializedViewCount, 2, "Mappable; refresh semantics need review.");
        add(report, "Sequences", s.sequenceCount, 0, "Direct mapping to Postgres sequences.");
        add(report, "Simple triggers", s.simpleTriggerCount, 1, "Row-level triggers translate reasonably directly.");
        add(report, "Complex triggers", s.complexTriggerCount, 5, "Non-trivial body -- needs manual PL/pgSQL review.");
        add(report, "Packages", s.packageCount, 8, "Postgres has no package construct -- needs schema/naming-convention rework.");
        add(report, "Standalone procedures", s.standaloneProcedureCount, 3, "PL/SQL -> PL/pgSQL, generally mappable.");
        add(report, "Standalone functions", s.standaloneFunctionCount, 3, "PL/SQL -> PL/pgSQL, generally mappable.");
        add(report, "Database links", s.dbLinkCount, 10, "Needs postgres_fdw/dblink reconfiguration per link.");
        add(report, "Scheduled jobs (DBMS_SCHEDULER)", s.scheduledJobCount, 8, "No direct Postgres equivalent -- known hard gap.");
        add(report, "Partitioned tables", s.partitionedTableCount, 4, "Postgres declarative partitioning differs structurally.");

        s.builtinPackageUsage.forEach((pkg, callSites) ->
            add(report, "Built-in: " + pkg, callSites, ORACLE_BUILTIN_WEIGHT.getOrDefault(pkg, 5),
                "Call sites found by source-text scan (first-pass signal, refine with workload capture)."));

        s.syntaxConstructUsage.forEach((construct, hits) ->
            add(report, "Syntax: " + construct, hits, ORACLE_SYNTAX_WEIGHT.getOrDefault(construct, 3),
                "Portability-risk construct found by source-text scan."));

        return finish(report);
    }

    private MigrationScoreReport scoreMySql(CatalogSnapshot s) {
        MigrationScoreReport report = newReport(s);

        add(report, "Tables", s.tableCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Views", s.viewCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Sequences", s.sequenceCount, 0, "Direct mapping to Postgres sequences.");
        add(report, "Triggers", s.simpleTriggerCount, 1, "Row-level triggers translate reasonably directly.");
        add(report, "Standalone procedures", s.standaloneProcedureCount, 3, "MySQL procedural SQL -> PL/pgSQL, generally mappable.");
        add(report, "Standalone functions", s.standaloneFunctionCount, 3, "MySQL procedural SQL -> PL/pgSQL, generally mappable.");
        add(report, "Scheduled events", s.scheduledJobCount, 6, "No built-in Postgres equivalent -- pg_cron covers a subset.");
        add(report, "Partitioned tables", s.partitionedTableCount, 4, "Postgres declarative partitioning differs structurally.");

        s.builtinPackageUsage.forEach((engine, tableCount) ->
            add(report, engine.replace("ENGINE=", "Storage engine: "), tableCount, MYSQL_ENGINE_WEIGHT.getOrDefault(engine, 4),
                "Non-InnoDB storage engine -- semantics differ from Postgres tables in ways that need review."));

        s.syntaxConstructUsage.forEach((construct, hits) ->
            add(report, "Syntax: " + construct, hits, MYSQL_SYNTAX_WEIGHT.getOrDefault(construct, 2),
                "Portability-risk construct found by source-text scan."));

        return finish(report);
    }

    private MigrationScoreReport scoreSqlServer(CatalogSnapshot s) {
        MigrationScoreReport report = newReport(s);

        add(report, "Tables", s.tableCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Views", s.viewCount, 0, "Direct DDL mapping -- straightforward.");
        add(report, "Sequences", s.sequenceCount, 0, "Direct mapping to Postgres sequences.");
        add(report, "Synonyms", s.synonymCount, 2, "No direct Postgres equivalent -- views or search_path can usually substitute.");
        add(report, "Triggers", s.simpleTriggerCount, 1, "Row-level triggers translate reasonably directly.");
        add(report, "Standalone procedures", s.standaloneProcedureCount, 3, "T-SQL -> PL/pgSQL, generally mappable.");
        add(report, "Standalone functions", s.standaloneFunctionCount, 3, "T-SQL -> PL/pgSQL, generally mappable.");
        add(report, "Linked servers", s.dbLinkCount, 10, "Needs postgres_fdw/dblink reconfiguration per linked server.");
        add(report, "SQL Server Agent jobs", s.scheduledJobCount, 8, "No direct Postgres equivalent -- known hard gap.");
        add(report, "Partitioned tables", s.partitionedTableCount, 4, "Postgres declarative partitioning differs structurally.");

        s.syntaxConstructUsage.forEach((construct, hits) ->
            add(report, "Syntax: " + construct, hits, SQLSERVER_SYNTAX_WEIGHT.getOrDefault(construct, 3),
                "Portability-risk construct found by source-text scan."));

        return finish(report);
    }

    private MigrationScoreReport newReport(CatalogSnapshot s) {
        MigrationScoreReport report = new MigrationScoreReport();
        report.sourceVersion = s.sourceVersion;
        report.warnings.addAll(s.warnings);
        return report;
    }

    private MigrationScoreReport finish(MigrationScoreReport report) {
        report.totalScore = report.findings.stream().mapToInt(f -> f.points).sum();
        report.tier = tierFor(report.totalScore);
        return report;
    }

    private void add(MigrationScoreReport report, String feature, int count, int weightPerUnit, String note) {
        if (count <= 0) return;
        int points = count * weightPerUnit;
        report.findings.add(new ScoreFinding(feature, count, weightPerUnit, points, note));
    }

    /** Thresholds are a starting point -- recalibrate once real migrations validate actual effort per tier. */
    private String tierFor(int totalScore) {
        if (totalScore <= 20) return "EASY -- schema+data migration is likely one-click viable.";
        if (totalScore <= 60) return "MEDIUM -- assisted rewrite required; plan for manual review.";
        return "HARD -- significant remediation effort; treat as a phased migration project.";
    }

    public static class MigrationScoreReport {
        public String sourceVersion;
        public int totalScore;
        public String tier;
        public List<ScoreFinding> findings = new ArrayList<>();
        public List<String> warnings = new ArrayList<>();
    }

    public record ScoreFinding(String feature, int count, int weightPerUnit, int points, String note) {}
}
