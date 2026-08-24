package com.polygres.advisor.score;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.polygres.advisor.catalog.CatalogSnapshot;
import com.polygres.advisor.core.SourceDialect;
import com.polygres.advisor.score.MigrationScorer.MigrationScoreReport;
import com.polygres.advisor.score.MigrationScorer.ScoreFinding;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * First real test coverage for {@link MigrationScorer} -- {@code advisor/} had zero automated
 * tests of any kind before this, despite scoring being the one deterministic, reproducible
 * component the whole product's headline claim ("run it twice, get the same score") rests on.
 * {@link MigrationScorer} is pure and dependency-free (no DB, no LLM, no I/O), which is exactly
 * why it's the highest-value place to start: every assertion here is a real regression guard, not
 * a live-infra integration test standing in for one.
 */
class MigrationScorerTest {

    private static CatalogSnapshot snapshot(SourceDialect dialect) {
        CatalogSnapshot s = new CatalogSnapshot();
        s.dialect = dialect;
        s.sourceVersion = "test";
        return s;
    }

    private static Optional<ScoreFinding> findingFor(MigrationScoreReport report, String feature) {
        return report.findings.stream().filter(f -> f.feature().equals(feature)).findFirst();
    }

    // -- Oracle ---------------------------------------------------------------------------

    @Test
    void oracleBuiltinPackageUsesItsSpecificWeightNotTheDefault() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.builtinPackageUsage.put("DBMS_AQ", 2); // weighted 12 in ORACLE_BUILTIN_WEIGHT -- a known hard gap
        MigrationScoreReport report = new MigrationScorer().score(s);

        ScoreFinding finding = findingFor(report, "Built-in: DBMS_AQ").orElseThrow();
        assertEquals(2, finding.count());
        assertEquals(12, finding.weightPerUnit());
        assertEquals(24, finding.points());
        assertEquals(24, report.totalScore);
    }

    @Test
    void oracleUnrecognizedBuiltinPackageFallsBackToDefaultWeightOfFive() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.builtinPackageUsage.put("DBMS_SOME_FUTURE_PACKAGE", 3);
        MigrationScoreReport report = new MigrationScorer().score(s);

        ScoreFinding finding = findingFor(report, "Built-in: DBMS_SOME_FUTURE_PACKAGE").orElseThrow();
        assertEquals(5, finding.weightPerUnit(), "an untracked builtin package must still score, at the documented default weight");
    }

    @Test
    void oracleSyntaxConstructUsesItsSpecificWeight() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.syntaxConstructUsage.put("autonomous transaction", 1); // weighted 6
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(6, findingFor(report, "Syntax: autonomous transaction").orElseThrow().points());
    }

    @Test
    void oracleZeroCountFeaturesAreOmittedFromFindings() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.tableCount = 0;
        s.packageCount = 5; // only this one should produce a finding
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertTrue(findingFor(report, "Tables").isEmpty(), "a zero count must not appear as a finding at all");
        assertTrue(findingFor(report, "Packages").isPresent());
    }

    @Test
    void oraclePackagesHaveNoDirectPostgresEquivalent() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.packageCount = 3;
        MigrationScoreReport report = new MigrationScorer().score(s);

        // Weight 8 per package is the single highest structural (non-builtin-call-site) weight in
        // the Oracle rubric -- pin it explicitly so a future rubric edit can't silently soften it.
        assertEquals(24, findingFor(report, "Packages").orElseThrow().points());
    }

    // -- MySQL/MariaDB ----------------------------------------------------------------------

    @Test
    void mysqlStorageEngineFlagUsesItsSpecificWeight() {
        CatalogSnapshot s = snapshot(SourceDialect.MYSQL);
        s.builtinPackageUsage.put("ENGINE=FEDERATED", 1); // weighted 10 -- the highest MySQL engine weight
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(10, findingFor(report, "Storage engine: FEDERATED").orElseThrow().points());
    }

    @Test
    void mariadbSharesTheMySqlRubric() {
        CatalogSnapshot s = snapshot(SourceDialect.MARIADB);
        s.builtinPackageUsage.put("ENGINE=MyISAM", 2);
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(12, findingFor(report, "Storage engine: MyISAM").orElseThrow().points());
    }

    @Test
    void mysqlUnrecognizedSyntaxConstructFallsBackToDefaultWeightOfTwo() {
        CatalogSnapshot s = snapshot(SourceDialect.MYSQL);
        s.syntaxConstructUsage.put("SOME_FUTURE_CONSTRUCT", 4);
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(2, findingFor(report, "Syntax: SOME_FUTURE_CONSTRUCT").orElseThrow().weightPerUnit());
    }

    // -- SQL Server ---------------------------------------------------------------------------

    @Test
    void sqlServerHasNoBuiltinPackageRubricUnlikeOracleAndMySql() {
        // The gap this test exists to pin down: SQL_SERVER_SYNTAX_WEIGHT covers only 4 syntax
        // patterns, with no equivalent to Oracle's 15-entry DBMS_* builtin-package table or
        // MySQL's storage-engine table. builtinPackageUsage is silently never consulted by
        // scoreSqlServer at all -- populating it must not change the score.
        CatalogSnapshot s = snapshot(SourceDialect.SQL_SERVER);
        s.builtinPackageUsage.put("xp_cmdshell", 5);
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(0, report.totalScore, "SQL Server scoring has no rubric for builtinPackageUsage at all -- this is a real coverage gap, not a bug in this test");
        assertTrue(report.findings.isEmpty());
    }

    @Test
    void sqlServerLinkedServersAndAgentJobsScoreAsHardGaps() {
        CatalogSnapshot s = snapshot(SourceDialect.SQL_SERVER);
        s.dbLinkCount = 2;
        s.scheduledJobCount = 1;
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(20, findingFor(report, "Linked servers").orElseThrow().points());
        assertEquals(8, findingFor(report, "SQL Server Agent jobs").orElseThrow().points());
    }

    // -- Tier thresholds ------------------------------------------------------------------------

    @Test
    void tierBoundariesAreInclusiveOfTwentyAndSixty() {
        assertTrue(tierFor(20).startsWith("EASY"));
        assertTrue(tierFor(21).startsWith("MEDIUM"));
        assertTrue(tierFor(60).startsWith("MEDIUM"));
        assertTrue(tierFor(61).startsWith("HARD"));
    }

    /** DBMS_OUTPUT is weighted 1 per call site in ORACLE_BUILTIN_WEIGHT -- one call site per
     * point, so this hits any exact target score without needing to reason about remainders. */
    private String tierFor(int totalScore) {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.builtinPackageUsage.put("DBMS_OUTPUT", totalScore);
        MigrationScoreReport report = new MigrationScorer().score(s);
        assertEquals(totalScore, report.totalScore, "test fixture construction must hit the exact target score");
        return report.tier;
    }

    // -- Dialects with no rubric ------------------------------------------------------------

    @Test
    void postgresAsASourceDialectHasNoRubricAndThrows() {
        // POSTGRES exists in SourceDialect as the migration *target*, never a source Advisor
        // profiles (see that enum's javadoc) -- score() must refuse rather than silently return 0.
        CatalogSnapshot s = snapshot(SourceDialect.POSTGRES);
        assertThrows(IllegalArgumentException.class, () -> new MigrationScorer().score(s));
    }

    @Test
    void warningsFromTheSnapshotCarryThroughToTheReport() {
        CatalogSnapshot s = snapshot(SourceDialect.ORACLE);
        s.warnings.add("could not determine schema size -- missing grant");
        MigrationScoreReport report = new MigrationScorer().score(s);

        assertEquals(List.of("could not determine schema size -- missing grant"), report.warnings);
    }
}
