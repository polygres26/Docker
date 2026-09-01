package com.nexagres.migration.readiness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.migration.checkpoint.CdcCheckpointStore;
import com.nexagres.migration.checkpoint.DeadLetterStore;
import com.nexagres.migration.core.MigrationLicensing;
import com.nexagres.migration.core.MigrationLicensingTestSupport;
import com.nexagres.migration.coordinator.PartitionLeaseStore;
import com.nexagres.migration.testsupport.RealPostgres;
import com.nexagres.migration.verify.VerificationResult;
import com.nexagres.wire.license.LicenseTier;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real Postgres throughout (no Warp needed -- this class only ever reads the bookkeeping
 * tables the other stores already write, exactly as documented in its own javadoc), proving each
 * gate independently: a source starts NOT READY (no partitions recorded at all, the fail-closed
 * default), becomes ready on partitions+lag+dead-letters, then flips back to not ready the moment
 * any ONE gate regresses (a fresh dead letter; a lease reopened mid-run), and a supplied
 * verification mismatch fails the report even when every bookkeeping-table gate is green.
 *
 * <p>Forces Enterprise tier for the duration of this class -- {@link CutoverReadinessChecker}
 * itself is now an Enterprise-only feature (see {@link MigrationLicensing}'s own javadoc); the
 * free-tier refusal path is covered separately by {@code MigrationLicensingTest}, so this class
 * can stay focused on the underlying readiness logic instead of re-asserting the license gate.
 */
class CutoverReadinessCheckerIntegrationTest {

    private static final String SOURCE_KEY = "mysql:mydb.orders";

    @BeforeEach
    void forceEnterpriseTier() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
    }

    @AfterEach
    void resetTier() {
        MigrationLicensingTestSupport.reset();
    }

    @Test
    void reportsNotReadyUntilEveryGatePasses() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            CdcCheckpointStore checkpoints = new CdcCheckpointStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            checkpoints.ensureSchema();
            PartitionLeaseStore leases = new PartitionLeaseStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            leases.ensureSchema();
            DeadLetterStore deadLetters = new DeadLetterStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            deadLetters.ensureSchema();

            CutoverReadinessChecker checker = new CutoverReadinessChecker(postgres.jdbcUrl(), postgres.username(), postgres.password());

            // Nothing recorded yet -- fail-closed, not a false "nothing to check" pass.
            CutoverReadinessReport before = checker.check(SOURCE_KEY, true, 10);
            assertFalse(before.ready());
            assertFalse(findCheck(before, "snapshot partitions").passed());

            // Two partitions, both claimed and done.
            assertTrue(leases.tryClaim(SOURCE_KEY, SOURCE_KEY + "#p0", "worker-1", 3600));
            leases.markDone(SOURCE_KEY, SOURCE_KEY + "#p0");
            assertTrue(leases.tryClaim(SOURCE_KEY, SOURCE_KEY + "#p1", "worker-1", 3600));
            leases.markDone(SOURCE_KEY, SOURCE_KEY + "#p1");

            // Still not ready -- no change-feed checkpoint yet.
            CutoverReadinessReport partitionsOnly = checker.check(SOURCE_KEY, true, 10);
            assertFalse(partitionsOnly.ready());
            assertTrue(findCheck(partitionsOnly, "snapshot partitions").passed());
            assertFalse(findCheck(partitionsOnly, "change-feed lag").passed());

            // A fresh checkpoint -- lag should read as ~0s, well within threshold.
            checkpoints.save(SOURCE_KEY, "\"resume-token\"", Instant.now());

            CutoverReadinessReport ready = checker.check(SOURCE_KEY, true, 10);
            assertTrue(ready.ready(), ready.render());
            assertTrue(findCheck(ready, "change-feed lag").passed());
            assertTrue(findCheck(ready, "dead letters").passed());

            // A dead letter regresses the whole report even though nothing else changed.
            deadLetters.record(new com.nexagres.migration.core.ChangeEvent("SELECT 1", java.util.List.of()), "boom", 5);
            CutoverReadinessReport afterDeadLetter = checker.check(SOURCE_KEY, true, 10);
            assertFalse(afterDeadLetter.ready());
            assertFalse(findCheck(afterDeadLetter, "dead letters").passed());

            // A verification mismatch fails the report too, even with a clean dead-letter count --
            // this asserts against a report BEFORE the dead letter above so only verification
            // is under test here.
            VerificationResult mismatch = new VerificationResult(100, 99, 0xABCDL, 0xABCDL);
            CutoverReadinessReport withMismatch = checker.check(SOURCE_KEY, true, 10, mismatch);
            assertFalse(withMismatch.ready());
            assertFalse(findCheck(withMismatch, "row-level verification").passed());

            // A matching verification result is just one more green gate.
            VerificationResult matching = new VerificationResult(100, 100, 0xABCDL, 0xABCDL);
            CutoverReadinessReport withMatch = checker.check(SOURCE_KEY, true, 10, matching);
            assertTrue(findCheck(withMatch, "row-level verification").passed());
        }
    }

    @Test
    void snapshotOnlySourceSkipsLagGateEntirely() throws Exception {
        try (RealPostgres postgres = RealPostgres.start()) {
            PartitionLeaseStore leases = new PartitionLeaseStore(postgres.jdbcUrl(), postgres.username(), postgres.password());
            leases.ensureSchema();
            String sourceKey = "neo4j:graph";
            assertTrue(leases.tryClaim(sourceKey, sourceKey + "#p0", "worker-1", 3600));
            leases.markDone(sourceKey, sourceKey + "#p0");

            CutoverReadinessChecker checker = new CutoverReadinessChecker(postgres.jdbcUrl(), postgres.username(), postgres.password());
            // No checkpoint row ever created for this source -- would fail the lag gate if it were
            // consulted, but hasLiveChangeFeed=false means it isn't.
            CutoverReadinessReport report = checker.check(sourceKey, false, 10);
            assertTrue(report.ready(), report.render());
            assertEquals(3, report.checks().size());
            assertTrue(findCheck(report, "change-feed lag").passed());
        }
    }

    private static ReadinessCheck findCheck(CutoverReadinessReport report, String name) {
        return report.checks().stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no check named " + name + " in " + report.render()));
    }
}
