package com.nexagres.migration.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.nexagres.wire.license.LicenseTier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The paid/free gate itself, in isolation from any real migration mechanics -- no Postgres, no
 * Docker, pure logic. Covers both directions: the real, default (no {@code
 * WARP_LICENSE_KEY} set anywhere in this test environment) free/Developer-tier path every
 * OTHER test in this module also runs under, plus the forced-Enterprise path via {@link
 * MigrationLicensingTestSupport} (see its own javadoc for why a genuine key can't be minted here).
 */
class MigrationLicensingTest {

    @AfterEach
    void resetOverride() {
        MigrationLicensingTestSupport.reset();
    }

    @Test
    void freeTierClampsParallelismToOneButNeverThrows() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(1));
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(8));
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(0));
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(-5));
    }

    @Test
    void freeTierRefusesDistributedCoordinationOutright() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                MigrationLicensing::requireEnterpriseForDistributedCoordination);
        // The message is a real operator-facing instruction, not just "forbidden" -- assert it
        // actually names both halves of the fix (get a key, or use Coordinator instead).
        assertEquals(true, e.getMessage().contains("WARP_LICENSE_KEY"));
        assertEquals(true, e.getMessage().contains("Coordinator"));
    }

    @Test
    void enterpriseTierAllowsFullParallelismAndDistributedCoordination() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertEquals(8, MigrationLicensing.enforceLocalParallelism(8));
        assertDoesNotThrow(MigrationLicensing::requireEnterpriseForDistributedCoordination);
    }

    @Test
    void freeTierDisallowsResilientRetryAndOneMoreConcurrentJobAndCutoverReadiness() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        assertEquals(false, MigrationLicensing.resilientRetryAndDeadLetterAllowed());

        assertDoesNotThrow(() -> MigrationLicensing.requireCapacityForAnotherConcurrentJob(0));
        IllegalStateException capacity = assertThrows(IllegalStateException.class,
                () -> MigrationLicensing.requireCapacityForAnotherConcurrentJob(1));
        assertEquals(true, capacity.getMessage().contains("WARP_LICENSE_KEY"));

        IllegalStateException cutover = assertThrows(IllegalStateException.class,
                MigrationLicensing::requireEnterpriseForCutoverReadiness);
        assertEquals(true, cutover.getMessage().contains("WARP_LICENSE_KEY"));
    }

    @Test
    void enterpriseTierAllowsResilientRetryUnlimitedConcurrentJobsAndCutoverReadiness() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertEquals(true, MigrationLicensing.resilientRetryAndDeadLetterAllowed());
        assertDoesNotThrow(() -> MigrationLicensing.requireCapacityForAnotherConcurrentJob(50));
        assertDoesNotThrow(MigrationLicensing::requireEnterpriseForCutoverReadiness);
    }

    @Test
    void freeTierCannotCustomizeThrottleButEnterpriseCan() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                MigrationLicensing::requireEnterpriseForCustomThrottle);
        assertEquals(true, e.getMessage().contains("WARP_LICENSE_KEY"));

        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertDoesNotThrow(MigrationLicensing::requireEnterpriseForCustomThrottle);
    }

    @Test
    void automaticCutoverIsItsOwnGateSeparateFromManualCutoverReadiness() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                MigrationLicensing::requireEnterpriseForAutomaticCutover);
        assertEquals(true, e.getMessage().contains("WARP_LICENSE_KEY"));
        assertEquals(true, e.getMessage().contains("AutomaticCutoverScheduler"));

        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertDoesNotThrow(MigrationLicensing::requireEnterpriseForAutomaticCutover);
    }

    @Test
    void enterpriseTierStillFloorsParallelismAtOne() {
        MigrationLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(0));
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(-3));
    }

    @Test
    void withNoOverrideThisTestEnvironmentIsRealFreeTier() {
        // No WARP_LICENSE_KEY is set anywhere in this test run (see every other connector
        // integration test in this module, which all rely on exactly this) -- proves the default,
        // real (non-overridden) resolution path is genuinely free-tier, not just the override path.
        assertEquals(1, MigrationLicensing.enforceLocalParallelism(4));
        assertThrows(IllegalStateException.class, MigrationLicensing::requireEnterpriseForDistributedCoordination);
    }
}
