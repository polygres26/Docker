package com.sayonora.dms.core;

import com.sayonora.wire.license.LicenseTier;

/**
 * Test-only seam into {@link DmsLicensing}'s package-private override -- same shape and same
 * reasoning as {@code com.sayonora.migration.core.MigrationLicensingTestSupport} (a genuine
 * Enterprise key needs wire's offline signing private key, unavailable to this module's tests).
 * Lives only in {@code src/test/java}, never shipped.
 */
public final class DmsLicensingTestSupport {

    private DmsLicensingTestSupport() {
    }

    public static void forceTier(LicenseTier tier) {
        DmsLicensing.overrideTierForTests(tier);
    }

    public static void reset() {
        DmsLicensing.overrideTierForTests(null);
    }
}
