package com.nexagres.wire.license;

/**
 * Test-only seam into {@link DbCompatLicensing}'s package-private override -- same shape and same
 * reasoning as {@code com.nexagres.migration.core.MigrationLicensingTestSupport} in the migration
 * module: a genuine Enterprise {@code WARP_LICENSE_KEY} needs the real, deliberately-offline
 * Ed25519 signing private key (see {@link License}'s own javadoc), unavailable to this module's
 * tests except where {@link LicenseIntegrationTest} deliberately commits it for that one
 * end-to-end proof. Lives only in {@code src/test/java}, never shipped in the built jar.
 */
public final class DbCompatLicensingTestSupport {

    private DbCompatLicensingTestSupport() {
    }

    public static void forceTier(LicenseTier tier) {
        DbCompatLicensing.overrideTierForTests(tier);
    }

    public static void reset() {
        DbCompatLicensing.overrideTierForTests(null);
    }
}
