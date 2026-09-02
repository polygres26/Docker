package com.sayonora.migration.core;

import com.sayonora.wire.license.LicenseTier;

/**
 * Test-only seam into {@link MigrationLicensing}'s package-private override -- lives in this
 * module's own {@code src/test/java}, never shipped in the built jar, so it's reachable only from
 * this module's own test suite, never from a real deployment. Exists because a genuine Enterprise
 * {@code WARP_LICENSE_KEY} requires wire's real, deliberately-offline signing private key
 * (see {@code License}'s own javadoc), which is not, and should not be, available to this
 * module's tests -- this lets {@link com.sayonora.migration.coordinator.DistributedCoordinator}'s
 * own real distributed-coordination mechanics still get full end-to-end test coverage without
 * needing that key, while every OTHER test (which doesn't call {@link #forceTier}) keeps
 * exercising this project's actual, real, unmodified free/Developer-tier behavior by default.
 *
 * <p>Always call {@link #reset()} in a {@code finally}/{@code @AfterEach} -- the override is a
 * static, JVM-wide field, and a test that forgets to reset it leaks Enterprise tier into every
 * other test that runs afterward in the same JVM (surefire's default one-JVM-per-module
 * behavior), silently hiding a real free-tier regression in an unrelated test.
 */
public final class MigrationLicensingTestSupport {

    private MigrationLicensingTestSupport() {
    }

    public static void forceTier(LicenseTier tier) {
        MigrationLicensing.overrideTierForTests(tier);
    }

    public static void reset() {
        MigrationLicensing.overrideTierForTests(null);
    }
}
