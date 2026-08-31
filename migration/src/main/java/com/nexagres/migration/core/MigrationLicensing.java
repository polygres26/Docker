package com.nexagres.migration.core;

import com.nexagres.wire.license.License;
import com.nexagres.wire.license.LicenseTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The paid/free line for {@code nexagres-migration}, per this session's own instruction: "for
 * difference between paid vs free, lets make the parallelism the paid option -- for developers
 * lets give serial way to move data, for a real massively parallel way to move data [that's the
 * paid tier]." Deliberately reuses Polywire's OWN {@link License}/{@link LicenseTier} machinery
 * (already a compile-scope dependency of this module -- see {@code migration/pom.xml}'s own
 * comment on reusing {@code nexagres-wire}) rather than inventing a second, separate licensing
 * system for this module: one {@code POLYWIRE_LICENSE_KEY} unlocks both Polywire's own
 * Enterprise-tier caps AND massively-parallel migration, one key, one offline Ed25519-signature
 * trust model, one thing for a customer to buy. See {@link License}'s own javadoc for the full
 * "deliberately offline, fails closed" design this class inherits by simply delegating to it.
 *
 * <p>Two independent gates, matching this module's own two levels of parallelism:
 * <ul>
 *   <li>{@link #enforceLocalParallelism} -- {@link Coordinator}'s local thread pool, N partitions
 *   read concurrently within ONE process. Free/Developer tier still works, just serially (one
 *   partition at a time) -- correctness is identical either way, only throughput differs.
 *   <li>{@link #requireEnterpriseForDistributedCoordination} -- {@link DistributedCoordinator}
 *   itself, i.e. running MULTIPLE worker processes/containers against the same source. This is
 *   the "real massively parallel way to move data" the paid tier is actually selling: local
 *   thread-pool parallelism alone tops out at one machine's CPU count, but a fleet of worker
 *   processes does not. Gated at construction (throws, doesn't silently degrade) because degrading
 *   it the same way as local parallelism (quietly forcing parallelism=1 per process) would NOT
 *   actually enforce anything -- a free-tier user could still launch 50 single-partition-at-a-time
 *   processes and get real fleet-wide parallelism for free. Refusing to construct the object at
 *   all is the only enforcement point that's actually enforceable here, the same reasoning
 *   {@code ConnectionGate}'s per-instance cap in {@code wire} is built around.
 * </ul>
 */
public final class MigrationLicensing {

    private static final Logger log = LoggerFactory.getLogger(MigrationLicensing.class);

    // Test-only escape hatch: null (the real, always-used-in-production path) means "resolve via
    // License.current().tier(), the genuine offline-signature-verified value." Package-private so
    // nothing outside this package -- and certainly nothing in a real deployment, which has no way
    // to reach a package-private static setter -- can ever set it; the only caller is
    // MigrationLicensingTestSupport, which lives in src/test/java (this module's own test
    // sources), never in the shipped jar. This exists because a genuine Enterprise
    // POLYWIRE_LICENSE_KEY can only be produced by LicenseKeyGenTool using the real signing
    // private key, which is deliberately held offline (see License's own javadoc) and not
    // available to this module's tests -- there is no way to test the Enterprise-unlocked path
    // through the real signature-verification path from outside wire's own test suite.
    private static volatile LicenseTier tierOverrideForTests;

    private MigrationLicensing() {
    }

    static void overrideTierForTests(LicenseTier tier) {
        tierOverrideForTests = tier;
    }

    private static LicenseTier currentTier() {
        LicenseTier override = tierOverrideForTests;
        return override != null ? override : License.current().tier();
    }

    /** Clamps {@code requestedParallelism} to {@code 1} unless a valid Enterprise
     * {@code POLYWIRE_LICENSE_KEY} is present -- never throws, since a serial migration is a
     * completely correct (if slower) way to move the same data, not a broken one. */
    public static int enforceLocalParallelism(int requestedParallelism) {
        int requested = Math.max(1, requestedParallelism);
        if (requested <= 1) {
            return requested;
        }
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return requested;
        }
        log.warn("migration: parallelism={} was requested, but no Enterprise POLYWIRE_LICENSE_KEY is "
                + "set -- massively parallel migration is a paid feature. Running serial instead "
                + "(parallelism=1, one partition at a time): this still completes correctly, just "
                + "without the throughput of reading multiple partitions concurrently. Set a valid "
                + "POLYWIRE_LICENSE_KEY to unlock full parallelism.", requestedParallelism);
        return 1;
    }

    /** Throws if the current process isn't licensed for {@link DistributedCoordinator} at all --
     * see this class's own javadoc for why this gate refuses outright rather than degrading like
     * {@link #enforceLocalParallelism} does. */
    public static void requireEnterpriseForDistributedCoordination() {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        throw new IllegalStateException("Distributed migration coordination (multiple worker "
                + "processes/containers claiming partitions off a shared PartitionLeaseStore) is an "
                + "Enterprise feature -- set a valid POLYWIRE_LICENSE_KEY to run DistributedCoordinator. "
                + "On the free/Developer tier, use Coordinator instead: a single process that migrates "
                + "the same data correctly, serially (one partition at a time) without a license key.");
    }
}
