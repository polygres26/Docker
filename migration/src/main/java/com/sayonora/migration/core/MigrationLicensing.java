package com.sayonora.migration.core;

import com.sayonora.wire.license.License;
import com.sayonora.wire.license.LicenseTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The paid/free line for {@code sayonora-migration}, per this session's own instruction: "for
 * difference between paid vs free, lets make the parallelism the paid option -- for developers
 * lets give serial way to move data, for a real massively parallel way to move data [that's the
 * paid tier]." Deliberately reuses Warp's OWN {@link License}/{@link LicenseTier} machinery
 * (already a compile-scope dependency of this module -- see {@code migration/pom.xml}'s own
 * comment on reusing {@code sayonora-wire}) rather than inventing a second, separate licensing
 * system for this module: one {@code WARP_LICENSE_KEY} unlocks both Warp's own
 * Enterprise-tier caps AND massively-parallel migration, one key, one offline Ed25519-signature
 * trust model, one thing for a customer to buy. See {@link License}'s own javadoc for the full
 * "deliberately offline, fails closed" design this class inherits by simply delegating to it.
 *
 * <p>Gates, matching this module's own published free/Enterprise packaging table (see the
 * homepage's "Suggested Sayonora DMS packaging" section):
 * <ul>
 *   <li>{@link #enforceLocalParallelism} -- {@link Coordinator}'s local thread pool, N partitions
 *   read concurrently within ONE process ("Parallel snapshot workers" / "Parallel table/partition
 *   migration" rows). Free/Developer tier still works, just serially (one partition at a time) --
 *   correctness is identical either way, only throughput differs.
 *   <li>{@link #requireEnterpriseForDistributedCoordination} -- {@link DistributedCoordinator}
 *   itself, i.e. running MULTIPLE worker processes/containers against the same source ("HA
 *   migration workers" row). This is the "real massively parallel way to move data" the paid tier
 *   is actually selling: local thread-pool parallelism alone tops out at one machine's CPU count,
 *   but a fleet of worker processes does not. Gated at construction (throws, doesn't silently
 *   degrade) because degrading it the same way as local parallelism (quietly forcing
 *   parallelism=1 per process) would NOT actually enforce anything -- a free-tier user could still
 *   launch 50 single-partition-at-a-time processes and get real fleet-wide parallelism for free.
 *   Refusing to construct the object at all is the only enforcement point that's actually
 *   enforceable here, the same reasoning {@code ConnectionGate}'s per-instance cap in
 *   {@code wire} is built around.
 *   <li>{@link #resilientRetryAndDeadLetterAllowed} -- whether a failed write gets retried and,
 *   on exhausted retries, dead-lettered rather than immediately killing the run ("Failed-row
 *   retry / DLQ" row). Free tier: any write failure is fatal to the run, the same "fail loud on
 *   the first bad row" behavior every connector had before {@code ResilientSink} existed.
 *   <li>{@link #requireCapacityForAnotherConcurrentJob} -- caps a free/Developer install to one
 *   RUNNING migration job at a time ("Multiple migrations concurrently: 1 vs. unlimited" row).
 *   Refuses to start a second job outright (same refuse-don't-degrade reasoning as distributed
 *   coordination -- there's no meaningful "degraded" way to run two jobs as if they were one).
 *   <li>{@link #requireEnterpriseForCutoverReadiness} -- {@code CutoverReadinessChecker} itself
 *   ("Zero/minimal-downtime cutover" row): the single ready/not-ready signal an operator actually
 *   cuts a connection over on. Free tier still has every underlying signal it rolls up (checkpoint
 *   lag, dead-letter count) individually readable via {@code MigrationStatusStore}; what's gated
 *   is the packaged go/no-go verdict itself.
 *   <li>{@link #requireEnterpriseForCustomThrottle} -- {@code ThrottledSink}'s configurable rate
 *   ("Bandwidth / workload throttling" row). The DEFAULT rate ({@link
 *   #DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND}) always applies on every tier ("Source
 *   production protection" row) -- what's gated is only the ability to override that default.
 *   <li>{@link #requireEnterpriseForAutomaticCutover} -- {@code AutomaticCutoverScheduler}
 *   ("Automatic cutover at the right time" row): a SEPARATE, higher gate than readiness itself --
 *   free/Developer tier can still ask "are we ready?" on demand via {@code CutoverReadinessCli}
 *   with a key (readiness itself is Enterprise per the gate above), but actually watching
 *   continuously and firing the cutover action itself, unattended, the instant every gate turns
 *   green, is the paid automation on top of that manual check.
 * </ul>
 *
 * <p>Deliberately NOT gated, honestly stated rather than silently implied: CDC itself (every
 * connector's live change-feed tail runs identically regardless of license -- only how many
 * partitions/processes read it in parallel is gated), row-level validation/reconciliation
 * ({@code RowChecksum}/{@code VerificationResult} are plain, license-free utility classes),
 * migration-progress monitoring ({@code MigrationStatusStore} reads every bookkeeping table for
 * any caller), bandwidth/workload throttling, source-side production protection, and SSO/RBAC/
 * audit for the DMS admin console -- none of these have a real implementation to gate yet. See
 * the packaging table's own footnote for the current, honest state of each.
 */
public final class MigrationLicensing {

    private static final Logger log = LoggerFactory.getLogger(MigrationLicensing.class);

    /** The always-applied default for {@code ThrottledSink} when a caller doesn't override it --
     * see {@link #requireEnterpriseForCustomThrottle()}'s own javadoc. Generous enough that a
     * normal migration (thousands to low millions of rows) never notices it, conservative enough
     * that an accidental initial-sync-against-a-tiny-production-replica doesn't turn into a
     * thundering herd. */
    public static final double DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND = 2000.0;

    // Test-only escape hatch: null (the real, always-used-in-production path) means "resolve via
    // License.current().tier(), the genuine offline-signature-verified value." Package-private so
    // nothing outside this package -- and certainly nothing in a real deployment, which has no way
    // to reach a package-private static setter -- can ever set it; the only caller is
    // MigrationLicensingTestSupport, which lives in src/test/java (this module's own test
    // sources), never in the shipped jar. This exists because a genuine Enterprise
    // WARP_LICENSE_KEY can only be produced by LicenseKeyGenTool using the real signing
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
     * {@code WARP_LICENSE_KEY} is present -- never throws, since a serial migration is a
     * completely correct (if slower) way to move the same data, not a broken one. */
    public static int enforceLocalParallelism(int requestedParallelism) {
        int requested = Math.max(1, requestedParallelism);
        if (requested <= 1) {
            return requested;
        }
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return requested;
        }
        log.warn("migration: parallelism={} was requested, but no Enterprise WARP_LICENSE_KEY is "
                + "set -- massively parallel migration is a paid feature. Running serial instead "
                + "(parallelism=1, one partition at a time): this still completes correctly, just "
                + "without the throughput of reading multiple partitions concurrently. Set a valid "
                + "WARP_LICENSE_KEY to unlock full parallelism.", requestedParallelism);
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
                + "Enterprise feature -- set a valid WARP_LICENSE_KEY to run DistributedCoordinator. "
                + "On the free/Developer tier, use Coordinator instead: a single process that migrates "
                + "the same data correctly, serially (one partition at a time) without a license key.");
    }

    /** Whether a failed write should be retried (and, on exhausted retries, dead-lettered) rather
     * than propagating immediately and killing the run -- {@code true} only under a valid
     * Enterprise license. A caller on the free tier should skip wrapping its sink in
     * {@code ResilientSink} entirely when this returns {@code false}, so a failure fails loud and
     * immediately (the pre-{@code ResilientSink} behavior every connector had), rather than
     * silently degrading retry count to zero (which would look identical to a hang, not a clear
     * failure). */
    public static boolean resilientRetryAndDeadLetterAllowed() {
        return currentTier() == LicenseTier.ENTERPRISE;
    }

    /** Throws if starting one more concurrent migration job would exceed the free/Developer tier's
     * cap of one RUNNING job at a time. {@code currentlyRunningJobs} is the caller's own count of
     * jobs already in a RUNNING state (this class has no job registry of its own -- {@code
     * MigrationJobRunner} owns that). Enterprise: never throws, no cap. */
    public static void requireCapacityForAnotherConcurrentJob(int currentlyRunningJobs) {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        if (currentlyRunningJobs >= 1) {
            throw new IllegalStateException("The free/Developer tier runs one migration job at a time "
                    + "(" + currentlyRunningJobs + " already running) -- set a valid WARP_LICENSE_KEY "
                    + "to run multiple migrations concurrently. Wait for the current job to finish, or "
                    + "stop it, before starting another.");
        }
    }

    /** Throws unless the current process is Enterprise-licensed -- {@code
     * CutoverReadinessChecker} itself is a paid feature: the packaged single ready/not-ready
     * signal, not the underlying bookkeeping it reads (every individual signal -- checkpoint lag,
     * dead-letter count, partition completion -- stays freely readable via {@code
     * MigrationStatusStore} on every tier; what's gated is rolling them into one verdict an
     * operator actually cuts a connection over on). */
    public static void requireEnterpriseForCutoverReadiness() {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        throw new IllegalStateException("Cutover-readiness checking (the packaged ready/not-ready "
                + "signal CutoverReadinessChecker produces) is an Enterprise feature -- set a valid "
                + "WARP_LICENSE_KEY to use it. The underlying signals it rolls up (checkpoint lag, "
                + "dead-letter count, partition completion) are still freely readable via "
                + "MigrationStatusStore on the free/Developer tier -- you can assess cutover readiness "
                + "manually from those, just not via this single packaged verdict.");
    }

    /** Throws unless Enterprise -- required only when a caller wants a DIFFERENT rate than {@link
     * #DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND}. The default itself always applies regardless
     * of license (see {@code ThrottledSink}'s own javadoc: a free-tier migration is never
     * literally unthrottled), so this gate is "can you configure it," not "does it run at all." */
    public static void requireEnterpriseForCustomThrottle() {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        throw new IllegalStateException("A custom throughput cap (anything other than the default "
                + DEFAULT_SOURCE_PROTECTION_EVENTS_PER_SECOND + " events/sec) is an Enterprise feature "
                + "-- set a valid WARP_LICENSE_KEY to configure maxEventsPerSecond. The "
                + "free/Developer tier always runs at the default cap, which protects the source "
                + "without needing to be tuned.");
    }

    /** Throws unless the current process is Enterprise-licensed -- {@code
     * AutomaticCutoverScheduler} itself is a paid feature, one step up from {@link
     * #requireEnterpriseForCutoverReadiness}: continuously polling readiness and firing the
     * cutover action automatically, unattended, the moment every gate turns green, rather than an
     * operator running a manual check and deciding when to act on it themselves. */
    public static void requireEnterpriseForAutomaticCutover() {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        throw new IllegalStateException("Automatic cutover (continuously polling readiness and firing "
                + "the cutover action the moment every gate turns green, unattended) is an Enterprise "
                + "feature -- set a valid WARP_LICENSE_KEY to use AutomaticCutoverScheduler. On the "
                + "free/Developer tier, run CutoverReadinessCli yourself (or poll "
                + "CutoverReadinessChecker from your own script) and decide when to cut over by hand.");
    }
}
