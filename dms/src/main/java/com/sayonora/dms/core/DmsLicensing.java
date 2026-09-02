package com.sayonora.dms.core;

import com.sayonora.wire.license.License;
import com.sayonora.wire.license.LicenseTier;

/**
 * The paid/free line for Sayonora DMS's own admin-console features -- the RBAC/audit/SSO row of
 * the "Suggested Sayonora DMS packaging" table, as distinct from {@code
 * com.sayonora.migration.core.MigrationLicensing} (which gates the actual data-movement
 * mechanics: parallelism, retry/DLQ, cutover). Both classes are thin wrappers around the SAME
 * underlying {@link License}/{@link LicenseTier} -- one {@code WARP_LICENSE_KEY} still unlocks
 * everything across Warp, sayonora-migration, and Sayonora DMS -- kept as a separate class
 * only because these are genuinely different capabilities gated for different reasons (admin
 * console access control, not migration throughput), living in a different module.
 *
 * <p>Gates:
 * <ul>
 *   <li>{@link #requireEnterpriseForRbac()} -- a second (viewer) admin-console account beyond the
 *   single default admin. Not gated at construction/throw the way {@code
 *   DistributedCoordinator} is -- misconfiguring this shouldn't take down the whole admin
 *   console, so {@link com.sayonora.dms.http.auth.AdminAuth} calls this itself and DEGRADES
 *   (ignores the viewer credentials, logs a warning) rather than refusing to start, the same
 *   "clamp, don't throw" reasoning {@code enforceLocalParallelism} uses.
 *   <li>{@link #auditLoggingEnabled()} -- whether {@link AuditLogStore} actually persists
 *   anything. A non-throwing boolean (like {@code resilientRetryAndDeadLetterAllowed}), since
 *   "no audit trail" isn't an error state for the caller, just the free-tier default.
 *   <li>{@link #requireEnterpriseForSso()} -- bearer-token (SSO-style) login via {@code
 *   SsoAuth}, an alternate path alongside the always-available username/password login.
 * </ul>
 */
public final class DmsLicensing {

    // Test-only escape hatch, same shape as MigrationLicensingTestSupport's own override --
    // package-private so only DmsLicensingTestSupport (src/test/java, never shipped) can reach it.
    private static volatile LicenseTier tierOverrideForTests;

    private DmsLicensing() {
    }

    static void overrideTierForTests(LicenseTier tier) {
        tierOverrideForTests = tier;
    }

    private static LicenseTier currentTier() {
        LicenseTier override = tierOverrideForTests;
        return override != null ? override : License.current().tier();
    }

    /** {@code true} only under a valid Enterprise license -- see this class's own javadoc for why
     * a second admin-console account degrades rather than throws when this is {@code false}. */
    public static boolean rbacAllowed() {
        return currentTier() == LicenseTier.ENTERPRISE;
    }

    /** Throws unless Enterprise -- kept as a throwing form too for a caller (like a future admin
     * API endpoint that explicitly requests creating a second account) where refusing outright,
     * not silently ignoring, is the right response. */
    public static void requireEnterpriseForRbac() {
        if (rbacAllowed()) {
            return;
        }
        throw new IllegalStateException("A second (viewer) admin-console account is an Enterprise "
                + "feature -- set a valid WARP_LICENSE_KEY to enable role-based access control. "
                + "The free/Developer tier has a single shared admin account, always full access.");
    }

    /** Whether {@link AuditLogStore} should actually persist entries -- {@code false} on the
     * free/Developer tier, where every mutating action still happens, it's just not recorded to
     * an audit trail beyond this process's own SLF4J logs. */
    public static boolean auditLoggingEnabled() {
        return currentTier() == LicenseTier.ENTERPRISE;
    }

    /** Throws unless the current process is Enterprise-licensed -- {@code SsoAuth} itself, an
     * alternate bearer-token login path alongside the always-available username/password login. */
    public static void requireEnterpriseForSso() {
        if (currentTier() == LicenseTier.ENTERPRISE) {
            return;
        }
        throw new IllegalStateException("SSO (bearer-token) login is an Enterprise feature -- set a "
                + "valid WARP_LICENSE_KEY to enable SsoAuth. The free/Developer tier logs in with "
                + "the SAYONORA_ADMIN_USER/SAYONORA_ADMIN_PASSWORD username/password pair only.");
    }
}
