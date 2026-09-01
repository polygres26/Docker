package com.nexagres.wire.license;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The paid/free line for the Polygres DB compatibility layer (the {@code pg_oracle}/{@code
 * pg_mysql}/{@code pg_sqlserver} Postgres extensions, {@code db/} in this repo, published
 * separately at {@code github.com/polygres26/DB}) -- gated here, in {@code nexagres-wire}, rather
 * than inside the extensions themselves: a Postgres C extension has no way to check a JVM
 * license, but every session that would actually BENEFIT from it (orawire/mywire/mssqlwire)
 * already passes through this module's own {@code NativeRlsSessionInitializer}, which is exactly
 * where {@code SET db_emulation = '...'} gets issued (see {@code OraclePgEmulationSessionInitializer}/
 * {@code MySqlPgEmulationSessionInitializer}/{@code MssqlPgEmulationSessionInitializer}). Gating
 * there is both the only enforceable point and the natural one: same {@code WARP_LICENSE_KEY}
 * as everything else Warp gates, one key, one thing to buy.
 *
 * <p>Deliberately a non-throwing boolean, not a throwing {@code requireEnterpriseForX()} --
 * skipping the {@code SET db_emulation} on the free/Developer tier isn't an error, it's the
 * free-tier behavior: the session just runs against plain, unmodified Postgres, exactly as it
 * would if the {@code db/} extensions weren't installed on the target database at all (see each
 * initializer's own "extension not installed" degrade path, which this reuses the exact same
 * shape as). A migrated Oracle/MySQL/SQL Server application still connects and runs ordinary SQL
 * either way -- what's gated is only the unqualified name-resolution convenience (V$ views, the
 * DBA/DBMS/UTL packages, SQL Server's sys schema, and MySQL's built-in function names) those
 * three extensions add on top.
 */
public final class DbCompatLicensing {

    private static final Logger log = LoggerFactory.getLogger(DbCompatLicensing.class);
    private static volatile boolean warnedOnce = false;

    // Test-only escape hatch, same shape as MigrationLicensing's own override in the migration
    // module -- package-private so nothing outside this package can set it, and nothing in a real
    // deployment (which has no way to reach a package-private static setter) ever does.
    private static volatile LicenseTier tierOverrideForTests;

    private DbCompatLicensing() {
    }

    static void overrideTierForTests(LicenseTier tier) {
        tierOverrideForTests = tier;
    }

    private static LicenseTier currentTier() {
        LicenseTier override = tierOverrideForTests;
        return override != null ? override : License.current().tier();
    }

    /** Whether the caller should issue {@code SET db_emulation = '...'} for this session --
     * {@code true} only under a valid Enterprise {@code WARP_LICENSE_KEY}. Logs a one-time
     * (per-process) informational note on the free/Developer tier so an operator who installed
     * {@code pg_oracle}/{@code pg_mysql}/{@code pg_sqlserver} and expected the compatibility
     * surface to just work has somewhere to look, without logging on every single connection. */
    public static boolean dbEmulationAllowed() {
        boolean allowed = currentTier() == LicenseTier.ENTERPRISE;
        if (!allowed && !warnedOnce) {
            warnedOnce = true;
            log.info("db compatibility layer (Oracle/MySQL/SQL Server V$/DBA_*/DBMS_*/sys.*/function "
                    + "compatibility via pg_oracle/pg_mysql/pg_sqlserver) is an Enterprise feature -- "
                    + "set a valid WARP_LICENSE_KEY to enable it. Sessions on the free/Developer "
                    + "tier still connect and run ordinary SQL normally, just against plain, "
                    + "unmodified Postgres semantics, the same as if these extensions weren't "
                    + "installed at all.");
        }
        return allowed;
    }
}
