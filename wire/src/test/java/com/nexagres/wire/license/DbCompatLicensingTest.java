package com.nexagres.wire.license;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DbCompatLicensingTest {

    @AfterEach
    void resetOverride() {
        DbCompatLicensingTestSupport.reset();
    }

    @Test
    void freeTierDisallowsDbEmulation() {
        DbCompatLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        assertEquals(false, DbCompatLicensing.dbEmulationAllowed());
    }

    @Test
    void enterpriseTierAllowsDbEmulation() {
        DbCompatLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertEquals(true, DbCompatLicensing.dbEmulationAllowed());
    }

    @Test
    void withNoOverrideThisTestEnvironmentIsRealFreeTier() {
        // No WARP_LICENSE_KEY is set anywhere in this test run -- proves the default,
        // real (non-overridden) resolution path is genuinely free-tier, not just the override
        // path, the same assumption every other license-gated test in this project relies on.
        assertEquals(false, DbCompatLicensing.dbEmulationAllowed());
    }
}
