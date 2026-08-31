package com.nexagres.dms.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nexagres.wire.license.LicenseTier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DmsLicensingTest {

    @AfterEach
    void resetOverride() {
        DmsLicensingTestSupport.reset();
    }

    @Test
    void freeTierRefusesRbacAndSsoAndDisablesAuditLogging() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        assertEquals(false, DmsLicensing.rbacAllowed());
        assertEquals(false, DmsLicensing.auditLoggingEnabled());

        IllegalStateException rbac = assertThrows(IllegalStateException.class, DmsLicensing::requireEnterpriseForRbac);
        assertEquals(true, rbac.getMessage().contains("POLYWIRE_LICENSE_KEY"));

        IllegalStateException sso = assertThrows(IllegalStateException.class, DmsLicensing::requireEnterpriseForSso);
        assertEquals(true, sso.getMessage().contains("POLYWIRE_LICENSE_KEY"));
    }

    @Test
    void enterpriseTierAllowsRbacSsoAndAuditLogging() {
        DmsLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        assertEquals(true, DmsLicensing.rbacAllowed());
        assertEquals(true, DmsLicensing.auditLoggingEnabled());
        assertDoesNotThrow(DmsLicensing::requireEnterpriseForRbac);
        assertDoesNotThrow(DmsLicensing::requireEnterpriseForSso);
    }
}
