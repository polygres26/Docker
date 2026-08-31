package com.nexagres.dms.http.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.dms.core.AuditLogStore;
import com.nexagres.dms.core.DmsLicensingTestSupport;
import com.nexagres.wire.license.LicenseTier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AdminAuthTest {

    private AuditLogStore newAuditStore(String testName) {
        String poolKey = "test-audit-adminauth-" + testName;
        return new AuditLogStore(poolKey, "jdbc:hsqldb:mem:" + poolKey + ";shutdown=true");
    }

    @AfterEach
    void resetOverride() {
        DmsLicensingTestSupport.reset();
    }

    @Test
    void adminLoginAlwaysWorksRegardlessOfLicense() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        AdminAuth auth = new AdminAuth(newAuditStore("admin-free"), "admin", "s3cret", null, null);

        var token = auth.login("admin", "s3cret");
        assertTrue(token.isPresent());
        assertEquals(Role.ADMIN, auth.sessionOf(token.get()).orElseThrow().role());
        assertTrue(auth.login("admin", "wrong").isEmpty());
    }

    @Test
    void viewerAccountIsIgnoredOnFreeTierEvenIfConfigured() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        AdminAuth auth = new AdminAuth(newAuditStore("viewer-free"), "admin", "s3cret", "viewer", "v13w");

        assertTrue(auth.login("viewer", "v13w").isEmpty(), "viewer creds should be inert without Enterprise");
        assertTrue(auth.login("admin", "s3cret").isPresent(), "admin login still works");
    }

    @Test
    void viewerAccountWorksUnderEnterpriseAndGetsViewerRole() {
        DmsLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        AdminAuth auth = new AdminAuth(newAuditStore("viewer-ent"), "admin", "s3cret", "viewer", "v13w");

        var token = auth.login("viewer", "v13w");
        assertTrue(token.isPresent());
        assertEquals(Role.VIEWER, auth.sessionOf(token.get()).orElseThrow().role());
    }

    @Test
    void ssoIssuedSessionsAreAlwaysAdminRole() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        AdminAuth auth = new AdminAuth(newAuditStore("sso"), "admin", "s3cret", null, null);
        String token = auth.issueSessionFor("someone@example.com");
        assertEquals(Role.ADMIN, auth.sessionOf(token).orElseThrow().role());
        assertEquals("someone@example.com", auth.usernameOf(token));
    }

    @Test
    void logoutInvalidatesTheSession() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        AdminAuth auth = new AdminAuth(newAuditStore("logout"), "admin", "s3cret", null, null);
        String token = auth.login("admin", "s3cret").orElseThrow();
        assertTrue(auth.isValid(token));
        auth.logout(token);
        assertFalse(auth.isValid(token));
    }
}
