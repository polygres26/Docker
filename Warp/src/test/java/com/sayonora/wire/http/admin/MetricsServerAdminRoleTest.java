package com.sayonora.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.wire.audit.AuditEvent;
import com.sayonora.wire.audit.AuditLog;
import com.sayonora.wire.core.AccessContext;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Covers the admin console's SSO-vs-shared-token role resolution introduced alongside real OIDC
 * enforcement: the static {@code WARP_ADMIN_TOKEN} always grants full {@code ADMIN} (the
 * pre-existing, simpler path -- kept fully intact for developer testing, CI, and any customer who
 * just wants one shared secret, not replaced by SSO), while a real SSO identity's roles claim
 * (Okta group, Entra ID app role, or any OIDC-compliant IdP's own role/group claim) is checked
 * against the configured admin/viewer role-name sets to grant read-only {@code VIEWER} or full
 * {@code ADMIN} instead. Exercises the real static methods {@code MetricsServer} uses on every
 * admin request, not a reimplementation of the logic.
 */
class MetricsServerAdminRoleTest {

    private static final Set<String> ADMIN_ROLES = Set.of("admin");
    private static final Set<String> VIEWER_ROLES = Set.of("viewer");

    @Test
    void staticAdminTokenGrantsAdminRegardlessOfSsoIdentity() {
        assertEquals(MetricsServer.AdminRole.ADMIN,
                MetricsServer.resolveAdminRole("Bearer secret-token", AccessContext.ANONYMOUS,
                        "secret-token", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void staticAdminTokenStillWorksWithNoOidcConfiguredAtAll() {
        // The simple, pre-existing setup this feature must not regress: no AccessContext at all
        // (as MetricsServer.DISABLED's resolver always hands back AccessContext.ANONYMOUS), just
        // the shared token.
        assertEquals(MetricsServer.AdminRole.ADMIN,
                MetricsServer.resolveAdminRole("Bearer dev-token", AccessContext.ANONYMOUS,
                        "dev-token", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void ssoIdentityWithAdminRoleGrantsAdminWhenNoTokenPresented() {
        AccessContext ctx = new AccessContext("alice@example.com", Set.of("admin"), java.util.Map.of());
        assertEquals(MetricsServer.AdminRole.ADMIN,
                MetricsServer.resolveAdminRole(null, ctx, "unrelated-secret", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void ssoIdentityWithViewerRoleGrantsViewerOnly() {
        AccessContext ctx = new AccessContext("bob@example.com", Set.of("viewer"), java.util.Map.of());
        assertEquals(MetricsServer.AdminRole.VIEWER,
                MetricsServer.resolveAdminRole(null, ctx, "unrelated-secret", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void ssoIdentityWithNoRecognizedRoleIsDefaultDenied() {
        AccessContext ctx = new AccessContext("carol@example.com", Set.of("some-other-group"), java.util.Map.of());
        assertEquals(MetricsServer.AdminRole.NONE,
                MetricsServer.resolveAdminRole(null, ctx, "unrelated-secret", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void anonymousCallerWithNoTokenAndNoSsoIsNone() {
        assertEquals(MetricsServer.AdminRole.NONE,
                MetricsServer.resolveAdminRole(null, AccessContext.ANONYMOUS, "some-token", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void wrongTokenDoesNotFallThroughToAdmin() {
        assertEquals(MetricsServer.AdminRole.NONE,
                MetricsServer.resolveAdminRole("Bearer wrong", AccessContext.ANONYMOUS,
                        "correct-token", ADMIN_ROLES, VIEWER_ROLES));
    }

    @Test
    void viewerCanReadButNotWrite() {
        assertTrue(MetricsServer.authorized("GET", MetricsServer.AdminRole.VIEWER));
        assertTrue(MetricsServer.authorized("HEAD", MetricsServer.AdminRole.VIEWER));
        assertFalse(MetricsServer.authorized("POST", MetricsServer.AdminRole.VIEWER));
        assertFalse(MetricsServer.authorized("PUT", MetricsServer.AdminRole.VIEWER));
        assertFalse(MetricsServer.authorized("DELETE", MetricsServer.AdminRole.VIEWER));
    }

    @Test
    void adminCanReadAndWrite() {
        assertTrue(MetricsServer.authorized("GET", MetricsServer.AdminRole.ADMIN));
        assertTrue(MetricsServer.authorized("POST", MetricsServer.AdminRole.ADMIN));
        assertTrue(MetricsServer.authorized("PUT", MetricsServer.AdminRole.ADMIN));
        assertTrue(MetricsServer.authorized("DELETE", MetricsServer.AdminRole.ADMIN));
    }

    @Test
    void noRoleCanNeitherReadNorWrite() {
        assertFalse(MetricsServer.authorized("GET", MetricsServer.AdminRole.NONE));
        assertFalse(MetricsServer.authorized("POST", MetricsServer.AdminRole.NONE));
    }

    @Test
    void splitRolesFallsBackToDefaultWhenUnset() {
        assertEquals(Set.of("admin"), MetricsServer.splitRoles(null, "admin"));
        assertEquals(Set.of("admin"), MetricsServer.splitRoles("  ", "admin"));
    }

    @Test
    void splitRolesParsesCommaSeparatedCustomNames() {
        assertEquals(Set.of("warp-admin", "platform-team"),
                MetricsServer.splitRoles("warp-admin, platform-team", "admin"));
    }

    @Test
    void bearerTokenValidRejectsMissingOrMalformedHeader() {
        assertFalse(MetricsServer.bearerTokenValid(null, "secret"));
        assertFalse(MetricsServer.bearerTokenValid("secret", "secret"));
        assertFalse(MetricsServer.bearerTokenValid("Bearer wrong", "secret"));
        assertTrue(MetricsServer.bearerTokenValid("Bearer secret", "secret"));
    }

    @Test
    void bearerTokenValidIsDisabledWhenNoAdminTokenConfigured() {
        assertFalse(MetricsServer.bearerTokenValid("Bearer anything", null));
        assertFalse(MetricsServer.bearerTokenValid("Bearer anything", ""));
    }

    @Test
    void adminActionBySsoIdentityIsAttributedToTheRealUser() {
        AuditLog auditLog = new AuditLog();
        AccessContext ctx = new AccessContext("alice@example.com", Set.of("admin"), java.util.Map.of());
        MetricsServer.recordAdminAction(auditLog, ctx, "PUT", "/api/config");
        List<AuditEvent> events = auditLog.recent(10);
        assertEquals(1, events.size());
        assertEquals(AuditEvent.Type.ADMIN_ACTION, events.get(0).type());
        assertEquals("alice@example.com", events.get(0).userId());
        assertEquals("PUT /api/config", events.get(0).summary());
    }

    @Test
    void adminActionByTheSharedTokenIsAttributedPlainlyNotToAFakeUser() {
        AuditLog auditLog = new AuditLog();
        MetricsServer.recordAdminAction(auditLog, AccessContext.ANONYMOUS, "DELETE", "/api/queues/orders");
        List<AuditEvent> events = auditLog.recent(10);
        assertEquals(1, events.size());
        assertEquals("shared-admin-token", events.get(0).userId());
        assertEquals("DELETE /api/queues/orders", events.get(0).summary());
    }
}
