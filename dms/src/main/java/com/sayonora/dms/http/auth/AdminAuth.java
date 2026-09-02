package com.sayonora.dms.http.auth;

import com.sayonora.dms.core.AuditLogStore;
import com.sayonora.dms.core.DmsLicensing;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session auth for the admin console -- one always-available {@code ADMIN} account (unchanged
 * from before RBAC existed), plus an OPTIONAL second, Enterprise-only {@code VIEWER} account (see
 * {@code DmsLicensing#rbacAllowed}). Omnigate has a real OIDC-backed admin console ({@code
 * com.omnigate.http.admin.auth}); this -- plus {@link SsoAuth} for bearer-token login -- is a
 * much smaller version that can be swapped for something like that later if a full identity
 * system becomes a real requirement.
 *
 * <p>Admin credentials: {@code SAYONORA_ADMIN_USER} (default {@code admin}) and
 * {@code SAYONORA_ADMIN_PASSWORD}. If the password env var is unset, a random one is generated at
 * startup and printed to the log exactly once -- loud on purpose, so this never silently runs with
 * a guessable default in anything other than a throwaway local session.
 *
 * <p>Viewer credentials: {@code SAYONORA_VIEWER_USER}/{@code SAYONORA_VIEWER_PASSWORD}, both
 * optional -- if set but {@link DmsLicensing#rbacAllowed()} is {@code false} at startup, they're
 * logged as ignored and never activated (degrade, don't throw: an admin-console process
 * shouldn't refuse to start over a licensing mismatch on an optional second account -- see {@code
 * MigrationLicensing#enforceLocalParallelism}'s own javadoc for the same "clamp, don't throw"
 * reasoning applied elsewhere in this project).
 *
 * <p>Sessions: opaque random tokens in an in-process map, each carrying a {@link Role} and the
 * username that created it, fixed TTL. Lost on process restart (no persistence) -- acceptable for
 * an admin tool, not appropriate to copy as-is for a customer-facing session store.
 */
public class AdminAuth {

    private static final Logger log = LoggerFactory.getLogger(AdminAuth.class);
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000; // 12h

    /** One active session -- username plus role, so {@link AuthGuard} and the audit log can both
     * report who did what without a second lookup. */
    public record Session(String username, Role role, long expiresAtMillis) {
    }

    private final String adminUser;
    private final String adminPassword;
    private final String viewerUser;
    private final String viewerPassword;
    private final AuditLogStore auditLog;
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public AdminAuth() {
        this(new AuditLogStore());
    }

    public AdminAuth(AuditLogStore auditLog) {
        this(auditLog,
                System.getenv().getOrDefault("SAYONORA_ADMIN_USER", "admin"),
                System.getenv("SAYONORA_ADMIN_PASSWORD"),
                System.getenv("SAYONORA_VIEWER_USER"),
                System.getenv("SAYONORA_VIEWER_PASSWORD"));
    }

    /** Test-only seam taking credentials directly rather than via env vars, which {@code
     * System.getenv} offers no portable way to override from a test -- see {@code
     * AdminAuthTest}. Package-private on purpose; a real deployment always goes through one of the
     * two public constructors above. */
    AdminAuth(AuditLogStore auditLog, String adminUser, String configuredAdminPassword,
            String configuredViewerUser, String configuredViewerPassword) {
        this.auditLog = auditLog;
        this.adminUser = adminUser;
        if (configuredAdminPassword == null || configuredAdminPassword.isBlank()) {
            this.adminPassword = generateRandomPassword();
            log.warn("SAYONORA_ADMIN_PASSWORD not set -- generated a one-time password for this "
                + "process. Set SAYONORA_ADMIN_PASSWORD explicitly for anything beyond local dev. "
                + "Admin user: {}  Admin password: {}", adminUser, adminPassword);
        } else {
            this.adminPassword = configuredAdminPassword;
        }

        boolean viewerConfigured = configuredViewerUser != null && !configuredViewerUser.isBlank()
                && configuredViewerPassword != null && !configuredViewerPassword.isBlank();
        if (viewerConfigured && !DmsLicensing.rbacAllowed()) {
            log.warn("SAYONORA_VIEWER_USER/SAYONORA_VIEWER_PASSWORD are set, but role-based access "
                    + "control (a second admin-console account) is an Enterprise feature -- set a "
                    + "valid WARP_LICENSE_KEY to activate the viewer account. Ignoring it for now; "
                    + "only the single admin account is usable.");
            this.viewerUser = null;
            this.viewerPassword = null;
        } else if (viewerConfigured) {
            this.viewerUser = configuredViewerUser;
            this.viewerPassword = configuredViewerPassword;
            log.info("viewer account '{}' activated (read-only)", viewerUser);
        } else {
            this.viewerUser = null;
            this.viewerPassword = null;
        }
    }

    public Optional<String> login(String username, String password) {
        Role role;
        if (adminUser.equals(username) && constantTimeEquals(adminPassword, password)) {
            role = Role.ADMIN;
        } else if (viewerUser != null && viewerUser.equals(username) && constantTimeEquals(viewerPassword, password)) {
            role = Role.VIEWER;
        } else {
            auditLog.record(username, "login.failed", null);
            return Optional.empty();
        }
        String token = generateToken();
        sessions.put(token, new Session(username, role, System.currentTimeMillis() + SESSION_TTL_MILLIS));
        auditLog.record(username, "login.succeeded", "role=" + role);
        return Optional.of(token);
    }

    /** Mints a session for an identity already verified some other way -- {@link SsoAuth}'s own
     * caller uses this after {@link SsoAuth#verify} succeeds, rather than this class re-checking a
     * password that was never involved. SSO-authenticated identities are always granted {@link
     * Role#ADMIN}: mapping an external IdP's own groups/claims to {@code VIEWER} is real future
     * work, not built in this pass -- see {@link SsoAuth}'s own javadoc on this feature's scope. */
    public String issueSessionFor(String username) {
        String token = generateToken();
        sessions.put(token, new Session(username, Role.ADMIN, System.currentTimeMillis() + SESSION_TTL_MILLIS));
        auditLog.record(username, "login.succeeded.sso", null);
        return token;
    }

    public boolean isValid(String token) {
        return sessionOf(token).isPresent();
    }

    /** The still-live session for {@code token}, or empty if it's missing/expired -- expired
     * entries are evicted as a side effect, same as {@link #isValid} always did. */
    public Optional<Session> sessionOf(String token) {
        if (token == null) return Optional.empty();
        Session session = sessions.get(token);
        if (session == null) return Optional.empty();
        if (session.expiresAtMillis() < System.currentTimeMillis()) {
            sessions.remove(token);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    public void logout(String token) {
        Session session = token == null ? null : sessions.remove(token);
        if (session != null) {
            auditLog.record(session.username(), "logout", null);
        }
    }

    /** For routes/guards that need to attribute an action to a user without threading the whole
     * {@link Session} through -- {@code "(unknown)"} if the token isn't a live session. */
    public String usernameOf(String token) {
        return sessionOf(token).map(Session::username).orElse("(unknown)");
    }

    AuditLogStore auditLog() {
        return auditLog;
    }

    private String generateToken() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private String generateRandomPassword() {
        byte[] bytes = new byte[18];
        random.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] aBytes = a.getBytes();
        byte[] bBytes = b.getBytes();
        if (aBytes.length != bBytes.length) return false;
        int result = 0;
        for (int i = 0; i < aBytes.length; i++) {
            result |= aBytes[i] ^ bBytes[i];
        }
        return result == 0;
    }
}
