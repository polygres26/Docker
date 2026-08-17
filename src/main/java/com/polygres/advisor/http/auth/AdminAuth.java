package com.polygres.advisor.http.auth;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-admin-account session auth -- deliberately minimal (no OIDC/SSO, no multi-user roles),
 * scoped to "an admin" as asked for, not a full identity system. Omnigate has a real OIDC-backed
 * admin console ({@code com.omnigate.http.admin.auth}); this is a much smaller MVP version that
 * can be swapped for something like that later if multi-admin/SSO becomes a real requirement.
 *
 * <p>Credentials: {@code POLYGRES_ADMIN_USER} (default {@code admin}) and
 * {@code POLYGRES_ADMIN_PASSWORD}. If the password env var is unset, a random one is generated at
 * startup and printed to the log exactly once -- loud on purpose, so this never silently runs with
 * a guessable default in anything other than a throwaway local session.
 *
 * <p>Sessions: opaque random tokens in an in-process map with a fixed TTL. Lost on process
 * restart (no persistence) -- acceptable for an admin tool, not appropriate to copy as-is for a
 * customer-facing session store.
 */
public class AdminAuth {

    private static final Logger log = LoggerFactory.getLogger(AdminAuth.class);
    private static final long SESSION_TTL_MILLIS = 12 * 60 * 60 * 1000; // 12h

    private final String adminUser;
    private final String adminPassword;
    private final ConcurrentHashMap<String, Long> sessions = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public AdminAuth() {
        this.adminUser = System.getenv().getOrDefault("POLYGRES_ADMIN_USER", "admin");
        String configured = System.getenv("POLYGRES_ADMIN_PASSWORD");
        if (configured == null || configured.isBlank()) {
            this.adminPassword = generateRandomPassword();
            log.warn("POLYGRES_ADMIN_PASSWORD not set -- generated a one-time password for this "
                + "process. Set POLYGRES_ADMIN_PASSWORD explicitly for anything beyond local dev. "
                + "Admin user: {}  Admin password: {}", adminUser, adminPassword);
        } else {
            this.adminPassword = configured;
        }
    }

    public Optional<String> login(String username, String password) {
        if (!adminUser.equals(username) || !constantTimeEquals(adminPassword, password)) {
            return Optional.empty();
        }
        String token = generateToken();
        sessions.put(token, System.currentTimeMillis() + SESSION_TTL_MILLIS);
        return Optional.of(token);
    }

    public boolean isValid(String token) {
        if (token == null) return false;
        Long expiry = sessions.get(token);
        if (expiry == null) return false;
        if (expiry < System.currentTimeMillis()) {
            sessions.remove(token);
            return false;
        }
        return true;
    }

    public void logout(String token) {
        if (token != null) sessions.remove(token);
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
