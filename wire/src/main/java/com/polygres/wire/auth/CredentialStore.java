package com.polygres.wire.auth;

/**
 * Placeholder credential lookup for O5LOGON verification. Stores a single
 * plaintext dev credential (overridable via env vars) — NOT tied to real
 * Postgres roles yet. A real implementation needs a proper user/secret
 * store and must never keep plaintext passwords at rest; this exists only
 * to unblock testing the auth handshake end to end.
 */
public final class CredentialStore {

    private final String username;
    private final byte[] password;

    public CredentialStore() {
        this.username = System.getenv().getOrDefault("POLYWIRE_AUTH_USER", "orapg");
        this.password = System.getenv().getOrDefault("POLYWIRE_AUTH_PASSWORD", "orapg")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Returns the stored password bytes for username, or null if unknown.
     *
     * <p>Case-insensitive: found live against the real Java thin driver (ojdbc11) — Oracle
     * clients uppercase unquoted usernames before putting them on the wire (standard Oracle SQL
     * identifier folding), so a real driver sends {@code "POSTGRES"} for a user who typed
     * {@code postgres}. python-oracledb doesn't do this client-side folding, which is why an
     * exact-case comparison happened to work against it but silently rejected every real ojdbc11
     * login with ORA-1017 regardless of how correct the O5LOGON crypto/terminator bytes were.
     */
    public byte[] lookupPassword(String username) {
        return this.username.equalsIgnoreCase(username) ? password : null;
    }
}
