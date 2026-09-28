package com.sayonora.warp.auth;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * Verifies wire-protocol logins (orawire's O5LOGON, mywire, and pgwire/mssqlwire's non-{@code
 * postgres_roles} fallback) against one of two credential shapes:
 *
 * <ul>
 *   <li>The historical default -- a single shared username/password from {@code
 *       WARP_AUTH_USER}/{@code WARP_AUTH_PASSWORD} (both default to {@code orapg}). Every
 *       client presents the same credential; nothing distinguishes one caller's identity from
 *       another's, so callers stay {@link com.sayonora.warp.core.AccessContext#ANONYMOUS} and
 *       native RLS/audit propagation has nothing real to key on.
 *   <li>{@code WARP_AUTH_CREDENTIALS} -- a semicolon-separated {@code
 *       username=password;username2=password2} list of real, distinguishable per-caller
 *       credentials, structurally mirroring {@link
 *       com.sayonora.warp.dynamowire.auth.AwsIamCredentialStore}'s {@code
 *       WARP_AWS_IAM_CREDENTIALS} list. When set, this is what makes orawire's O5LOGON login
 *       (which -- unlike pgwire's SCRAM -- needs the real plaintext password server-side to
 *       verify the client's challenge response, so it can never be satisfied from Postgres's own
 *       hashed {@code pg_authid} verifiers the way {@code PgRoleAuthCache} is) a real identity a
 *       session handler can carry into {@link com.sayonora.warp.core.AccessContext} and from
 *       there into {@link com.sayonora.warp.core.access.PostgresRlsSessionInitializer}.
 * </ul>
 *
 * <p>Every password value (single-user or per-entry in the multi-user spec) is a {@link
 * com.sayonora.warp.secrets.SecretRef} string, resolved fresh on every lookup via {@link
 * com.sayonora.warp.secrets.SecretResolver} -- a plain literal keeps working unchanged
 * (backward compatible), but {@code vault:...}/{@code cyberark:...} references now work here too,
 * exactly as they already do for {@code WARP_BACKENDS} entries (see {@code
 * com.sayonora.warp.core.BackendTarget#borrow}). This is what lets Bridge-mode logins (see {@link
 * #fromEnv}) verify a migrated app's *real* Oracle password sourced from Vault/CyberArk, instead
 * of a separate Warp-managed secret the app was never given.
 *
 * <p>{@link #isMultiUser()} tells a caller (see {@code orawire.session.SessionHandler}) which
 * shape is active, exactly the same role {@code roleAuthCache != null} plays for pgwire/mssqlwire
 * deciding whether a login is worth propagating as a real identity.
 */
public final class CredentialStore {

    private final String singleUsername;
    private final String singlePasswordRef;
    private final Map<String, String> passwordRefsByUsername;

    public CredentialStore() {
        this(System.getenv("WARP_AUTH_CREDENTIALS"), System.getenv("WARP_AUTH_USER"),
                System.getenv("WARP_AUTH_PASSWORD"), true);
    }

    /**
     * A separate, independently-configured credential set read from its own env var names instead
     * of the global {@code WARP_AUTH_*} ones -- e.g. Bridge mode's {@code
     * WARP_ORACLE_BRIDGE_LOGIN_CREDENTIALS}/{@code _USER}/{@code _PASSWORD}, which must verify a
     * migrated app's real Oracle password, not the separate credential {@code WARP_AUTH_*}
     * configures for ordinary (JDBC/Adapt) sessions. Unlike the no-arg constructor, an entirely
     * unconfigured set here denies every login rather than silently accepting the {@code
     * orapg}/{@code orapg} default -- that default isn't a real backend account for this use case,
     * so falling back to it would let an operator believe Bridge logins are secured when nothing
     * has actually been configured.
     */
    public static CredentialStore fromEnv(String multiUserEnvVar, String singleUsernameEnvVar,
            String singlePasswordEnvVar) {
        return new CredentialStore(System.getenv(multiUserEnvVar), System.getenv(singleUsernameEnvVar),
                System.getenv(singlePasswordEnvVar), false);
    }

    CredentialStore(String multiUserSpec, String singleUsernameEnv, String singlePasswordEnv) {
        this(multiUserSpec, singleUsernameEnv, singlePasswordEnv, true);
    }

    // Package-private (not private) so CredentialStoreTest can exercise the secure-by-default
    // "nothing configured" deny path directly, without going through real env vars.
    CredentialStore(String multiUserSpec, String singleUsernameEnv, String singlePasswordEnv,
            boolean allowInsecureDefault) {
        boolean nothingConfigured = singleUsernameEnv == null && singlePasswordEnv == null
                && (multiUserSpec == null || multiUserSpec.isBlank());
        if (nothingConfigured && !allowInsecureDefault) {
            this.singleUsername = null;
            this.singlePasswordRef = null;
        } else {
            this.singleUsername = singleUsernameEnv == null ? "orapg" : singleUsernameEnv;
            this.singlePasswordRef = singlePasswordEnv == null ? "orapg" : singlePasswordEnv;
        }
        this.passwordRefsByUsername = parseMultiUserSpec(multiUserSpec);
    }

    private static Map<String, String> parseMultiUserSpec(String spec) {
        // Case-insensitive, matching Oracle's own unquoted-identifier semantics -- a real Oracle
        // client (ojdbc, sqlplus, python-oracledb) uppercases an unquoted username before it ever
        // reaches the wire, so a lowercase *_CREDENTIALS entry must still match it.
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (spec == null || spec.isBlank()) {
            return result;
        }
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "credential spec entry must be \"username=password\", got: " + trimmed);
            }
            result.put(trimmed.substring(0, eq), trimmed.substring(eq + 1));
        }
        return result;
    }

    /** True once the multi-user spec configures real, distinguishable per-user credentials
     * instead of the single shared fallback. */
    public boolean isMultiUser() {
        return !passwordRefsByUsername.isEmpty();
    }

    public byte[] lookupPassword(String username) {
        String ref;
        if (isMultiUser()) {
            ref = passwordRefsByUsername.get(username);
        } else if (singleUsername != null && singleUsername.equalsIgnoreCase(username)) {
            ref = singlePasswordRef;
        } else {
            ref = null;
        }
        if (ref == null) {
            return null;
        }
        String resolved = com.sayonora.warp.secrets.SecretResolver.resolve(ref);
        return resolved == null ? null : resolved.getBytes(StandardCharsets.UTF_8);
    }
}
