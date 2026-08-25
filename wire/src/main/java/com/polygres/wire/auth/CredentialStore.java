package com.polygres.wire.auth;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * Verifies wire-protocol logins (orawire's O5LOGON, mywire, and pgwire/mssqlwire's non-{@code
 * postgres_roles} fallback) against one of two credential shapes:
 *
 * <ul>
 *   <li>The historical default -- a single shared username/password from {@code
 *       POLYWIRE_AUTH_USER}/{@code POLYWIRE_AUTH_PASSWORD} (both default to {@code orapg}). Every
 *       client presents the same credential; nothing distinguishes one caller's identity from
 *       another's, so callers stay {@link com.polygres.wire.core.AccessContext#ANONYMOUS} and
 *       native RLS/audit propagation has nothing real to key on.
 *   <li>{@code POLYWIRE_AUTH_CREDENTIALS} -- a semicolon-separated {@code
 *       username=password;username2=password2} list of real, distinguishable per-caller
 *       credentials, structurally mirroring {@link
 *       com.polygres.wire.dynamowire.auth.AwsIamCredentialStore}'s {@code
 *       POLYWIRE_AWS_IAM_CREDENTIALS} list. When set, this is what makes orawire's O5LOGON login
 *       (which -- unlike pgwire's SCRAM -- needs the real plaintext password server-side to
 *       verify the client's challenge response, so it can never be satisfied from Postgres's own
 *       hashed {@code pg_authid} verifiers the way {@code PgRoleAuthCache} is) a real identity a
 *       session handler can carry into {@link com.polygres.wire.core.AccessContext} and from
 *       there into {@link com.polygres.wire.core.access.PostgresRlsSessionInitializer}.
 * </ul>
 *
 * <p>{@link #isMultiUser()} tells a caller (see {@code orawire.session.SessionHandler}) which
 * shape is active, exactly the same role {@code roleAuthCache != null} plays for pgwire/mssqlwire
 * deciding whether a login is worth propagating as a real identity.
 */
public final class CredentialStore {

    private final String singleUsername;
    private final byte[] singlePassword;
    private final Map<String, byte[]> passwordsByUsername;

    public CredentialStore() {
        this(System.getenv("POLYWIRE_AUTH_CREDENTIALS"), System.getenv("POLYWIRE_AUTH_USER"),
                System.getenv("POLYWIRE_AUTH_PASSWORD"));
    }

    CredentialStore(String multiUserSpec, String singleUsernameEnv, String singlePasswordEnv) {
        this.singleUsername = singleUsernameEnv == null ? "orapg" : singleUsernameEnv;
        this.singlePassword = (singlePasswordEnv == null ? "orapg" : singlePasswordEnv).getBytes(StandardCharsets.UTF_8);
        this.passwordsByUsername = parseMultiUserSpec(multiUserSpec);
    }

    private static Map<String, byte[]> parseMultiUserSpec(String spec) {
        // Case-insensitive, matching Oracle's own unquoted-identifier semantics -- a real Oracle
        // client (ojdbc, sqlplus, python-oracledb) uppercases an unquoted username before it ever
        // reaches the wire, so a lowercase POLYWIRE_AUTH_CREDENTIALS entry must still match it.
        Map<String, byte[]> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
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
                        "POLYWIRE_AUTH_CREDENTIALS entry must be \"username=password\", got: " + trimmed);
            }
            result.put(trimmed.substring(0, eq), trimmed.substring(eq + 1).getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    /** True once {@code POLYWIRE_AUTH_CREDENTIALS} configures real, distinguishable per-user
     * credentials instead of the single shared fallback. */
    public boolean isMultiUser() {
        return !passwordsByUsername.isEmpty();
    }

    public byte[] lookupPassword(String username) {
        if (isMultiUser()) {
            return passwordsByUsername.get(username);
        }
        return this.singleUsername.equalsIgnoreCase(username) ? singlePassword : null;
    }
}
