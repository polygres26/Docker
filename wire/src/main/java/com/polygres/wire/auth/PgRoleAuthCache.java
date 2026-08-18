package com.polygres.wire.auth;

import com.polygres.wire.pgwire.PgConnections;
import com.polygres.wire.server.ServerOptions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real per-role Postgres authentication -- opt-in via {@code POLYWIRE_AUTH_MODE=postgres_roles}
 * (default {@code shared_secret}, {@link CredentialStore}'s single dev credential, unchanged).
 * pgbouncer-style {@code auth_query} design: an admin connection (this project's existing
 * {@code ORAPG_PG_*} backend credentials -- must be a superuser, since {@code pg_authid} is
 * superuser-only) periodically fetches every loginable role's real password verifier from
 * {@code pg_authid} into an in-memory cache, refreshed on a timer
 * ({@code POLYWIRE_AUTH_REFRESH_SECONDS}, default 30s) rather than a live query per login.
 *
 * <p><b>Every login is verified entirely against this local cache</b> -- {@link #verify} never
 * opens a connection itself, so a bad *or* good login attempt costs zero extra Postgres backend
 * connections, same property {@link CredentialStore}'s single shared secret already had; the only
 * traffic this adds to Postgres is the periodic background refresh, not per-client-attempt.
 *
 * <p><b>Only wired into pgwire and mssqlwire</b> -- both collect the client's password as
 * cleartext already (pgwire's own {@code AuthenticationCleartextPassword}; mssqlwire's TDS
 * LOGIN7 sends the password as plain UCS-2 text, no challenge-response), so verifying that
 * cleartext against a stored hash is exactly what Postgres's own server does. orawire's O5LOGON
 * and mywire's {@code mysql_native_password} are both real challenge-response protocols instead
 * -- the client never sends a plaintext password at all, it sends a value computed *from* the
 * plaintext via that protocol's own crypto, which the server can only verify by independently
 * knowing the actual plaintext (or an algorithm-matching hash Postgres doesn't produce). A
 * Postgres {@code md5}/{@code SCRAM-SHA-256} verifier can't be reversed to get that plaintext back
 * (that's the entire point of it being a one-way hash), so those two protocols are not candidates
 * for this cache and still use {@link CredentialStore}'s shared secret -- a real, documented scope
 * limit, not an oversight.
 */
public final class PgRoleAuthCache {

    private static final Logger log = LoggerFactory.getLogger(PgRoleAuthCache.class);

    private final ServerOptions options;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "polywire-auth-role-refresh");
                t.setDaemon(true);
                return t;
            });
    private volatile Map<String, String> verifiersByRole = Map.of();

    public PgRoleAuthCache(ServerOptions options) {
        this.options = options;
        refresh(); // populate synchronously before accepting any logins -- an empty cache would
                   // reject every real role until the first scheduled tick otherwise.
        int refreshSeconds = parseIntEnv("POLYWIRE_AUTH_REFRESH_SECONDS", 30);
        scheduler.scheduleWithFixedDelay(this::refreshSafely, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
    }

    /** True if {@code presentedPassword} is the real current password for {@code username}, per the last refresh. */
    public boolean verify(String username, String presentedPassword) {
        String verifier = verifiersByRole.get(username.toLowerCase(java.util.Locale.ROOT));
        return verifier != null && PostgresPasswordVerifier.verify(verifier, username, presentedPassword);
    }

    private void refreshSafely() {
        try {
            refresh();
        } catch (Exception e) {
            // Never let a refresh failure crash the scheduler or take down already-cached roles --
            // a transient backend hiccup should leave the last-known-good cache in place, same
            // "stale but available beats an outage" posture as PolyWireCluster's discovery.
            log.warn("PgRoleAuthCache refresh failed, keeping previous cache ({} roles): {}",
                    verifiersByRole.size(), e.getMessage());
        }
    }

    private void refresh() {
        Map<String, String> fresh = new ConcurrentHashMap<>();
        try (Connection admin = PgConnections.open(options);
                Statement stmt = admin.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT rolname, rolpassword FROM pg_authid "
                                + "WHERE rolcanlogin AND rolpassword IS NOT NULL")) {
            while (rs.next()) {
                fresh.put(rs.getString("rolname").toLowerCase(java.util.Locale.ROOT), rs.getString("rolpassword"));
            }
        } catch (Exception e) {
            throw new RuntimeException("PgRoleAuthCache: failed to query pg_authid "
                    + "(the ORAPG_PG_* admin connection must be a real superuser -- pg_authid.rolpassword "
                    + "is superuser-only)", e);
        }
        verifiersByRole = fresh;
        log.info("PgRoleAuthCache: refreshed {} loginable role(s) from pg_authid", fresh.size());
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(raw.trim());
    }
}
