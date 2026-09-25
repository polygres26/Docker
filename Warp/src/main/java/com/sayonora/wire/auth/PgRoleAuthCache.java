package com.sayonora.wire.auth;

import com.sayonora.wire.pgwire.PgConnections;
import com.sayonora.wire.server.ServerOptions;
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

public final class PgRoleAuthCache {

    private static final Logger log = LoggerFactory.getLogger(PgRoleAuthCache.class);

    private final ServerOptions options;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "warp-auth-role-refresh");
                t.setDaemon(true);
                return t;
            });
    private volatile Map<String, String> verifiersByRole = Map.of();

    public PgRoleAuthCache(ServerOptions options) {
        this.options = options;
        refresh();
                   
        int refreshSeconds = parseIntEnv("WARP_AUTH_REFRESH_SECONDS", 30);
        scheduler.scheduleWithFixedDelay(this::refreshSafely, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
    }

    public boolean verify(String username, String presentedPassword) {
        String verifier = verifiersByRole.get(username.toLowerCase(java.util.Locale.ROOT));
        return verifier != null && PostgresPasswordVerifier.verify(verifier, username, presentedPassword);
    }

    private void refreshSafely() {
        try {
            refresh();
        } catch (Exception e) {
            
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
                    + "(the WARP_* admin connection must be a real superuser -- pg_authid.rolpassword "
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
