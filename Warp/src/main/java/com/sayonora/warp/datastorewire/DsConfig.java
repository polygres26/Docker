package com.sayonora.warp.datastorewire;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * datastorewire configuration (environment): {@code WARP_DATASTOREWIRE_TOKENS} -- comma list of accepted OAuth2 bearer tokens (not
 * validated as JWTs). Empty (default): no authentication, like the official emulator (which also accepts the
 * {@code Authorization: Bearer owner} header client libraries send when DATASTORE_EMULATOR_HOST is set).
 */
final class DsConfig {

    final Set<String> tokens;

    DsConfig(Set<String> tokens) {
        this.tokens = Set.copyOf(tokens);
    }

    static DsConfig fromEnv() {
        Set<String> t = new LinkedHashSet<>();
        String v = System.getenv("WARP_DATASTOREWIRE_TOKENS");
        if (v != null) {
            for (String p : v.split(",")) {
                if (!p.isBlank()) {
                    t.add(p.trim());
                }
            }
        }
        return new DsConfig(t);
    }

    boolean authorized(String authorization) {
        if (tokens.isEmpty()) {
            return true;
        }
        if (authorization == null) {
            return false;
        }
        String a = authorization.trim();
        if (a.regionMatches(true, 0, "Bearer ", 0, 7)) {
            a = a.substring(7).trim();
        }
        return tokens.contains(a);
    }
}
