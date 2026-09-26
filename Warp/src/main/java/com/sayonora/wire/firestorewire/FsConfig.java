package com.sayonora.wire.firestorewire;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * firestorewire configuration (environment).
 * <ul>
 *   <li>{@code WARP_FIRESTOREWIRE_TOKENS} -- comma list of accepted OAuth2 bearer tokens (not validated as JWTs). Empty (default):
 *       no authentication, like the official emulator, which also accepts the {@code Authorization: Bearer owner} header client
 *       libraries send when FIRESTORE_EMULATOR_HOST is set.</li>
 *   <li>{@code WARP_FIRESTOREWIRE_HISTORY_SECONDS} -- how long document versions are kept for read_time reads, read-only
 *       transactions and Listen resume tokens (default 3600, like Firestore's one-hour read_time window).</li>
 * </ul>
 */
final class FsConfig {

    final Set<String> tokens;
    final long historySeconds;

    FsConfig(Set<String> tokens, long historySeconds) {
        this.tokens = Set.copyOf(tokens);
        this.historySeconds = historySeconds;
    }

    static FsConfig fromEnv() {
        Set<String> t = new LinkedHashSet<>();
        String v = System.getenv("WARP_FIRESTOREWIRE_TOKENS");
        if (v != null) {
            for (String p : v.split(",")) {
                if (!p.isBlank()) {
                    t.add(p.trim());
                }
            }
        }
        long h = 3600;
        String hs = System.getenv("WARP_FIRESTOREWIRE_HISTORY_SECONDS");
        if (hs != null && !hs.isBlank()) {
            h = Long.parseLong(hs.trim());
        }
        return new FsConfig(t, h);
    }

    /** True when the Authorization header (a "Bearer x" value) is acceptable. */
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
