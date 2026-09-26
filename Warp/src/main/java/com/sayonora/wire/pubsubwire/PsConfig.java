package com.sayonora.wire.pubsubwire;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Environment configuration of pubsubwire. */
final class PsConfig {

    /** Bearer tokens accepted (WARP_PUBSUBWIRE_TOKENS, comma separated); empty = no authentication, like the emulator. */
    final Set<String> tokens;
    /** How long a Pull without return_immediately waits for a message (WARP_PUBSUBWIRE_PULL_WAIT_MS). */
    final long pullWaitMs;
    final int maxStreamsPerSubscription;

    PsConfig(Set<String> tokens, long pullWaitMs, int maxStreamsPerSubscription) {
        this.tokens = tokens;
        this.pullWaitMs = pullWaitMs;
        this.maxStreamsPerSubscription = maxStreamsPerSubscription;
    }

    static PsConfig fromEnv() {
        String t = System.getenv("WARP_PUBSUBWIRE_TOKENS");
        Set<String> tokens = new HashSet<>();
        if (t != null) {
            Arrays.stream(t.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(tokens::add);
        }
        return new PsConfig(tokens, longEnv("WARP_PUBSUBWIRE_PULL_WAIT_MS", 20_000), (int) longEnv("WARP_PUBSUBWIRE_MAX_STREAMS", 1000));
    }

    static long longEnv(String name, long def) {
        String v = System.getenv(name);
        try {
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
