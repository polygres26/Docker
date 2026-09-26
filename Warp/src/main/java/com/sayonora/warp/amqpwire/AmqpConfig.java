package com.sayonora.warp.amqpwire;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Environment configuration of amqpwire. */
final class AmqpConfig {

    /** Allowed vhosts (WARP_AMQPWIRE_VHOSTS, comma separated, default "/"); "*" allows any vhost. */
    Set<String> vhosts = new HashSet<>(Set.of("/"));
    boolean authRequired;
    int heartbeat = 60;
    int frameMax = 131072;
    int channelMax = 2047;
    long pollMs = 200;
    long sweepMs = 1000;
    long topologyTtlMs = 250;
    long maxMessageBytes = 16L * 1024 * 1024;
    String productName = "Warp AMQP";

    static AmqpConfig fromEnv() {
        AmqpConfig c = new AmqpConfig();
        String v = System.getenv("WARP_AMQPWIRE_VHOSTS");
        if (v != null && !v.isBlank()) {
            c.vhosts = new HashSet<>();
            Arrays.stream(v.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(c.vhosts::add);
        }
        String a = System.getenv("WARP_AMQPWIRE_AUTH");
        String creds = System.getenv("WARP_AUTH_CREDENTIALS");
        c.authRequired = a != null ? "true".equalsIgnoreCase(a) : creds != null && !creds.isBlank();
        c.heartbeat = (int) longEnv("WARP_AMQPWIRE_HEARTBEAT", 60);
        c.frameMax = (int) longEnv("WARP_AMQPWIRE_FRAME_MAX", 131072);
        c.pollMs = longEnv("WARP_AMQPWIRE_POLL_MS", 200);
        c.sweepMs = longEnv("WARP_AMQPWIRE_SWEEP_MS", 1000);
        c.topologyTtlMs = longEnv("WARP_AMQPWIRE_TOPOLOGY_TTL_MS", 250);
        c.maxMessageBytes = longEnv("WARP_AMQPWIRE_MAX_MESSAGE_BYTES", 16L * 1024 * 1024);
        return c;
    }

    boolean vhostAllowed(String v) {
        return vhosts.contains("*") || vhosts.contains(v);
    }

    static long longEnv(String n, long d) {
        String v = System.getenv(n);
        try {
            return v == null || v.isBlank() ? d : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }
}
