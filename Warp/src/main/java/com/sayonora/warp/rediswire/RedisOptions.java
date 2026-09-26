package com.sayonora.warp.rediswire;

/** Environment-driven settings of the Redis frontend. */
record RedisOptions(int port, String password, String advertiseHost, long sweepMs, int databases, int maxClients) {

    static RedisOptions fromEnv(int port) {
        return new RedisOptions(port, blankToNull(System.getenv("WARP_REDISWIRE_PASSWORD")),
                blankToNull(System.getenv("WARP_REDISWIRE_ADVERTISE_HOST")),
                longEnv("WARP_REDISWIRE_SWEEP_MS", 1000), (int) longEnv("WARP_REDISWIRE_DATABASES", 16),
                (int) longEnv("WARP_REDISWIRE_MAX_CLIENTS", 10000));
    }

    static RedisOptions of(int port, String password) {
        return new RedisOptions(port, password, null, 1000, 16, 10000);
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private static long longEnv(String name, long dflt) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return dflt;
        }
    }
}
