package com.sayonora.warp.pubsubwire;

/** Retry-policy backoff: exponential from the minimum, doubling per failed delivery, capped at the maximum. */
final class PsBackoff {

    static final long DEFAULT_MIN_MS = 10_000L;
    static final long DEFAULT_MAX_MS = 600_000L;

    private PsBackoff() {
    }

    /** Delay before redelivery after the {@code attempt}-th delivery (1-based) failed. */
    static long delayMillis(long minMs, long maxMs, int attempt) {
        if (attempt <= 1) {
            return Math.min(minMs, maxMs);
        }
        long d = minMs;
        for (int i = 1; i < attempt; i++) {
            d = d * 2;
            if (d >= maxMs) {
                return maxMs;
            }
        }
        return Math.min(d, maxMs);
    }
}
