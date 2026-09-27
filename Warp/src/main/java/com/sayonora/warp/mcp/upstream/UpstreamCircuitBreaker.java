package com.sayonora.warp.mcp.upstream;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-upstream circuit breaker: after {@code failureThreshold} consecutive failures (timeouts,
 * connection errors, repeated 5xx) the breaker OPENS and short-circuits calls for
 * {@code cooldownMillis} without attempting the network -- so a single down/slow upstream cannot
 * make {@code tools/list} (which fans out to every included upstream) slow or fail for the whole
 * endpoint. After the cooldown it goes HALF_OPEN (one probe allowed); success closes it, failure
 * re-opens it and doubles the next cooldown up to {@code maxCooldownMillis}.
 */
public final class UpstreamCircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final long baseCooldownMillis;
    private final long maxCooldownMillis;
    private final Clock clock;

    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicReference<Instant> openUntil = new AtomicReference<>(Instant.EPOCH);
    private volatile long currentCooldownMillis;

    public UpstreamCircuitBreaker() {
        this(3, 5_000, 60_000, Clock.systemUTC());
    }

    public UpstreamCircuitBreaker(int failureThreshold, long baseCooldownMillis, long maxCooldownMillis, Clock clock) {
        this.failureThreshold = failureThreshold;
        this.baseCooldownMillis = baseCooldownMillis;
        this.maxCooldownMillis = maxCooldownMillis;
        this.currentCooldownMillis = baseCooldownMillis;
        this.clock = clock;
    }

    /** Whether a call may be attempted right now (CLOSED or HALF_OPEN); false when OPEN and still
     * within the cooldown window. */
    public boolean allowRequest() {
        State s = state.get();
        if (s == State.CLOSED) {
            return true;
        }
        if (s == State.OPEN) {
            if (clock.instant().isAfter(openUntil.get())) {
                state.set(State.HALF_OPEN);
                return true;
            }
            return false;
        }
        return true; // HALF_OPEN: allow the single probe
    }

    public void recordSuccess() {
        consecutiveFailures.set(0);
        currentCooldownMillis = baseCooldownMillis;
        state.set(State.CLOSED);
    }

    public void recordFailure() {
        int failures = consecutiveFailures.incrementAndGet();
        if (state.get() == State.HALF_OPEN || failures >= failureThreshold) {
            state.set(State.OPEN);
            openUntil.set(clock.instant().plusMillis(currentCooldownMillis));
            currentCooldownMillis = Math.min(currentCooldownMillis * 2, maxCooldownMillis);
        }
    }

    public State state() {
        return state.get();
    }

    public String statusDescription() {
        return switch (state.get()) {
            case CLOSED -> "healthy";
            case HALF_OPEN -> "probing";
            case OPEN -> "circuit open until " + openUntil.get();
        };
    }
}
