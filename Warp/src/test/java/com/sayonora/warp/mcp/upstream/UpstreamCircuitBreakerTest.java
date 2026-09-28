package com.sayonora.warp.mcp.upstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class UpstreamCircuitBreakerTest {

    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @Test
    void staysClosedUnderThreshold() {
        UpstreamCircuitBreaker b = new UpstreamCircuitBreaker(3, 1000, 5000, Clock.systemUTC());
        b.recordFailure();
        b.recordFailure();
        assertEquals(UpstreamCircuitBreaker.State.CLOSED, b.state());
        assertTrue(b.allowRequest());
    }

    @Test
    void opensAtThresholdAndBlocksUntilCooldown() {
        MutableClock clock = new MutableClock();
        UpstreamCircuitBreaker b = new UpstreamCircuitBreaker(3, 1000, 5000, clock);
        b.recordFailure();
        b.recordFailure();
        b.recordFailure();
        assertEquals(UpstreamCircuitBreaker.State.OPEN, b.state());
        assertFalse(b.allowRequest());
        clock.now = clock.now.plusMillis(1001);
        assertTrue(b.allowRequest()); // half-open probe allowed
        assertEquals(UpstreamCircuitBreaker.State.HALF_OPEN, b.state());
    }

    @Test
    void successInHalfOpenCloses() {
        MutableClock clock = new MutableClock();
        UpstreamCircuitBreaker b = new UpstreamCircuitBreaker(2, 1000, 5000, clock);
        b.recordFailure();
        b.recordFailure();
        clock.now = clock.now.plusMillis(1001);
        assertTrue(b.allowRequest());
        b.recordSuccess();
        assertEquals(UpstreamCircuitBreaker.State.CLOSED, b.state());
        assertTrue(b.allowRequest());
    }

    @Test
    void failureInHalfOpenReopensWithLongerCooldown() {
        MutableClock clock = new MutableClock();
        UpstreamCircuitBreaker b = new UpstreamCircuitBreaker(2, 1000, 60000, clock);
        b.recordFailure();
        b.recordFailure();
        clock.now = clock.now.plusMillis(1001);
        assertTrue(b.allowRequest()); // -> HALF_OPEN
        b.recordFailure();
        assertEquals(UpstreamCircuitBreaker.State.OPEN, b.state());
        // cooldown doubled to 2000ms: 1500ms later still blocked
        clock.now = clock.now.plusMillis(1500);
        assertFalse(b.allowRequest());
        clock.now = clock.now.plusMillis(600);
        assertTrue(b.allowRequest());
    }
}
