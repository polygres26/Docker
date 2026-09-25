package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Endpoint expiry/auth/scope-narrowing logic with an injected Clock (no sleeping). */
class McpEndpointsTest {

    /** A clock the test moves by hand. */
    static final class MutableClock extends Clock {
        Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private static McpEndpoints.Endpoint endpoint(String token, Instant expiresAt) {
        return new McpEndpoints.Endpoint("ep_1", "n", "db:pg2", "d", Instant.parse("2026-01-01T00:00:00Z"), "admin",
                expiresAt, McpEndpoints.hash(token));
    }

    private static McpEndpoints loaded(MutableClock clock, McpEndpoints.Endpoint e) {
        McpEndpoints eps = new McpEndpoints(clock);
        eps.load(McpEndpoints.serialize(List.of(e)));
        return eps;
    }

    @Test
    void expiryIsCheckedAgainstTheClockOnEveryRequest() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints eps = loaded(clock, endpoint("wmcp_tok", Instant.parse("2026-06-01T12:00:10Z")));
        String bearer = "Bearer wmcp_tok";
        assertTrue(eps.authenticate("/e/ep_1", bearer).ok());
        clock.advance(Duration.ofSeconds(9));
        assertTrue(eps.authenticate("/e/ep_1", bearer).ok());
        clock.advance(Duration.ofSeconds(1));                       // exactly at expiresAt: expired
        McpEndpoints.Auth a = eps.authenticate("/e/ep_1", bearer);
        assertFalse(a.ok());
        assertTrue(a.reason().contains("expired"));
        assertEquals(McpEndpoints.CLIENT_MESSAGE, McpEndpoints.CLIENT_MESSAGE);   // same text for every failure
    }

    @Test
    void neverExpiringEndpointOutlivesAnyClock() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints eps = loaded(clock, endpoint("wmcp_tok", null));
        clock.advance(Duration.ofDays(365 * 50));
        assertTrue(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
    }

    @Test
    void wrongOrMissingTokenAndUnknownIdAreRefused() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints eps = loaded(clock, endpoint("wmcp_tok", null));
        assertFalse(eps.authenticate("/e/ep_1", "Bearer wmcp_other").ok());
        assertFalse(eps.authenticate("/e/ep_1", null).ok());
        assertFalse(eps.authenticate("/e/ep_1", "Basic abc").ok());
        assertFalse(eps.authenticate("/e/ep_2", "Bearer wmcp_tok").ok());
        assertFalse(eps.authenticate("/", "Bearer wmcp_tok").ok());
    }

    @Test
    void revocationIsAReloadWithoutTheEndpoint() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints eps = loaded(clock, endpoint("wmcp_tok", null));
        assertTrue(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
        eps.load(null);
        assertFalse(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
    }

    @Test
    void malformedConfigKeepsThePreviousSet() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints eps = loaded(clock, endpoint("wmcp_tok", null));
        eps.load("{not json");
        assertTrue(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
    }

    @Test
    void extendingAnExpiredEndpointRevivesIt() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        McpEndpoints.Endpoint e = endpoint("wmcp_tok", Instant.parse("2026-06-01T12:00:05Z"));
        McpEndpoints eps = loaded(clock, e);
        clock.advance(Duration.ofSeconds(10));
        assertFalse(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
        eps.load(McpEndpoints.serialize(List.of(e.withExpiresAt(clock.instant().plusSeconds(60)))));
        assertTrue(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
        eps.load(McpEndpoints.serialize(List.of(e.withExpiresAt(null))));
        clock.advance(Duration.ofDays(3650));
        assertTrue(eps.authenticate("/e/ep_1", "Bearer wmcp_tok").ok());
    }

    @Test
    void expiryInputsAreParsedWithTimezonesAndValidated() {
        MutableClock clock = new MutableClock(Instant.parse("2026-06-01T12:00:00Z"));
        assertEquals(Instant.parse("2026-06-01T12:00:30Z"), McpEndpoints.resolveExpiry(null, 30L, clock).get());
        assertEquals(Instant.parse("2026-06-01T17:00:00Z"),
                McpEndpoints.resolveExpiry("2026-06-01T12:00:00-05:00", null, clock).get());
        assertTrue(McpEndpoints.resolveExpiry(null, null, clock).isEmpty());
        assertTrue(McpEndpoints.resolveExpiry("  ", null, clock).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry("2026-06-01T12:00:00Z", null, clock)); // not future
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry("2026-06-01T11:59:59Z", null, clock));
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry("2027-01-01T00:00:00", null, clock)); // no zone
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry("soon", null, clock));
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry("2027-01-01T00:00:00Z", 5L, clock));
        assertThrows(IllegalArgumentException.class, () -> McpEndpoints.resolveExpiry(null, 0L, clock));
    }

    @Test
    void anEndpointNarrowsTheListenerScopeButNeverWidensIt() {
        java.util.function.Function<String, List<String>> members = g -> g.equals("g") ? List.of("a", "b") : List.of();
        McpScope all = McpScope.all();
        assertEquals(McpScope.database("a"), McpEndpoints.narrow(all, McpScope.database("a"), members));
        assertEquals(McpScope.group("g"), McpEndpoints.narrow(all, McpScope.group("g"), members));
        assertEquals(McpScope.database("a"), McpEndpoints.narrow(McpScope.group("g"), McpScope.database("a"), members));
        assertNull(McpEndpoints.narrow(McpScope.group("g"), McpScope.database("z"), members));
        assertNull(McpEndpoints.narrow(McpScope.group("g"), McpScope.all(), members));
        assertNull(McpEndpoints.narrow(McpScope.database("a"), McpScope.database("b"), members));
        assertNull(McpEndpoints.narrow(McpScope.database("a"), McpScope.all(), members));
        assertNotNull(McpEndpoints.narrow(McpScope.database("a"), McpScope.database("a"), members));
    }

    @Test
    void tokensAreHighEntropyAndOnlyTheirHashIsStored() {
        String t1 = McpEndpoints.newToken();
        String t2 = McpEndpoints.newToken();
        assertTrue(t1.startsWith("wmcp_") && t1.length() > 40);
        assertFalse(t1.equals(t2));
        String stored = McpEndpoints.serialize(List.of(endpoint(t1, null)));
        assertFalse(stored.contains(t1));
        assertTrue(stored.contains(McpEndpoints.hash(t1)));
    }
}
