package com.sayonora.warp.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/** URL-token path parsing, redaction and the opt-in / revoke / expiry semantics of header-less auth. */
class McpEndpointUrlTokenTest {

    @Test
    void parsesTheTokenSegmentOnly() {
        assertEquals("wmcp_abc", McpEndpoints.urlTokenFromPath("/e/ep_1/t/wmcp_abc"));
        assertEquals("wmcp_abc", McpEndpoints.urlTokenFromPath("/e/ep_1/t/wmcp_abc/"));
        assertEquals("wmcp_abc", McpEndpoints.urlTokenFromPath("/e/ep_1/t/wmcp_abc/anything"));
        assertNull(McpEndpoints.urlTokenFromPath("/e/ep_1"));
        assertNull(McpEndpoints.urlTokenFromPath("/e/ep_1/"));
        assertNull(McpEndpoints.urlTokenFromPath("/e/ep_1/t/"));
        assertNull(McpEndpoints.urlTokenFromPath("/e/ep_1/kinds/x"));
        assertNull(McpEndpoints.urlTokenFromPath("/kinds/t/x"));
        assertNull(McpEndpoints.urlTokenFromPath(null));
        assertEquals("ep_1", McpEndpoints.idFromPath("/e/ep_1/t/wmcp_abc"));
    }

    @Test
    void redactionRemovesTheTokenFromAnyLoggedPath() {
        assertEquals("/e/ep_1/t/***", McpEndpoints.redactPath("/e/ep_1/t/wmcp_secret"));
        assertEquals("/e/ep_1/t/***/x", McpEndpoints.redactPath("/e/ep_1/t/wmcp_secret/x"));
        assertEquals("/e/ep_1", McpEndpoints.redactPath("/e/ep_1"));
        assertFalse(McpEndpoints.redactPath("/e/ep_1/t/wmcp_secret").contains("secret"));
    }

    private static McpEndpoints.Endpoint ep(String id, String token, boolean urlToken, Instant expires) {
        return new McpEndpoints.Endpoint(id, "n-" + id, "all", null, Instant.parse("2026-01-01T00:00:00Z"), "t", expires,
                McpEndpoints.hash(token), urlToken);
    }

    @Test
    void urlTokenIsRejectedUnlessTheEndpointOptedIn() {
        McpEndpoints eps = new McpEndpoints(Clock.fixed(Instant.parse("2026-06-01T00:00:00Z"), ZoneOffset.UTC));
        eps.load(McpEndpoints.serialize(List.of(ep("off", "wmcp_a", false, null), ep("on", "wmcp_b", true, null))));
        assertFalse(eps.authenticate("/e/off/t/wmcp_a", null).ok());
        assertFalse(eps.authenticate("/e/off/t/wmcp_a", "Bearer wmcp_a").ok(), "a header must not rescue a disabled URL token");
        assertTrue(eps.authenticate("/e/off", "Bearer wmcp_a").ok(), "header mode is unchanged");
        assertTrue(eps.authenticate("/e/on/t/wmcp_b", null).ok());
        assertFalse(eps.authenticate("/e/on/t/wmcp_WRONG", null).ok());
        assertFalse(eps.authenticate("/e/on/t/wmcp_a", null).ok(), "another endpoint's token");
        assertFalse(eps.authenticate("/e/on", null).ok());
        assertTrue(eps.authenticate("/e/on", "Bearer wmcp_b").ok());
    }

    @Test
    void urlTokenExpiresAndRevokesLikeTheHeaderToken() {
        Instant now = Instant.parse("2026-06-01T00:00:00Z");
        McpEndpoints live = new McpEndpoints(Clock.fixed(now, ZoneOffset.UTC));
        McpEndpoints.Endpoint e = ep("x", "wmcp_x", true, now.plusSeconds(60));
        live.load(McpEndpoints.serialize(List.of(e)));
        assertTrue(live.authenticate("/e/x/t/wmcp_x", null).ok());
        McpEndpoints later = new McpEndpoints(Clock.fixed(now.plusSeconds(61), ZoneOffset.UTC));
        later.load(McpEndpoints.serialize(List.of(e)));
        assertFalse(later.authenticate("/e/x/t/wmcp_x", null).ok());
        live.load(McpEndpoints.serialize(List.of()));
        assertFalse(live.authenticate("/e/x/t/wmcp_x", null).ok(), "revoked");
    }

    @Test
    void refusalReasonNeverContainsTheToken() {
        McpEndpoints eps = new McpEndpoints();
        eps.load(McpEndpoints.serialize(List.of(ep("on", "wmcp_b", true, null))));
        assertFalse(eps.authenticate("/e/on/t/wmcp_SECRETVALUE", null).reason().contains("SECRETVALUE"));
        assertFalse(eps.authenticate("/e/nope/t/wmcp_SECRETVALUE", null).reason().contains("SECRETVALUE"));
    }

    @Test
    void urlTokenFlagRoundTripsAndOldDocumentsDefaultToOff() {
        String json = McpEndpoints.serialize(List.of(ep("a", "t", true, null), ep("b", "t", false, null)));
        List<McpEndpoints.Endpoint> back = McpEndpoints.parse(json);
        assertTrue(back.get(0).urlToken());
        assertFalse(back.get(1).urlToken());
        String legacy = "[{\"id\":\"old\",\"name\":\"o\",\"scope\":\"all\",\"tokenHash\":\"h\"}]";
        assertFalse(McpEndpoints.parse(legacy).get(0).urlToken());
        assertTrue(McpEndpoints.view(back.get(0), Instant.now()).get("urlToken").getAsBoolean());
    }
}
