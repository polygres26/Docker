package com.sayonora.warp.mcp.upstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class McpUpstreamTest {

    private McpUpstream sample() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        return new McpUpstream("mup_1", "Acme", "acme", "https://acme.example/mcp",
                McpUpstream.Transport.STREAMABLE_HTTP, McpUpstream.AuthMode.BEARER, true, false,
                "plaintext-token-no-key-set", null, null, null, null, null, null, null, null,
                "unknown", null, 0, now, now);
    }

    @Test
    void serializeRoundTripsAllFields() {
        McpUpstream u = sample();
        String json = McpUpstream.serialize(List.of(u));
        List<McpUpstream> back = McpUpstream.parse(json);
        assertEquals(1, back.size());
        assertEquals(u.id(), back.get(0).id());
        assertEquals(u.prefix(), back.get(0).prefix());
        assertEquals(u.baseUrl(), back.get(0).baseUrl());
        assertEquals(u.authMode(), back.get(0).authMode());
    }

    @Test
    void viewNeverIncludesSecretValuesOnlyPresenceFlags() {
        McpUpstream u = sample();
        var view = u.view();
        assertFalse(view.toString().contains("plaintext-token-no-key-set"));
        assertTrue(view.get("hasBearerToken").getAsBoolean());
        assertFalse(view.has("bearerTokenEncrypted"));
        assertFalse(view.has("bearerToken"));
    }

    @Test
    void oauthTokenExpiredHonorsThirtySecondSkew() {
        McpUpstream u = sample();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        McpUpstream notYetExpired = u.withOAuthTokens("a", "r", now.plusSeconds(60), now);
        assertFalse(notYetExpired.oauthTokenExpired(now));
        McpUpstream withinSkew = u.withOAuthTokens("a", "r", now.plusSeconds(10), now);
        assertTrue(withinSkew.oauthTokenExpired(now));
        McpUpstream alreadyExpired = u.withOAuthTokens("a", "r", now.minusSeconds(5), now);
        assertTrue(alreadyExpired.oauthTokenExpired(now));
    }

    @Test
    void suggestPrefixDelegatesToNamespaceUtil() {
        assertEquals(NamespaceUtil.suggestPrefix("Acme Weather"), McpUpstream.suggestPrefix("Acme Weather"));
    }
}
