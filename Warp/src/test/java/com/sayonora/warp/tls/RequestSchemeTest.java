package com.sayonora.warp.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RequestSchemeTest {

    @Test
    void connectorSchemeIsUsedByDefault() {
        assertEquals("https", RequestScheme.pick("https", null));
        assertEquals("http", RequestScheme.pick("http", null));
        assertEquals("http", RequestScheme.pick(null, ""));
    }

    @Test
    void forwardedProtoFromAProxyWins() {
        assertEquals("https", RequestScheme.pick("http", "https"));
        assertEquals("https", RequestScheme.pick("http", "HTTPS, http"));
        assertEquals("http", RequestScheme.pick("https", "http"));
        assertEquals("https", RequestScheme.pick("https", "gopher"));
    }
}
