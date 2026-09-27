package com.sayonora.warp.mcp.upstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class NamespaceUtilTest {

    @Test
    void suggestPrefixLowercasesAndCollapsesNonAlnum() {
        assertEquals("acme_weather_api", NamespaceUtil.suggestPrefix("Acme Weather API"));
        assertEquals("upstream", NamespaceUtil.suggestPrefix(""));
        assertEquals("upstream", NamespaceUtil.suggestPrefix(null));
        assertEquals("upstream", NamespaceUtil.suggestPrefix("!!!"));
    }

    @Test
    void suggestPrefixCapsLength() {
        String longName = "a".repeat(50);
        assertTrue(NamespaceUtil.suggestPrefix(longName).length() <= 24);
    }

    @Test
    void validPrefixRules() {
        assertTrue(NamespaceUtil.isValidPrefix("acme"));
        assertTrue(NamespaceUtil.isValidPrefix("acme_weather"));
        assertFalse(NamespaceUtil.isValidPrefix("3acme")); // must start with a letter
        assertFalse(NamespaceUtil.isValidPrefix("Acme")); // must be lowercase
        assertFalse(NamespaceUtil.isValidPrefix(""));
        assertFalse(NamespaceUtil.isValidPrefix(null));
        assertFalse(NamespaceUtil.isValidPrefix("a".repeat(33))); // too long
    }

    @Test
    void uniquePrefixIsCaseInsensitive() {
        Set<String> existing = Set.of("acme", "weatherco");
        assertFalse(NamespaceUtil.isUniquePrefix("ACME", existing));
        assertFalse(NamespaceUtil.isUniquePrefix("acme", existing));
        assertTrue(NamespaceUtil.isUniquePrefix("other", existing));
    }

    @Test
    void sanitizeToolNameCollapsesIllegalChars() {
        assertEquals("get_weather", NamespaceUtil.sanitizeToolName("get-weather"));
        assertEquals("get_weather_now", NamespaceUtil.sanitizeToolName("get.weather now"));
        assertEquals("tool", NamespaceUtil.sanitizeToolName(""));
        assertEquals("tool", NamespaceUtil.sanitizeToolName(null));
        assertEquals("tool", NamespaceUtil.sanitizeToolName("!!!"));
    }

    @Test
    void namespacedToolNameJoinsWithUnderscore() {
        assertEquals("acme_get_weather", NamespaceUtil.namespacedToolName("acme", "get-weather"));
    }

    @Test
    void stripPrefixOnlyMatchesOwnPrefix() {
        assertEquals("get_weather", NamespaceUtil.stripPrefix("acme", "acme_get_weather"));
        assertEquals(null, NamespaceUtil.stripPrefix("other", "acme_get_weather"));
    }

    @Test
    void descriptionSuffixNamesTheUpstream() {
        assertTrue(NamespaceUtil.descriptionSuffix("Acme Weather").contains("Acme Weather"));
    }

    @Test
    void collisionAcrossTwoUpstreamsIsDetectedBeforeRegistration() {
        // Simulates the admin-API uniqueness check for two upstreams both suggesting "acme".
        String p1 = NamespaceUtil.suggestPrefix("Acme Corp");
        String p2 = NamespaceUtil.suggestPrefix("ACME Corp");
        assertEquals(p1, p2);
        assertFalse(NamespaceUtil.isUniquePrefix(p2, Set.of(p1)));
    }
}
