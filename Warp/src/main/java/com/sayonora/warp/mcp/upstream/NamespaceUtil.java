package com.sayonora.warp.mcp.upstream;

import java.util.Set;

/**
 * Prefix suggestion, sanitization and namespacing for upstream MCP tools ("namespaced passthrough"
 * aggregation): an upstream registered with prefix {@code p} has its tool {@code t} advertised
 * through Warp as {@code p_t}, sanitized to the MCP tool-name charset ({@code [A-Za-z0-9_-]}, we
 * use lowercase letters/digits/underscore only for names Warp itself generates).
 */
public final class NamespaceUtil {

    private NamespaceUtil() {
    }

    /** Suggests a short prefix from an upstream's display name: lowercased, non [a-z0-9] runs
     * collapsed to a single underscore, trimmed of leading/trailing underscores, capped at 24 chars,
     * and never empty (falls back to "upstream"). */
    public static String suggestPrefix(String name) {
        if (name == null) {
            return "upstream";
        }
        String s = name.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
        if (s.isEmpty()) {
            return "upstream";
        }
        return s.length() > 24 ? s.substring(0, 24).replaceAll("_+$", "") : s;
    }

    /** True iff {@code prefix} is a legal, non-reserved namespace prefix: 1-32 chars, [a-z0-9_],
     * must start with a letter (never a digit, to keep the resulting tool name unambiguous). */
    public static boolean isValidPrefix(String prefix) {
        return prefix != null && prefix.matches("[a-z][a-z0-9_]{0,31}");
    }

    /** Rejects a prefix that collides (case-insensitively) with one already used by another
     * upstream in the same set (e.g. the same MCP endpoint's included upstreams). */
    public static boolean isUniquePrefix(String prefix, Set<String> existingPrefixesLowercase) {
        return prefix != null && !existingPrefixesLowercase.contains(prefix.toLowerCase(java.util.Locale.ROOT));
    }

    /** Sanitizes an upstream's own tool name to the charset Warp's own tool names use
     * ({@code [a-z0-9_]}), so a vendor tool with e.g. dots/dashes/spaces still forms a legal
     * namespaced name. Collapses illegal runs to a single underscore. */
    public static String sanitizeToolName(String rawName) {
        if (rawName == null || rawName.isBlank()) {
            return "tool";
        }
        String s = rawName.trim().replaceAll("[^A-Za-z0-9_]+", "_").replaceAll("^_+|_+$", "");
        return s.isEmpty() ? "tool" : s;
    }

    /** The full namespaced tool name Warp advertises for an upstream tool, e.g. prefix "acme",
     * upstream tool "get-weather" -&gt; "acme_get_weather". */
    public static String namespacedToolName(String prefix, String upstreamToolName) {
        return prefix + "_" + sanitizeToolName(upstreamToolName);
    }

    /** Splits a namespaced tool name against a known prefix, returning the original (sanitized)
     * upstream-side suffix, or {@code null} if {@code fullName} does not start with
     * {@code prefix + "_"}. Note: since sanitization is lossy/non-invertible, the upstream is
     * looked up by matching its OWN tools' sanitized names against this suffix (see
     * McpUpstreamGateway#resolveCall), not by literally reversing this string. */
    public static String stripPrefix(String prefix, String fullName) {
        String marker = prefix + "_";
        return fullName != null && fullName.startsWith(marker) ? fullName.substring(marker.length()) : null;
    }

    /** Short suffix appended to a namespaced tool's description noting the real upstream, e.g.
     * " (via MCP upstream \"Acme Weather\")". */
    public static String descriptionSuffix(String upstreamName) {
        return " (via MCP upstream \"" + upstreamName + "\")";
    }
}
