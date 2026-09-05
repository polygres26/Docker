package com.sayonora.wire.mcp;

/**
 * Real, enforced access boundary for one MCP endpoint -- the answer to a real gap raised directly:
 * a {@code scope} ARGUMENT on {@code inspect_schema} only constrains what an agent chooses to ask
 * for; nothing stops it from calling {@code execute_sql} against a backend outside that scope
 * anyway. A boundary bound to the ENDPOINT itself (which port/connection an agent was even given)
 * is a real one -- the agent has no path to a backend it was never handed.
 *
 * <p>Declared via {@code WARP_MCP_SCOPE}: {@code db:<backendName>}, {@code group:<groupName>}, or
 * {@code all} (also the default when unset -- today's unscoped, argument-driven behavior).
 * Operationally: an operator who wants real isolation between teams/agents runs SEPARATE Warp MCP
 * listeners, each with its own {@code WARP_MCP_SCOPE}, rather than one flexible endpoint everyone
 * shares -- the same "one fixed setting per instance" shape {@code WARP_MYWIRE_BACKEND}'s own
 * native-mode toggle already uses, not a runtime-selectable option.
 *
 * <p><b>Real, disclosed limitation</b>: {@link Type#DATABASE} scope is fully enforced -- it forces
 * ALL execution (not just discovery) onto one direct connection, bypassing {@code RouterStage}'s
 * general routing entirely (see {@code WarpMcpServer#runSql}), so there is no path to any other
 * backend at all. {@link Type#GROUP} scope is enforced for auto-discovery ({@code query_federated}
 * and {@code inspect_schema}'s multi-backend listing) -- both only ever see the named group's
 * members. It does NOT constrain a plain {@code execute_sql}/{@code run_sql} call that happens to
 * be routed elsewhere by an operator's OWN {@code WARP_ROUTER_*} rule -- those still resolve via
 * the normal shared pipeline, which isn't scope-aware. Closing that gap needs real per-statement
 * enforcement threaded through {@code RoutingBackendExecutor} itself, a larger, separate piece of
 * work not built here.
 */
public record McpScope(Type type, String name) {

    public enum Type {
        DATABASE, GROUP, ALL
    }

    public static McpScope all() {
        return new McpScope(Type.ALL, null);
    }

    public static McpScope database(String backendName) {
        return new McpScope(Type.DATABASE, backendName);
    }

    public static McpScope group(String groupName) {
        return new McpScope(Type.GROUP, groupName);
    }

    public boolean isAll() {
        return type == Type.ALL;
    }

    public static McpScope fromEnv() {
        return fromSpec(System.getenv("WARP_MCP_SCOPE"));
    }

    /** {@code WARP_MCP_ROLE_SCOPES} grammar: {@code role1=spec1|role2=spec2}, {@code |}-separated
     * (same delimiter convention {@code WARP_BACKEND_GROUPS} uses), each {@code spec} itself
     * {@code db:<name>}/{@code group:<name>}/{@code all}. Lets a token's OWN role claim determine
     * its scope, rather than every caller being stuck with whatever the endpoint defaults to --
     * see {@link #resolveForCaller}. */
    public static java.util.Map<String, McpScope> roleScopesFromEnv() {
        return parseRoleScopes(System.getenv("WARP_MCP_ROLE_SCOPES"));
    }

    static java.util.Map<String, McpScope> parseRoleScopes(String spec) {
        if (spec == null || spec.isBlank()) {
            return java.util.Map.of();
        }
        java.util.Map<String, McpScope> result = new java.util.LinkedHashMap<>();
        for (String entry : spec.split("\\|")) {
            if (entry.isBlank()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("WARP_MCP_ROLE_SCOPES entry \"" + entry
                        + "\" is missing \"=\" -- expected role=db:<name>|group:<name>|all");
            }
            String role = entry.substring(0, eq).trim();
            McpScope roleScope = fromSpec(entry.substring(eq + 1).trim());
            result.put(role, roleScope);
        }
        return java.util.Map.copyOf(result);
    }

    /**
     * The real per-request scope for one authenticated caller -- checked in priority order: (1) a
     * direct {@code warp_scope} claim on the caller's OWN token (see {@code
     * AccessContextResolver}'s own extraction of it into {@code AccessContext.attributes()}) wins
     * outright, letting a token carry its scope regardless of which endpoint it happens to hit;
     * (2) failing that, {@code roleScopes} (an ordered map, insertion order preserved from {@code
     * WARP_MCP_ROLE_SCOPES}' own declaration order) is walked in THAT order, and the first entry
     * whose role the caller actually has wins -- deterministic and operator-controlled (declare
     * the higher-priority role mapping first), unlike iterating the caller's own {@code roles()}
     * (a {@link java.util.Set}, whose iteration order is never a real priority order); (3) failing both, {@code
     * fallback} -- the endpoint's own configured {@code WARP_MCP_SCOPE} (or {@link #all()} if that
     * was never set either). A malformed {@code warp_scope} claim value (fails {@link #fromSpec})
     * is treated as absent, falling through to (2)/(3), rather than failing the whole request.
     */
    public static McpScope resolveForCaller(com.sayonora.wire.core.AccessContext accessContext,
            java.util.Map<String, McpScope> roleScopes, McpScope fallback) {
        if (accessContext != null) {
            String claim = accessContext.attributes().get("warp_scope");
            if (claim != null && !claim.isBlank()) {
                try {
                    return fromSpec(claim);
                } catch (IllegalArgumentException ignoredMalformedClaim) {
                    // falls through to role-based/fallback resolution below
                }
            }
            for (java.util.Map.Entry<String, McpScope> entry : roleScopes.entrySet()) {
                if (accessContext.roles().contains(entry.getKey())) {
                    return entry.getValue();
                }
            }
        }
        return fallback;
    }

    /** {@code spec} grammar: {@code db:<backendName>}, {@code group:<groupName>}, {@code all}, or
     * {@code null}/blank (also "all" -- today's default, unscoped behavior). Anything else is a
     * real config error, thrown loudly at startup rather than silently falling back to unscoped
     * (a typo here should never silently widen an intended boundary). */
    public static McpScope fromSpec(String spec) {
        if (spec == null || spec.isBlank() || spec.trim().equalsIgnoreCase("all")) {
            return all();
        }
        String trimmed = spec.trim();
        int colon = trimmed.indexOf(':');
        if (colon <= 0 || colon == trimmed.length() - 1) {
            throw new IllegalArgumentException("WARP_MCP_SCOPE \"" + spec
                    + "\" is invalid -- expected \"db:<backendName>\", \"group:<groupName>\", or \"all\"");
        }
        String kind = trimmed.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT);
        String name = trimmed.substring(colon + 1).trim();
        return switch (kind) {
            case "db", "database" -> database(name);
            case "group" -> group(name);
            default -> throw new IllegalArgumentException("WARP_MCP_SCOPE \"" + spec
                    + "\" has an unknown kind \"" + kind + "\" -- expected \"db\", \"group\", or \"all\"");
        };
    }
}
