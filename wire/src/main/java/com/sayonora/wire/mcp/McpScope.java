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
