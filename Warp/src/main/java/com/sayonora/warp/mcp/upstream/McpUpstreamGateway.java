package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ties together the pieces under {@code com.sayonora.warp.mcp.upstream} into the single object
 * {@link com.sayonora.warp.mcp.WarpMcpServer} talks to: holds the currently-loaded set of
 * {@link McpUpstream} registrations (kept in sync with the {@code mcpUpstreams} config field by
 * {@link #load}, called at startup and on every hot-reload -- see {@code Main.java}), a live
 * {@link UpstreamHttpClient} + {@link UpstreamCircuitBreaker} per upstream, and a short (60s)
 * tools/list cache per upstream so a busy endpoint doesn't re-discover on every {@code tools/list}.
 *
 * <p>Resilience: {@link #namespacedToolsFor} never lets one upstream's failure affect another's
 * tools or native tools on the same endpoint -- each upstream is discovered independently, guarded
 * by its own circuit breaker and a per-call timeout, and a failure is swallowed (logged into that
 * upstream's health, not thrown) so the merged {@code tools/list} always returns whatever DID
 * succeed.
 */
public final class McpUpstreamGateway {

    private static final Duration CACHE_TTL = Duration.ofSeconds(60);
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(
            Long.parseLong(System.getenv().getOrDefault("WARP_MCP_UPSTREAM_TIMEOUT_MS", "10000")));

    private final Clock clock;
    private final boolean readOnlyMode;
    private volatile Map<String, McpUpstream> byId = Map.of();
    private final Map<String, UpstreamHttpClient> clients = new ConcurrentHashMap<>();
    private final Map<String, UpstreamCircuitBreaker> breakers = new ConcurrentHashMap<>();
    private final Map<String, ToolsCacheEntry> toolsCache = new ConcurrentHashMap<>();

    public McpUpstreamGateway() {
        this(Clock.systemUTC(), false);
    }

    public McpUpstreamGateway(Clock clock) {
        this(clock, false);
    }

    /** {@code readOnlyMode}: mirrors {@code WARP_MCP_READ_ONLY} -- when {@code true}, an upstream
     * tool without an explicit {@code annotations.readOnlyHint: true} is hidden entirely from
     * {@link #namespacedToolsFor} (not just blocked on {@code tools/call}), matching how Warp's own
     * write-tagged native tools are hidden from {@code tools/list} under the same flag. */
    public McpUpstreamGateway(Clock clock, boolean readOnlyMode) {
        this.clock = clock;
        this.readOnlyMode = readOnlyMode;
    }

    private record ToolsCacheEntry(Instant fetchedAt, List<JsonObject> rawTools, String error) {
    }

    /** Reloads the upstream set from the persisted {@code mcpUpstreams} JSON (see
     * {@link McpUpstream#parse}). Called at startup and on every config hot-reload; upstreams whose
     * definition changed keep their circuit-breaker state but lose their cached tools/session (a
     * changed base URL, auth mode, etc. must not keep talking to the old target under stale
     * credentials). Removed upstreams are dropped entirely. */
    public void load(String mcpUpstreamsJson) {
        List<McpUpstream> parsed = McpUpstream.parse(mcpUpstreamsJson);
        Map<String, McpUpstream> next = new ConcurrentHashMap<>();
        for (McpUpstream u : parsed) {
            next.put(u.id(), u);
        }
        Map<String, McpUpstream> previous = byId;
        for (Map.Entry<String, McpUpstream> e : next.entrySet()) {
            McpUpstream was = previous.get(e.getKey());
            if (was == null || !definitionEquals(was, e.getValue())) {
                invalidate(e.getKey());
            }
        }
        for (String removedId : previous.keySet()) {
            if (!next.containsKey(removedId)) {
                invalidate(removedId);
                breakers.remove(removedId);
            }
        }
        byId = Map.copyOf(next);
    }

    private static boolean definitionEquals(McpUpstream a, McpUpstream b) {
        return a.baseUrl().equals(b.baseUrl()) && a.authMode() == b.authMode() && a.transport() == b.transport()
                && a.enabled() == b.enabled() && java.util.Objects.equals(a.bearerTokenEncrypted(), b.bearerTokenEncrypted())
                && java.util.Objects.equals(a.oauthAccessTokenEncrypted(), b.oauthAccessTokenEncrypted());
    }

    /** Drops the cached client/session/tools for one upstream, e.g. after its OAuth tokens were
     * refreshed or its definition changed, so the next call picks up the new credentials/target. */
    public void invalidate(String upstreamId) {
        clients.remove(upstreamId);
        toolsCache.remove(upstreamId);
    }

    public void refreshTools(String upstreamId) {
        toolsCache.remove(upstreamId);
    }

    public java.util.Optional<McpUpstream> get(String id) {
        return java.util.Optional.ofNullable(byId.get(id));
    }

    /** {@code POST /api/mcp-upstreams/{id}/test}: connect + initialize + tools/list ONLY, never
     * calling a tool, bypassing the cache and the circuit breaker (an explicit operator-triggered
     * test should always hit the network). */
    public TestResult test(McpUpstream u) {
        try {
            UpstreamHttpClient client = new UpstreamHttpClient(u, CALL_TIMEOUT);
            client.initialize();
            JsonArray tools = client.listTools();
            return new TestResult(true, null, tools.size());
        } catch (UpstreamAuthException e) {
            return new TestResult(false, "authentication failed: " + e.getMessage(), 0);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new TestResult(false, String.valueOf(e.getMessage()), 0);
        } catch (RuntimeException e) {
            return new TestResult(false, String.valueOf(e.getMessage()), 0);
        }
    }

    public record TestResult(boolean ok, String error, int toolCount) {
    }

    /** Merges the namespaced tool list from every upstream in {@code upstreamIds} (an endpoint's
     * included set). Each upstream is discovered independently with its own 60s cache and circuit
     * breaker; a disabled, circuit-open, or failing upstream is silently skipped here (its status is
     * visible via the admin API / {@link McpUpstream#healthStatus()}, not by breaking this list). */
    public List<JsonObject> namespacedToolsFor(List<String> upstreamIds) {
        List<JsonObject> out = new ArrayList<>();
        for (String id : upstreamIds) {
            McpUpstream u = byId.get(id);
            if (u == null || !u.enabled()) {
                continue;
            }
            for (JsonObject rawTool : toolsFor(u)) {
                if (readOnlyMode && !readOnlyHint(rawTool)) {
                    continue;
                }
                out.add(namespacedToolDef(u, rawTool));
            }
        }
        return out;
    }

    private List<JsonObject> toolsFor(McpUpstream u) {
        u = refreshIfNeeded(u);
        ToolsCacheEntry cached = toolsCache.get(u.id());
        if (cached != null && clock.instant().isBefore(cached.fetchedAt().plus(CACHE_TTL))) {
            return cached.rawTools();
        }
        UpstreamCircuitBreaker breaker = breakers.computeIfAbsent(u.id(), k -> new UpstreamCircuitBreaker());
        if (!breaker.allowRequest()) {
            return cached != null ? cached.rawTools() : List.of();
        }
        try {
            UpstreamHttpClient client = clientFor(u);
            client.initialize();
            JsonArray raw = client.listTools();
            List<JsonObject> tools = new ArrayList<>();
            for (var el : raw) {
                tools.add(el.getAsJsonObject());
            }
            breaker.recordSuccess();
            toolsCache.put(u.id(), new ToolsCacheEntry(clock.instant(), tools, null));
            return tools;
        } catch (RuntimeException | IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            breaker.recordFailure();
            toolsCache.put(u.id(), new ToolsCacheEntry(clock.instant(), cached != null ? cached.rawTools() : List.of(),
                    String.valueOf(e.getMessage())));
            return cached != null ? cached.rawTools() : List.of();
        }
    }

    private UpstreamHttpClient clientFor(McpUpstream u) {
        return clients.computeIfAbsent(u.id(), k -> new UpstreamHttpClient(u, CALL_TIMEOUT));
    }

    /**
     * Transparent OAuth access-token refresh: if {@code u} is OAUTH-authenticated and its token is
     * expired (or within the 30s skew), exchanges the refresh token for a new access token and
     * swaps the in-memory registration for the refreshed one (so {@link #clientFor} builds a fresh
     * client using the new token). <b>Known gap</b> (see NOTES.md): this updates the IN-MEMORY copy
     * only, not the persisted {@code mcpUpstreams} config row -- a config hot-reload or restart
     * before the admin API separately re-persists a refresh will see the old, expired token again
     * and simply refresh once more (self-healing, just not optimally persisted every time).
     */
    private McpUpstream refreshIfNeeded(McpUpstream u) {
        if (u.authMode() != McpUpstream.AuthMode.OAUTH || !u.oauthTokenExpired(clock.instant())) {
            return u;
        }
        String refreshToken = u.oauthRefreshTokenPlain();
        if (refreshToken == null || refreshToken.isBlank() || u.oauthTokenUrl() == null) {
            return u;
        }
        try {
            UpstreamOAuthClient oauth = new UpstreamOAuthClient();
            UpstreamOAuthClient.TokenResult tr = oauth.refresh(u.oauthTokenUrl(), u.oauthClientId(),
                    u.oauthClientSecretPlain(), refreshToken);
            if (tr.accessToken() == null) {
                return u;
            }
            McpUpstream updated = u.withOAuthTokens(
                    com.sayonora.warp.secrets.FieldCipher.encrypt(tr.accessToken()),
                    com.sayonora.warp.secrets.FieldCipher.encrypt(tr.refreshToken() != null ? tr.refreshToken() : refreshToken),
                    tr.expiresAt(), clock.instant());
            Map<String, McpUpstream> copy = new java.util.LinkedHashMap<>(byId);
            copy.put(u.id(), updated);
            byId = Map.copyOf(copy);
            clients.remove(u.id());
            return updated;
        } catch (IOException | InterruptedException | RuntimeException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return u; // fall through with the stale token; the call will 401 and surface as an auth error
        }
    }

    private static JsonObject namespacedToolDef(McpUpstream u, JsonObject rawTool) {
        JsonObject out = new JsonObject();
        String rawName = rawTool.has("name") ? rawTool.get("name").getAsString() : "tool";
        out.addProperty("name", NamespaceUtil.namespacedToolName(u.prefix(), rawName));
        String desc = rawTool.has("description") && !rawTool.get("description").isJsonNull()
                ? rawTool.get("description").getAsString() : "";
        out.addProperty("description", desc + NamespaceUtil.descriptionSuffix(u.name()));
        out.add("inputSchema", rawTool.has("inputSchema") ? rawTool.get("inputSchema") : emptyObjectSchema());
        if (rawTool.has("annotations")) {
            out.add("annotations", rawTool.get("annotations"));
        }
        return out;
    }

    private static JsonObject emptyObjectSchema() {
        JsonObject s = new JsonObject();
        s.addProperty("type", "object");
        s.add("properties", new JsonObject());
        return s;
    }

    /** Resolves a namespaced tool name (as advertised by {@link #namespacedToolsFor}) back to the
     * upstream and original tool name it came from, by matching {@code toolName} against each
     * candidate upstream's OWN (cached) tool list -- sanitization is lossy/non-invertible, so this
     * does not just strip the prefix and guess; see {@link NamespaceUtil#stripPrefix} javadoc.
     * Returns {@code null} when no included upstream's prefix+tool matches (native dispatch then
     * proceeds as before). */
    public CallTarget resolve(String toolName, List<String> upstreamIds) {
        for (String id : upstreamIds) {
            McpUpstream u = byId.get(id);
            if (u == null || !u.enabled()) {
                continue;
            }
            String suffix = NamespaceUtil.stripPrefix(u.prefix(), toolName);
            if (suffix == null) {
                continue;
            }
            for (JsonObject rawTool : toolsFor(u)) {
                String rawName = rawTool.has("name") ? rawTool.get("name").getAsString() : "tool";
                if (NamespaceUtil.sanitizeToolName(rawName).equals(suffix)) {
                    return new CallTarget(u, rawName, readOnlyHint(rawTool));
                }
            }
        }
        return null;
    }

    private static boolean readOnlyHint(JsonObject rawTool) {
        return rawTool.has("annotations") && rawTool.getAsJsonObject("annotations").has("readOnlyHint")
                && !rawTool.getAsJsonObject("annotations").get("readOnlyHint").isJsonNull()
                && rawTool.getAsJsonObject("annotations").get("readOnlyHint").getAsBoolean();
    }

    /** {@code upstream}: which upstream to call. {@code originalToolName}: the ORIGINAL
     * (upstream-side) tool name to send, not the namespaced one. {@code readOnlyHint}: {@code false}
     * unless the upstream's own tool definition explicitly says {@code annotations.readOnlyHint:
     * true} -- governance (WARP_MCP_READ_ONLY) treats an unknown-safety upstream tool as
     * write/mutating. */
    public record CallTarget(McpUpstream upstream, String originalToolName, boolean readOnlyHint) {
    }

    /** Forwards a {@code tools/call} to the resolved upstream over its own HTTP/session, applying
     * the per-call timeout and this upstream's circuit breaker. Throws {@link UpstreamRpcException}
     * for the upstream's own JSON-RPC error (relayed verbatim by the caller), {@link
     * UpstreamAuthException} for a 401/403, or {@link IOException}/{@link InterruptedException} for
     * a transport-level failure (timeout, connection refused, ...). */
    public JsonObject callTool(CallTarget target, JsonObject arguments) throws IOException, InterruptedException {
        McpUpstream u = refreshIfNeeded(target.upstream());
        UpstreamCircuitBreaker breaker = breakers.computeIfAbsent(u.id(), k -> new UpstreamCircuitBreaker());
        if (!breaker.allowRequest()) {
            throw new IOException("upstream \"" + u.name() + "\" is temporarily unavailable (circuit open: "
                    + breaker.statusDescription() + ")");
        }
        try {
            UpstreamHttpClient client = clientFor(u);
            JsonObject result = client.callTool(target.originalToolName(), arguments);
            breaker.recordSuccess();
            return result;
        } catch (UpstreamRpcException e) {
            // The upstream itself answered (just with a tool-level or protocol-level JSON-RPC
            // error) -- that's a working upstream, not a breaker failure.
            breaker.recordSuccess();
            throw e;
        } catch (IOException | InterruptedException e) {
            breaker.recordFailure();
            throw e;
        }
    }
}
