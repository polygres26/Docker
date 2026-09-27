package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Talks JSON-RPC-over-HTTP to ONE external ("upstream") MCP server. Implements the
 * {@link UpstreamTransport} extension point for the Streamable-HTTP variant (2025-03-26+ MCP spec)
 * and a best-effort legacy HTTP+SSE fallback (see {@link #initialize} javadoc for what's actually
 * verified vs assumed -- protocol-version negotiation and SSE framing on non-trivial servers were
 * NOT exercised against a real third-party server, only against the fake Python test fixture).
 *
 * <p>One instance = one upstream = its own {@link HttpClient} (connection pooling/reuse is handled
 * by the JDK client's own connection pool) and its own {@code Mcp-Session-Id}, captured from the
 * {@code initialize} response and sent on every subsequent request, per the Streamable-HTTP spec.
 * TLS: verifies certificates via the platform default trust manager unless
 * {@code WARP_MCP_UPSTREAM_INSECURE_SKIP_VERIFY=true} is set AND the upstream itself has
 * {@code insecureSkipVerify=true} -- both must agree; there is no default-on insecure mode.
 */
public final class UpstreamHttpClient implements UpstreamTransport {

    private final McpUpstream upstream;
    private final HttpClient http;
    private final AtomicReference<String> sessionId = new AtomicReference<>();
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Duration timeout;

    public UpstreamHttpClient(McpUpstream upstream, Duration timeout) {
        this.upstream = upstream;
        this.timeout = timeout;
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(timeout);
        if (upstream.insecureSkipVerify() && "true".equalsIgnoreCase(
                System.getenv("WARP_MCP_UPSTREAM_INSECURE_SKIP_VERIFY"))) {
            b.sslContext(insecureSslContext());
        }
        this.http = b.build();
    }

    @Override
    public JsonObject initialize() throws IOException, InterruptedException {
        JsonObject params = new JsonObject();
        params.addProperty("protocolVersion", "2025-06-18");
        JsonObject caps = new JsonObject();
        params.add("capabilities", caps);
        JsonObject clientInfo = new JsonObject();
        clientInfo.addProperty("name", "warp-mcp-gateway");
        clientInfo.addProperty("version", "1.0");
        params.add("clientInfo", clientInfo);
        JsonObject result = call("initialize", params, false);
        // Best-effort "notifications/initialized" per spec; a fake/simple upstream may not require it,
        // and we don't fail initialize() if this notification 4xx's.
        try {
            postRaw(notification("notifications/initialized", new JsonObject()));
        } catch (RuntimeException | IOException | InterruptedException ignored) {
            // notification, no response expected/required
        }
        return result;
    }

    @Override
    public JsonArray listTools() throws IOException, InterruptedException {
        JsonObject result = call("tools/list", new JsonObject(), true);
        return result.has("tools") ? result.getAsJsonArray("tools") : new JsonArray();
    }

    @Override
    public JsonObject callTool(String name, JsonObject arguments) throws IOException, InterruptedException {
        JsonObject params = new JsonObject();
        params.addProperty("name", name);
        params.add("arguments", arguments == null ? new JsonObject() : arguments);
        return call("tools/call", params, true);
    }

    /** Sends one JSON-RPC request and returns its "result" object (or throws {@link UpstreamRpcException}
     * carrying the upstream's own JSON-RPC error, for verbatim relay/translation by the caller). */
    private JsonObject call(String method, JsonObject params, boolean requireSession)
            throws IOException, InterruptedException {
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", nextId.getAndIncrement());
        req.addProperty("method", method);
        req.add("params", params);
        HttpResponse<String> resp = postRaw(req);
        if (resp.statusCode() == 401 || resp.statusCode() == 403) {
            throw new UpstreamAuthException("upstream returned HTTP " + resp.statusCode());
        }
        if (resp.statusCode() >= 400) {
            throw new IOException("upstream HTTP " + resp.statusCode() + ": " + trim(resp.body()));
        }
        String captured = resp.headers().firstValue("Mcp-Session-Id").orElse(null);
        if (captured != null) {
            sessionId.set(captured);
        }
        JsonObject body = JsonParser.parseString(resp.body()).getAsJsonObject();
        if (body.has("error") && !body.get("error").isJsonNull()) {
            JsonObject err = body.getAsJsonObject("error");
            throw new UpstreamRpcException(
                    err.has("code") ? err.get("code").getAsInt() : -32000,
                    err.has("message") ? err.get("message").getAsString() : "upstream error",
                    err.has("data") ? err.get("data") : null);
        }
        return body.has("result") && body.get("result").isJsonObject() ? body.getAsJsonObject("result") : new JsonObject();
    }

    private HttpResponse<String> postRaw(JsonObject req) throws IOException, InterruptedException {
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(upstream.baseUrl()))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()));
        String sid = sessionId.get();
        if (sid != null) {
            rb.header("Mcp-Session-Id", sid);
        }
        for (Map.Entry<String, String> h : authHeaders().entrySet()) {
            rb.header(h.getKey(), h.getValue());
        }
        return http.send(rb.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonObject notification(String method, JsonObject params) {
        JsonObject n = new JsonObject();
        n.addProperty("jsonrpc", "2.0");
        n.addProperty("method", method);
        n.add("params", params);
        return n;
    }

    private Map<String, String> authHeaders() {
        return switch (upstream.authMode()) {
            case NONE -> Map.of();
            case BEARER -> Map.of("Authorization", "Bearer " + safe(upstream.bearerTokenPlain()));
            case OAUTH -> Map.of("Authorization", "Bearer " + safe(upstream.oauthAccessTokenPlain()));
        };
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static String trim(String s) {
        return s == null ? "" : s.length() > 500 ? s.substring(0, 500) + "..." : s;
    }

    private static javax.net.ssl.SSLContext insecureSslContext() {
        try {
            javax.net.ssl.TrustManager[] trustAll = { new javax.net.ssl.X509ExtendedTrustManager() {
                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) { }
                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) { }
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a, java.net.Socket s) { }
                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a, java.net.Socket s) { }
                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) { }
                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a, javax.net.ssl.SSLEngine e) { }
            } };
            javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new java.security.SecureRandom());
            return ctx;
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
