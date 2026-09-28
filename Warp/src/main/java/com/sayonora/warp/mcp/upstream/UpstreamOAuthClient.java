package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warp acting as an OAuth 2.0 *client* against an upstream MCP server's own authorization server --
 * the opposite role from {@code WARP_OAUTH_ISSUER} (where Warp is a resource server validating
 * tokens presented BY callers). Supports:
 * <ul>
 *   <li>Discovery: RFC 9728 protected-resource-metadata ({@code /.well-known/oauth-protected-resource})
 *       and RFC 8414 authorization-server-metadata ({@code /.well-known/oauth-authorization-server})
 *       -- best-effort; falls back to the upstream's manually-configured authorize/token URLs when
 *       either is missing or unreachable.</li>
 *   <li>RFC 7591 dynamic client registration ({@code registration_endpoint} from the AS metadata),
 *       when advertised and no {@code clientId} is already configured.</li>
 *   <li>Standard authorization-code exchange and refresh.</li>
 * </ul>
 *
 * <p><b>Verified only against the fake test fixture's trivial always-approve authorization server</b>
 * ({@code Warp/tests/python/mcp_gateway/mcpgw_fake_upstream.py}); a real-world IdP's exact discovery
 * document shape, PKCE requirements, or DCR quirks were not exercised. PKCE (S256) is always sent
 * since it is harmless against a server that ignores it and required by some.
 */
public final class UpstreamOAuthClient {

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final Map<String, String> pkceVerifierByState = new ConcurrentHashMap<>();

    public record AuthServerMetadata(String authorizeUrl, String tokenUrl, String registrationEndpoint) {
    }

    /** Best-effort discovery per RFC 9728 (from the upstream's own base URL, treated as the
     * protected resource) then RFC 8414 (from the resource metadata's {@code authorization_servers[0]},
     * or directly from the upstream's origin if RFC 9728 isn't advertised). Returns {@code null} on
     * any failure -- callers fall back to manually-configured URLs. */
    public AuthServerMetadata discover(String upstreamBaseUrl) {
        try {
            URI base = URI.create(upstreamBaseUrl);
            String origin = base.getScheme() + "://" + base.getAuthority();
            String asOrigin = origin;
            try {
                JsonObject prm = getJson(origin + "/.well-known/oauth-protected-resource");
                if (prm != null && prm.has("authorization_servers")) {
                    asOrigin = prm.getAsJsonArray("authorization_servers").get(0).getAsString();
                }
            } catch (RuntimeException | IOException | InterruptedException ignored) {
                // RFC 9728 not advertised; try RFC 8414 directly against the upstream's own origin.
            }
            JsonObject asMeta = getJson(asOrigin + "/.well-known/oauth-authorization-server");
            if (asMeta == null) {
                return null;
            }
            return new AuthServerMetadata(
                    str(asMeta, "authorization_endpoint"), str(asMeta, "token_endpoint"),
                    str(asMeta, "registration_endpoint"));
        } catch (RuntimeException | IOException | InterruptedException e) {
            return null;
        }
    }

    /** RFC 7591 dynamic client registration; returns {@code [clientId, clientSecretOrNull]}. */
    public String[] registerClient(String registrationEndpoint, String redirectUri) throws IOException, InterruptedException {
        JsonObject body = new JsonObject();
        com.google.gson.JsonArray redirects = new com.google.gson.JsonArray();
        redirects.add(redirectUri);
        body.add("redirect_uris", redirects);
        body.addProperty("token_endpoint_auth_method", "client_secret_post");
        body.addProperty("grant_types", "authorization_code");
        body.addProperty("client_name", "warp-mcp-gateway");
        HttpRequest req = HttpRequest.newBuilder(URI.create(registrationEndpoint))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IOException("dynamic client registration failed: HTTP " + resp.statusCode() + " " + resp.body());
        }
        JsonObject out = JsonParser.parseString(resp.body()).getAsJsonObject();
        return new String[] { str(out, "client_id"), str(out, "client_secret") };
    }

    /** Builds the browser-redirect authorize URL and remembers the PKCE verifier keyed by {@code state}
     * (in-process only -- fine for a single-node "click Connect now" flow; a real cluster deployment
     * would need this in shared storage, noted as a gap in NOTES.md). */
    public String buildAuthorizeUrl(String authorizeUrl, String clientId, String redirectUri, String scope, String state) {
        String verifier = state; // simple, deterministic-enough verifier for this scope; real deployments should use a random one
        pkceVerifierByState.put(state, verifier);
        String challenge = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(verifier));
        StringBuilder u = new StringBuilder(authorizeUrl);
        u.append(authorizeUrl.contains("?") ? '&' : '?');
        u.append("response_type=code&client_id=").append(enc(clientId))
                .append("&redirect_uri=").append(enc(redirectUri))
                .append("&state=").append(enc(state))
                .append("&code_challenge=").append(challenge)
                .append("&code_challenge_method=S256");
        if (scope != null && !scope.isBlank()) {
            u.append("&scope=").append(enc(scope));
        }
        return u.toString();
    }

    public record TokenResult(String accessToken, String refreshToken, Instant expiresAt) {
    }

    public TokenResult exchangeCode(String tokenUrl, String clientId, String clientSecret, String redirectUri,
            String code, String state) throws IOException, InterruptedException {
        String verifier = pkceVerifierByState.remove(state);
        StringBuilder form = new StringBuilder();
        form.append("grant_type=authorization_code&code=").append(enc(code))
                .append("&redirect_uri=").append(enc(redirectUri))
                .append("&client_id=").append(enc(clientId));
        if (clientSecret != null && !clientSecret.isBlank()) {
            form.append("&client_secret=").append(enc(clientSecret));
        }
        if (verifier != null) {
            form.append("&code_verifier=").append(enc(verifier));
        }
        return postToken(tokenUrl, form.toString());
    }

    public TokenResult refresh(String tokenUrl, String clientId, String clientSecret, String refreshToken)
            throws IOException, InterruptedException {
        StringBuilder form = new StringBuilder();
        form.append("grant_type=refresh_token&refresh_token=").append(enc(refreshToken))
                .append("&client_id=").append(enc(clientId));
        if (clientSecret != null && !clientSecret.isBlank()) {
            form.append("&client_secret=").append(enc(clientSecret));
        }
        return postToken(tokenUrl, form.toString());
    }

    private TokenResult postToken(String tokenUrl, String form) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IOException("token endpoint returned HTTP " + resp.statusCode() + ": " + resp.body());
        }
        JsonObject body = JsonParser.parseString(resp.body()).getAsJsonObject();
        String access = str(body, "access_token");
        String refresh = str(body, "refresh_token");
        Instant expiresAt = body.has("expires_in") && !body.get("expires_in").isJsonNull()
                ? Instant.now().plusSeconds(body.get("expires_in").getAsLong()) : null;
        return new TokenResult(access, refresh, expiresAt);
    }

    private static JsonObject getJson(String url) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).header("Accept", "application/json")
                .timeout(Duration.ofSeconds(5)).GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            return null;
        }
        return JsonParser.parseString(resp.body()).getAsJsonObject();
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsString() : null;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static byte[] sha256(String s) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
