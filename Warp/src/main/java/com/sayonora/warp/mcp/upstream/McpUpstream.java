package com.sayonora.warp.mcp.upstream;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.secrets.FieldCipher;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * A registered "other people's MCP server" upstream that Warp re-exposes, namespaced, through its
 * own governed MCP endpoints. This is a SEPARATE concept from {@link com.sayonora.warp.mcp.McpBackend}
 * (Warp's own SQL/cloud-store backends) and from Warp acting as an OAuth *resource server* for its
 * own MCP endpoints (see WARP_OAUTH_ISSUER / AccessContextResolver) -- here Warp is the OAuth
 * *client*, authenticating itself to somebody else's MCP server.
 *
 * <p>Persisted the same way as {@link com.sayonora.warp.mcp.McpEndpoints}: one JSON array in a
 * {@code mcpUpstreams} field of the versioned {@code warp_config} document, so every Warp instance
 * sharing that config DB hot-reloads a create/edit/delete through the same LISTEN/NOTIFY path used
 * for backends and endpoints. Secret fields ({@code bearerToken}, {@code clientSecret},
 * {@code accessToken}, {@code refreshToken}) are stored via {@link FieldCipher} (AES-256-GCM,
 * {@code SAYONORA_ENCRYPTION_KEY}) and are NEVER included in {@link #view}.
 */
public record McpUpstream(
        String id,
        String name,
        String prefix,
        String baseUrl,
        Transport transport,
        AuthMode authMode,
        boolean enabled,
        boolean insecureSkipVerify,
        String bearerTokenEncrypted,
        String oauthAuthorizeUrl,
        String oauthTokenUrl,
        String oauthClientId,
        String oauthClientSecretEncrypted,
        String oauthScope,
        String oauthAccessTokenEncrypted,
        String oauthRefreshTokenEncrypted,
        Instant oauthTokenExpiresAt,
        String healthStatus,
        String lastError,
        int discoveredToolCount,
        Instant createdAt,
        Instant updatedAt) {

    public enum Transport { STREAMABLE_HTTP, HTTP_SSE }

    public enum AuthMode { NONE, BEARER, OAUTH }

    private static final SecureRandom RANDOM = new SecureRandom();

    public static String newId() {
        byte[] b = new byte[9];
        RANDOM.nextBytes(b);
        return "mup_" + Base64.getUrlEncoder().withoutPadding().encodeToString(b).replace('-', 'x').replace('_', 'y');
    }

    /** Suggests a short lowercase[a-z0-9_] prefix from a human-given upstream name, e.g. "Acme Weather API" -&gt;
     * "acme_weather". Truncated to 24 chars; the operator can always override it (uniqueness is enforced by
     * the caller, per-endpoint's upstream set, not here). */
    public static String suggestPrefix(String name) {
        return NamespaceUtil.suggestPrefix(name);
    }

    public McpUpstream withHealth(String status, String error, int toolCount, Instant now) {
        return new McpUpstream(id, name, prefix, baseUrl, transport, authMode, enabled, insecureSkipVerify,
                bearerTokenEncrypted, oauthAuthorizeUrl, oauthTokenUrl, oauthClientId, oauthClientSecretEncrypted,
                oauthScope, oauthAccessTokenEncrypted, oauthRefreshTokenEncrypted, oauthTokenExpiresAt,
                status, error, toolCount, createdAt, now);
    }

    public McpUpstream withOAuthTokens(String accessTokenEncrypted, String refreshTokenEncrypted,
            Instant expiresAt, Instant now) {
        return new McpUpstream(id, name, prefix, baseUrl, transport, authMode, enabled, insecureSkipVerify,
                bearerTokenEncrypted, oauthAuthorizeUrl, oauthTokenUrl, oauthClientId, oauthClientSecretEncrypted,
                oauthScope, accessTokenEncrypted, refreshTokenEncrypted, expiresAt, healthStatus, lastError,
                discoveredToolCount, createdAt, now);
    }

    public McpUpstream withEnabled(boolean e, Instant now) {
        return new McpUpstream(id, name, prefix, baseUrl, transport, authMode, e, insecureSkipVerify,
                bearerTokenEncrypted, oauthAuthorizeUrl, oauthTokenUrl, oauthClientId, oauthClientSecretEncrypted,
                oauthScope, oauthAccessTokenEncrypted, oauthRefreshTokenEncrypted, oauthTokenExpiresAt,
                healthStatus, lastError, discoveredToolCount, createdAt, now);
    }

    /** Decrypted bearer token (BEARER mode) for outbound calls only -- never returned to the admin API. */
    public String bearerTokenPlain() {
        return FieldCipher.decrypt(bearerTokenEncrypted);
    }

    public String oauthClientSecretPlain() {
        return FieldCipher.decrypt(oauthClientSecretEncrypted);
    }

    public String oauthAccessTokenPlain() {
        return FieldCipher.decrypt(oauthAccessTokenEncrypted);
    }

    public String oauthRefreshTokenPlain() {
        return FieldCipher.decrypt(oauthRefreshTokenEncrypted);
    }

    public boolean oauthTokenExpired(Instant now) {
        return oauthTokenExpiresAt != null && !now.isBefore(oauthTokenExpiresAt.minusSeconds(30));
    }

    // ---- JSON persistence (mcpUpstreams config field: a JSON array) ----

    public static List<McpUpstream> parse(String json) {
        List<McpUpstream> out = new ArrayList<>();
        if (json == null || json.isBlank()) {
            return out;
        }
        JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
        for (JsonElement el : arr) {
            JsonObject o = el.getAsJsonObject();
            out.add(new McpUpstream(
                    str(o, "id"), str(o, "name"), str(o, "prefix"), str(o, "baseUrl"),
                    Transport.valueOf(str(o, "transport") == null ? "STREAMABLE_HTTP" : str(o, "transport")),
                    AuthMode.valueOf(str(o, "authMode") == null ? "NONE" : str(o, "authMode")),
                    bool(o, "enabled", true), bool(o, "insecureSkipVerify", false),
                    str(o, "bearerTokenEncrypted"),
                    str(o, "oauthAuthorizeUrl"), str(o, "oauthTokenUrl"), str(o, "oauthClientId"),
                    str(o, "oauthClientSecretEncrypted"), str(o, "oauthScope"),
                    str(o, "oauthAccessTokenEncrypted"), str(o, "oauthRefreshTokenEncrypted"),
                    instant(o, "oauthTokenExpiresAt"),
                    str(o, "healthStatus"), str(o, "lastError"),
                    o.has("discoveredToolCount") && !o.get("discoveredToolCount").isJsonNull()
                            ? o.get("discoveredToolCount").getAsInt() : 0,
                    instant(o, "createdAt"), instant(o, "updatedAt")));
        }
        return out;
    }

    public static String serialize(List<McpUpstream> all) {
        JsonArray arr = new JsonArray();
        for (McpUpstream u : all) {
            JsonObject o = new JsonObject();
            o.addProperty("id", u.id());
            o.addProperty("name", u.name());
            o.addProperty("prefix", u.prefix());
            o.addProperty("baseUrl", u.baseUrl());
            o.addProperty("transport", u.transport().name());
            o.addProperty("authMode", u.authMode().name());
            o.addProperty("enabled", u.enabled());
            o.addProperty("insecureSkipVerify", u.insecureSkipVerify());
            o.addProperty("bearerTokenEncrypted", u.bearerTokenEncrypted());
            o.addProperty("oauthAuthorizeUrl", u.oauthAuthorizeUrl());
            o.addProperty("oauthTokenUrl", u.oauthTokenUrl());
            o.addProperty("oauthClientId", u.oauthClientId());
            o.addProperty("oauthClientSecretEncrypted", u.oauthClientSecretEncrypted());
            o.addProperty("oauthScope", u.oauthScope());
            o.addProperty("oauthAccessTokenEncrypted", u.oauthAccessTokenEncrypted());
            o.addProperty("oauthRefreshTokenEncrypted", u.oauthRefreshTokenEncrypted());
            o.addProperty("oauthTokenExpiresAt", u.oauthTokenExpiresAt() == null ? null : u.oauthTokenExpiresAt().toString());
            o.addProperty("healthStatus", u.healthStatus());
            o.addProperty("lastError", u.lastError());
            o.addProperty("discoveredToolCount", u.discoveredToolCount());
            o.addProperty("createdAt", u.createdAt() == null ? null : u.createdAt().toString());
            o.addProperty("updatedAt", u.updatedAt() == null ? null : u.updatedAt().toString());
            arr.add(o);
        }
        return arr.toString();
    }

    /** Admin-facing view: NEVER includes bearerTokenEncrypted / oauthClientSecretEncrypted /
     * oauthAccessTokenEncrypted / oauthRefreshTokenEncrypted, only whether each is set. */
    public JsonObject view() {
        JsonObject o = new JsonObject();
        o.addProperty("id", id);
        o.addProperty("name", name);
        o.addProperty("prefix", prefix);
        o.addProperty("baseUrl", baseUrl);
        o.addProperty("transport", transport.name());
        o.addProperty("authMode", authMode.name());
        o.addProperty("enabled", enabled);
        o.addProperty("insecureSkipVerify", insecureSkipVerify);
        o.addProperty("hasBearerToken", bearerTokenEncrypted != null && !bearerTokenEncrypted.isBlank());
        o.addProperty("oauthAuthorizeUrl", oauthAuthorizeUrl);
        o.addProperty("oauthTokenUrl", oauthTokenUrl);
        o.addProperty("oauthClientId", oauthClientId);
        o.addProperty("hasOauthClientSecret", oauthClientSecretEncrypted != null && !oauthClientSecretEncrypted.isBlank());
        o.addProperty("oauthScope", oauthScope);
        o.addProperty("oauthConnected", oauthAccessTokenEncrypted != null && !oauthAccessTokenEncrypted.isBlank());
        o.addProperty("oauthTokenExpiresAt", oauthTokenExpiresAt == null ? null : oauthTokenExpiresAt.toString());
        o.addProperty("healthStatus", healthStatus);
        o.addProperty("lastError", lastError);
        o.addProperty("discoveredToolCount", discoveredToolCount);
        o.addProperty("createdAt", createdAt == null ? null : createdAt.toString());
        o.addProperty("updatedAt", updatedAt == null ? null : updatedAt.toString());
        return o;
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsString() : null;
    }

    private static boolean bool(JsonObject o, String k, boolean def) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsBoolean() : def;
    }

    private static Instant instant(JsonObject o, String k) {
        String s = str(o, k);
        return s == null ? null : Instant.parse(s);
    }
}
