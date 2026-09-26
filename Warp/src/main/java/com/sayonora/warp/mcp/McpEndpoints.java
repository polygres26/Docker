package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * User-creatable MCP endpoints: a scoped, credentialed access handle served by the existing MCP
 * listener at {@code /e/<id>} with {@code Authorization: Bearer <token>}. Many endpoints coexist on
 * one listener (no port per endpoint); each has a {@link McpScope} (one backend, one backend group,
 * or all) that can only NARROW what the listener/caller scope already allows, an optional
 * description, and an optional {@code expiresAt} (null = never expires).
 *
 * <p>Persistence: the endpoints live as one JSON array in the {@code mcpEndpoints} field of the
 * versioned {@code warp_config} document, so they hot-reload on every Warp instance sharing that
 * config database through the same LISTEN/NOTIFY path as backends. Only a SHA-256 hash of the
 * token is stored; the token itself is returned exactly once, at creation.
 *
 * <p>Expiry is evaluated against the injected {@link Clock} on EVERY request ({@link #authenticate}),
 * never cached: an endpoint whose {@code expiresAt} has been reached serves nothing.
 */
public final class McpEndpoints {

    public static final String PATH_PREFIX = "/e/";
    public static final String TOKEN_PREFIX = "wmcp_";
    private static final SecureRandom RANDOM = new SecureRandom();

    /** One stored endpoint. {@code expiresAt == null} means it never expires. */
    public record Endpoint(String id, String name, String scope, String description, Instant createdAt,
            String createdBy, Instant expiresAt, String tokenHash) {

        public boolean expiredAt(Instant now) {
            return expiresAt != null && !now.isBefore(expiresAt);
        }

        public McpScope mcpScope() {
            return McpScope.fromSpec(scope);
        }

        public Endpoint withExpiresAt(Instant newExpiry) {
            return new Endpoint(id, name, scope, description, createdAt, createdBy, newExpiry, tokenHash);
        }

        public Endpoint withDescription(String d) {
            return new Endpoint(id, name, scope, d, createdAt, createdBy, expiresAt, tokenHash);
        }

        public Endpoint withName(String n) {
            return new Endpoint(id, n, scope, description, createdAt, createdBy, expiresAt, tokenHash);
        }
    }

    /** Outcome of {@link #authenticate}. Every failure carries the same client-facing message
     * (unknown id, wrong token, expired, revoked are indistinguishable to the caller); the precise
     * {@code reason} is for server logs only. */
    public record Auth(Endpoint endpoint, String reason) {
        public boolean ok() {
            return endpoint != null;
        }
    }

    public static final String CLIENT_MESSAGE = "MCP endpoint is not valid (unknown, expired or revoked) -- "
            + "check the endpoint URL and token";

    private final Clock clock;
    private volatile Map<String, Endpoint> byId = Map.of();

    public McpEndpoints() {
        this(Clock.systemUTC());
    }

    public McpEndpoints(Clock clock) {
        this.clock = clock;
    }

    public Clock clock() {
        return clock;
    }

    /** Replaces the live set from a {@code mcpEndpoints} config value (null/blank = none). A
     * malformed document keeps the previous set (fail closed on nothing new, never fail open). */
    public void load(String json) {
        try {
            Map<String, Endpoint> m = new LinkedHashMap<>();
            for (Endpoint e : parse(json)) {
                m.put(e.id(), e);
            }
            this.byId = Map.copyOf(m);
        } catch (RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(McpEndpoints.class)
                    .warn("mcpEndpoints config is malformed ({}); keeping the previous endpoint set", e.toString());
        }
    }

    public List<Endpoint> all() {
        return List.copyOf(byId.values());
    }

    public boolean isEmpty() {
        return byId.isEmpty();
    }

    /** Checks a request to {@code path} carrying {@code authorizationHeader}: the endpoint must
     * exist, the bearer token must hash to the stored hash (constant-time compare), and the endpoint
     * must not be expired at the clock's current instant. */
    public Auth authenticate(String path, String authorizationHeader) {
        String id = idFromPath(path);
        if (id == null) {
            return new Auth(null, "no endpoint id in path");
        }
        Endpoint e = byId.get(id);
        if (e == null) {
            return new Auth(null, "unknown or revoked endpoint " + id);
        }
        String token = bearer(authorizationHeader);
        if (token == null || !MessageDigest.isEqual(hash(token).getBytes(StandardCharsets.UTF_8),
                e.tokenHash().getBytes(StandardCharsets.UTF_8))) {
            return new Auth(null, "bad token for endpoint " + id);
        }
        if (e.expiredAt(clock.instant())) {
            return new Auth(null, "endpoint " + id + " expired at " + e.expiresAt());
        }
        return new Auth(e, null);
    }

    public static String idFromPath(String path) {
        if (path == null || !path.startsWith(PATH_PREFIX)) {
            return null;
        }
        String rest = path.substring(PATH_PREFIX.length());
        int slash = rest.indexOf('/');
        String id = slash < 0 ? rest : rest.substring(0, slash);
        return id.isBlank() ? null : id;
    }

    private static String bearer(String header) {
        if (header == null || header.length() < 8 || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return null;
        }
        String t = header.substring(7).trim();
        return t.isEmpty() ? null : t;
    }

    // ---- creation helpers (used by the admin API) ----

    public static String newToken() {
        byte[] b = new byte[32];
        RANDOM.nextBytes(b);
        return TOKEN_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    public static String newId() {
        byte[] b = new byte[9];
        RANDOM.nextBytes(b);
        return "ep_" + Base64.getUrlEncoder().withoutPadding().encodeToString(b).replace('-', 'x').replace('_', 'y');
    }

    public static String hash(String token) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte x : d) {
                sb.append(String.format("%02x", x));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Resolves the creation/update expiry inputs. Exactly one of {@code expiresAt} (ISO-8601 with
     * an explicit offset or {@code Z}) or {@code ttlSeconds} may be given; neither = never expires.
     * Unparseable, zone-less, or not-in-the-future values are rejected. */
    public static Optional<Instant> resolveExpiry(String expiresAt, Long ttlSeconds, Clock clock) {
        if (expiresAt != null && !expiresAt.isBlank() && ttlSeconds != null) {
            throw new IllegalArgumentException("give either expiresAt or ttlSeconds, not both");
        }
        Instant now = clock.instant();
        if (ttlSeconds != null) {
            if (ttlSeconds <= 0) {
                throw new IllegalArgumentException("ttlSeconds must be positive");
            }
            return Optional.of(now.plusSeconds(ttlSeconds));
        }
        if (expiresAt == null || expiresAt.isBlank()) {
            return Optional.empty();
        }
        Instant parsed;
        try {
            parsed = OffsetDateTime.parse(expiresAt.trim()).toInstant();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("expiresAt \"" + expiresAt + "\" is not an ISO-8601 date-time with a "
                    + "timezone offset (e.g. 2026-12-31T23:59:00Z or 2026-12-31T18:59:00-05:00)");
        }
        if (!parsed.isAfter(now)) {
            throw new IllegalArgumentException("expiresAt " + parsed + " is not in the future (server time "
                    + now + ")");
        }
        return Optional.of(parsed);
    }

    /** {@code requested} narrowed against {@code base} (the scope the listener/caller already
     * allows); {@code null} when {@code requested} would WIDEN it. */
    public static McpScope narrow(McpScope base, McpScope requested,
            java.util.function.Function<String, java.util.List<String>> groupMembers) {
        return switch (base.type()) {
            case ALL -> requested;
            case DATABASE -> requested.type() == McpScope.Type.DATABASE && requested.name().equals(base.name())
                    ? requested : null;
            case GROUP -> {
                if (requested.type() == McpScope.Type.GROUP && requested.name().equals(base.name())) {
                    yield requested;
                }
                if (requested.type() == McpScope.Type.DATABASE
                        && groupMembers.apply(base.name()).contains(requested.name())) {
                    yield requested;
                }
                yield null;
            }
        };
    }

    // ---- JSON ----

    public static List<Endpoint> parse(String json) {
        List<Endpoint> out = new ArrayList<>();
        if (json == null || json.isBlank()) {
            return out;
        }
        JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
        for (JsonElement el : arr) {
            JsonObject o = el.getAsJsonObject();
            out.add(new Endpoint(str(o, "id"), str(o, "name"), str(o, "scope"), str(o, "description"),
                    o.has("createdAt") && !o.get("createdAt").isJsonNull() ? Instant.parse(str(o, "createdAt")) : null,
                    str(o, "createdBy"),
                    o.has("expiresAt") && !o.get("expiresAt").isJsonNull() ? Instant.parse(str(o, "expiresAt")) : null,
                    str(o, "tokenHash")));
        }
        return out;
    }

    public static String serialize(List<Endpoint> endpoints) {
        JsonArray arr = new JsonArray();
        for (Endpoint e : endpoints) {
            JsonObject o = new JsonObject();
            o.addProperty("id", e.id());
            o.addProperty("name", e.name());
            o.addProperty("scope", e.scope());
            o.addProperty("description", e.description());
            o.addProperty("createdAt", e.createdAt() == null ? null : e.createdAt().toString());
            o.addProperty("createdBy", e.createdBy());
            o.addProperty("expiresAt", e.expiresAt() == null ? null : e.expiresAt().toString());
            o.addProperty("tokenHash", e.tokenHash());
            arr.add(o);
        }
        return arr.toString();
    }

    /** Admin-facing view: never includes the token hash. */
    public static JsonObject view(Endpoint e, Instant now) {
        JsonObject o = new JsonObject();
        o.addProperty("id", e.id());
        o.addProperty("name", e.name());
        o.addProperty("scope", e.scope());
        o.addProperty("description", e.description());
        o.addProperty("createdAt", e.createdAt() == null ? null : e.createdAt().toString());
        o.addProperty("createdBy", e.createdBy());
        o.addProperty("expiresAt", e.expiresAt() == null ? null : e.expiresAt().toString());
        o.addProperty("status", e.expiredAt(now) ? "expired" : "active");
        o.addProperty("path", PATH_PREFIX + e.id());
        return o;
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) && !o.get(k).isJsonNull() ? o.get(k).getAsString() : null;
    }
}
