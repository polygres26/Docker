package com.nexagres.wire.http.auth;

import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nexagres.wire.core.AccessContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AccessContextResolver {

    private static final Logger log = LoggerFactory.getLogger(AccessContextResolver.class);

    public static final AccessContextResolver DISABLED = new AccessContextResolver(null, null, null, null);

    public sealed interface Result {
        record NoToken() implements Result {
        }

        record Valid(AccessContext accessContext) implements Result {
        }

        record Invalid(String reason) implements Result {
        }
    }

    private volatile String issuer;
    private volatile String audience;
    private volatile String userIdClaim;
    private volatile String rolesClaim;
    private volatile JWKSet jwkSet = new JWKSet();
    private volatile boolean jwksRefreshStarted;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private AccessContextResolver(String issuer, String audience, String userIdClaim, String rolesClaim) {
        this.issuer = issuer;
        this.audience = audience;
        this.userIdClaim = userIdClaim;
        this.rolesClaim = rolesClaim;
    }

    public static AccessContextResolver fromEnv() {
        String issuer = System.getenv("POLYWIRE_OAUTH_ISSUER");
        if (issuer == null || issuer.isBlank()) {
            return DISABLED;
        }
        AccessContextResolver resolver = new AccessContextResolver(null, null, null, null);
        resolver.reload(issuer, System.getenv("POLYWIRE_OAUTH_AUDIENCE"),
                System.getenv("POLYWIRE_OAUTH_USERID_CLAIM"), System.getenv("POLYWIRE_OAUTH_ROLES_CLAIM"));
        return resolver;
    }

    public static AccessContextResolver create(String issuer, String audience, String userIdClaim, String rolesClaim) {
        AccessContextResolver resolver = new AccessContextResolver(null, null, null, null);
        resolver.reload(issuer, audience, userIdClaim, rolesClaim);
        return resolver;
    }

    public synchronized void reload(String issuer, String audience, String userIdClaim, String rolesClaim) {
        this.issuer = (issuer == null || issuer.isBlank()) ? null : issuer;
        this.audience = audience;
        this.userIdClaim = userIdClaim == null || userIdClaim.isBlank() ? "sub" : userIdClaim;
        this.rolesClaim = rolesClaim == null || rolesClaim.isBlank() ? "roles" : rolesClaim;
        if (this.issuer == null) {
            log.info("OAuth: reloaded -- disabled (no issuer configured)");
            return;
        }
        refreshJwks();
        if (!jwksRefreshStarted) {
            jwksRefreshStarted = true;
            startJwksRefreshLoop();
        }
        log.info("OAuth: reloaded -- issuer={}, userIdClaim={}, rolesClaim={}, audience={}",
                this.issuer, this.userIdClaim, this.rolesClaim, audience == null ? "(not checked)" : audience);
    }

    private void startJwksRefreshLoop() {
        int refreshSeconds = parseIntEnv("POLYWIRE_OAUTH_JWKS_REFRESH_SECONDS", 300);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-oauth-jwks-refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::refreshJwks, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
    }

    private void refreshJwks() {
        String currentIssuer = issuer;
        if (currentIssuer == null) {
            return;
        }
        try {
            String jwksUriOverride = System.getenv("POLYWIRE_OAUTH_JWKS_URI");
            String jwksUri = jwksUriOverride != null && !jwksUriOverride.isBlank()
                    ? jwksUriOverride : discoverJwksUri(currentIssuer);
            jwkSet = fetchJwkSet(jwksUri);
            log.info("OAuth: refreshed JWKS from {} -- {} key(s)", jwksUri, jwkSet.getKeys().size());
        } catch (Exception e) {
            log.warn("OAuth: JWKS refresh failed, keeping previous key set ({} keys): {}",
                    jwkSet.getKeys().size(), e.getMessage());
        }
    }

    private String discoverJwksUri(String currentIssuer) throws Exception {
        String discoveryUrl = currentIssuer.endsWith("/") ? currentIssuer + ".well-known/openid-configuration"
                : currentIssuer + "/.well-known/openid-configuration";
        HttpRequest request = HttpRequest.newBuilder(URI.create(discoveryUrl)).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("discovery document fetch failed (HTTP " + response.statusCode() + "): " + discoveryUrl);
        }
        com.google.gson.JsonObject doc = com.google.gson.JsonParser.parseString(response.body()).getAsJsonObject();
        if (!doc.has("jwks_uri")) {
            throw new RuntimeException("discovery document at " + discoveryUrl + " has no jwks_uri");
        }
        return doc.get("jwks_uri").getAsString();
    }

    private JWKSet fetchJwkSet(String jwksUri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(jwksUri)).GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("JWKS fetch failed (HTTP " + response.statusCode() + "): " + jwksUri);
        }
        return JWKSet.parse(response.body());
    }

    public AccessContext enforce(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        Result result = resolve(request);
        if (result instanceof Result.Valid valid) {
            return valid.accessContext();
        }
        if (issuer == null) {
            return AccessContext.ANONYMOUS;
        }
        String reason = result instanceof Result.Invalid invalid ? invalid.reason() : "missing Authorization: Bearer token";
        log.warn("OAuth: rejecting request -- {}", reason);
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", "Bearer realm=\"polywire\", error=\"invalid_token\"");
        response.getWriter().write("unauthorized: " + reason);
        return null;
    }

    public Result resolve(HttpServletRequest request) {
        String currentIssuer = issuer;
        if (currentIssuer == null) {
            return new Result.Valid(AccessContext.ANONYMOUS);
        }
        String header = request.getHeader("Authorization");
        if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return new Result.NoToken();
        }
        String token = header.substring(7).trim();
        try {
            SignedJWT jwt = SignedJWT.parse(token);
            JWK key = jwkSet.getKeyByKeyId(jwt.getHeader().getKeyID());
            if (key == null) {
                return new Result.Invalid("no matching key for kid=" + jwt.getHeader().getKeyID()
                        + " in cached JWKS (stale cache, or token from an unexpected issuer)");
            }
            JWSVerifier verifier = new DefaultJWSVerifierFactory().createJWSVerifier(jwt.getHeader(), publicKeyMaterial(key));
            if (!jwt.verify(verifier)) {
                return new Result.Invalid("signature verification failed");
            }
            JWTClaimsSet claims = jwt.getJWTClaimsSet();
            Date now = new Date();
            if (claims.getExpirationTime() != null && claims.getExpirationTime().before(now)) {
                return new Result.Invalid("token expired");
            }
            if (claims.getIssuer() != null && !claims.getIssuer().equals(currentIssuer)) {
                return new Result.Invalid("issuer mismatch: expected " + currentIssuer + ", got " + claims.getIssuer());
            }
            String currentAudience = audience;
            if (currentAudience != null && !currentAudience.isBlank() && !claims.getAudience().contains(currentAudience)) {
                return new Result.Invalid("audience mismatch: expected " + currentAudience + " in " + claims.getAudience());
            }
            String userId = claims.getStringClaim(userIdClaim);
            Set<String> roles = extractRoles(claims);
            Map<String, String> attributes = Map.of();
            return new Result.Valid(new AccessContext(userId, roles, attributes));
        } catch (Exception e) {
            return new Result.Invalid("token parse/verify error: " + e.getMessage());
        }
    }

    private Set<String> extractRoles(JWTClaimsSet claims) {
        String claimName = rolesClaim;
        try {
            List<String> asList = claims.getStringListClaim(claimName);
            if (asList != null) {
                return new HashSet<>(asList);
            }
        } catch (Exception ignoredNotAStringList) {
            
        }
        String scalar = claims.getClaim(claimName) == null ? null : String.valueOf(claims.getClaim(claimName));
        return scalar == null ? Set.of() : new HashSet<>(Arrays.asList(scalar.split("\\s+")));
    }

    private static java.security.Key publicKeyMaterial(JWK key) throws Exception {
        if (key instanceof com.nimbusds.jose.jwk.RSAKey rsaKey) {
            return rsaKey.toRSAPublicKey();
        }
        if (key instanceof com.nimbusds.jose.jwk.ECKey ecKey) {
            return ecKey.toECPublicKey();
        }
        throw new IllegalArgumentException("unsupported JWK key type: " + key.getKeyType());
    }

    private static int parseIntEnv(String name, int defaultValue) {
        String raw = System.getenv(name);
        return raw == null || raw.isBlank() ? defaultValue : Integer.parseInt(raw.trim());
    }
}
