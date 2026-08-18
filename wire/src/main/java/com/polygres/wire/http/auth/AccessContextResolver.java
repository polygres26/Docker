package com.polygres.wire.http.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.polygres.wire.core.AccessContext;
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

/**
 * OAuth2/OIDC bearer-token authentication for every HTTP frontend ({@code MetricsServer},
 * {@code DynamoWireServer}, {@code PolyWireMcpServer}) -- the class {@link
 * com.polygres.wire.core.AccessContext}, {@link com.polygres.wire.core.Statement}, and {@link
 * com.polygres.wire.core.AdHocQueryRunner} already named in their own javadoc as how a frontend
 * that authenticates end users attaches identity, before this pass actually built it (same
 * "already scoped, never implemented" pattern as {@code PolyWireMcpServer} itself).
 *
 * <p>Standard OAuth2 resource-server posture, the same shape Spring Security's {@code
 * oauth2ResourceServer} or Envoy's/Kong's JWT filters implement -- nothing Okta-specific. Any
 * standards-compliant OIDC provider works identically (Okta, Auth0, Azure AD/Entra ID, Google
 * Identity, Keycloak, AWS Cognito, ...) since they all publish the same {@code
 * .well-known/openid-configuration} discovery document and issue standard signed JWTs.
 *
 * <p>Opt-in via {@code POLYWIRE_OAUTH_ISSUER} -- unset means every HTTP frontend behaves exactly
 * as before this feature existed ({@link #DISABLED}, every request resolves to {@link
 * AccessContext#ANONYMOUS} with no token check at all).
 *
 * <p><b>Claim mapping is configurable</b> ({@code POLYWIRE_OAUTH_USERID_CLAIM} default {@code sub},
 * {@code POLYWIRE_OAUTH_ROLES_CLAIM} default {@code roles}) since different IdPs shape their
 * tokens differently -- Okta's own default authorization-server groups claim is commonly
 * {@code groups}, not {@code roles}, for example; this project doesn't assume one shape.
 *
 * <p><b>JWKS caching</b>: fetched once at startup and refreshed on a timer ({@code
 * POLYWIRE_OAUTH_JWKS_REFRESH_SECONDS}, default 300s) via the discovery document's own {@code
 * jwks_uri} -- same "cache locally, refresh periodically, never a live round-trip per request"
 * posture {@link com.polygres.wire.auth.PgRoleAuthCache} already established for role passwords.
 */
public final class AccessContextResolver {

    private static final Logger log = LoggerFactory.getLogger(AccessContextResolver.class);

    public static final AccessContextResolver DISABLED = new AccessContextResolver(null, null, null, null);

    /** Outcome of resolving one request's bearer token. */
    public sealed interface Result {
        record NoToken() implements Result {
        }

        record Valid(AccessContext accessContext) implements Result {
        }

        record Invalid(String reason) implements Result {
        }
    }

    private final String issuer;
    private final String audience;
    private final String userIdClaim;
    private final String rolesClaim;
    private volatile JWKSet jwkSet = new JWKSet();
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
        String audience = System.getenv("POLYWIRE_OAUTH_AUDIENCE");
        String userIdClaim = System.getenv().getOrDefault("POLYWIRE_OAUTH_USERID_CLAIM", "sub");
        String rolesClaim = System.getenv().getOrDefault("POLYWIRE_OAUTH_ROLES_CLAIM", "roles");
        AccessContextResolver resolver = new AccessContextResolver(issuer, audience, userIdClaim, rolesClaim);
        String jwksUri = System.getenv("POLYWIRE_OAUTH_JWKS_URI");
        resolver.startJwksRefresh(jwksUri);
        log.info("OAuth: POLYWIRE_OAUTH_ISSUER={} -- HTTP frontends now require a valid bearer token "
                + "(userIdClaim={}, rolesClaim={}, audience={})", issuer, userIdClaim, rolesClaim,
                audience == null ? "(not checked)" : audience);
        return resolver;
    }

    private void startJwksRefresh(String explicitJwksUri) {
        Runnable refresh = () -> {
            try {
                String jwksUri = explicitJwksUri != null && !explicitJwksUri.isBlank()
                        ? explicitJwksUri : discoverJwksUri();
                jwkSet = fetchJwkSet(jwksUri);
                log.info("OAuth: refreshed JWKS from {} -- {} key(s)", jwksUri, jwkSet.getKeys().size());
            } catch (Exception e) {
                log.warn("OAuth: JWKS refresh failed, keeping previous key set ({} keys): {}",
                        jwkSet.getKeys().size(), e.getMessage());
            }
        };
        refresh.run(); // populate synchronously before accepting any requests
        int refreshSeconds = parseIntEnv("POLYWIRE_OAUTH_JWKS_REFRESH_SECONDS", 300);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "polywire-oauth-jwks-refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(refresh, refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
    }

    private String discoverJwksUri() throws Exception {
        String discoveryUrl = issuer.endsWith("/") ? issuer + ".well-known/openid-configuration"
                : issuer + "/.well-known/openid-configuration";
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

    /**
     * Convenience for the three HTTP frontends' {@code Handler.handle()}: resolves the caller and,
     * once OAuth is enabled ({@code this != DISABLED}), enforces it -- writes {@code 401} and
     * returns {@code null} for a missing or invalid token (enabling OAuth means requiring it, not
     * silently falling back to anonymous), otherwise returns the resolved {@link AccessContext}
     * ({@link AccessContext#ANONYMOUS} when {@code this == DISABLED}, unchanged pre-OAuth
     * behavior).
     */
    public AccessContext enforce(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        Result result = resolve(request);
        if (result instanceof Result.Valid valid) {
            return valid.accessContext();
        }
        if (this == DISABLED) {
            return AccessContext.ANONYMOUS; // unreachable in practice (resolve() always returns Valid when DISABLED), kept for clarity
        }
        String reason = result instanceof Result.Invalid invalid ? invalid.reason() : "missing Authorization: Bearer token";
        log.warn("OAuth: rejecting request -- {}", reason);
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", "Bearer realm=\"polywire\", error=\"invalid_token\"");
        response.getWriter().write("unauthorized: " + reason);
        return null;
    }

    /** Resolves the caller's {@link AccessContext} from a request's {@code Authorization: Bearer <token>} header. */
    public Result resolve(HttpServletRequest request) {
        if (this == DISABLED) {
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
            if (claims.getIssuer() != null && !claims.getIssuer().equals(issuer)) {
                return new Result.Invalid("issuer mismatch: expected " + issuer + ", got " + claims.getIssuer());
            }
            if (audience != null && !audience.isBlank() && !claims.getAudience().contains(audience)) {
                return new Result.Invalid("audience mismatch: expected " + audience + " in " + claims.getAudience());
            }
            String userId = claims.getStringClaim(userIdClaim);
            Set<String> roles = extractRoles(claims);
            Map<String, String> attributes = Map.of(); // narrow-slice: only userId/roles mapped today, not arbitrary custom claims
            return new Result.Valid(new AccessContext(userId, roles, attributes));
        } catch (Exception e) {
            return new Result.Invalid("token parse/verify error: " + e.getMessage());
        }
    }

    private Set<String> extractRoles(JWTClaimsSet claims) {
        try {
            List<String> asList = claims.getStringListClaim(rolesClaim);
            if (asList != null) {
                return new HashSet<>(asList);
            }
        } catch (Exception ignoredNotAStringList) {
            // fall through to the scalar case below (some IdPs send a single space-delimited "scope" string)
        }
        String scalar = claims.getClaim(rolesClaim) == null ? null : String.valueOf(claims.getClaim(rolesClaim));
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
