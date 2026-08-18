package com.polygres.wire.http.auth;

import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
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
 * <p>Config source: {@code POLYWIRE_OAUTH_ISSUER}/{@code _AUDIENCE}/{@code _USERID_CLAIM}/
 * {@code _ROLES_CLAIM} (bootstrap default) or {@code polywire_config.oauthIssuer}/etc.
 * (hot-reloadable -- see {@link #reload}, called from {@code Main}'s config-apply callback).
 * Unset issuer means every HTTP frontend behaves exactly as before this feature existed ({@link
 * #DISABLED}, every request resolves to {@link AccessContext#ANONYMOUS} with no token check at
 * all) -- see the class-level note on {@link #DISABLED} for why that constant is never itself
 * reloaded.
 *
 * <p><b>Claim mapping is configurable</b> (default {@code sub}/{@code roles}) since different
 * IdPs shape their tokens differently -- Okta's own default authorization-server groups claim is
 * commonly {@code groups}, not {@code roles}, for example; this project doesn't assume one shape.
 *
 * <p><b>JWKS caching</b>: fetched once when OAuth becomes enabled (construction or the first
 * {@link #reload} that sets a real issuer) and refreshed on a timer ({@code
 * POLYWIRE_OAUTH_JWKS_REFRESH_SECONDS}, default 300s) via the discovery document's own {@code
 * jwks_uri} -- same "cache locally, refresh periodically, never a live round-trip per request"
 * posture {@link com.polygres.wire.auth.PgRoleAuthCache} already established for role passwords.
 */
public final class AccessContextResolver {

    private static final Logger log = LoggerFactory.getLogger(AccessContextResolver.class);

    /** A plain default value ("OAuth not configured") -- never itself reloaded; see {@link com.polygres.wire.acl.ClientAcl}'s class javadoc for the identical reasoning. */
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

    // volatile, not final -- see ClientAcl's identical field javadoc for why: a reload (from
    // Main's polywire_config LISTEN callback) and a concurrent request's own read both need to see
    // consistent values, never a torn combination, without a lock on the hot path.
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

    /** Builds a real, independently-reloadable instance, even when {@code issuer} is null (OAuth starts disabled but can be enabled later via {@link #reload}) -- unlike {@link #fromEnv}, never returns the shared {@link #DISABLED} constant. */
    public static AccessContextResolver create(String issuer, String audience, String userIdClaim, String rolesClaim) {
        AccessContextResolver resolver = new AccessContextResolver(null, null, null, null);
        resolver.reload(issuer, audience, userIdClaim, rolesClaim);
        return resolver;
    }

    /**
     * Swaps in freshly-configured issuer/audience/claim-mapping; a blank/null {@code issuer}
     * disables OAuth on this instance again (every request goes back to {@link
     * AccessContext#ANONYMOUS}, no token check). Starts the background JWKS refresh loop the
     * first time a real issuer is set (idempotent -- a later reload with a different issuer just
     * changes what {@link #discoverJwksUri} resolves against on the loop's existing schedule,
     * plus this call always does one synchronous fetch immediately so a changed issuer takes
     * effect without waiting for the next scheduled tick).
     */
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
        String currentIssuer = issuer; // one volatile read
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

    /**
     * Convenience for the three HTTP frontends' {@code Handler.handle()}: resolves the caller and,
     * once OAuth is enabled, enforces it -- writes {@code 401} and returns {@code null} for a
     * missing or invalid token (enabling OAuth means requiring it, not silently falling back to
     * anonymous), otherwise returns the resolved {@link AccessContext} ({@link
     * AccessContext#ANONYMOUS} when OAuth is disabled, unchanged pre-OAuth behavior).
     */
    public AccessContext enforce(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
        Result result = resolve(request);
        if (result instanceof Result.Valid valid) {
            return valid.accessContext();
        }
        if (issuer == null) {
            return AccessContext.ANONYMOUS; // unreachable in practice (resolve() always returns Valid when disabled), kept for clarity
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
        String currentIssuer = issuer; // one volatile read for a consistent view across this call
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
            Map<String, String> attributes = Map.of(); // narrow-slice: only userId/roles mapped today, not arbitrary custom claims
            return new Result.Valid(new AccessContext(userId, roles, attributes));
        } catch (Exception e) {
            return new Result.Invalid("token parse/verify error: " + e.getMessage());
        }
    }

    private Set<String> extractRoles(JWTClaimsSet claims) {
        String claimName = rolesClaim; // one volatile read
        try {
            List<String> asList = claims.getStringListClaim(claimName);
            if (asList != null) {
                return new HashSet<>(asList);
            }
        } catch (Exception ignoredNotAStringList) {
            // fall through to the scalar case below (some IdPs send a single space-delimited "scope" string)
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
