package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.Date;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Real proof that a caller's OWN signed token -- not just which endpoint/port it hit -- determines
 * its {@link McpScope}, per the gap raised directly: should a specific endpoint be authorized (and
 * scoped) by a token? Goes through the REAL {@code AccessContextResolver} JWT verification path (a
 * real RSA-signed token, a real local JWKS server Warp fetches from), not a shortcut -- since this
 * is exactly the security-critical path the whole feature exists for.
 */
class McpTokenScopeIntegrationTest {

    private static final String ISSUER = "https://test-issuer.example";

    private RealPostgres backendA;
    private RealPostgres backendB;
    private WarpProcess warp;
    private HttpServer jwksServer;
    private RSAPrivateKey privateKey;
    private String keyId;

    @AfterEach
    void stopInfra() {
        if (warp != null) warp.close();
        if (jwksServer != null) jwksServer.stop(0);
        if (backendB != null) backendB.close();
        if (backendA != null) backendA.close();
    }

    /** Serves a real JWKS document (one RSA public key) from a plain JDK HTTP server -- no new
     * test dependency, and it exercises Warp's real {@code WARP_OAUTH_JWKS_URI} fetch path exactly
     * as a real identity provider's endpoint would be hit. */
    private int startJwksServer(RSAKey rsaKey) throws Exception {
        jwksServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        String jwksJson = new com.nimbusds.jose.jwk.JWKSet(rsaKey.toPublicJWK()).toString();
        jwksServer.createContext("/jwks", exchange -> {
            byte[] body = jwksJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("content-type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        jwksServer.start();
        return jwksServer.getAddress().getPort();
    }

    private String signedToken(String warpScopeClaim) throws Exception {
        JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                .subject("test-agent")
                .issuer(ISSUER)
                .expirationTime(new Date(System.currentTimeMillis() + Duration.ofMinutes(5).toMillis()));
        if (warpScopeClaim != null) {
            claims.claim("warp_scope", warpScopeClaim);
        }
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(keyId).build(), claims.build());
        jwt.sign(new RSASSASigner(privateKey));
        return jwt.serialize();
    }

    private static HttpResponse<String> mcpCall(int mcpPort, String toolName, JsonObject arguments, String bearerToken) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "tools/call");
        req.add("params", params);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()));
        if (bearerToken != null) {
            builder.header("Authorization", "Bearer " + bearerToken);
        }
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aTokensOwnWarpScopeClaimOverridesTheEndpointsConfiguredDefault() throws Exception {
        backendA = RealPostgres.start();
        backendB = RealPostgres.start();
        try (Connection c = DriverManager.getConnection(backendA.jdbcUrl(), backendA.username(), backendA.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE default_only (id INTEGER PRIMARY KEY)");
        }
        try (Connection c = DriverManager.getConnection(backendB.jdbcUrl(), backendB.username(), backendB.password());
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE backend_b_only (id INTEGER PRIMARY KEY)");
        }

        java.security.KeyPairGenerator gen = java.security.KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        java.security.KeyPair pair = gen.generateKeyPair();
        privateKey = (RSAPrivateKey) pair.getPrivate();
        keyId = "test-key-1";
        RSAKey rsaKey = new RSAKey.Builder((RSAPublicKey) pair.getPublic())
                .privateKey(privateKey).keyID(keyId).build();
        int jwksPort = startJwksServer(rsaKey);

        String backends = "default=" + backendA.jdbcUrl() + "|" + backendA.username() + "|" + backendA.password()
                + ";backend_b=" + backendB.jdbcUrl() + "|" + backendB.username() + "|" + backendB.password();
        warp = WarpProcess.builder()
                .pgBackend(backendA.host(), backendA.port(), backendA.database(), backendA.username(), backendA.password())
                .frontend("mcp", "WARP_MCP_PORT")
                .env("WARP_BACKENDS", backends)
                // The endpoint's OWN default is "default" -- the token's claim below must override it.
                .env("WARP_MCP_SCOPE", "db:default")
                .env("WARP_ADMIN_TOKEN", "test-admin-token")
                .env("WARP_OAUTH_ISSUER", ISSUER)
                .env("WARP_OAUTH_JWKS_URI", "http://localhost:" + jwksPort + "/jwks")
                .env("WARP_TRUSTED_BACKEND_HOSTS", "localhost")
                .env("WARP_DYNAMOWIRE_CACHE_ENABLED", "false")
                .env("WARP_MONGOWIRE_CACHE_ENABLED", "false")
                .env("WARP_OTEL_ENDPOINT", "disabled")
                .start();
        int port = warp.port("mcp");

        // No token's own claim -- falls back to the endpoint's configured default (backendA).
        HttpResponse<String> noClaim = mcpCall(port, "inspect_schema", new JsonObject(), signedToken(null));
        assertTrue(noClaim.body().contains("default_only"), "no warp_scope claim must fall back to the endpoint's own default");
        assertFalse(noClaim.body().contains("backend_b_only"), "must not see backend_b without an overriding claim");

        // A token with its OWN warp_scope claim overrides the endpoint's default entirely.
        HttpResponse<String> withClaim = mcpCall(port, "inspect_schema", new JsonObject(), signedToken("db:backend_b"));
        assertTrue(withClaim.body().contains("backend_b_only"),
                "a token's own warp_scope claim must override the endpoint's configured default -- got: " + withClaim.body());
        assertFalse(withClaim.body().contains("default_only"),
                "the token-scoped caller must not see the endpoint's own default backend either -- got: " + withClaim.body());
    }
}
