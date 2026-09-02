package com.sayonora.dms.http.auth;

import com.sayonora.dms.core.DmsLicensing;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SSO-style login: verify a bearer JWT instead of a username/password pair, so an operator can
 * front the admin console with whatever identity provider they already run (Okta, Auth0, an
 * internal IdP) rather than provisioning a Sayonora-specific password for every admin.
 *
 * <p>Deliberately scoped to HS256 (a shared signing secret, {@code SAYONORA_SSO_JWT_SECRET}) for
 * this pass, not a full OIDC client with RS256/JWKS discovery -- a real, independently-verifiable
 * signature check (HMAC-SHA256 over header.payload, constant-time compared, {@code exp} and
 * {@code iss} both checked), just narrower in scope than a production IdP integration. Most
 * identity providers can mint an HS256 token for a service integration even when their end-user
 * login flow is RS256; a full JWKS-based RS256 client is the natural next step if a specific
 * customer's IdP requires it, not built here. This is explicit in the class's own name and this
 * javadoc, not silently implied to be more than it is.
 *
 * <p>Enterprise-only, refuse-to-construct (see {@link DmsLicensing#requireEnterpriseForSso()}) --
 * unlike the viewer-account RBAC gate in {@link AdminAuth}, which degrades rather than throws,
 * SSO is an explicit alternate login path a caller opts into (constructing this class at all), so
 * refusing outright at that point is the right response, the same reasoning {@code
 * DistributedCoordinator} and {@code CutoverReadinessChecker} are built around.
 */
public final class SsoAuth {

    private static final Logger log = LoggerFactory.getLogger(SsoAuth.class);

    private final byte[] secret;
    private final String expectedIssuer;

    /**
     * @param secret the shared HS256 signing secret, raw bytes (not base64) -- typically {@code
     *     System.getenv("SAYONORA_SSO_JWT_SECRET").getBytes(StandardCharsets.UTF_8)}
     * @param expectedIssuer the required {@code iss} claim, or {@code null} to skip that check
     * @throws IllegalStateException if the current process isn't Enterprise-licensed
     */
    public SsoAuth(byte[] secret, String expectedIssuer) {
        DmsLicensing.requireEnterpriseForSso();
        if (secret == null || secret.length == 0) {
            throw new IllegalArgumentException("SSO signing secret must not be empty");
        }
        this.secret = secret;
        this.expectedIssuer = expectedIssuer;
    }

    /** The verified token's {@code sub} claim (the identity to attribute the resulting session
     * to), or empty if the token is missing, malformed, has a bad signature, is expired, or fails
     * the issuer check. Never throws on a bad token -- an invalid bearer token is exactly as
     * ordinary as a wrong password, not an exceptional condition. */
    public java.util.Optional<String> verify(String bearerToken) {
        if (bearerToken == null || bearerToken.isBlank()) {
            return java.util.Optional.empty();
        }
        String[] parts = bearerToken.split("\\.");
        if (parts.length != 3) {
            return java.util.Optional.empty();
        }
        try {
            byte[] expectedSig = hmacSha256(secret, parts[0] + "." + parts[1]);
            byte[] actualSig = Base64.getUrlDecoder().decode(pad(parts[2]));
            if (!constantTimeEquals(expectedSig, actualSig)) {
                return java.util.Optional.empty();
            }
            Map<?, ?> payload = new com.google.gson.Gson().fromJson(
                    new String(Base64.getUrlDecoder().decode(pad(parts[1])), StandardCharsets.UTF_8), Map.class);
            Object exp = payload.get("exp");
            if (exp instanceof Number expNumber && Instant.ofEpochSecond(expNumber.longValue()).isBefore(Instant.now())) {
                return java.util.Optional.empty();
            }
            if (expectedIssuer != null && !expectedIssuer.equals(payload.get("iss"))) {
                return java.util.Optional.empty();
            }
            Object sub = payload.get("sub");
            return sub == null ? java.util.Optional.empty() : java.util.Optional.of(sub.toString());
        } catch (RuntimeException e) {
            log.debug("SSO bearer token rejected -- malformed", e);
            return java.util.Optional.empty();
        }
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    private static String pad(String base64Url) {
        int rem = base64Url.length() % 4;
        return rem == 0 ? base64Url : base64Url + "====".substring(rem);
    }

    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) return false;
        int result = 0;
        for (int i = 0; i < a.length; i++) {
            result |= a[i] ^ b[i];
        }
        return result == 0;
    }
}
