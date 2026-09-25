package com.sayonora.dms.http.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.dms.core.DmsLicensingTestSupport;
import com.sayonora.wire.license.LicenseTier;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SsoAuthTest {

    private static final byte[] SECRET = "test-shared-secret-32-bytes-long!".getBytes(StandardCharsets.UTF_8);

    @AfterEach
    void resetOverride() {
        DmsLicensingTestSupport.reset();
    }

    @Test
    void freeTierRefusesToConstruct() {
        DmsLicensingTestSupport.forceTier(LicenseTier.DEVELOPER);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new SsoAuth(SECRET, null));
        assertTrue(e.getMessage().contains("WARP_LICENSE_KEY"));
    }

    @Test
    void enterpriseVerifiesAGenuineTokenAndRejectsATamperedOne() {
        DmsLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        SsoAuth sso = new SsoAuth(SECRET, "my-issuer");

        String good = mintToken(SECRET, "alice@example.com", "my-issuer", Instant.now().plusSeconds(3600));
        assertEquals("alice@example.com", sso.verify(good).orElseThrow());

        // Tamper with the payload -- signature no longer matches.
        String[] parts = good.split("\\.");
        String tampered = parts[0] + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"sub\":\"mallory@example.com\"}".getBytes(StandardCharsets.UTF_8)) + "." + parts[2];
        assertTrue(sso.verify(tampered).isEmpty());
    }

    @Test
    void enterpriseRejectsExpiredAndWrongIssuerTokens() {
        DmsLicensingTestSupport.forceTier(LicenseTier.ENTERPRISE);
        SsoAuth sso = new SsoAuth(SECRET, "my-issuer");

        String expired = mintToken(SECRET, "alice@example.com", "my-issuer", Instant.now().minusSeconds(60));
        assertTrue(sso.verify(expired).isEmpty());

        String wrongIssuer = mintToken(SECRET, "alice@example.com", "someone-else", Instant.now().plusSeconds(3600));
        assertTrue(sso.verify(wrongIssuer).isEmpty());

        assertTrue(sso.verify(null).isEmpty());
        assertTrue(sso.verify("not-a-jwt").isEmpty());
    }

    private static String mintToken(byte[] secret, String subject, String issuer, Instant expiry) {
        String header = base64Url("{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        String payload = base64Url(String.format(
                "{\"sub\":\"%s\",\"iss\":\"%s\",\"exp\":%d}", subject, issuer, expiry.getEpochSecond()));
        String signingInput = header + "." + payload;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            byte[] sig = mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
            return signingInput + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (java.security.GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static String base64Url(String json) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}
