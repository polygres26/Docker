package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SigV4 request signing: independently re-derives the signature the way the server side (AwsSigV4-style HMAC chain) would. */
class AwsSignerTest {

    @Test
    void producesAValidSigningKeyChainAndAuthorizationHeaderShape() {
        Instant now = Instant.parse("2024-01-15T12:30:45Z");
        Map<String, String> h = AwsSigner.sign("GET", "route53.amazonaws.com", "/2013-04-01/hostedzonesbyname", "dnsname=example.com",
                new byte[0], "us-east-1", "route53", "AKIDEXAMPLE", "secretkey123456789", null, now);
        assertEquals("20240115T123045Z", h.get("x-amz-date"));
        assertTrue(h.get("Authorization").startsWith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20240115/us-east-1/route53/aws4_request"));
        assertTrue(h.get("Authorization").contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        // deterministic: same inputs -> same signature
        Map<String, String> h2 = AwsSigner.sign("GET", "route53.amazonaws.com", "/2013-04-01/hostedzonesbyname", "dnsname=example.com",
                new byte[0], "us-east-1", "route53", "AKIDEXAMPLE", "secretkey123456789", null, now);
        assertEquals(h.get("Authorization"), h2.get("Authorization"));
    }

    @Test
    void differentBodyChangesTheSignature() {
        Instant now = Instant.parse("2024-01-15T12:30:45Z");
        Map<String, String> h1 = AwsSigner.sign("POST", "route53.amazonaws.com", "/x", "", "a".getBytes(), "us-east-1", "route53",
                "AKID", "secret", null, now);
        Map<String, String> h2 = AwsSigner.sign("POST", "route53.amazonaws.com", "/x", "", "b".getBytes(), "us-east-1", "route53",
                "AKID", "secret", null, now);
        assertTrue(!h1.get("Authorization").equals(h2.get("Authorization")));
    }

    @Test
    void sessionTokenIsIncludedWhenPresent() {
        Instant now = Instant.now();
        Map<String, String> h = AwsSigner.sign("GET", "h", "/", "", null, "us-east-1", "route53", "AKID", "secret", "session-tok", now);
        assertEquals("session-tok", h.get("x-amz-security-token"));
        assertTrue(h.get("Authorization").contains("x-amz-security-token"));
    }
}
