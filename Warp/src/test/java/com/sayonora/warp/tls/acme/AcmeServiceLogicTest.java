package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Pure decision logic: needsIssuance() and backoff() (order-state / retry-policy correctness, no network or process). */
class AcmeServiceLogicTest {

    private static CertInfo cert(List<String> domains, Instant notAfter, boolean placeholder) {
        KeyPair k = AcmeCrypto.newEcKey();
        // notBefore way in the past keeps a wide notAfter reachable via the placeholder generator's fixed 30-day window
        // for the "placeholder" case; for the non-placeholder case we build directly through a real X509 leaf.
        if (placeholder) {
            byte[] der = AcmeCrypto.placeholderCertificate(k, domains, notAfter.minus(Duration.ofDays(30)));
            try {
                X509Certificate c = (X509Certificate) java.security.cert.CertificateFactory.getInstance("X.509")
                        .generateCertificate(new java.io.ByteArrayInputStream(der));
                return CertInfo.of(c);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new CertInfo("CN=" + domains.get(0), "CN=test-ca", new java.util.TreeSet<>(domains), notAfter.minus(Duration.ofDays(60)),
                notAfter, false, "deadbeef", "01");
    }

    @Test
    void noCertificateAtAllNeedsIssuance() {
        assertTrue(AcmeService.needsIssuance(null, List.of("a.example.com"), 30, Instant.now()));
    }

    @Test
    void placeholderAlwaysNeedsIssuance() {
        CertInfo c = cert(List.of("a.example.com"), Instant.now().plus(Duration.ofDays(60)), true);
        assertTrue(c.placeholder());
        assertTrue(AcmeService.needsIssuance(c, List.of("a.example.com"), 30, Instant.now()));
    }

    @Test
    void domainSetChangeForcesReissuanceEvenWithPlentyOfTimeLeft() {
        CertInfo c = cert(List.of("a.example.com"), Instant.now().plus(Duration.ofDays(60)), false);
        assertTrue(AcmeService.needsIssuance(c, List.of("a.example.com", "b.example.com"), 30, Instant.now()));
        assertFalse(AcmeService.needsIssuance(c, List.of("a.example.com"), 30, Instant.now()));
    }

    @Test
    void renewDaysThresholdIsRespected() {
        Instant now = Instant.now();
        CertInfo soon = cert(List.of("a.example.com"), now.plus(Duration.ofDays(10)), false);
        CertInfo later = cert(List.of("a.example.com"), now.plus(Duration.ofDays(90)), false);
        assertTrue(AcmeService.needsIssuance(soon, List.of("a.example.com"), 30, now));
        assertFalse(AcmeService.needsIssuance(later, List.of("a.example.com"), 30, now));
        // exactly at the boundary: notAfter - renewDays == now -> due
        CertInfo boundary = cert(List.of("a.example.com"), now.plus(Duration.ofDays(30)), false);
        assertTrue(AcmeService.needsIssuance(boundary, List.of("a.example.com"), 30, now));
    }

    @Test
    void backoffDoublesUpToSixHoursThenCaps() {
        long base = 60;
        assertEquals(Duration.ofSeconds(60), AcmeService.backoff(1, base, false, null));
        assertEquals(Duration.ofSeconds(120), AcmeService.backoff(2, base, false, null));
        assertEquals(Duration.ofSeconds(240), AcmeService.backoff(3, base, false, null));
        Duration d10 = AcmeService.backoff(10, base, false, null);
        assertEquals(Duration.ofHours(6), d10);
        Duration d20 = AcmeService.backoff(20, base, false, null);
        assertEquals(Duration.ofHours(6), d20);   // stays capped, does not keep growing
    }

    @Test
    void rateLimitedHonoursRetryAfterAndHasAOneHourFloorWithoutIt() {
        Duration withRetryAfter = AcmeService.backoff(1, 60, true, Duration.ofMinutes(45));
        assertEquals(Duration.ofMinutes(45), withRetryAfter);
        Duration withoutRetryAfter = AcmeService.backoff(1, 60, true, null);
        assertEquals(Duration.ofHours(1), withoutRetryAfter);
        // a huge Retry-After is clamped to 24h
        Duration huge = AcmeService.backoff(1, 60, true, Duration.ofDays(10));
        assertEquals(Duration.ofHours(24), huge);
    }
}
