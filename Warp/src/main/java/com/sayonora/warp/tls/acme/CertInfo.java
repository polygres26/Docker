package com.sayonora.warp.tls.acme;

import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/** What we need to know about a leaf certificate. */
public record CertInfo(String subject, String issuer, Set<String> names, Instant notBefore, Instant notAfter, boolean placeholder,
        String sha256, String serial) {

    public static CertInfo of(X509Certificate c) {
        Set<String> names = new TreeSet<>();
        try {
            Collection<List<?>> sans = c.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> e : sans) {
                    if (((Integer) e.get(0)) == 2) {
                        names.add(String.valueOf(e.get(1)).toLowerCase(Locale.ROOT));
                    }
                }
            }
        } catch (java.security.cert.CertificateParsingException ignored) {
            // no SANs
        }
        String fp;
        try {
            fp = AwsSigner.hex(MessageDigest.getInstance("SHA-256").digest(c.getEncoded()));
        } catch (Exception e) {
            fp = "";
        }
        return new CertInfo(c.getSubjectX500Principal().getName(), c.getIssuerX500Principal().getName(), names,
                c.getNotBefore().toInstant(), c.getNotAfter().toInstant(), AcmeCrypto.isPlaceholder(c), fp,
                c.getSerialNumber().toString(16));
    }

    public long daysLeft(Instant now) {
        return Duration.between(now, notAfter).toDays();
    }
}
