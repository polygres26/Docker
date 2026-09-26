package com.sayonora.warp.s3wire;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Path-style ({@code host/bucket/key}) versus virtual-hosted-style ({@code bucket.host/key}) addressing.
 * A host is virtual-hosted when it is {@code <bucket>.<domain>} for a configured domain
 * ({@code WARP_S3WIRE_VHOST_DOMAIN}, comma separated), any {@code <bucket>[.s3].localhost} name (the
 * localhost-friendly mode: browsers and SDKs resolve {@code *.localhost} to loopback with no DNS; the public
 * wildcard-DNS names {@code localhost.localstack.cloud} and {@code localhost.floci.io} are treated the same way), or a
 * real AWS S3 endpoint name ({@code bucket.s3.amazonaws.com}, {@code bucket.s3.<region>.amazonaws.com},
 * {@code bucket.s3-<region>.amazonaws.com}, dualstack/fips variants). IP literals and dotless hosts are
 * always path-style. A host that IS a configured domain (or {@code s3.<domain>}) is the bucketless service endpoint.
 */
final class S3Addressing {
    private static final Pattern AWS = Pattern.compile(
            "^(.+?)\\.s3(?:-fips|-accelerate)?(?:[.-](?:dualstack\\.)?[a-z0-9-]+)?\\.amazonaws\\.com(?:\\.cn)?$");
    /** Names that always resolve to loopback (no DNS setup): {@code *.localhost} and the public wildcard-DNS test domains. */
    private static final List<String> LOOPBACK_DOMAINS = List.of("localhost", "localhost.localstack.cloud", "localhost.floci.io");
    private static final Pattern IPV4 = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

    private S3Addressing() {
    }

    static List<String> parseDomains(String env) {
        List<String> out = new ArrayList<>();
        if (env != null) {
            for (String d : env.split("[,;]")) {
                String t = d.trim().toLowerCase(Locale.ROOT);
                if (t.startsWith(".")) {
                    t = t.substring(1);
                }
                if (!t.isEmpty()) {
                    out.add(t);
                }
            }
        }
        return out;
    }

    /** The bucket named by a virtual-hosted {@code Host} header, or null for path-style / the service endpoint. */
    static String bucketFromHost(String hostHeader, List<String> domains) {
        if (hostHeader == null || hostHeader.isBlank()) {
            return null;
        }
        String h = hostHeader.trim().toLowerCase(Locale.ROOT);
        if (h.startsWith("[")) {
            return null; // IPv6 literal
        }
        int colon = h.lastIndexOf(':');
        if (colon >= 0) {
            h = h.substring(0, colon);
        }
        if (h.endsWith(".")) {
            h = h.substring(0, h.length() - 1);
        }
        if (h.isEmpty() || !h.contains(".") || IPV4.matcher(h).matches()) {
            return null;
        }
        for (String d : domains) {
            if (h.equals(d) || h.equals("s3." + d)) {
                return null;
            }
            if (h.endsWith("." + d)) {
                String label = h.substring(0, h.length() - d.length() - 1);
                if (label.endsWith(".s3")) {
                    label = label.substring(0, label.length() - 3);
                }
                return label.isEmpty() || label.equals("s3") ? null : label;
            }
        }
        for (String d : LOOPBACK_DOMAINS) {
            if (h.endsWith("." + d)) {
                String label = h.substring(0, h.length() - d.length() - 1);
                if (label.endsWith(".s3")) {
                    label = label.substring(0, label.length() - 3);
                }
                return label.isEmpty() || label.equals("s3") ? null : label;
            }
        }
        Matcher m = AWS.matcher(h);
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }
}
