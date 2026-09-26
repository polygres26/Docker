package com.sayonora.warp.ab;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Outbound AWS Signature V4 (header form): what Warp uses to re-sign a forwarded request for the cloud. */
public final class AbSigV4 {

    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private AbSigV4() {
    }

    /**
     * @param headers headers to sign (lower-case name -> value), MUST include {@code host}; x-amz-date,
     *                x-amz-security-token and x-amz-content-sha256 are added here
     * @param query   already canonical-encoded query string (sorted), or ""
     * @return the headers to send: all input headers plus x-amz-date, x-amz-security-token (if any), authorization
     */
    public static Map<String, String> sign(String method, String encodedPath, String query, Map<String, String> headers,
            String payloadHash, AbAuthProvider.AwsCreds creds, String region, String service, Instant now,
            boolean sendContentSha) {
        TreeMap<String, String> h = new TreeMap<>();
        headers.forEach((k, v) -> h.put(k.toLowerCase(Locale.ROOT), v.trim()));
        String amzDate = AMZ_DATE.format(now);
        h.put("x-amz-date", amzDate);
        if (sendContentSha) {
            h.put("x-amz-content-sha256", payloadHash);
        }
        if (creds.sessionToken() != null && !creds.sessionToken().isBlank()) {
            h.put("x-amz-security-token", creds.sessionToken());
        }
        StringBuilder canonHeaders = new StringBuilder();
        List<String> names = new ArrayList<>();
        for (var e : h.entrySet()) {
            canonHeaders.append(e.getKey()).append(':').append(e.getValue().replaceAll("\\s+", " ")).append('\n');
            names.add(e.getKey());
        }
        String signed = String.join(";", names);
        String canonical = method + "\n" + encodedPath + "\n" + query + "\n" + canonHeaders + "\n" + signed + "\n" + payloadHash;
        String scope = DAY.format(now) + "/" + region + "/" + service + "/aws4_request";
        String sts = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex(sha256(canonical.getBytes(StandardCharsets.UTF_8)));
        byte[] k = hmac(("AWS4" + creds.secretAccessKey()).getBytes(StandardCharsets.UTF_8), DAY.format(now));
        k = hmac(k, region);
        k = hmac(k, service);
        k = hmac(k, "aws4_request");
        String sig = hex(hmac(k, sts));
        h.put("authorization", "AWS4-HMAC-SHA256 Credential=" + creds.accessKeyId() + "/" + scope + ", SignedHeaders=" + signed
                + ", Signature=" + sig);
        return h;
    }

    /** RFC 3986 encoding as AWS wants it: unreserved kept, everything else %XX; {@code keepSlash} for paths. */
    public static String enc(String s, boolean keepSlash) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
                    || c == '~' || (keepSlash && c == '/')) {
                sb.append((char) c);
            } else {
                sb.append('%').append(String.format("%02X", c));
            }
        }
        return sb.toString();
    }

    /** Percent-decode (no plus handling for paths). */
    public static String decode(String s, boolean plusIsSpace) {
        try {
            return URLDecoder.decode(plusIsSpace ? s : s.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return s;
        }
    }

    /** Canonical query string from a raw one: decode, drop {@code drop} names, encode, sort by name then value. */
    public static String canonicalQuery(String raw, java.util.Set<String> drop) {
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        List<String[]> kv = new ArrayList<>();
        for (String part : raw.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            String k = decode(eq < 0 ? part : part.substring(0, eq), true);
            String v = eq < 0 ? "" : decode(part.substring(eq + 1), true);
            if (drop.contains(k)) {
                continue;
            }
            kv.add(new String[] {enc(k, false), enc(v, false)});
        }
        kv.sort((a, b) -> a[0].equals(b[0]) ? a[1].compareTo(b[1]) : a[0].compareTo(b[0]));
        StringBuilder sb = new StringBuilder();
        for (String[] p : kv) {
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(p[0]).append('=').append(p[1]);
        }
        return sb.toString();
    }

    public static byte[] sha256(byte[] d) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(d);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] hmac(byte[] key, String data) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
