package com.sayonora.warp.awswire;

import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * SigV4 verification for the JSON/Query AWS APIs (any service name in the credential scope, body hashed here, unlike
 * s3wire's verifier which is bound to {@code s3} and {@code x-amz-content-sha256}). Only used when
 * {@code WARP_AWS_IAM_CREDENTIALS} is set. Validated: the {@code Authorization} header format, that the access key is
 * a configured pair or an unexpired STS session issued by this Warp (whose {@code X-Amz-Security-Token} must match),
 * the credential-scope date matches {@code X-Amz-Date}, a 15 minute clock skew, and the HMAC-SHA256 signature over the
 * canonical request (method, path, query, the signed headers, SHA-256 of the body). Not validated: authorization
 * (every valid key may call every operation), the credential-scope region, session policies.
 */
public final class AwsSigV4 {

    private static final Pattern AUTH = Pattern.compile(
            "AWS4-HMAC-SHA256\\s+Credential=([^/,\\s]+)/(\\d{8})/([^/]+)/([^/]+)/aws4_request,\\s*"
                    + "SignedHeaders=([^,\\s]+),\\s*Signature=([0-9a-fA-F]+)");
    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private AwsSigV4() {
    }

    /** Secret and (for STS sessions) the session token an access key id maps to. */
    public record Secret(String secret, String sessionToken) {
    }

    public record Result(boolean valid, String accessKey, String service, int status, String code, String message) {
    }

    private static Result fail(int status, String code, String message) {
        return new Result(false, null, null, status, code, message);
    }

    /** The service in the credential scope, or null when the request is not SigV4-signed. */
    public static String scopeService(HttpServletRequest r) {
        String auth = r.getHeader("Authorization");
        if (auth != null) {
            Matcher m = AUTH.matcher(auth.trim());
            if (m.find()) {
                return m.group(4);
            }
        }
        String q = r.getQueryString();
        if (q != null && q.contains("X-Amz-Credential=")) {
            for (String p : q.split("&")) {
                if (p.startsWith("X-Amz-Credential=")) {
                    String[] cred = java.net.URLDecoder.decode(p.substring(17), StandardCharsets.UTF_8).split("/");
                    if (cred.length >= 4) {
                        return cred[3];
                    }
                }
            }
        }
        return null;
    }

    public static String accessKey(HttpServletRequest r) {
        String auth = r.getHeader("Authorization");
        if (auth != null) {
            Matcher m = AUTH.matcher(auth.trim());
            if (m.find()) {
                return m.group(1);
            }
        }
        return null;
    }

    public static Result verify(HttpServletRequest r, byte[] body, Function<String, Secret> lookup, Instant now) {
        String auth = r.getHeader("Authorization");
        if (auth == null) {
            return fail(403, "MissingAuthenticationToken", "Missing Authentication Token");
        }
        Matcher m = AUTH.matcher(auth.trim());
        if (!m.matches()) {
            return fail(403, "IncompleteSignature", "Authorization header requires existence of Credential/SignedHeaders/Signature");
        }
        String accessKey = m.group(1), scopeDate = m.group(2), region = m.group(3), service = m.group(4);
        String signedHeadersSpec = m.group(5), provided = m.group(6).toLowerCase(Locale.ROOT);
        Secret secret = lookup.apply(accessKey);
        if (secret == null) {
            return fail(403, "UnrecognizedClientException", "The security token included in the request is invalid.");
        }
        String token = r.getHeader("X-Amz-Security-Token");
        if (secret.sessionToken() != null && !secret.sessionToken().equals(token)) {
            return fail(403, "InvalidClientTokenId", "The security token included in the request is invalid.");
        }
        String amzDate = r.getHeader("X-Amz-Date");
        if (amzDate == null) {
            amzDate = r.getHeader("Date");
        }
        Instant t;
        try {
            t = Instant.from(AMZ_DATE.parse(amzDate == null ? "" : amzDate));
        } catch (RuntimeException e) {
            return fail(403, "IncompleteSignature", "Date must be in ISO 8601 'basic format'.");
        }
        if (Math.abs(now.getEpochSecond() - t.getEpochSecond()) > 15 * 60) {
            return fail(403, "RequestExpired", "Signature expired: " + amzDate + " is now earlier than " + now);
        }
        if (!amzDate.startsWith(scopeDate)) {
            return fail(403, "SignatureDoesNotMatch", "Credential scope date does not match X-Amz-Date.");
        }
        // Jetty replaces a Content-Type header value with its own cached spelling ("charset=UTF-8"), so a client that signed
        // "charset=utf-8" (botocore) is retried with that spelling
        Result first = attempt(r, body, secret, accessKey, scopeDate, region, service, signedHeadersSpec, provided, amzDate, false);
        if (first.valid() || !"SignatureDoesNotMatch".equals(first.code())) {
            return first;
        }
        String ct = r.getContentType();
        if (ct != null && ct.contains("charset=UTF-8")) {
            Result second = attempt(r, body, secret, accessKey, scopeDate, region, service, signedHeadersSpec, provided, amzDate, true);
            if (second.valid()) {
                return second;
            }
        }
        return first;
    }

    private static Result attempt(HttpServletRequest r, byte[] body, Secret secret, String accessKey, String scopeDate, String region,
            String service, String signedHeadersSpec, String provided, String amzDate, boolean lowerCharset) {
        try {
            StringBuilder canonicalHeaders = new StringBuilder();
            for (String h : signedHeadersSpec.split(";")) {
                List<String> vals = Collections.list(r.getHeaders(h));
                if (vals.isEmpty()) {
                    return fail(403, "SignatureDoesNotMatch", "Signed header missing from request: " + h);
                }
                List<String> cleaned = new ArrayList<>();
                for (String v : vals) {
                    String x = v.trim().replaceAll("\\s+", " ");
                    cleaned.add(lowerCharset && h.equalsIgnoreCase("content-type") ? x.replace("charset=UTF-8", "charset=utf-8") : x);
                }
                canonicalHeaders.append(h.toLowerCase(Locale.ROOT)).append(':').append(String.join(",", cleaned)).append('\n');
            }
            String rawPath = r.getRequestURI() == null || r.getRequestURI().isEmpty() ? "/" : r.getRequestURI();
            String payloadHash = hex(sha256(body));
            String canonicalRequest = r.getMethod() + "\n" + enc(rawPath, false) + "\n" + canonicalQuery(r.getQueryString())
                    + "\n" + canonicalHeaders + "\n" + signedHeadersSpec + "\n" + payloadHash;
            String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scopeDate + "/" + region + "/" + service
                    + "/aws4_request\n" + hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
            byte[] k = hmac(("AWS4" + secret.secret()).getBytes(StandardCharsets.UTF_8), scopeDate);
            k = hmac(k, region);
            k = hmac(k, service);
            k = hmac(k, "aws4_request");
            String computed = hex(hmac(k, stringToSign));
            if (!MessageDigest.isEqual(computed.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8))) {
                if ("true".equalsIgnoreCase(System.getenv("WARP_AWSWIRE_DEBUG_SIGV4"))) {
                    org.slf4j.LoggerFactory.getLogger(AwsSigV4.class).warn("SigV4 mismatch, canonical request:\n{}\nstring to sign:\n{}",
                            canonicalRequest, stringToSign);
                }
                return fail(403, "SignatureDoesNotMatch",
                        "The request signature we calculated does not match the signature you provided.");
            }
            return new Result(true, accessKey, service, 200, null, null);
        } catch (Exception e) {
            return fail(400, "IncompleteSignature", "Signature verification error: " + e.getMessage());
        }
    }

    private static String canonicalQuery(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        List<String[]> pairs = new ArrayList<>();
        for (String p : raw.split("&")) {
            if (p.isEmpty()) {
                continue;
            }
            int eq = p.indexOf('=');
            String k = java.net.URLDecoder.decode(eq < 0 ? p : p.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : java.net.URLDecoder.decode(p.substring(eq + 1), StandardCharsets.UTF_8);
            pairs.add(new String[] {enc(k, true), enc(v, true)});
        }
        pairs.sort((a, b) -> a[0].equals(b[0]) ? a[1].compareTo(b[1]) : a[0].compareTo(b[0]));
        StringBuilder sb = new StringBuilder();
        for (String[] p : pairs) {
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(p[0]).append('=').append(p[1]);
        }
        return sb.toString();
    }

    static String enc(String s, boolean slash) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
                    || c == '~' || (c == '/' && !slash)) {
                sb.append((char) c);
            } else {
                sb.append('%').append(String.format("%02X", c));
            }
        }
        return sb.toString();
    }

    static byte[] sha256(byte[] d) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(d);
    }

    static byte[] hmac(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
