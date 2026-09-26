package com.sayonora.wire.s3wire;

import com.sayonora.wire.dynamowire.auth.AwsIamCredentialStore;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * AWS Signature V4 verification for S3 requests (header-based {@code Authorization} and presigned
 * query-string auth).
 *
 * <p><b>Validated:</b> the access key is one of the configured pairs; the signature over the
 * canonical request (method, URI-encoded path, sorted query, signed headers exactly as sent, and
 * the {@code x-amz-content-sha256} value the client claims) matches; the request time is within 15
 * minutes (presigned: within its X-Amz-Expires). Scope service must be {@code s3}; any region is
 * accepted.
 *
 * <p><b>Not validated here:</b> per-chunk signatures of {@code STREAMING-*} uploads (only the seed
 * signature is); whether a claimed hex payload hash matches the body is checked by the server
 * after streaming; there is no per-key authorization -- every valid key can access every bucket.
 */
public final class S3SigV4Verifier {

    private static final Pattern AUTH = Pattern.compile(
            "AWS4-HMAC-SHA256\\s+Credential=([^/,\\s]+)/(\\d{8})/([^/]+)/([^/]+)/aws4_request,\\s*"
                    + "SignedHeaders=([^,\\s]+),\\s*Signature=([0-9a-fA-F]+)");
    private static final long MAX_SKEW_SECONDS = 15 * 60;
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    /** What per-chunk signature verification of {@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD} bodies needs. */
    public record ChunkAuth(byte[] signingKey, String amzDate, String scope, String seedSignature) {
    }

    public record Result(boolean valid, String accessKeyId, int status, String code, String message, ChunkAuth chunk) {
        static Result ok(String key, ChunkAuth chunk) {
            return new Result(true, key, 200, null, null, chunk);
        }

        static Result ok(String key) {
            return new Result(true, key, 200, null, null, null);
        }

        static Result fail(int status, String code, String message) {
            return new Result(false, null, status, code, message, null);
        }
    }

    private final AwsIamCredentialStore credentials;

    public S3SigV4Verifier(AwsIamCredentialStore credentials) {
        this.credentials = credentials;
    }

    /**
     * @param rawPath  the request path exactly as sent (percent-encoded)
     * @param rawQuery the query string exactly as sent, or null
     * @param headers  header name (lower-case) to its values
     */
    public Result verify(String method, String rawPath, String rawQuery, Map<String, List<String>> headers, Instant now) {
        Map<String, String> query = parseQuery(rawQuery);
        String auth = first(headers, "authorization");
        String accessKey;
        String scopeDate;
        String region;
        String service;
        String signedHeadersSpec;
        String provided;
        String amzDate;
        String payloadHash;
        boolean presigned = false;
        if (auth != null) {
            Matcher m = AUTH.matcher(auth.trim());
            if (!m.matches()) {
                return Result.fail(400, "AuthorizationHeaderMalformed", "The authorization header is malformed.");
            }
            accessKey = m.group(1);
            scopeDate = m.group(2);
            region = m.group(3);
            service = m.group(4);
            signedHeadersSpec = m.group(5);
            provided = m.group(6).toLowerCase(Locale.ROOT);
            amzDate = first(headers, "x-amz-date");
            if (amzDate == null) {
                amzDate = first(headers, "date");
            }
            payloadHash = first(headers, "x-amz-content-sha256");
            if (payloadHash == null) {
                return Result.fail(400, "InvalidRequest", "Missing required header x-amz-content-sha256.");
            }
        } else if (query.containsKey("X-Amz-Signature")) {
            presigned = true;
            if (!"AWS4-HMAC-SHA256".equals(query.get("X-Amz-Algorithm"))) {
                return Result.fail(400, "AuthorizationQueryParametersError", "Unsupported X-Amz-Algorithm.");
            }
            String[] cred = String.valueOf(query.get("X-Amz-Credential")).split("/");
            if (cred.length != 5 || !"aws4_request".equals(cred[4])) {
                return Result.fail(400, "AuthorizationQueryParametersError", "Error parsing the X-Amz-Credential parameter.");
            }
            accessKey = cred[0];
            scopeDate = cred[1];
            region = cred[2];
            service = cred[3];
            signedHeadersSpec = query.get("X-Amz-SignedHeaders");
            provided = query.get("X-Amz-Signature").toLowerCase(Locale.ROOT);
            amzDate = query.get("X-Amz-Date");
            payloadHash = "UNSIGNED-PAYLOAD";
            if (signedHeadersSpec == null || query.get("X-Amz-Expires") == null) {
                return Result.fail(400, "AuthorizationQueryParametersError", "Missing presigned URL parameters.");
            }
        } else {
            return Result.fail(403, "AccessDenied", "Access Denied: request is not signed.");
        }
        if (!"s3".equals(service)) {
            return Result.fail(403, "SignatureDoesNotMatch", "Credential scope service must be s3.");
        }
        String secret = credentials.secretFor(accessKey);
        if (secret == null) {
            return Result.fail(403, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.");
        }
        Instant requestTime;
        try {
            requestTime = Instant.from(AMZ_DATE.parse(amzDate == null ? "" : amzDate));
        } catch (RuntimeException e) {
            return Result.fail(400, "AuthorizationHeaderMalformed", "Missing or malformed X-Amz-Date.");
        }
        long skew = now.getEpochSecond() - requestTime.getEpochSecond();
        if (presigned) {
            long expires;
            try {
                expires = Long.parseLong(query.get("X-Amz-Expires"));
            } catch (NumberFormatException e) {
                return Result.fail(400, "AuthorizationQueryParametersError", "X-Amz-Expires must be a number.");
            }
            if (skew < -MAX_SKEW_SECONDS) {
                return Result.fail(403, "AccessDenied", "Request is not yet valid.");
            }
            if (skew > expires) {
                return Result.fail(403, "AccessDenied", "Request has expired.");
            }
        } else if (Math.abs(skew) > MAX_SKEW_SECONDS) {
            return Result.fail(403, "RequestTimeTooSkewed",
                    "The difference between the request time and the current time is too large.");
        }
        if (!amzDate.startsWith(scopeDate)) {
            return Result.fail(403, "SignatureDoesNotMatch", "Credential scope date does not match X-Amz-Date.");
        }

        try {
            List<String> signedHeaders = new ArrayList<>(Arrays.asList(signedHeadersSpec.split(";")));
            StringBuilder canonicalHeaders = new StringBuilder();
            for (String h : signedHeaders) {
                List<String> vals = headers.get(h.toLowerCase(Locale.ROOT));
                if (vals == null) {
                    return Result.fail(403, "SignatureDoesNotMatch", "Signed header missing from request: " + h);
                }
                List<String> cleaned = new ArrayList<>();
                for (String v : vals) {
                    cleaned.add(v.trim().replaceAll("\\s+", " "));
                }
                canonicalHeaders.append(h.toLowerCase(Locale.ROOT)).append(':').append(String.join(",", cleaned)).append('\n');
            }
            String canonicalRequest = method + "\n" + canonicalUri(rawPath) + "\n"
                    + canonicalQuery(rawQuery, presigned) + "\n" + canonicalHeaders + "\n"
                    + signedHeadersSpec + "\n" + payloadHash;
            String scope = scopeDate + "/" + region + "/s3/aws4_request";
            String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n"
                    + hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
            byte[] k = hmac(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), scopeDate);
            k = hmac(k, region);
            k = hmac(k, "s3");
            k = hmac(k, "aws4_request");
            String computed = hex(hmac(k, stringToSign));
            if (!MessageDigest.isEqual(computed.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8))) {
                return Result.fail(403, "SignatureDoesNotMatch",
                        "The request signature we calculated does not match the signature you provided.");
            }
            return Result.ok(accessKey, new ChunkAuth(k, amzDate, scope, computed));
        } catch (Exception e) {
            return Result.fail(400, "InvalidRequest", "Signature verification error: " + e.getMessage());
        }
    }

    public static String canonicalUri(String rawPath) {
        String decoded = percentDecode(rawPath == null || rawPath.isEmpty() ? "/" : rawPath, false);
        return encode(decoded, false);
    }

    private static String canonicalQuery(String rawQuery, boolean excludeSignature) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }
        List<String[]> pairs = new ArrayList<>();
        for (String part : rawQuery.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            String name = percentDecode(eq < 0 ? part : part.substring(0, eq), true);
            String value = eq < 0 ? "" : percentDecode(part.substring(eq + 1), true);
            if (excludeSignature && name.equals("X-Amz-Signature")) {
                continue;
            }
            pairs.add(new String[] {encode(name, true), encode(value, true)});
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

    public static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return out;
        }
        for (String part : rawQuery.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            int eq = part.indexOf('=');
            String name = percentDecode(eq < 0 ? part : part.substring(0, eq), true);
            String value = eq < 0 ? "" : percentDecode(part.substring(eq + 1), true);
            out.putIfAbsent(name, value);
        }
        return out;
    }

    /** Percent-decodes UTF-8; {@code plusIsSpace} only for query strings ('+' is literal in paths). */
    public static String percentDecode(String s, boolean plusIsSpace) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < raw.length; i++) {
            byte b = raw[i];
            if (b == '%' && i + 2 < raw.length && Character.digit(raw[i + 1], 16) >= 0
                    && Character.digit(raw[i + 2], 16) >= 0) {
                out.write((Character.digit(raw[i + 1], 16) << 4) | Character.digit(raw[i + 2], 16));
                i += 2;
            } else if (b == '+' && plusIsSpace) {
                out.write(' ');
            } else {
                out.write(b);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /** RFC 3986 encoding as SigV4 defines it; '/' left alone in paths. */
    public static String encode(String s, boolean encodeSlash) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == '~' || (c == '/' && !encodeSlash)) {
                sb.append((char) c);
            } else {
                sb.append('%').append(String.format(Locale.ROOT, "%02X", c));
            }
        }
        return sb.toString();
    }

    private static String first(Map<String, List<String>> headers, String name) {
        List<String> v = headers.get(name);
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    private static byte[] sha256(byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(data);
    }

    public static byte[] hmacBytes(byte[] key, String data) throws Exception {
        return hmac(key, data);
    }

    private static byte[] hmac(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    public static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }
}
