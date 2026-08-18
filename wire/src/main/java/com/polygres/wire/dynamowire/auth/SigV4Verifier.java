package com.polygres.wire.dynamowire.auth;

import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * AWS Signature Version 4 request verification for dynamowire -- closes the gap {@code
 * DynamoWireServer}'s own class javadoc already documented ("accepts any {@code Authorization}
 * header (SigV4-shaped or not) without verifying the signature"). Real DynamoDB's actual client
 * protocol always authenticates this way (every AWS SDK signs every request with SigV4, with no
 * option to send a bearer token instead) -- unlike the generic OAuth2/OIDC bearer-token support
 * added for the other HTTP frontends, which doesn't apply here since a real DynamoDB client never
 * sends one.
 *
 * <p>Reimplements AWS's own published SigV4 algorithm directly (<a
 * href="https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html">Creating
 * a canonical request</a>) -- the same well-documented, stable HMAC-chain scheme real
 * AWS-API-compatible open-source servers (LocalStack, MinIO's S3 gateway) already implement
 * server-side; nothing novel here, just the standard verification-side computation.
 *
 * <p><b>Narrow-slice, stated plainly</b>: DynamoDB's real requests are always {@code POST /} with
 * no query string, so canonical URI/query-string handling is hardcoded to that shape rather than
 * general-purpose; a canonical-header value is trimmed but not whitespace-collapsed (real AWS SDKs
 * send simple single-line header values for DynamoDB requests, so this doesn't need the fuller RFC
 * 7230 folding logic AWS's own spec describes for the general case). {@code X-Amz-Date} is checked
 * against a ±15-minute clock-skew window, the same default AWS itself uses.
 */
public final class SigV4Verifier {

    private static final Pattern AUTH_HEADER = Pattern.compile(
            "AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/aws4_request, "
                    + "SignedHeaders=([^,]+), Signature=([0-9a-f]+)");
    private static final long MAX_CLOCK_SKEW_MILLIS = 15 * 60 * 1000;

    public record Result(boolean valid, String accessKeyId, String reason) {
        public static Result ok(String accessKeyId) {
            return new Result(true, accessKeyId, null);
        }

        public static Result fail(String reason) {
            return new Result(false, null, reason);
        }
    }

    private final AwsIamCredentialStore credentials;

    public SigV4Verifier(AwsIamCredentialStore credentials) {
        this.credentials = credentials;
    }

    public Result verify(HttpServletRequest request, String rawBody) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader == null) {
            return Result.fail("missing Authorization header");
        }
        Matcher m = AUTH_HEADER.matcher(authHeader);
        if (!m.matches()) {
            return Result.fail("Authorization header is not a well-formed AWS4-HMAC-SHA256 signature");
        }
        String accessKeyId = m.group(1);
        String dateStamp = m.group(2);
        String region = m.group(3);
        String service = m.group(4);
        String signedHeadersSpec = m.group(5);
        String providedSignature = m.group(6);

        String secretKey = credentials.secretFor(accessKeyId);
        if (secretKey == null) {
            return Result.fail("unknown access key id: " + accessKeyId);
        }

        String amzDate = request.getHeader("X-Amz-Date");
        if (amzDate == null) {
            return Result.fail("missing X-Amz-Date header");
        }
        if (!withinClockSkew(amzDate)) {
            return Result.fail("X-Amz-Date is outside the allowed clock-skew window (15 minutes)");
        }

        try {
            List<String> signedHeaders = new ArrayList<>(List.of(signedHeadersSpec.split(";")));
            Collections.sort(signedHeaders);
            String canonicalHeaders = buildCanonicalHeaders(request, signedHeaders);
            String hashedPayload = hex(sha256(rawBody.getBytes(StandardCharsets.UTF_8)));

            // DynamoDB's real requests are always POST / with no query string -- see class javadoc.
            String canonicalRequest = "POST\n/\n\n" + canonicalHeaders + "\n"
                    + String.join(";", signedHeaders) + "\n" + hashedPayload;

            String credentialScope = dateStamp + "/" + region + "/" + service + "/aws4_request";
            String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + credentialScope + "\n"
                    + hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));

            byte[] kDate = hmacSha256(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), dateStamp);
            byte[] kRegion = hmacSha256(kDate, region);
            byte[] kService = hmacSha256(kRegion, service);
            byte[] kSigning = hmacSha256(kService, "aws4_request");
            String computedSignature = hex(hmacSha256(kSigning, stringToSign));

            if (!MessageDigest.isEqual(
                    computedSignature.getBytes(StandardCharsets.UTF_8), providedSignature.getBytes(StandardCharsets.UTF_8))) {
                return Result.fail("signature mismatch");
            }
            return Result.ok(accessKeyId);
        } catch (Exception e) {
            return Result.fail("verification error: " + e.getMessage());
        }
    }

    private static String buildCanonicalHeaders(HttpServletRequest request, List<String> sortedLowercaseSignedHeaders) {
        StringBuilder sb = new StringBuilder();
        for (String headerName : sortedLowercaseSignedHeaders) {
            String value = findHeaderCaseInsensitive(request, headerName);
            sb.append(headerName).append(':').append(value == null ? "" : value.trim()).append('\n');
        }
        return sb.toString();
    }

    private static String findHeaderCaseInsensitive(HttpServletRequest request, String headerName) {
        Enumeration<String> names = request.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (name.equalsIgnoreCase(headerName)) {
                return request.getHeader(name);
            }
        }
        return null;
    }

    private static boolean withinClockSkew(String amzDate) {
        try {
            java.time.Instant requestTime = java.time.Instant.from(
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                            .withZone(java.time.ZoneOffset.UTC).parse(amzDate));
            long diff = Math.abs(java.time.Instant.now().toEpochMilli() - requestTime.toEpochMilli());
            return diff <= MAX_CLOCK_SKEW_MILLIS;
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] sha256(byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(data);
    }

    private static byte[] hmacSha256(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }
}
