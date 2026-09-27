package com.sayonora.warp.tls.acme;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** AWS Signature Version 4 request signing (the existing AwsSigV4 class only verifies incoming requests). */
final class AwsSigner {

    private AwsSigner() {
    }

    /**
     * @return headers to add to the request: {@code x-amz-date}, optional {@code x-amz-security-token}, {@code x-amz-content-sha256}
     *         and {@code Authorization}. {@code host} must equal what the HTTP client sends (authority incl. non-default port).
     */
    static Map<String, String> sign(String method, String host, String path, String canonicalQuery, byte[] body, String region,
            String service, String accessKey, String secretKey, String sessionToken, Instant now) {
        String amzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(now);
        String date = amzDate.substring(0, 8);
        String payloadHash = hex(AcmeCrypto.sha256(body == null ? new byte[0] : body));
        Map<String, String> h = new LinkedHashMap<>();
        h.put("host", host);
        h.put("x-amz-content-sha256", payloadHash);
        h.put("x-amz-date", amzDate);
        if (sessionToken != null) {
            h.put("x-amz-security-token", sessionToken);
        }
        StringBuilder canonHeaders = new StringBuilder();
        StringBuilder signed = new StringBuilder();
        for (Map.Entry<String, String> e : new java.util.TreeMap<>(h).entrySet()) {
            canonHeaders.append(e.getKey()).append(':').append(e.getValue().trim()).append('\n');
            signed.append(signed.length() == 0 ? "" : ";").append(e.getKey());
        }
        String canonical = method + "\n" + path + "\n" + canonicalQuery + "\n" + canonHeaders + "\n" + signed + "\n" + payloadHash;
        String scope = date + "/" + region + "/" + service + "/aws4_request";
        String toSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex(AcmeCrypto.sha256(canonical.getBytes(StandardCharsets.UTF_8)));
        byte[] k = hmac(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), date);
        k = hmac(k, region);
        k = hmac(k, service);
        k = hmac(k, "aws4_request");
        String sig = hex(hmac(k, toSign));
        Map<String, String> out = new LinkedHashMap<>();
        out.put("x-amz-date", amzDate);
        out.put("x-amz-content-sha256", payloadHash);
        if (sessionToken != null) {
            out.put("x-amz-security-token", sessionToken);
        }
        out.put("Authorization", "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + sig);
        return out;
    }

    static byte[] hmac(byte[] key, String data) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
