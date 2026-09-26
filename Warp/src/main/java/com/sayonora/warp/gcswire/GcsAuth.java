package com.sayonora.warp.gcswire;

import com.sayonora.warp.gcswire.GcsModel.HmacKey;
import com.sayonora.warp.s3wire.S3SigV4Verifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Request authentication of gcswire.
 *
 * <ul>
 *   <li>OAuth2 bearer ({@code Authorization: Bearer} or {@code access_token=}): accepted when it is one of
 *       {@code WARP_GCSWIRE_TOKENS}; with tokens configured a wrong token is 401 even if anonymous access is allowed. With no
 *       token list, any bearer is accepted only when {@code WARP_GCSWIRE_ALLOW_ANONYMOUS=true};</li>
 *   <li>HMAC keys (the XML API): {@code AWS4-HMAC-SHA256} and {@code GOOG4-HMAC-SHA256} signatures, header or presigned query,
 *       verified against keys created through the JSON API ({@code /projects/{p}/hmacKeys}); INACTIVE keys are refused;</li>
 *   <li>V4 signed URLs ({@code GOOG4-RSA-SHA256}) and V2 signed URLs ({@code GoogleAccessId}/{@code Expires}/{@code Signature})
 *       verified with the public keys of {@code WARP_GCSWIRE_SIGNING_KEYS};</li>
 *   <li>otherwise anonymous, when allowed.</li>
 * </ul>
 * There is no per-bucket or per-object authorization: every accepted credential can do everything.
 */
final class GcsAuth {

    private static final Pattern AUTH = Pattern.compile(
            "(AWS4-HMAC-SHA256|GOOG4-HMAC-SHA256)\\s+Credential=([^/,\\s]+)/(\\d{8})/([^/]+)/([^/]+)/([^/,\\s]+),\\s*"
                    + "SignedHeaders=([^,\\s]+),\\s*Signature=([0-9a-fA-F]+)");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    private static final long MAX_SKEW = 15 * 60;

    private final GcsConfig cfg;
    private final GcsStore store;

    GcsAuth(GcsConfig cfg, GcsStore store) {
        this.cfg = cfg;
        this.store = store;
    }

    static GcsException unauthorized(String message) {
        return new GcsException(401, "required", "AccessDenied", message)
                .at("header", "Authorization").header("WWW-Authenticate", "Bearer realm=\"https://accounts.google.com/\"");
    }

    static GcsException invalidCredentials() {
        return new GcsException(401, "authError", "InvalidSecurity", "Invalid Credentials")
                .at("header", "Authorization").header("WWW-Authenticate", "Bearer realm=\"https://accounts.google.com/\"");
    }

    /** @return the principal label; @throws GcsException 401/403/400 */
    String authenticate(GcsReq r) {
        String auth = r.header("authorization");
        String qtoken = r.q("access_token");
        if (auth != null && (auth.startsWith("AWS4-HMAC-SHA256") || auth.startsWith("GOOG4-HMAC-SHA256"))) {
            return hmacHeader(r, auth);
        }
        if (r.has("X-Goog-Algorithm")) {
            String algo = r.q("X-Goog-Algorithm");
            return "GOOG4-RSA-SHA256".equals(algo) ? signedUrlV4(r) : hmacQuery(r, algo, "X-Goog-");
        }
        if (r.has("X-Amz-Algorithm")) {
            return hmacQuery(r, r.q("X-Amz-Algorithm"), "X-Amz-");
        }
        if (r.has("GoogleAccessId") && r.has("Signature")) {
            return signedUrlV2(r);
        }
        String token = null;
        if (auth != null && auth.regionMatches(true, 0, "Bearer ", 0, 7)) {
            token = auth.substring(7).trim();
        } else if (qtoken != null) {
            token = qtoken;
        } else if (auth != null) {
            throw invalidCredentials();
        }
        if (token != null) {
            if (!cfg.tokens.isEmpty()) {
                if (!cfg.tokens.contains(token)) {
                    throw invalidCredentials();
                }
                return "bearer";
            }
            if (cfg.allowAnonymous) {
                return "bearer";
            }
            throw invalidCredentials();
        }
        if (cfg.allowAnonymous) {
            return "anonymous";
        }
        throw unauthorized("Anonymous caller does not have storage access to this resource. Provide credentials "
                + "(Authorization: Bearer <token> from WARP_GCSWIRE_TOKENS, an HMAC signature or a signed URL).");
    }

    // ------------------------------------------------------------------------------------------ HMAC (SigV4 family)

    private String hmacHeader(GcsReq r, String auth) {
        Matcher m = AUTH.matcher(auth.trim());
        if (!m.matches()) {
            throw new GcsException(400, "invalid", "InvalidArgument", "The authorization header is malformed.");
        }
        String amzDate = first(r, "x-goog-date");
        if (amzDate == null) {
            amzDate = first(r, "x-amz-date");
        }
        if (amzDate == null) {
            amzDate = first(r, "date");
        }
        String payload = first(r, "x-goog-content-sha256");
        if (payload == null) {
            payload = first(r, "x-amz-content-sha256");
        }
        if (payload == null) {
            payload = "UNSIGNED-PAYLOAD";
        }
        verifyHmac(r, m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8).toLowerCase(Locale.ROOT),
                amzDate, payload, false, null);
        return "hmac:" + m.group(2);
    }

    private String hmacQuery(GcsReq r, String algo, String p) {
        if (!"GOOG4-HMAC-SHA256".equals(algo) && !"AWS4-HMAC-SHA256".equals(algo)) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Unsupported signing algorithm " + algo);
        }
        String cred = r.q(p + "Credential");
        String[] c = cred == null ? new String[0] : cred.split("/");
        String signature = r.q(p + "Signature");
        if (c.length != 5 || signature == null || r.q(p + "SignedHeaders") == null || r.q(p + "Date") == null || r.q(p + "Expires") == null) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Missing or malformed presigned URL parameters.");
        }
        verifyHmac(r, algo, c[0], c[1], c[2], c[3], c[4], r.q(p + "SignedHeaders"), signature.toLowerCase(Locale.ROOT), r.q(p + "Date"),
                "UNSIGNED-PAYLOAD", true, p);
        return "hmac:" + c[0];
    }

    private void verifyHmac(GcsReq r, String algo, String accessId, String date, String region, String service, String term,
            String signedHeaders, String provided, String amzDate, String payloadHash, boolean presigned, String qp) {
        HmacKey key = store.getHmac(accessId);
        if (key == null) {
            throw new GcsException(403, "forbidden", "InvalidAccessKeyId", "The access ID you provided does not exist.");
        }
        if (!"ACTIVE".equals(key.state())) {
            throw new GcsException(403, "forbidden", "AccessDenied", "The HMAC key is not active.");
        }
        Instant t = time(amzDate);
        long skew = Instant.now().getEpochSecond() - t.getEpochSecond();
        if (presigned) {
            long expires = parseLong(r.q(qp + "Expires"), 0);
            if (skew > expires) {
                throw new GcsException(400, "invalid", "ExpiredToken", "Request expired.");
            }
        } else if (Math.abs(skew) > MAX_SKEW) {
            throw new GcsException(403, "forbidden", "RequestTimeTooSkewed",
                    "The difference between the request time and the current time is too large.");
        }
        String canonical = canonicalRequest(r, signedHeaders, payloadHash, qp == null ? null : qp + "Signature");
        String scope = date + "/" + region + "/" + service + "/" + term;
        String toSign = algo + "\n" + amzDate + "\n" + scope + "\n" + sha256Hex(canonical.getBytes(StandardCharsets.UTF_8));
        try {
            String prefix = algo.startsWith("GOOG4") ? "GOOG4" : "AWS4";
            byte[] k = S3SigV4Verifier.hmacBytes((prefix + key.secret()).getBytes(StandardCharsets.UTF_8), date);
            k = S3SigV4Verifier.hmacBytes(k, region);
            k = S3SigV4Verifier.hmacBytes(k, service);
            k = S3SigV4Verifier.hmacBytes(k, term);
            String computed = S3SigV4Verifier.hex(S3SigV4Verifier.hmacBytes(k, toSign));
            if (!MessageDigest.isEqual(computed.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8))) {
                throw new GcsException(403, "forbidden", "SignatureDoesNotMatch",
                        "The request signature we calculated does not match the signature you provided.");
            }
        } catch (GcsException e) {
            throw e;
        } catch (Exception e) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Signature verification error: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------ RSA signed URLs

    private String signedUrlV4(GcsReq r) {
        String cred = r.q("X-Goog-Credential");
        String[] c = cred == null ? new String[0] : cred.split("/");
        String signature = r.q("X-Goog-Signature");
        String signedHeaders = r.q("X-Goog-SignedHeaders");
        String date = r.q("X-Goog-Date");
        if (c.length != 5 || signature == null || signedHeaders == null || date == null || r.q("X-Goog-Expires") == null) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Missing or malformed signed URL parameters.");
        }
        PublicKey key = cfg.signingKeys.get(c[0]);
        if (key == null) {
            throw new GcsException(403, "forbidden", "AccessDenied", "No signing key is configured for " + c[0]
                    + " (WARP_GCSWIRE_SIGNING_KEYS).");
        }
        long expires = parseLong(r.q("X-Goog-Expires"), -1);
        if (expires < 1 || expires > 604800) {
            throw new GcsException(400, "invalid", "InvalidArgument", "X-Goog-Expires must be between 1 and 604800 seconds.");
        }
        Instant t = time(date);
        Instant now = Instant.now();
        if (now.getEpochSecond() - t.getEpochSecond() > expires) {
            throw new GcsException(400, "invalid", "ExpiredToken", "Request expired. Current time is " + now + ", expiration time is "
                    + t.plusSeconds(expires));
        }
        if (t.getEpochSecond() - now.getEpochSecond() > MAX_SKEW) {
            throw new GcsException(403, "forbidden", "AccessDenied", "Request is not yet valid.");
        }
        String canonical = canonicalRequest(r, signedHeaders, "UNSIGNED-PAYLOAD", "X-Goog-Signature");
        String toSign = "GOOG4-RSA-SHA256\n" + date + "\n" + String.join("/", Arrays.copyOfRange(c, 1, 5)) + "\n"
                + sha256Hex(canonical.getBytes(StandardCharsets.UTF_8));
        verifyRsa(key, toSign, hexBytes(signature));
        return "signed-url:" + c[0];
    }

    private String signedUrlV2(GcsReq r) {
        String email = r.q("GoogleAccessId");
        PublicKey key = cfg.signingKeys.get(email);
        if (key == null) {
            throw new GcsException(403, "forbidden", "AccessDenied", "No signing key is configured for " + email
                    + " (WARP_GCSWIRE_SIGNING_KEYS).");
        }
        long expires = parseLong(r.q("Expires"), -1);
        if (expires < 0) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Missing or malformed Expires parameter.");
        }
        if (Instant.now().getEpochSecond() > expires) {
            throw new GcsException(400, "invalid", "ExpiredToken", "Request expired.");
        }
        StringBuilder ext = new StringBuilder();
        TreeMap<String, String> goog = new TreeMap<>();
        r.headers.forEach((k, v) -> {
            if (k.startsWith("x-goog-") && !k.equals("x-goog-encryption-key")) {
                goog.put(k, String.join(",", v).trim());
            }
        });
        goog.forEach((k, v) -> ext.append(k).append(':').append(v).append('\n'));
        String resource = S3SigV4Verifier.percentDecode(r.rawPath, false);
        String toSign = r.method + "\n" + nz(r.header("content-md5")) + "\n" + nz(r.header("content-type")) + "\n" + expires + "\n" + ext
                + resource;
        verifyRsa(key, toSign, Base64.getMimeDecoder().decode(r.q("Signature").replace(' ', '+')));
        return "signed-url:" + email;
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static void verifyRsa(PublicKey key, String data, byte[] sig) {
        try {
            Signature s = Signature.getInstance("SHA256withRSA");
            s.initVerify(key);
            s.update(data.getBytes(StandardCharsets.UTF_8));
            if (!s.verify(sig)) {
                throw new GcsException(403, "forbidden", "SignatureDoesNotMatch",
                        "The request signature we calculated does not match the signature you provided.");
            }
        } catch (GcsException e) {
            throw e;
        } catch (Exception e) {
            throw new GcsException(403, "forbidden", "SignatureDoesNotMatch", "Invalid signature: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

    static String canonicalRequest(GcsReq r, String signedHeaders, String payloadHash, String excludeParam) {
        List<String[]> pairs = new ArrayList<>();
        if (r.rawQuery != null && !r.rawQuery.isEmpty()) {
            for (String part : r.rawQuery.split("&")) {
                if (part.isEmpty()) {
                    continue;
                }
                int eq = part.indexOf('=');
                String name = S3SigV4Verifier.percentDecode(eq < 0 ? part : part.substring(0, eq), true);
                String value = eq < 0 ? "" : S3SigV4Verifier.percentDecode(part.substring(eq + 1), true);
                if (name.equals(excludeParam)) {
                    continue;
                }
                pairs.add(new String[] {S3SigV4Verifier.encode(name, true), S3SigV4Verifier.encode(value, true)});
            }
        }
        pairs.sort((a, b) -> a[0].equals(b[0]) ? a[1].compareTo(b[1]) : a[0].compareTo(b[0]));
        StringBuilder q = new StringBuilder();
        for (String[] p : pairs) {
            q.append(q.length() > 0 ? "&" : "").append(p[0]).append('=').append(p[1]);
        }
        StringBuilder h = new StringBuilder();
        for (String name : signedHeaders.split(";")) {
            List<String> vals = r.headers.get(name.toLowerCase(Locale.ROOT));
            if (vals == null) {
                if (name.equalsIgnoreCase("host") && r.host != null) {
                    vals = List.of(r.host);
                } else {
                    throw new GcsException(403, "forbidden", "SignatureDoesNotMatch", "Signed header missing from request: " + name);
                }
            }
            List<String> cleaned = new ArrayList<>();
            for (String v : vals) {
                cleaned.add(v.trim().replaceAll("\\s+", " "));
            }
            h.append(name.toLowerCase(Locale.ROOT)).append(':').append(String.join(",", cleaned)).append('\n');
        }
        return r.method + "\n" + S3SigV4Verifier.canonicalUri(r.rawPath) + "\n" + q + "\n" + h + "\n" + signedHeaders + "\n" + payloadHash;
    }

    private static String first(GcsReq r, String name) {
        List<String> v = r.headers.get(name);
        return v == null || v.isEmpty() ? null : v.get(0);
    }

    private static Instant time(String amzDate) {
        try {
            return LocalDateTime.parse(amzDate, DATE).toInstant(ZoneOffset.UTC);
        } catch (RuntimeException e) {
            throw new GcsException(400, "invalid", "InvalidArgument", "Missing or malformed date.");
        }
    }

    private static long parseLong(String s, long dflt) {
        try {
            return Long.parseLong(s.trim());
        } catch (RuntimeException e) {
            return dflt;
        }
    }

    static String sha256Hex(byte[] b) {
        try {
            return S3SigV4Verifier.hex(MessageDigest.getInstance("SHA-256").digest(b));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] hexBytes(String h) {
        if (h.length() % 2 != 0) {
            throw new GcsException(403, "forbidden", "SignatureDoesNotMatch", "Malformed signature.");
        }
        byte[] out = new byte[h.length() / 2];
        try {
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
            }
        } catch (NumberFormatException e) {
            throw new GcsException(403, "forbidden", "SignatureDoesNotMatch", "Malformed signature.");
        }
        return out;
    }

    @SuppressWarnings("unused")
    private static Map<String, String> unused() {
        return Map.of();
    }
}
