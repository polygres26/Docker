package com.sayonora.warp.cosmoswire;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Cosmos DB REST authorization. Master key: {@code type=master&ver=1.0&sig=<base64 HMAC-SHA256>} over
 * {@code lower(verb) \n lower(resourceType) \n resourceLink \n lower(x-ms-date) \n "" \n} with the base64-decoded key (documented in
 * "Access control in the Azure Cosmos DB SQL REST API"). Also accepted: {@code type=resource} tokens and {@code type=aad} bearer tokens
 * from the configured allow-lists (neither is cryptographically validated: see {@link CosmosConfig}).
 */
final class CosmosAuth {

    private final CosmosConfig cfg;

    CosmosAuth(CosmosConfig cfg) {
        this.cfg = cfg;
    }

    /** The resource type and link Cosmos signs for a request path (no leading slash, segments percent-decoded or not). */
    static String[] resourceOf(String path) {
        String p = path.startsWith("/") ? path.substring(1) : path;
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        if (p.isEmpty()) {
            return new String[] {"", ""};
        }
        String[] seg = p.split("/");
        if (seg.length % 2 == 1) {
            String type = seg[seg.length - 1];
            return new String[] {type, p.substring(0, Math.max(0, p.length() - type.length() - 1))};
        }
        return new String[] {seg[seg.length - 2], p};
    }

    static String sign(byte[] key, String verb, String type, String link, String date) {
        String payload = verb.toLowerCase(Locale.ROOT) + "\n" + type.toLowerCase(Locale.ROOT) + "\n" + link + "\n" + date.toLowerCase(Locale.ROOT)
                + "\n\n";
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return Base64.getEncoder().encodeToString(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean eq(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    /** Verifies a request; throws 401 (or 403 for a resource token outside its scope). */
    void check(String verb, String rawPath, String decodedPath, String date, String authorization) {
        if (!cfg.authRequired) {
            return;
        }
        if (authorization == null || authorization.isBlank()) {
            throw unauthorized("Authorization header is missing.", null);
        }
        String type = null;
        String sig = null;
        // SDKs percent-encode the whole header value ("type%3Dmaster%26ver%3D1.0%26sig%3D..."); a literal '+' stays a '+'
        String decoded = URLDecoder.decode(authorization.replace("+", "%2B"), StandardCharsets.UTF_8);
        for (String part : decoded.split("&")) {
            int eq = part.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String k = part.substring(0, eq).toLowerCase(Locale.ROOT);
            String v = part.substring(eq + 1);
            if (k.equals("type")) {
                type = v.toLowerCase(Locale.ROOT);
            } else if (k.equals("sig")) {
                sig = v;
            }
        }
        if (authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            type = "aad";
            sig = authorization.substring(7).trim();
        }
        if (type == null || sig == null) {
            throw unauthorized("The authorization header is malformed.", null);
        }
        if (type.equals("aad")) {
            if (cfg.aadTokens.contains("*") || cfg.aadTokens.contains(sig)) {
                return;
            }
            throw unauthorized("The AAD bearer token is not accepted by this Warp (WARP_COSMOSWIRE_AAD_TOKENS).", null);
        }
        String[] r = resourceOf(decodedPath);
        if (type.equals("resource")) {
            String scope = cfg.resourceTokens.get(sig);
            if (scope == null) {
                throw unauthorized("The resource token is not accepted by this Warp (WARP_COSMOSWIRE_RESOURCE_TOKENS).", null);
            }
            if (!scope.isEmpty() && !(r[1].equals(scope) || r[1].startsWith(scope + "/") || decodedPath.replaceFirst("^/", "").startsWith(scope))) {
                throw new CosmosException(403, "Forbidden", "The resource token does not grant access to " + decodedPath);
            }
            return;
        }
        if (!type.equals("master")) {
            throw unauthorized("Unsupported authorization type '" + type + "'.", null);
        }
        if (date == null || date.isBlank()) {
            throw unauthorized("The x-ms-date (or Date) header is required.", null);
        }
        try {
            long t = ZonedDateTime.parse(date, DateTimeFormatter.RFC_1123_DATE_TIME).toEpochSecond();
            if (Math.abs(Instant.now().getEpochSecond() - t) > cfg.maxSkewSeconds) {
                throw unauthorized("The request date is outside the allowed clock skew of " + cfg.maxSkewSeconds + " seconds.", null);
            }
        } catch (java.time.format.DateTimeParseException e) {
            throw unauthorized("The x-ms-date header is not a valid HTTP date.", null);
        }
        String rawLink = resourceOf(rawPath)[1];
        // name-based links are signed whole ("dbs/db/colls/c"); rid-based ones by the last resource id only ("<rid>", any case)
        String last = r[1].contains("/") ? r[1].substring(r[1].lastIndexOf('/') + 1) : r[1];
        String[] candidates = {r[1], r[1].toLowerCase(Locale.ROOT), rawLink, last, last.toLowerCase(Locale.ROOT)};
        for (byte[] key : cfg.keys) {
            for (String link : candidates) {
                if (eq(sign(key, verb, r[0], link, date), sig)) {
                    return;
                }
            }
        }
        String payload = verb.toLowerCase(Locale.ROOT) + "\n" + r[0].toLowerCase(Locale.ROOT) + "\n" + r[1] + "\n" + date.toLowerCase(Locale.ROOT) + "\n\n";
        throw unauthorized("The input authorization token can't serve the request. The wrong key is being used or the expected payload is not built "
                + "as per the protocol.", payload);
    }

    private static CosmosException unauthorized(String why, String payload) {
        String m = why + (payload == null ? "" : " Server used the following payload to sign: '" + payload.replace("\n", "\\n") + "'");
        return new CosmosException(401, "Unauthorized", m);
    }
}
