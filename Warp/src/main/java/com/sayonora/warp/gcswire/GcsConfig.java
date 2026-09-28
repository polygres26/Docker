package com.sayonora.warp.gcswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * gcswire configuration (environment).
 *
 * <ul>
 *   <li>{@code WARP_GCSWIRE_TOKENS} -- comma list of accepted OAuth2 bearer tokens (not validated as JWTs)</li>
 *   <li>{@code WARP_GCSWIRE_ALLOW_ANONYMOUS=true} -- accept requests with no credentials (what fake-gcs-server does)</li>
 *   <li>{@code WARP_GCSWIRE_SIGNING_KEYS} -- {@code email=/path/key.pem;email2=/path/service-account.json}: keys that verify V2/V4
 *       signed URLs. A value may be a PEM public key, a PEM certificate, a PEM PKCS#8 private key or a service-account JSON
 *       file (its {@code private_key}; the email defaults to its {@code client_email})</li>
 *   <li>{@code WARP_GCSWIRE_DOMAIN} -- host suffix for virtual-hosted XML requests, default {@code storage.googleapis.com}</li>
 *   <li>{@code WARP_GCSWIRE_LOCATION} -- location of buckets created without one, default {@code US}</li>
 *   <li>{@code WARP_GCSWIRE_CHUNK_BYTES} / {@code WARP_GCSWIRE_PROBE_OTHER_SHARDS} / {@code WARP_GCSWIRE_GC_GRACE_SECONDS} /
 *       {@code WARP_GCSWIRE_SESSION_TTL_SECONDS} -- storage tunables</li>
 * </ul>
 */
final class GcsConfig {

    final Set<String> tokens;
    final boolean allowAnonymous;
    final Map<String, PublicKey> signingKeys;
    final String domain;
    final String location;
    final int chunkBytes;
    final boolean probeOtherShards;
    final long gcGraceSeconds;
    final long sessionTtlSeconds;

    GcsConfig(Set<String> tokens, boolean allowAnonymous, Map<String, PublicKey> signingKeys, String domain, String location,
            int chunkBytes, boolean probeOtherShards, long gcGraceSeconds, long sessionTtlSeconds) {
        this.tokens = Set.copyOf(tokens);
        this.allowAnonymous = allowAnonymous;
        this.signingKeys = Map.copyOf(signingKeys);
        this.domain = domain == null || domain.isBlank() ? "storage.googleapis.com" : domain.trim().toLowerCase(Locale.ROOT);
        this.location = location == null || location.isBlank() ? "US" : location.trim().toUpperCase(Locale.ROOT);
        this.chunkBytes = chunkBytes;
        this.probeOtherShards = probeOtherShards;
        this.gcGraceSeconds = gcGraceSeconds;
        this.sessionTtlSeconds = sessionTtlSeconds;
    }

    static GcsConfig testDefaults(boolean anonymous) {
        return new GcsConfig(Set.of(), anonymous, Map.of(), null, null, 4 * 1024 * 1024, false, 600, 7 * 86400);
    }

    static GcsConfig fromEnv() {
        Set<String> tokens = new LinkedHashSet<>();
        String t = System.getenv("WARP_GCSWIRE_TOKENS");
        if (t != null) {
            for (String p : t.split(",")) {
                if (!p.isBlank()) {
                    tokens.add(p.trim());
                }
            }
        }
        Map<String, PublicKey> keys = new LinkedHashMap<>();
        String k = System.getenv("WARP_GCSWIRE_SIGNING_KEYS");
        if (k != null) {
            for (String part : k.split(";")) {
                if (part.isBlank()) {
                    continue;
                }
                int eq = part.indexOf('=');
                String email = eq > 0 ? part.substring(0, eq).trim() : null;
                String path = eq > 0 ? part.substring(eq + 1).trim() : part.trim();
                try {
                    String content = Files.readString(Path.of(path), StandardCharsets.UTF_8);
                    if (content.trim().startsWith("{")) {
                        var o = JsonParser.parseString(content).getAsJsonObject();
                        if (email == null) {
                            email = str(o, "client_email");
                        }
                        content = str(o, "private_key");
                    }
                    if (email == null) {
                        throw new IllegalArgumentException("no service-account email for " + path);
                    }
                    keys.put(email, parseKey(content));
                } catch (Exception e) {
                    throw new IllegalStateException("WARP_GCSWIRE_SIGNING_KEYS entry \"" + part + "\": " + e.getMessage(), e);
                }
            }
        }
        return new GcsConfig(tokens, "true".equalsIgnoreCase(System.getenv("WARP_GCSWIRE_ALLOW_ANONYMOUS")), keys,
                System.getenv("WARP_GCSWIRE_DOMAIN"), System.getenv("WARP_GCSWIRE_LOCATION"),
                (int) Math.max(64 * 1024, Math.min(64 << 20, num("WARP_GCSWIRE_CHUNK_BYTES", 4 << 20))),
                "true".equalsIgnoreCase(System.getenv("WARP_GCSWIRE_PROBE_OTHER_SHARDS")),
                num("WARP_GCSWIRE_GC_GRACE_SECONDS", 600), num("WARP_GCSWIRE_SESSION_TTL_SECONDS", 7 * 86400));
    }

    private static String str(com.google.gson.JsonObject o, String k) {
        JsonElement e = o.get(k);
        return e == null || e.isJsonNull() ? null : e.getAsString();
    }

    private static long num(String name, long dflt) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return dflt;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be an integer, got \"" + v + "\"");
        }
    }

    /** Parses a PEM public key, certificate or PKCS#8 private key (then its public half) into an RSA public key. */
    static PublicKey parseKey(String pem) throws Exception {
        String body = pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(body);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        if (pem.contains("BEGIN CERTIFICATE")) {
            return CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der)).getPublicKey();
        }
        if (pem.contains("BEGIN PRIVATE KEY")) {
            RSAPrivateCrtKey pk = (RSAPrivateCrtKey) kf.generatePrivate(new PKCS8EncodedKeySpec(der));
            return kf.generatePublic(new RSAPublicKeySpec(pk.getModulus(), pk.getPublicExponent()));
        }
        return kf.generatePublic(new X509EncodedKeySpec(der));
    }
}
