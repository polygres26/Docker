package com.sayonora.warp.cosmoswire;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * cosmoswire configuration (environment).
 * <ul>
 *   <li>{@code WARP_COSMOSWIRE_KEYS} -- comma list of base64 master keys. Default: the well-known Cosmos DB emulator key.</li>
 *   <li>{@code WARP_COSMOSWIRE_AUTH=false} -- do not check the Authorization header (development).</li>
 *   <li>{@code WARP_COSMOSWIRE_MAX_SKEW_SECONDS} -- allowed difference between x-ms-date and now (default 900, like the service).</li>
 *   <li>{@code WARP_COSMOSWIRE_RU} -- a constant x-ms-request-charge for every request (default: derived from the payload size).</li>
 *   <li>{@code WARP_COSMOSWIRE_ADVERTISED_URL} -- the endpoint written into the account document (default: built from the Host header).</li>
 *   <li>{@code WARP_COSMOSWIRE_AAD_TOKENS} -- comma list of accepted {@code type=aad} bearer tokens ({@code *} accepts any: NOT validated as JWTs).</li>
 *   <li>{@code WARP_COSMOSWIRE_RESOURCE_TOKENS} -- comma list of {@code token} or {@code token@dbs/db/colls/c}: accepted {@code type=resource}
 *       tokens, optionally limited to a resource link prefix. Warp has no users/permissions API to mint them.</li>
 *   <li>{@code WARP_COSMOSWIRE_SWEEP_SECONDS} -- TTL sweeper interval (default 5).</li>
 * </ul>
 */
final class CosmosConfig {

    static final String EMULATOR_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    final List<byte[]> keys;
    final boolean authRequired;
    final long maxSkewSeconds;
    final Double fixedRu;
    final String advertisedUrl;
    final Set<String> aadTokens;
    final Map<String, String> resourceTokens;
    final long sweepSeconds;

    CosmosConfig(List<String> keys, boolean authRequired, long maxSkewSeconds, Double fixedRu, String advertisedUrl, Set<String> aad,
            Map<String, String> resourceTokens, long sweepSeconds) {
        List<byte[]> ks = new ArrayList<>();
        for (String k : keys) {
            ks.add(Base64.getDecoder().decode(k.trim()));
        }
        this.keys = ks;
        this.authRequired = authRequired;
        this.maxSkewSeconds = maxSkewSeconds;
        this.fixedRu = fixedRu;
        this.advertisedUrl = advertisedUrl;
        this.aadTokens = aad;
        this.resourceTokens = resourceTokens;
        this.sweepSeconds = sweepSeconds;
    }

    static CosmosConfig defaults() {
        return new CosmosConfig(List.of(EMULATOR_KEY), true, 900, null, null, Set.of(), Map.of(), 5);
    }

    private static String env(String n) {
        String v = System.getenv(n);
        return v == null || v.isBlank() ? null : v.trim();
    }

    static CosmosConfig fromEnv() {
        List<String> keys = new ArrayList<>();
        String k = env("WARP_COSMOSWIRE_KEYS");
        if (k != null) {
            for (String p : k.split(",")) {
                if (!p.isBlank()) {
                    keys.add(p.trim());
                }
            }
        }
        if (keys.isEmpty()) {
            keys.add(EMULATOR_KEY);
        }
        Set<String> aad = new LinkedHashSet<>();
        String a = env("WARP_COSMOSWIRE_AAD_TOKENS");
        if (a != null) {
            for (String p : a.split(",")) {
                if (!p.isBlank()) {
                    aad.add(p.trim());
                }
            }
        }
        Map<String, String> rt = new LinkedHashMap<>();
        String r = env("WARP_COSMOSWIRE_RESOURCE_TOKENS");
        if (r != null) {
            for (String p : r.split(",")) {
                if (!p.isBlank()) {
                    int at = p.indexOf('@');
                    rt.put(at < 0 ? p.trim() : p.substring(0, at).trim(), at < 0 ? "" : p.substring(at + 1).trim());
                }
            }
        }
        String ru = env("WARP_COSMOSWIRE_RU");
        long skew = 900;
        String sk = env("WARP_COSMOSWIRE_MAX_SKEW_SECONDS");
        if (sk != null) {
            skew = Long.parseLong(sk);
        }
        long sweep = 5;
        String sw = env("WARP_COSMOSWIRE_SWEEP_SECONDS");
        if (sw != null) {
            sweep = Math.max(1, Long.parseLong(sw));
        }
        return new CosmosConfig(keys, !"false".equalsIgnoreCase(env("WARP_COSMOSWIRE_AUTH")), skew, ru == null ? null : Double.parseDouble(ru),
                env("WARP_COSMOSWIRE_ADVERTISED_URL"), aad, rt, sweep);
    }
}
