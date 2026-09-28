package com.sayonora.warp.tls;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resolved TLS configuration for one listener. Global variables ({@code WARP_TLS_*}) apply to every listener; a
 * per-listener prefix ({@code WARP_<NAME>_TLS_*}, e.g. {@code WARP_MCP_TLS_CERT}) overrides them for that listener.
 *
 * <p>Key material source (first match wins, evaluated at the listener level and then globally):
 * <ol>
 *   <li>{@code *_TLS_KEYSTORE} (+ {@code *_TLS_KEYSTORE_PASSWORD}): PKCS12 or JKS (type auto-detected);</li>
 *   <li>{@code *_TLS_CERT} + {@code *_TLS_KEY}: PEM chain and PEM private key (+ optional {@code *_TLS_KEY_PASSWORD});</li>
 *   <li>{@code *_TLS_SELF_SIGNED=true}: an in-memory self-signed cert for localhost (development only).</li>
 * </ol>
 * Other options (listener level falling back to global): {@code TLS_CA} (PEM trust anchors for client certificates),
 * {@code TLS_CLIENT_AUTH=none|want|need}, {@code TLS_MIN_VERSION} (default TLSv1.2), {@code TLS_RELOAD_SECONDS}
 * (mtime poll, default 30, 0 disables). {@code WARP_<NAME>_TLS_DISABLED=true} opts a listener out of a global config.
 */
public record TlsSettings(Source source, Path keystore, String keystorePassword, Path cert, Path key, String keyPassword,
        Path ca, ClientAuth clientAuth, String minVersion, int reloadSeconds, List<String> selfSignedSans, String origin) {

    public enum Source { KEYSTORE, PEM, SELF_SIGNED }

    public enum ClientAuth { NONE, WANT, NEED }

    /** @return the settings for {@code listener} (e.g. "MCP"), or null when TLS is not configured for it. */
    public static TlsSettings resolve(String listener, Map<String, String> env) {
        String p = "WARP_" + listener.toUpperCase(Locale.ROOT) + "_TLS_";
        if (isTrue(env.get(p + "DISABLED"))) {
            return null;
        }
        TlsSettings s = fromPrefix(p, "WARP_" + listener.toUpperCase(Locale.ROOT) + "_TLS_*", p, env);
        if (s == null) {
            s = fromPrefix("WARP_TLS_", "WARP_TLS_*", p, env);
            // Built-in ACME supplies the global material unless the operator pointed WARP_TLS_CERT/KEY (or a keystore) at
            // their own files; a dev-only WARP_TLS_SELF_SIGNED does not override a real ACME certificate.
            java.nio.file.Path[] acme = com.sayonora.warp.tls.acme.AcmeService.managedPaths();
            if (acme != null && (s == null || s.source() == Source.SELF_SIGNED)) {
                Map<String, String> synth = new java.util.HashMap<>(env);
                synth.remove("WARP_TLS_KEYSTORE");
                synth.remove("WARP_TLS_SELF_SIGNED");
                synth.put("WARP_TLS_CERT", acme[0].toString());
                synth.put("WARP_TLS_KEY", acme[1].toString());
                s = fromPrefix("WARP_TLS_", "ACME (WARP_ACME_DIR)", p, synth);
            }
        }
        return s;
    }

    private static TlsSettings fromPrefix(String pre, String origin, String listenerPrefix, Map<String, String> env) {
        String ks = get(env, pre + "KEYSTORE");
        String cert = get(env, pre + "CERT");
        String key = get(env, pre + "KEY");
        boolean selfSigned = isTrue(env.get(pre + "SELF_SIGNED"));
        if (ks == null && cert == null && key == null && !selfSigned) {
            return null;
        }
        Source src;
        if (ks != null) {
            src = Source.KEYSTORE;
        } else if (cert != null || key != null) {
            if (cert == null || key == null) {
                throw new TlsException(pre + "CERT and " + pre + "KEY must both be set (got only one)");
            }
            src = Source.PEM;
        } else {
            src = Source.SELF_SIGNED;
        }
        // Tunables: the listener prefix wins, then the global prefix.
        String ca = first(env, listenerPrefix + "CA", "WARP_TLS_CA");
        String ca2 = ca;
        String auth = first(env, listenerPrefix + "CLIENT_AUTH", "WARP_TLS_CLIENT_AUTH");
        String min = first(env, listenerPrefix + "MIN_VERSION", "WARP_TLS_MIN_VERSION");
        String reload = first(env, listenerPrefix + "RELOAD_SECONDS", "WARP_TLS_RELOAD_SECONDS");
        ClientAuth ca3 = auth == null ? ClientAuth.NONE : switch (auth.toLowerCase(Locale.ROOT)) {
            case "none" -> ClientAuth.NONE;
            case "want" -> ClientAuth.WANT;
            case "need", "require", "required" -> ClientAuth.NEED;
            default -> throw new TlsException("TLS_CLIENT_AUTH must be none, want or need (got \"" + auth + "\")");
        };
        if (ca3 != ClientAuth.NONE && ca2 == null && src != Source.KEYSTORE) {
            throw new TlsException("TLS_CLIENT_AUTH=" + ca3.name().toLowerCase(Locale.ROOT)
                    + " needs WARP_TLS_CA (a PEM file of trusted client CAs)");
        }
        int reloadSecs = 30;
        if (reload != null) {
            try {
                reloadSecs = Integer.parseInt(reload.trim());
            } catch (NumberFormatException e) {
                throw new TlsException("TLS_RELOAD_SECONDS must be an integer (got \"" + reload + "\")");
            }
        }
        String minVersion = normalizeVersion(min == null ? "TLSv1.2" : min);
        List<String> sans = List.of();
        String sanSpec = first(env, listenerPrefix + "SELF_SIGNED_SANS", "WARP_TLS_SELF_SIGNED_SANS");
        if (sanSpec != null) {
            sans = java.util.Arrays.stream(sanSpec.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
        }
        return new TlsSettings(src, ks == null ? null : Path.of(ks), first(env, pre + "KEYSTORE_PASSWORD"),
                cert == null ? null : Path.of(cert), key == null ? null : Path.of(key),
                first(env, pre + "KEY_PASSWORD"), ca2 == null ? null : Path.of(ca2), ca3, minVersion, reloadSecs, sans,
                origin);
    }

    static String normalizeVersion(String v) {
        String t = v.trim().toLowerCase(Locale.ROOT).replace("tlsv", "").replace("tls", "").replace("_", ".");
        return switch (t) {
            case "1.2", "12" -> "TLSv1.2";
            case "1.3", "13" -> "TLSv1.3";
            default -> throw new TlsException("TLS_MIN_VERSION must be TLS1.2 or TLS1.3 (got \"" + v + "\")");
        };
    }

    /** Protocols enabled for this minimum version. */
    public String[] protocols() {
        return minVersion.equals("TLSv1.3") ? new String[] {"TLSv1.3"} : new String[] {"TLSv1.3", "TLSv1.2"};
    }

    /** Identity of the material: two listeners with an equal key share one provider. */
    public String cacheKey() {
        return source + "|" + keystore + "|" + cert + "|" + key + "|" + ca + "|" + clientAuth + "|" + minVersion + "|"
                + selfSignedSans;
    }

    private static String get(Map<String, String> env, String k) {
        String v = env.get(k);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private static String first(Map<String, String> env, String... keys) {
        for (String k : keys) {
            String v = get(env, k);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    private static boolean isTrue(String v) {
        return v != null && (v.equalsIgnoreCase("true") || v.equals("1") || v.equalsIgnoreCase("yes"));
    }
}
