package com.sayonora.warp.tls.acme;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** ACME configuration from the environment ({@code WARP_ACME_*}); see docs/WARP_GUIDE.md "Automatic certificates". */
public record AcmeConfig(List<String> domains, String email, String directory, boolean staging, Path dir,
        Challenge challenge, int renewDays, String keyType, int httpPort, String httpBind, boolean httpRedirect,
        String dnsProvider, String dnsHook, int dnsWaitSeconds, String dnsCheck, String dohUrl, long pollMillis,
        int pollTimeoutSeconds, String eabKid, String eabHmacKey, Path caBundle, boolean shared,
        long checkIntervalSeconds, int syncSeconds, Map<String, String> env) {

    public static final String LE_PROD = "https://acme-v02.api.letsencrypt.org/directory";
    public static final String LE_STAGING = "https://acme-staging-v02.api.letsencrypt.org/directory";

    public enum Challenge { HTTP_01, DNS_01 }

    /** Bad configuration: the message is the reason logged as {@code ACME is NOT enabled: <reason>}. */
    public static final class ConfigException extends RuntimeException {
        public ConfigException(String m) {
            super(m);
        }
    }

    /** @return null when ACME is not requested at all (WARP_ACME_DOMAINS unset). @throws ConfigException on bad settings. */
    public static AcmeConfig fromEnv(Map<String, String> env, Path workDir) {
        String doms = get(env, "WARP_ACME_DOMAINS");
        if (doms == null) {
            return null;
        }
        LinkedHashSet<String> domains = new LinkedHashSet<>();
        for (String d : doms.split(",")) {
            String t = d.trim().toLowerCase(Locale.ROOT);
            if (!t.isEmpty()) {
                domains.add(t);
            }
        }
        if (domains.isEmpty()) {
            throw new ConfigException("WARP_ACME_DOMAINS is empty");
        }
        if (!isTrue(env.get("WARP_ACME_TERMS_ACCEPTED"))) {
            throw new ConfigException("WARP_ACME_TERMS_ACCEPTED=true is required: you must explicitly accept the CA's "
                    + "subscriber agreement (Let's Encrypt: https://letsencrypt.org/repository/)");
        }
        String email = get(env, "WARP_ACME_EMAIL");
        if (email != null && !email.matches("[^@\\s,]+@[^@\\s,]+\\.[^@\\s,]+")) {
            throw new ConfigException("WARP_ACME_EMAIL is not an email address");
        }
        String chal = get(env, "WARP_ACME_CHALLENGE");
        Challenge challenge = chal == null || chal.equalsIgnoreCase("http-01") ? Challenge.HTTP_01
                : chal.equalsIgnoreCase("dns-01") ? Challenge.DNS_01 : null;
        if (challenge == null) {
            throw new ConfigException("WARP_ACME_CHALLENGE must be http-01 or dns-01 (got \"" + chal + "\")");
        }
        List<String> problems = new ArrayList<>();
        for (String d : domains) {
            String base = d.startsWith("*.") ? d.substring(2) : d;
            if (!base.matches("(?i)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z]{2,63}") && !base.matches("(?i)xn--.+")) {
                problems.add(d + " is not a valid DNS name");
            } else if (d.startsWith("*.") && challenge != Challenge.DNS_01) {
                problems.add(d + ": wildcard certificates require WARP_ACME_CHALLENGE=dns-01");
            }
            if (base.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") || base.equals("localhost")) {
                problems.add(d + ": public CAs do not issue certificates for IP addresses or localhost");
            }
        }
        if (!problems.isEmpty()) {
            throw new ConfigException(String.join("; ", problems));
        }
        boolean staging = isTrue(env.get("WARP_ACME_STAGING"));
        String directory = get(env, "WARP_ACME_DIRECTORY");
        if (directory == null) {
            directory = staging ? LE_STAGING : LE_PROD;
        } else if (!directory.startsWith("https://") && !directory.startsWith("http://")) {
            throw new ConfigException("WARP_ACME_DIRECTORY must be an http(s) URL");
        }
        String kt = get(env, "WARP_ACME_KEY_TYPE");
        String keyType = kt == null ? "ec256" : kt.toLowerCase(Locale.ROOT);
        if (!keyType.equals("ec256") && !keyType.equals("rsa2048")) {
            throw new ConfigException("WARP_ACME_KEY_TYPE must be ec256 or rsa2048 (got \"" + kt + "\")");
        }
        String dnsProvider = get(env, "WARP_ACME_DNS_PROVIDER");
        String dnsHook = get(env, "WARP_ACME_DNS_HOOK");
        if (challenge == Challenge.DNS_01) {
            if (dnsProvider == null) {
                throw new ConfigException("WARP_ACME_CHALLENGE=dns-01 needs WARP_ACME_DNS_PROVIDER=hook|cloudflare|route53");
            }
            dnsProvider = dnsProvider.toLowerCase(Locale.ROOT);
            switch (dnsProvider) {
                case "hook" -> {
                    if (dnsHook == null) {
                        throw new ConfigException("WARP_ACME_DNS_PROVIDER=hook needs WARP_ACME_DNS_HOOK (a command)");
                    }
                }
                case "cloudflare" -> {
                    if (get(env, "CLOUDFLARE_API_TOKEN") == null) {
                        throw new ConfigException("WARP_ACME_DNS_PROVIDER=cloudflare needs CLOUDFLARE_API_TOKEN");
                    }
                }
                case "route53" -> {
                    if (get(env, "AWS_ACCESS_KEY_ID") == null || get(env, "AWS_SECRET_ACCESS_KEY") == null) {
                        throw new ConfigException("WARP_ACME_DNS_PROVIDER=route53 needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");
                    }
                }
                default -> throw new ConfigException("WARP_ACME_DNS_PROVIDER must be hook, cloudflare or route53");
            }
        }
        String dnsCheck = get(env, "WARP_ACME_DNS_CHECK");
        dnsCheck = dnsCheck == null ? "none" : dnsCheck.toLowerCase(Locale.ROOT);
        if (!List.of("none", "system", "doh").contains(dnsCheck)) {
            throw new ConfigException("WARP_ACME_DNS_CHECK must be none, system or doh");
        }
        String eabKid = get(env, "WARP_ACME_EAB_KID");
        String eabKey = get(env, "WARP_ACME_EAB_HMAC_KEY");
        if ((eabKid == null) != (eabKey == null)) {
            throw new ConfigException("WARP_ACME_EAB_KID and WARP_ACME_EAB_HMAC_KEY must be set together");
        }
        String dir = get(env, "WARP_ACME_DIR");
        Path dirPath = dir == null ? workDir.resolve("acme") : Path.of(dir);
        String ca = get(env, "WARP_ACME_CA_BUNDLE");
        return new AcmeConfig(List.copyOf(domains), email, directory, staging, dirPath, challenge,
                intv(env, "WARP_ACME_RENEW_DAYS", 30, 1, 3650), keyType, intv(env, "WARP_ACME_HTTP_PORT", 8880, 0, 65535),
                get(env, "WARP_ACME_HTTP_BIND") == null ? "0.0.0.0" : get(env, "WARP_ACME_HTTP_BIND"),
                isTrue(env.get("WARP_ACME_HTTP_REDIRECT")), dnsProvider, dnsHook,
                intv(env, "WARP_ACME_DNS_WAIT_SECONDS", 30, 0, 3600), dnsCheck,
                get(env, "WARP_ACME_DOH_URL") == null ? "https://cloudflare-dns.com/dns-query" : get(env, "WARP_ACME_DOH_URL"),
                intv(env, "WARP_ACME_POLL_MILLIS", 2000, 10, 60000), intv(env, "WARP_ACME_POLL_TIMEOUT_SECONDS", 300, 5, 3600),
                eabKid, eabKey, ca == null ? null : Path.of(ca), !"false".equalsIgnoreCase(env.get("WARP_ACME_SHARED")),
                intv(env, "WARP_ACME_CHECK_SECONDS", 86400, 5, 86400 * 7), intv(env, "WARP_ACME_SYNC_SECONDS", 60, 1, 3600),
                env);
    }

    public Path fullchain() {
        return dir.resolve("fullchain.pem");
    }

    public Path privkey() {
        return dir.resolve("privkey.pem");
    }

    static String get(Map<String, String> env, String k) {
        String v = env.get(k);
        return v == null || v.isBlank() ? null : v.trim();
    }

    static boolean isTrue(String v) {
        return v != null && (v.equalsIgnoreCase("true") || v.equals("1") || v.equalsIgnoreCase("yes"));
    }

    private static int intv(Map<String, String> env, String k, int def, int min, int max) {
        String v = get(env, k);
        if (v == null) {
            return def;
        }
        try {
            int n = Integer.parseInt(v);
            if (n < min || n > max) {
                throw new ConfigException(k + " must be between " + min + " and " + max);
            }
            return n;
        } catch (NumberFormatException e) {
            throw new ConfigException(k + " must be an integer (got \"" + v + "\")");
        }
    }
}
