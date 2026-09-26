package com.sayonora.wire.azurewire;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * azurewire configuration (environment).
 *
 * <ul>
 *   <li>{@code WARP_AZURE_ACCOUNTS} -- {@code account1:base64key;account2:base64key} (the storage accounts clients sign with)</li>
 *   <li>{@code WARP_AZURE_DEV_ACCOUNT=true} -- opt in to Azurite's well-known {@code devstoreaccount1} and its PUBLIC dev key</li>
 *   <li>{@code WARP_AZURE_DOMAIN} -- domain for host-style addressing ({@code <account>.blob.<domain>}), default {@code localhost}</li>
 *   <li>{@code WARP_AZURE_BEARER_TOKEN} -- one static token accepted as an Entra ID bearer token (test use; not validated as a JWT)</li>
 * </ul>
 */
public final class AzureConfig {

    public static final String DEV_ACCOUNT = "devstoreaccount1";
    public static final String DEV_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    public record Account(String name, String keyBase64, byte[] key) {
    }

    private final Map<String, Account> accounts;
    private final String domain;
    private final String bearerToken;

    public AzureConfig(Map<String, String> accountKeys, String domain, String bearerToken) {
        Map<String, Account> m = new LinkedHashMap<>();
        accountKeys.forEach((n, k) -> m.put(n.toLowerCase(Locale.ROOT), new Account(n.toLowerCase(Locale.ROOT), k,
                Base64.getDecoder().decode(k.getBytes(StandardCharsets.US_ASCII)))));
        this.accounts = Map.copyOf(m);
        this.domain = domain == null || domain.isBlank() ? "localhost" : domain.trim().toLowerCase(Locale.ROOT);
        this.bearerToken = bearerToken == null || bearerToken.isBlank() ? null : bearerToken.trim();
    }

    public static AzureConfig fromEnv() {
        Map<String, String> keys = new LinkedHashMap<>();
        String spec = System.getenv("WARP_AZURE_ACCOUNTS");
        if (spec != null) {
            for (String part : spec.split(";")) {
                String p = part.trim();
                if (p.isEmpty()) {
                    continue;
                }
                int c = p.indexOf(':');
                if (c <= 0 || c == p.length() - 1) {
                    throw new IllegalArgumentException("WARP_AZURE_ACCOUNTS entries are account:base64key, got \"" + p + "\"");
                }
                String name = p.substring(0, c).trim();
                if (!name.matches("[a-z0-9]{3,24}")) {
                    throw new IllegalArgumentException("storage account name \"" + name
                            + "\" must be 3-24 lowercase letters/digits");
                }
                try {
                    Base64.getDecoder().decode(p.substring(c + 1).trim());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("account key of \"" + name + "\" is not valid base64");
                }
                keys.put(name, p.substring(c + 1).trim());
            }
        }
        if ("true".equalsIgnoreCase(System.getenv("WARP_AZURE_DEV_ACCOUNT"))) {
            keys.putIfAbsent(DEV_ACCOUNT, DEV_KEY);
        }
        return new AzureConfig(keys, System.getenv("WARP_AZURE_DOMAIN"), System.getenv("WARP_AZURE_BEARER_TOKEN"));
    }

    public boolean hasAccounts() {
        return !accounts.isEmpty();
    }

    public Account account(String name) {
        return name == null ? null : accounts.get(name.toLowerCase(Locale.ROOT));
    }

    public String domain() {
        return domain;
    }

    public String bearerToken() {
        return bearerToken;
    }
}
