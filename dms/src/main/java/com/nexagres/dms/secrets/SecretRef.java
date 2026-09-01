package com.nexagres.dms.secrets;

/**
 * A password field that isn't a literal password -- a pointer to where the real value lives.
 * Written inline wherever a plaintext password could go (a target connection's stored
 * credential), recognized by a scheme prefix so a plain password (no prefix) keeps working
 * exactly as before. Vault/CyberArk match the same grammar and resolver shape as Warp's own
 * {@code com.nexagres.wire.secrets} package -- no shared library between the two Maven modules,
 * so this is a deliberate small duplication rather than a new inter-module dependency. The three
 * hyperscaler schemes ({@code awssm:}/{@code azurekv:}/{@code gcpsm:}) are new to this module,
 * not ported from wire.
 *
 * <pre>
 *   vault:secret/data/prod/oracle#password
 *   cyberark:AppID=Advisor&amp;Safe=DB-Secrets&amp;Object=prod-oracle
 *   awssm:prod/oracle-password
 *   awssm:prod/oracle-password?region=us-east-1&amp;field=password
 *   azurekv:my-vault/oracle-password
 *   azurekv:my-vault/oracle-password?version=3fa2...
 *   gcpsm:my-project/oracle-password
 *   gcpsm:my-project/oracle-password?version=5
 * </pre>
 */
public sealed interface SecretRef {

    record Plaintext(String value) implements SecretRef {
    }

    record Vault(String path, String field) implements SecretRef {
    }

    record CyberArk(String query) implements SecretRef {
    }

    /** @param field a top-level JSON key within the secret string to extract, or {@code null} to
     *     use the whole secret string as-is (AWS Secrets Manager secrets are often a single
     *     password string, but can also be a JSON blob with multiple fields -- this mirrors
     *     {@link Vault}'s own {@code field} for the same reason). */
    record AwsSecretsManager(String secretId, String region, String field) implements SecretRef {
    }

    /** @param version the specific secret version, or {@code null} for Key Vault's own "current"
     *     version (Key Vault has no separate "latest" alias the way AWS/GCP do -- omitting the
     *     version segment from the URL is itself what means "current"). */
    record AzureKeyVault(String vaultName, String secretName, String version) implements SecretRef {
    }

    /** @param version a specific numbered version, or {@code null} for {@code "latest"}. */
    record GcpSecretManager(String projectId, String secretId, String version) implements SecretRef {
    }

    static SecretRef parse(String raw) {
        if (raw == null) {
            return new Plaintext(null);
        }
        if (raw.startsWith("vault:")) {
            String rest = raw.substring("vault:".length());
            int hash = rest.indexOf('#');
            int q = rest.indexOf('?');
            String path;
            String field = "password";
            if (hash >= 0) {
                path = rest.substring(0, hash);
                field = rest.substring(hash + 1);
            } else if (q >= 0) {
                path = rest.substring(0, q);
                String query = rest.substring(q + 1);
                for (String param : query.split("&")) {
                    String[] kv = param.split("=", 2);
                    if (kv.length == 2 && kv[0].equals("field")) {
                        field = kv[1];
                    }
                }
            } else {
                path = rest;
            }
            return new Vault(path, field);
        }
        if (raw.startsWith("cyberark:")) {
            return new CyberArk(raw.substring("cyberark:".length()));
        }
        if (raw.startsWith("awssm:")) {
            String rest = raw.substring("awssm:".length());
            int q = rest.indexOf('?');
            String secretId = q >= 0 ? rest.substring(0, q) : rest;
            String region = null;
            String field = null;
            if (q >= 0) {
                for (String param : rest.substring(q + 1).split("&")) {
                    String[] kv = param.split("=", 2);
                    if (kv.length == 2 && kv[0].equals("region")) {
                        region = kv[1];
                    } else if (kv.length == 2 && kv[0].equals("field")) {
                        field = kv[1];
                    }
                }
            }
            return new AwsSecretsManager(secretId, region, field);
        }
        if (raw.startsWith("azurekv:")) {
            String rest = raw.substring("azurekv:".length());
            int q = rest.indexOf('?');
            String pathPart = q >= 0 ? rest.substring(0, q) : rest;
            String version = null;
            if (q >= 0) {
                for (String param : rest.substring(q + 1).split("&")) {
                    String[] kv = param.split("=", 2);
                    if (kv.length == 2 && kv[0].equals("version")) {
                        version = kv[1];
                    }
                }
            }
            int slash = pathPart.indexOf('/');
            if (slash < 0) {
                throw new IllegalArgumentException("azurekv: reference must be \"vaultName/secretName\", got \""
                        + raw + "\"");
            }
            return new AzureKeyVault(pathPart.substring(0, slash), pathPart.substring(slash + 1), version);
        }
        if (raw.startsWith("gcpsm:")) {
            String rest = raw.substring("gcpsm:".length());
            int q = rest.indexOf('?');
            String pathPart = q >= 0 ? rest.substring(0, q) : rest;
            String version = null;
            if (q >= 0) {
                for (String param : rest.substring(q + 1).split("&")) {
                    String[] kv = param.split("=", 2);
                    if (kv.length == 2 && kv[0].equals("version")) {
                        version = kv[1];
                    }
                }
            }
            int slash = pathPart.indexOf('/');
            if (slash < 0) {
                throw new IllegalArgumentException("gcpsm: reference must be \"projectId/secretId\", got \""
                        + raw + "\"");
            }
            return new GcpSecretManager(pathPart.substring(0, slash), pathPart.substring(slash + 1), version);
        }
        return new Plaintext(raw);
    }

    default boolean isExternal() {
        return !(this instanceof Plaintext);
    }
}
