package com.nexagres.wire.secrets;

/**
 * A password field that isn't a literal password -- a pointer to where the real value lives.
 * Written inline wherever a plaintext password could go (backend specs, JDBC connect options),
 * recognized by a scheme prefix so a plain password (no prefix) keeps working exactly as before:
 *
 * <pre>
 *   vault:secret/data/prod/postgres#password          -- HashiCorp Vault KV v2
 *   vault:secret/data/prod/postgres?field=db_password  -- same, explicit field name
 *   cyberark:AppID=Warp&Safe=DB-Secrets&Object=prod-postgres  -- CyberArk Central Credential Provider
 * </pre>
 *
 * Resolved on every connection attempt (not cached across the process lifetime) so a rotated
 * secret in Vault/CyberArk takes effect on the next connect, not just on restart -- the same
 * reasoning as every other live-reloadable piece of Warp's config.
 */
public sealed interface SecretRef {

    record Plaintext(String value) implements SecretRef {
    }

    record Vault(String path, String field) implements SecretRef {
    }

    record CyberArk(String query) implements SecretRef {
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
        return new Plaintext(raw);
    }

    /** True if this is a reference to an external secret store, not a literal value already in hand. */
    default boolean isExternal() {
        return !(this instanceof Plaintext);
    }
}
