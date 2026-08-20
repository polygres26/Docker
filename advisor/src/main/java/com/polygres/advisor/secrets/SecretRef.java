package com.polygres.advisor.secrets;

/**
 * A password field that isn't a literal password -- a pointer to where the real value lives.
 * Written inline wherever a plaintext password could go (a target connection's stored
 * credential), recognized by a scheme prefix so a plain password (no prefix) keeps working
 * exactly as before. Same grammar and same resolver shape as PolyWire's own
 * {@code com.polygres.wire.secrets} package -- no shared library between the two Maven modules,
 * so this is a deliberate small duplication rather than a new inter-module dependency.
 *
 * <pre>
 *   vault:secret/data/prod/oracle#password
 *   cyberark:AppID=Advisor&Safe=DB-Secrets&Object=prod-oracle
 * </pre>
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

    default boolean isExternal() {
        return !(this instanceof Plaintext);
    }
}
