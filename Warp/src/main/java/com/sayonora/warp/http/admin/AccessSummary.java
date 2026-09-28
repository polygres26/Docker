package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Non-secret summary of how each frontend authenticates, for {@code GET /api/access-summary}. Everything is derived from the
 * same environment variables the frontends read at startup. It never returns a password, token or key: only the method,
 * whether it is enforced, principal NAMES (SQL user names, access-key ids, storage-account names) and counts.
 * Frontends not listed here are reported by the UI as "no auth method reported", not guessed.
 */
public final class AccessSummary {

    private AccessSummary() {
    }

    public static JsonObject toJson(Map<String, String> env) {
        JsonObject out = new JsonObject();
        String authMode = blankToNull(env.get("WARP_AUTH_MODE"));
        out.addProperty("authMode", authMode);
        boolean roles = "postgres_roles".equals(authMode);

        String creds = blankToNull(env.get("WARP_AUTH_CREDENTIALS"));
        TreeSet<String> users = names(creds, '=');
        JsonObject sql = new JsonObject();
        sql.addProperty("mode", users.isEmpty() ? "single-shared" : "multi-user");
        sql.add("users", array(users.isEmpty() ? new TreeSet<>(java.util.List.of(env.getOrDefault("WARP_AUTH_USER", "orapg"))) : users));
        out.add("sqlCredentials", sql);

        Map<String, JsonObject> f = new LinkedHashMap<>();
        for (String id : new String[] {"pgwire", "mssqlwire", "mssqlwire-native"}) {
            f.put(id, entry(roles ? "postgres-roles" : "sql-credentials", true,
                    roles ? "logins verified against Postgres roles (SCRAM)" : "user name and password from the credential store", null));
        }
        for (String id : new String[] {"mywire", "mywire-native", "orawire", "orawire-native", "orawire-tls"}) {
            f.put(id, entry("sql-credentials", true, "user name and password from the credential store", null));
        }
        for (String[] s : new String[][] {{"cqlwire", "WARP_CQLWIRE_AUTH"}, {"kafkawire", "WARP_KAFKAWIRE_AUTH"},
                {"gremlinwire", "WARP_GREMLINWIRE_AUTH"}, {"amqpwire", "WARP_AMQPWIRE_AUTH"}}) {
            String a = env.get(s[1]);
            boolean on = a != null ? "true".equalsIgnoreCase(a) : creds != null;
            f.put(s[0], entry("sasl-plain", on, on ? "against the credential store" : "open: set " + s[1] + "=true or WARP_AUTH_CREDENTIALS", users.isEmpty() ? null : users));
        }
        f.put("rediswire", entry("password", blankToNull(env.get("WARP_REDISWIRE_PASSWORD")) != null, "AUTH password", null));
        String[][] tokenFronts = {{"gcswire", "WARP_GCSWIRE_TOKENS"}, {"firestorewire", "WARP_FIRESTOREWIRE_TOKENS"},
                {"datastorewire", "WARP_DATASTOREWIRE_TOKENS"}, {"bigtablewire", "WARP_BIGTABLEWIRE_TOKENS"},
                {"pubsubwire", "WARP_PUBSUBWIRE_TOKENS"}, {"pubsubwire-rest", "WARP_PUBSUBWIRE_TOKENS"}};
        for (String[] t : tokenFronts) {
            int n = names(env.get(t[1]), ',').size();
            f.put(t[0], entry("bearer-token", n > 0, n + " static token(s) configured", null));
        }
        f.put("s3wire", entry("aws-sigv4", true, "requires client credentials at startup", names(firstNonBlank(env.get("WARP_S3WIRE_CREDENTIALS"), env.get("WARP_AWS_IAM_CREDENTIALS")), '=')));
        f.put("cosmoswire", entry("cosmos-master-key", !"false".equalsIgnoreCase(env.get("WARP_COSMOSWIRE_AUTH")), "master key / resource token / AAD token", null));
        f.put("influxwire", entry("token-or-basic", blankToNull(env.get("WARP_INFLUXWIRE_TOKEN")) != null || blankToNull(env.get("WARP_INFLUXWIRE_PASSWORD")) != null, "API token or basic auth; OAuth bearer also accepted when an issuer is set", null));
        TreeSet<String> azure = names(env.get("WARP_AZURE_ACCOUNTS"), ':');
        for (String id : new String[] {"azblobwire", "azqueuewire", "aztablewire", "azfilewire"}) {
            f.put(id, entry("shared-key", !azure.isEmpty() || blankToNull(env.get("WARP_AZURE_BEARER_TOKEN")) != null, azure.size() + " storage account(s)", azure.isEmpty() ? null : azure));
        }
        JsonArray arr = new JsonArray();
        f.forEach((id, o) -> {
            o.addProperty("id", id);
            arr.add(o);
        });
        out.add("frontends", arr);

        JsonObject tls = new JsonObject();
        tls.addProperty("serverKeystoreConfigured", blankToNull(env.get("WARP_TLS_KEYSTORE")) != null);
        tls.addProperty("clientCertificatesRequired", false);
        out.add("tls", tls);
        return out;
    }

    private static JsonObject entry(String method, boolean enforced, String detail, TreeSet<String> principals) {
        JsonObject o = new JsonObject();
        o.addProperty("method", method);
        o.addProperty("enforced", enforced);
        o.addProperty("detail", detail);
        if (principals != null) {
            o.add("principals", array(principals));
        }
        return o;
    }

    private static JsonArray array(TreeSet<String> values) {
        JsonArray a = new JsonArray();
        values.forEach(a::add);
        return a;
    }

    /** Leading names of {@code name<sep>secret} entries separated by ';' (or plain comma-separated tokens: the whole entry counts, never returned). */
    private static TreeSet<String> names(String spec, char sep) {
        TreeSet<String> out = new TreeSet<>();
        if (spec == null || spec.isBlank()) {
            return out;
        }
        if (sep == ',') {
            for (String p : spec.split(",")) {
                if (!p.isBlank()) {
                    out.add("#" + out.size());
                }
            }
            return out;
        }
        for (String part : spec.split(";")) {
            int i = part.indexOf(sep);
            if (i > 0) {
                out.add(part.substring(0, i).trim());
            }
        }
        return out;
    }

    private static String firstNonBlank(String a, String b) {
        return blankToNull(a) != null ? a : b;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
