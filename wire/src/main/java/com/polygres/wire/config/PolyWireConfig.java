package com.polygres.wire.config;

import java.util.LinkedHashMap;
import java.util.Map;

public record PolyWireConfig(
        String qosRatePerSec,
        String qosBurst,
        String qosMaxWaitMs,
        String qosClassLimits,
        String qosPoolWaitThreshold,
        String cacheTables,
        String cacheTtlMs,
        String backends,
        String shardBackends,
        String routerSchemaRules,
        String routerPredicateRules,
        String routerValueShardRules,
        String routerShardTables,
        String rollupDefinitionsYaml,
        String aclRules,
        String aclPpv2Enabled,
        String aclTrustedProxies,
        String oauthIssuer,
        String oauthAudience,
        String oauthUserIdClaim,
        String oauthRolesClaim,
        String awsIamCredentials) {

    public static PolyWireConfig fromEnvDefaults() {
        return new PolyWireConfig(
                System.getenv().getOrDefault("POLYWIRE_QOS_RATE_PER_SEC", "5"),
                System.getenv().getOrDefault("POLYWIRE_QOS_BURST", "5"),
                System.getenv("POLYWIRE_QOS_MAX_WAIT_MS"),
                System.getenv("POLYWIRE_QOS_CLASS_LIMITS"),
                System.getenv("POLYWIRE_QOS_POOL_WAIT_THRESHOLD"),
                System.getenv("POLYWIRE_CACHE_TABLES"),
                System.getenv("POLYWIRE_CACHE_TTL_MS"),
                System.getenv("POLYWIRE_BACKENDS"),
                System.getenv("POLYWIRE_SHARD_BACKENDS"),
                System.getenv("POLYWIRE_ROUTER_SCHEMA_RULES"),
                System.getenv("POLYWIRE_ROUTER_PREDICATE_RULES"),
                System.getenv("POLYWIRE_ROUTER_VALUE_SHARD_RULES"),
                System.getenv("POLYWIRE_ROUTER_SHARD_TABLES"),
                null,
                System.getenv("POLYWIRE_ACL_RULES"),
                System.getenv("POLYWIRE_ACL_PPV2_ENABLED"),
                System.getenv("POLYWIRE_ACL_TRUSTED_PROXIES"),
                System.getenv("POLYWIRE_OAUTH_ISSUER"),
                System.getenv("POLYWIRE_OAUTH_AUDIENCE"),
                System.getenv("POLYWIRE_OAUTH_USERID_CLAIM"),
                System.getenv("POLYWIRE_OAUTH_ROLES_CLAIM"),
                System.getenv("POLYWIRE_AWS_IAM_CREDENTIALS"));
    }

    public String toJson() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("qosRatePerSec", qosRatePerSec);
        fields.put("qosBurst", qosBurst);
        fields.put("qosMaxWaitMs", qosMaxWaitMs);
        fields.put("qosClassLimits", qosClassLimits);
        fields.put("qosPoolWaitThreshold", qosPoolWaitThreshold);
        fields.put("cacheTables", cacheTables);
        fields.put("cacheTtlMs", cacheTtlMs);
        fields.put("backends", backends);
        fields.put("shardBackends", shardBackends);
        fields.put("routerSchemaRules", routerSchemaRules);
        fields.put("routerPredicateRules", routerPredicateRules);
        fields.put("routerValueShardRules", routerValueShardRules);
        fields.put("routerShardTables", routerShardTables);
        fields.put("rollupDefinitionsYaml", rollupDefinitionsYaml);
        fields.put("aclRules", aclRules);
        fields.put("aclPpv2Enabled", aclPpv2Enabled);
        fields.put("aclTrustedProxies", aclTrustedProxies);
        fields.put("oauthIssuer", oauthIssuer);
        fields.put("oauthAudience", oauthAudience);
        fields.put("oauthUserIdClaim", oauthUserIdClaim);
        fields.put("oauthRolesClaim", oauthRolesClaim);
        fields.put("awsIamCredentials", awsIamCredentials);

        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('"').append(entry.getKey()).append("\":");
            json.append(entry.getValue() == null ? "null" : quote(entry.getValue()));
        }
        return json.append('}').toString();
    }

    public static PolyWireConfig fromJson(String json) {
        Map<String, String> fields = parseFlatObject(json);
        return new PolyWireConfig(
                fields.get("qosRatePerSec"),
                fields.get("qosBurst"),
                fields.get("qosMaxWaitMs"),
                fields.get("qosClassLimits"),
                fields.get("qosPoolWaitThreshold"),
                fields.get("cacheTables"),
                fields.get("cacheTtlMs"),
                fields.get("backends"),
                fields.get("shardBackends"),
                fields.get("routerSchemaRules"),
                fields.get("routerPredicateRules"),
                fields.get("routerValueShardRules"),
                fields.get("routerShardTables"),
                fields.get("rollupDefinitionsYaml"),
                fields.get("aclRules"),
                fields.get("aclPpv2Enabled"),
                fields.get("aclTrustedProxies"),
                fields.get("oauthIssuer"),
                fields.get("oauthAudience"),
                fields.get("oauthUserIdClaim"),
                fields.get("oauthRolesClaim"),
                fields.get("awsIamCredentials"));
    }

    private static String quote(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static Map<String, String> parseFlatObject(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        int i = json.indexOf('{');
        int end = json.lastIndexOf('}');
        if (i < 0 || end < 0) {
            throw new IllegalArgumentException("polywire_config payload: not a JSON object: " + json);
        }
        i++;
        while (i < end) {
            while (i < end && Character.isWhitespace(json.charAt(i))) {
                i++;
            }
            if (i >= end || json.charAt(i) == ',') {
                i++;
                continue;
            }
            if (json.charAt(i) != '"') {
                throw new IllegalArgumentException("polywire_config payload: expected a key at index " + i);
            }
            int[] keyEnd = new int[1];
            String key = parseString(json, i, keyEnd);
            i = keyEnd[0];
            while (i < end && (Character.isWhitespace(json.charAt(i)) || json.charAt(i) == ':')) {
                i++;
            }
            String value;
            if (json.startsWith("null", i)) {
                value = null;
                i += 4;
            } else if (json.charAt(i) == '"') {
                int[] valueEnd = new int[1];
                value = parseString(json, i, valueEnd);
                i = valueEnd[0];
            } else {
                throw new IllegalArgumentException("polywire_config payload: expected a string or null value at index " + i);
            }
            result.put(key, value);
            while (i < end && Character.isWhitespace(json.charAt(i))) {
                i++;
            }
            if (i < end && json.charAt(i) == ',') {
                i++;
            }
        }
        return result;
    }

    private static String parseString(String json, int start, int[] endOut) {
        StringBuilder sb = new StringBuilder();
        int i = start + 1;
        while (json.charAt(i) != '"') {
            char c = json.charAt(i);
            if (c == '\\') {
                char next = json.charAt(i + 1);
                switch (next) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(json.substring(i + 2, i + 6), 16));
                        i += 4;
                    }
                    default -> sb.append(next);
                }
                i += 2;
            } else {
                sb.append(c);
                i++;
            }
        }
        endOut[0] = i + 1;
        return sb.toString();
    }
}
