package com.sayonora.warp.ab;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sayonora.warp.secrets.FieldCipher;
import java.util.Map;
import java.util.Set;

/**
 * One cloud endpoint set plus the auth provider used to call it. The JSON form holds secrets
 * (access keys, session tokens, client secrets): at rest they are encrypted field by field with
 * {@link FieldCipher} (AES-256-GCM, {@code SAYONORA_ENCRYPTION_KEY}); {@link #publicJson} strips them and is the
 * ONLY serialisation the admin API, logs and metrics use.
 */
public final class AbTarget {

    /** Field names whose values are secrets anywhere inside a target's JSON. */
    static final Set<String> SECRET_KEYS = Set.of("secretAccessKey", "sessionToken", "clientSecret", "clientCertificate",
            "privateKey", "serviceAccountKey", "webIdentityToken", "password");

    public final String name;
    public final String region;
    public final String endpoint;
    public final Map<String, String> endpoints;
    public final String sqsAccountId;
    public final int timeoutSeconds;
    /** Full runtime JSON including PLAINTEXT secrets -- never serialised outside the store. */
    final JsonObject raw;
    private volatile AbAuthProvider provider;

    AbTarget(String name, JsonObject raw) {
        this.name = name;
        this.raw = raw;
        this.region = str(raw, "region", "us-east-1");
        this.endpoint = str(raw, "endpoint", null);
        java.util.LinkedHashMap<String, String> ep = new java.util.LinkedHashMap<>();
        if (raw.has("endpoints") && raw.get("endpoints").isJsonObject()) {
            raw.getAsJsonObject("endpoints").entrySet().forEach(e -> ep.put(e.getKey(), e.getValue().getAsString()));
        }
        this.endpoints = Map.copyOf(ep);
        this.sqsAccountId = str(raw, "sqsAccountId", null);
        this.timeoutSeconds = raw.has("timeoutSeconds") ? Math.max(1, raw.get("timeoutSeconds").getAsInt()) : 120;
        if (!raw.has("auth") || !raw.get("auth").isJsonObject()) {
            throw new IllegalArgumentException("cloud target '" + name + "' needs an auth object");
        }
        String type = str(raw.getAsJsonObject("auth"), "type", "");
        if (AbAwsAuth.isStub(type)) {
            throw new IllegalArgumentException("auth type '" + type + "' is UNIMPLEMENTED (interface stub only); "
                    + "supported: static, assume-role, web-identity, default-chain");
        }
        // constructs the provider: validates required fields now, not on the first request
        this.provider = AbAwsAuth.create(raw.getAsJsonObject("auth"), region);
    }

    private static String str(JsonObject o, String k, String d) {
        return o.has(k) && !o.get(k).isJsonNull() && !o.get(k).getAsString().isBlank() ? o.get(k).getAsString() : d;
    }

    public AbAuthProvider provider() {
        return provider;
    }

    void adoptProvider(AbTarget previous) {
        if (previous != null && previous.raw.equals(raw)) {
            this.provider = previous.provider; // keep refreshed temporary credentials across an unrelated reload
        }
    }

    /** Base URL for a service ({@code s3}, {@code dynamodb}, {@code sqs}). */
    public String endpointFor(String service) {
        String e = endpoints.get(service);
        if (e != null) {
            return e;
        }
        if (endpoint != null) {
            return endpoint;
        }
        return "https://" + service + "." + region + ".amazonaws.com";
    }

    /** Admin-API view: secrets removed (replaced by {@code <name>Set: true}). */
    public JsonObject publicJson() {
        JsonObject o = redact(raw);
        o.addProperty("name", name);
        return o;
    }

    static JsonObject redact(JsonObject in) {
        JsonObject out = new JsonObject();
        for (var e : in.entrySet()) {
            if (SECRET_KEYS.contains(e.getKey())) {
                out.addProperty(e.getKey() + "Set", e.getValue().isJsonPrimitive() && !e.getValue().getAsString().isBlank());
            } else if (e.getValue().isJsonObject()) {
                out.add(e.getKey(), redact(e.getValue().getAsJsonObject()));
            } else {
                out.add(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    /** True when the JSON carries any non-empty secret value. */
    static boolean hasSecret(JsonObject o) {
        for (var e : o.entrySet()) {
            JsonElement v = e.getValue();
            if (SECRET_KEYS.contains(e.getKey()) && v.isJsonPrimitive() && !v.getAsString().isBlank()) {
                return true;
            }
            if (v.isJsonObject() && hasSecret(v.getAsJsonObject())) {
                return true;
            }
        }
        return false;
    }

    /** Copy with every secret value encrypted (at-rest form). */
    static JsonObject transform(JsonObject in, java.util.function.UnaryOperator<String> f) {
        JsonObject out = new JsonObject();
        for (var e : in.entrySet()) {
            if (SECRET_KEYS.contains(e.getKey()) && e.getValue().isJsonPrimitive()) {
                out.addProperty(e.getKey(), f.apply(e.getValue().getAsString()));
            } else if (e.getValue().isJsonObject()) {
                out.add(e.getKey(), transform(e.getValue().getAsJsonObject(), f));
            } else {
                out.add(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    static JsonObject encrypt(JsonObject in) {
        return transform(in, FieldCipher::encrypt);
    }

    static JsonObject decrypt(JsonObject in) {
        return transform(in, FieldCipher::decrypt);
    }

    /** Merge an incoming target definition over the stored one: an omitted secret keeps the stored value. */
    static JsonObject mergeSecrets(JsonObject incoming, JsonObject stored) {
        JsonObject out = new JsonObject();
        for (var e : incoming.entrySet()) {
            if (e.getValue().isJsonObject()) {
                JsonObject s = stored != null && stored.has(e.getKey()) && stored.get(e.getKey()).isJsonObject()
                        ? stored.getAsJsonObject(e.getKey()) : null;
                out.add(e.getKey(), mergeSecrets(e.getValue().getAsJsonObject(), s));
            } else {
                out.add(e.getKey(), e.getValue());
            }
        }
        if (stored != null) {
            for (String k : SECRET_KEYS) {
                if (!incoming.has(k) && stored.has(k) && incoming.has("type") && sameType(incoming, stored)) {
                    out.add(k, stored.get(k));
                }
            }
        }
        return out;
    }

    private static boolean sameType(JsonObject a, JsonObject b) {
        return b.has("type") && a.get("type").equals(b.get("type"));
    }
}
