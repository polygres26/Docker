package com.sayonora.warp.config;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.secrets.FieldCipher;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Walks a JSON document that holds encrypted values among its strings (the MCP upstream list), without knowing its shape. */
final class SecretsJson {

    private SecretsJson() {
    }

    private static boolean encrypted(String s) {
        return s.startsWith("encv1:") || s.startsWith("encv2:");
    }

    /** The document with every encrypted string value replaced by {@code f(value)}; anything else is left as it is. */
    static String mapEncrypted(String json, UnaryOperator<String> f) {
        if (json == null || json.isBlank()) {
            return json;
        }
        return map(JsonParser.parseString(json), f).toString();
    }

    private static JsonElement map(JsonElement e, UnaryOperator<String> f) {
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isString() && encrypted(e.getAsString())) {
            return new com.google.gson.JsonPrimitive(f.apply(e.getAsString()));
        }
        if (e.isJsonArray()) {
            JsonArray out = new JsonArray();
            e.getAsJsonArray().forEach(x -> out.add(map(x, f)));
            return out;
        }
        if (e.isJsonObject()) {
            JsonObject out = new JsonObject();
            e.getAsJsonObject().entrySet().forEach(en -> out.add(en.getKey(), map(en.getValue(), f)));
            return out;
        }
        return e;
    }

    static boolean allCurrent(String json) {
        boolean[] current = {true};
        mapEncrypted(json, v -> {
            if (!FieldCipher.isCurrent(v)) {
                current[0] = false;
            }
            return v;
        });
        return current[0];
    }

    /** How many encrypted values there are per protection label ({@code encv1} or a key id). */
    static Map<String, Integer> protectionCounts(String json) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        mapEncrypted(json, v -> {
            counts.merge(FieldCipher.protection(v), 1, Integer::sum);
            return v;
        });
        return counts;
    }
}
