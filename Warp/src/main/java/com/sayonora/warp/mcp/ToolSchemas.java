package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tiny JSON-Schema builders for provider tool definitions. */
final class ToolSchemas {

    private ToolSchemas() {
    }

    static JsonObject prop(String type, String description) {
        JsonObject o = new JsonObject();
        o.addProperty("type", type);
        o.addProperty("description", description);
        return o;
    }

    static JsonObject str(String d) {
        return prop("string", d);
    }

    static JsonObject num(String d) {
        return prop("number", d);
    }

    static JsonObject bool(String d) {
        return prop("boolean", d);
    }

    static JsonObject strings(String d) {
        JsonObject o = prop("array", d);
        o.add("items", prop("string", "string"));
        return o;
    }

    static JsonObject obj(String d) {
        return prop("object", d);
    }

    static JsonObject arr(String d) {
        return prop("array", d);
    }

    /** Ordered properties: pass name, schema, name, schema, ... */
    static JsonObject schema(List<String> required, Object... nameSchemaPairs) {
        Map<String, JsonObject> props = new LinkedHashMap<>();
        for (int i = 0; i < nameSchemaPairs.length; i += 2) {
            props.put((String) nameSchemaPairs[i], (JsonObject) nameSchemaPairs[i + 1]);
        }
        JsonObject s = new JsonObject();
        s.addProperty("type", "object");
        JsonObject p = new JsonObject();
        props.forEach(p::add);
        s.add("properties", p);
        if (!required.isEmpty()) {
            JsonArray r = new JsonArray();
            required.forEach(r::add);
            s.add("required", r);
        }
        return s;
    }

    static String requireString(JsonObject a, String key) {
        if (!a.has(key) || a.get(key).isJsonNull()) {
            throw new IllegalArgumentException("missing required argument: " + key);
        }
        return a.get(key).getAsString();
    }

    static String optString(JsonObject a, String key) {
        return a.has(key) && !a.get(key).isJsonNull() ? a.get(key).getAsString() : null;
    }

    static Long optLong(JsonObject a, String key) {
        return a.has(key) && !a.get(key).isJsonNull() ? a.get(key).getAsLong() : null;
    }

    static boolean optBool(JsonObject a, String key, boolean dflt) {
        return a.has(key) && !a.get(key).isJsonNull() ? a.get(key).getAsBoolean() : dflt;
    }

    /** An int argument clamped to [1, max]; {@code dflt} when absent. */
    static int limit(JsonObject a, String key, int dflt, int max) {
        Integer v = optInt(a, key);
        return Math.max(1, Math.min(v == null ? dflt : v, max));
    }

    static Integer optInt(JsonObject a, String key) {
        return a.has(key) && !a.get(key).isJsonNull() ? a.get(key).getAsInt() : null;
    }

    /** JDBC cell -> JSON: numbers/booleans/strings natively, jsonb objects parsed, else toString. */
    static com.google.gson.JsonElement cell(Object v) {
        if (v == null) {
            return com.google.gson.JsonNull.INSTANCE;
        }
        if (v instanceof Number n) {
            return new com.google.gson.JsonPrimitive(n);
        }
        if (v instanceof Boolean b) {
            return new com.google.gson.JsonPrimitive(b);
        }
        if (v.getClass().getSimpleName().equals("PGobject")) {
            try {
                return com.google.gson.JsonParser.parseString(v.toString());
            } catch (RuntimeException ignored) {
                // fall through to text
            }
        }
        return new com.google.gson.JsonPrimitive(v.toString());
    }

    static com.google.gson.JsonArray rowsAsObjects(com.sayonora.warp.core.AdHocQueryRunner.Result r, int max) {
        com.google.gson.JsonArray out = new com.google.gson.JsonArray();
        for (java.util.List<Object> row : r.rows()) {
            if (out.size() >= max) {
                break;
            }
            JsonObject o = new JsonObject();
            for (int i = 0; i < r.columns().size(); i++) {
                o.add(r.columns().get(i), cell(row.get(i)));
            }
            out.add(o);
        }
        return out;
    }
}
