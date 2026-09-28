package com.sayonora.warp.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;

/** Tolerant accessors over a request {@link JsonObject}: the Query protocol delivers every scalar as a string. */
public final class Args {

    private Args() {
    }

    public static boolean has(JsonObject o, String k) {
        return o != null && o.has(k) && !o.get(k).isJsonNull();
    }

    public static String str(JsonObject o, String k) {
        if (!has(o, k)) {
            return null;
        }
        JsonElement e = o.get(k);
        return e.isJsonPrimitive() ? e.getAsString() : e.toString();
    }

    public static String req(JsonObject o, String k) {
        String v = str(o, k);
        if (v == null || v.isEmpty()) {
            throw AwsException.bad("InvalidParameter", "Invalid parameter: " + k);
        }
        return v;
    }

    public static Long lng(JsonObject o, String k) {
        String s = str(o, k);
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return new java.math.BigDecimal(s.trim()).longValue();
        } catch (NumberFormatException e) {
            throw AwsException.bad("InvalidParameter", "Invalid parameter: " + k + " must be a number");
        }
    }

    public static Integer integer(JsonObject o, String k) {
        Long l = lng(o, k);
        return l == null ? null : (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, l));
    }

    public static Double dbl(JsonObject o, String k) {
        String s = str(o, k);
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            throw AwsException.bad("InvalidParameter", "Invalid parameter: " + k + " must be a number");
        }
    }

    public static Boolean bool(JsonObject o, String k) {
        String s = str(o, k);
        return s == null ? null : Boolean.parseBoolean(s.trim());
    }

    public static JsonObject obj(JsonObject o, String k) {
        return has(o, k) && o.get(k).isJsonObject() ? o.getAsJsonObject(k) : null;
    }

    public static JsonArray arr(JsonObject o, String k) {
        return has(o, k) && o.get(k).isJsonArray() ? o.getAsJsonArray(k) : null;
    }

    public static List<String> strings(JsonObject o, String k) {
        List<String> out = new ArrayList<>();
        JsonArray a = arr(o, k);
        if (a != null) {
            for (JsonElement e : a) {
                out.add(e.isJsonPrimitive() ? e.getAsString() : e.toString());
            }
        }
        return out;
    }

    public static List<JsonObject> objects(JsonObject o, String k) {
        List<JsonObject> out = new ArrayList<>();
        JsonArray a = arr(o, k);
        if (a != null) {
            for (JsonElement e : a) {
                if (e.isJsonObject()) {
                    out.add(e.getAsJsonObject());
                }
            }
        }
        return out;
    }

    /** A base64 blob member. */
    public static byte[] bytes(JsonObject o, String k) {
        String s = str(o, k);
        if (s == null) {
            return null;
        }
        try {
            return java.util.Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException e) {
            throw AwsException.validation("Invalid base64 in " + k);
        }
    }

    public static JsonPrimitive p(String s) {
        return new JsonPrimitive(s);
    }
}
