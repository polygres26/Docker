package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Map;
import java.util.Set;

/**
 * Plain JSON <-> the typed Value JSON of Firestore ({@code mapValue.fields}) and Datastore ({@code entityValue.properties}).
 * Agents pass and receive plain JSON: strings, numbers (whole numbers become {@code integerValue}, others {@code doubleValue}),
 * booleans, null, arrays and objects. Special values use one-key objects: {@code {"$timestamp":"2024-01-02T03:04:05Z"}},
 * {@code {"$reference":"projects/.../documents/a/b"}} (Firestore), {@code {"$geoPoint":{"latitude":1,"longitude":2}}},
 * {@code {"$bytes":"<base64>"}}, {@code {"$key":{...Datastore key JSON...}}}. An object whose only key is a typed name such as
 * {@code stringValue} is passed through unchanged, so raw typed JSON also works.
 */
final class TypedValues {

    private static final Set<String> TYPED = Set.of("nullValue", "booleanValue", "integerValue", "doubleValue", "timestampValue",
            "stringValue", "bytesValue", "referenceValue", "geoPointValue", "arrayValue", "mapValue", "entityValue", "keyValue");

    private final boolean datastore;

    private TypedValues(boolean datastore) {
        this.datastore = datastore;
    }

    static final TypedValues FIRESTORE = new TypedValues(false);
    static final TypedValues DATASTORE = new TypedValues(true);

    /** A plain object as {@code fields}/{@code properties}. */
    JsonObject fields(JsonObject plain) {
        JsonObject out = new JsonObject();
        for (Map.Entry<String, JsonElement> e : plain.entrySet()) {
            out.add(e.getKey(), value(e.getValue()));
        }
        return out;
    }

    JsonObject value(JsonElement v) {
        JsonObject o = new JsonObject();
        if (v == null || v.isJsonNull()) {
            o.addProperty("nullValue", "NULL_VALUE");
        } else if (v.isJsonPrimitive()) {
            JsonPrimitive p = v.getAsJsonPrimitive();
            if (p.isBoolean()) {
                o.addProperty("booleanValue", p.getAsBoolean());
            } else if (p.isNumber()) {
                java.math.BigDecimal d = p.getAsBigDecimal();
                if (d.stripTrailingZeros().scale() <= 0 && d.abs().compareTo(new java.math.BigDecimal(Long.MAX_VALUE)) <= 0) {
                    o.addProperty("integerValue", String.valueOf(d.longValue()));
                } else {
                    o.addProperty("doubleValue", d.doubleValue());
                }
            } else {
                o.addProperty("stringValue", p.getAsString());
            }
        } else if (v.isJsonArray()) {
            JsonArray vals = new JsonArray();
            v.getAsJsonArray().forEach(x -> vals.add(value(x)));
            JsonObject arr = new JsonObject();
            arr.add("values", vals);
            o.add("arrayValue", arr);
        } else {
            JsonObject obj = v.getAsJsonObject();
            if (obj.size() == 1) {
                String k = obj.keySet().iterator().next();
                if (TYPED.contains(k)) {
                    return obj;
                }
                switch (k) {
                    case "$timestamp" -> {
                        o.add("timestampValue", obj.get(k));
                        return o;
                    }
                    case "$reference" -> {
                        o.add("referenceValue", obj.get(k));
                        return o;
                    }
                    case "$geoPoint" -> {
                        o.add("geoPointValue", obj.get(k));
                        return o;
                    }
                    case "$bytes" -> {
                        o.add("bytesValue", obj.get(k));
                        return o;
                    }
                    case "$key" -> {
                        o.add("keyValue", obj.get(k));
                        return o;
                    }
                    default -> {
                    }
                }
            }
            JsonObject inner = new JsonObject();
            inner.add(datastore ? "properties" : "fields", fields(obj));
            o.add(datastore ? "entityValue" : "mapValue", inner);
        }
        return o;
    }

    /** {@code fields}/{@code properties} to a plain object. */
    JsonObject plain(JsonObject fields) {
        JsonObject out = new JsonObject();
        if (fields != null) {
            for (Map.Entry<String, JsonElement> e : fields.entrySet()) {
                out.add(e.getKey(), plainValue(e.getValue().getAsJsonObject()));
            }
        }
        return out;
    }

    JsonElement plainValue(JsonObject v) {
        for (String k : v.keySet()) {
            JsonElement x = v.get(k);
            switch (k) {
                case "nullValue":
                    return JsonNull.INSTANCE;
                case "booleanValue", "stringValue":
                    return x;
                case "integerValue":
                    try {
                        return new JsonPrimitive(Long.parseLong(x.getAsString()));
                    } catch (NumberFormatException e) {
                        return x;
                    }
                case "doubleValue":
                    return x;
                case "timestampValue":
                    return wrap("$timestamp", x);
                case "referenceValue":
                    return wrap("$reference", x);
                case "geoPointValue":
                    return wrap("$geoPoint", x);
                case "bytesValue", "blobValue":
                    return wrap("$bytes", x);
                case "keyValue":
                    return wrap("$key", x);
                case "arrayValue": {
                    JsonArray out = new JsonArray();
                    JsonObject av = x.getAsJsonObject();
                    if (av.has("values")) {
                        av.getAsJsonArray("values").forEach(y -> out.add(plainValue(y.getAsJsonObject())));
                    }
                    return out;
                }
                case "mapValue":
                    return plain(x.getAsJsonObject().has("fields") ? x.getAsJsonObject().getAsJsonObject("fields") : null);
                case "entityValue":
                    return plain(x.getAsJsonObject().has("properties") ? x.getAsJsonObject().getAsJsonObject("properties") : null);
                default:
                    break;
            }
        }
        return JsonNull.INSTANCE;
    }

    private static JsonObject wrap(String k, JsonElement v) {
        JsonObject o = new JsonObject();
        o.add(k, v);
        return o;
    }
}
