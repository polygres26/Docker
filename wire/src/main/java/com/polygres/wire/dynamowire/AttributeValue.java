package com.polygres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * DynamoDB's typed attribute-value representation — the real wire shape clients send/receive,
 * e.g. {@code {"S": "hello"}}, {@code {"N": "123"}}, {@code {"M": {...}}}, {@code {"L": [...]}},
 * {@code {"BOOL": true}}, {@code {"NULL": true}}, {@code {"SS": [...]}}, {@code {"NS": [...]}},
 * {@code {"BS": [...]}} (binary — base64 in JSON). This class round-trips that shape faithfully
 * to/from Gson's {@link JsonElement} tree, and also to/from Postgres's JSONB storage (we persist
 * the same typed shape verbatim in the {@code item} JSONB column — see {@link PgItemStore} — so
 * there is no lossy intermediate representation between what a client sent and what lands in
 * Postgres).
 *
 * <p>Scope: {@code B}/{@code BS} (binary) are supported as base64 strings, matching the JSON wire
 * format; we do not decode/interpret binary contents specially. Numbers ({@code N}) are kept as
 * exact decimal strings (DynamoDB's own contract — arbitrary precision, no double rounding) using
 * {@link java.math.BigDecimal} only for comparisons, never for storage.
 */
public final class AttributeValue {

    public enum Type { S, N, B, BOOL, NULL, M, L, SS, NS, BS }

    public final Type type;
    public final String scalar;              // S, N, B (base64), BOOL ("true"/"false"), NULL ("true")
    public final Map<String, AttributeValue> map;    // M
    public final List<AttributeValue> list;          // L
    public final Set<String> stringSet;               // SS, NS (kept as strings), BS (base64 strings)

    private AttributeValue(Type type, String scalar, Map<String, AttributeValue> map,
            List<AttributeValue> list, Set<String> stringSet) {
        this.type = type;
        this.scalar = scalar;
        this.map = map;
        this.list = list;
        this.stringSet = stringSet;
    }

    public static AttributeValue ofS(String s) { return new AttributeValue(Type.S, s, null, null, null); }
    public static AttributeValue ofN(String n) { return new AttributeValue(Type.N, n, null, null, null); }
    public static AttributeValue ofBool(boolean b) { return new AttributeValue(Type.BOOL, String.valueOf(b), null, null, null); }
    public static AttributeValue ofNull() { return new AttributeValue(Type.NULL, "true", null, null, null); }
    public static AttributeValue ofM(Map<String, AttributeValue> m) { return new AttributeValue(Type.M, null, m, null, null); }
    public static AttributeValue ofL(List<AttributeValue> l) { return new AttributeValue(Type.L, null, null, l, null); }

    /** Parses one {@code {"S": "..."}}-style typed-value JSON object. */
    public static AttributeValue fromJson(JsonElement el) {
        if (!el.isJsonObject()) {
            throw new DynamoException("ValidationException", "Attribute value must be a JSON object with a type key");
        }
        JsonObject obj = el.getAsJsonObject();
        if (obj.entrySet().isEmpty()) {
            throw new DynamoException("ValidationException", "Attribute value has no type key");
        }
        Map.Entry<String, JsonElement> entry = obj.entrySet().iterator().next();
        String key = entry.getKey();
        JsonElement v = entry.getValue();
        return switch (key) {
            case "S" -> ofS(v.getAsString());
            case "N" -> ofN(v.getAsString());
            case "B" -> new AttributeValue(Type.B, v.getAsString(), null, null, null);
            case "BOOL" -> ofBool(v.getAsBoolean());
            case "NULL" -> ofNull();
            case "M" -> {
                Map<String, AttributeValue> m = new LinkedHashMap<>();
                for (Map.Entry<String, JsonElement> e : v.getAsJsonObject().entrySet()) {
                    m.put(e.getKey(), fromJson(e.getValue()));
                }
                yield ofM(m);
            }
            case "L" -> {
                List<AttributeValue> l = new ArrayList<>();
                for (JsonElement e : v.getAsJsonArray()) l.add(fromJson(e));
                yield ofL(l);
            }
            case "SS" -> new AttributeValue(Type.SS, null, null, null, toStringSet(v.getAsJsonArray()));
            case "NS" -> new AttributeValue(Type.NS, null, null, null, toStringSet(v.getAsJsonArray()));
            case "BS" -> new AttributeValue(Type.BS, null, null, null, toStringSet(v.getAsJsonArray()));
            default -> throw new DynamoException("ValidationException", "Unknown attribute value type: " + key);
        };
    }

    private static Set<String> toStringSet(JsonArray arr) {
        Set<String> s = new LinkedHashSet<>();
        for (JsonElement e : arr) s.add(e.getAsString());
        return s;
    }

    /** Emits this value back into DynamoDB's typed JSON shape. */
    public JsonElement toJson() {
        JsonObject obj = new JsonObject();
        switch (type) {
            case S -> obj.add("S", new JsonPrimitive(scalar));
            case N -> obj.add("N", new JsonPrimitive(scalar));
            case B -> obj.add("B", new JsonPrimitive(scalar));
            case BOOL -> obj.add("BOOL", new JsonPrimitive(Boolean.parseBoolean(scalar)));
            case NULL -> obj.add("NULL", new JsonPrimitive(true));
            case M -> {
                JsonObject mo = new JsonObject();
                for (Map.Entry<String, AttributeValue> e : map.entrySet()) mo.add(e.getKey(), e.getValue().toJson());
                obj.add("M", mo);
            }
            case L -> {
                JsonArray la = new JsonArray();
                for (AttributeValue v : list) la.add(v.toJson());
                obj.add("L", la);
            }
            case SS -> obj.add("SS", toArray(stringSet));
            case NS -> obj.add("NS", toArray(stringSet));
            case BS -> obj.add("BS", toArray(stringSet));
        }
        return obj;
    }

    private static JsonArray toArray(Set<String> s) {
        JsonArray a = new JsonArray();
        for (String v : s) a.add(v);
        return a;
    }

    public Map<String, AttributeValue> asMap() {
        if (type != Type.M) throw new DynamoException("ValidationException", "Expected a Map attribute value");
        return map;
    }

    /**
     * Ordering used for sort-key comparisons (Query's KeyConditionExpression) and comparison
     * operators in FilterExpression/ConditionExpression. Only defined between same-typed scalars,
     * matching DynamoDB's own rule that comparisons across types are not meaningful.
     */
    public int compareTo(AttributeValue other) {
        if (type != other.type) {
            throw new DynamoException("ValidationException", "Cannot compare attribute values of different types");
        }
        return switch (type) {
            case S -> scalar.compareTo(other.scalar);
            case N -> new java.math.BigDecimal(scalar).compareTo(new java.math.BigDecimal(other.scalar));
            case B -> scalar.compareTo(other.scalar);
            default -> throw new DynamoException("ValidationException", "Type " + type + " is not comparable");
        };
    }

    public boolean deepEquals(AttributeValue other) {
        if (other == null || type != other.type) return false;
        return switch (type) {
            case S, N, B, BOOL, NULL -> scalar.equals(other.scalar);
            case M -> {
                if (map.size() != other.map.size()) yield false;
                for (var e : map.entrySet()) {
                    AttributeValue ov = other.map.get(e.getKey());
                    if (ov == null || !e.getValue().deepEquals(ov)) yield false;
                }
                yield true;
            }
            case L -> {
                if (list.size() != other.list.size()) yield false;
                for (int i = 0; i < list.size(); i++) if (!list.get(i).deepEquals(other.list.get(i))) yield false;
                yield true;
            }
            case SS, NS, BS -> stringSet.equals(other.stringSet);
        };
    }

    @Override
    public String toString() {
        return toJson().toString();
    }
}
