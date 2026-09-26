package com.sayonora.warp.cosmoswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JSON value semantics of the Cosmos DB SQL API. "undefined" (a missing property, the result of most invalid operations) is the Java
 * {@code null}; JSON null is {@link JsonNull}. Type order for comparison and ORDER BY (documented "type order"): undefined < null <
 * boolean < number < string < array < object. Numbers compare as IEEE-754 doubles.
 */
final class CosmosJson {

    private CosmosJson() {
    }

    static final JsonElement NULL = JsonNull.INSTANCE;
    static final JsonElement TRUE = new JsonPrimitive(true);
    static final JsonElement FALSE = new JsonPrimitive(false);

    static JsonElement bool(boolean b) {
        return b ? TRUE : FALSE;
    }

    static JsonElement str(String s) {
        return new JsonPrimitive(s);
    }

    /** A number value; integral doubles are emitted as integers ({@code 5}, not {@code 5.0}); NaN/Infinity have no JSON form: undefined. */
    static JsonElement num(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return null;
        }
        if (d == Math.rint(d) && Math.abs(d) < 9.007199254740992E15) {
            return new JsonPrimitive((long) d);
        }
        return new JsonPrimitive(d);
    }

    static JsonElement num(long l) {
        return new JsonPrimitive(l);
    }

    static boolean isNull(JsonElement e) {
        return e != null && e.isJsonNull();
    }

    static boolean isBool(JsonElement e) {
        return e != null && e.isJsonPrimitive() && e.getAsJsonPrimitive().isBoolean();
    }

    static boolean isNum(JsonElement e) {
        return e != null && e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber();
    }

    static boolean isStr(JsonElement e) {
        return e != null && e.isJsonPrimitive() && e.getAsJsonPrimitive().isString();
    }

    static double dbl(JsonElement e) {
        return e.getAsJsonPrimitive().getAsDouble();
    }

    /** True when {@code e} is a number holding an integral value. */
    static boolean isInteger(JsonElement e) {
        if (!isNum(e)) {
            return false;
        }
        double d = dbl(e);
        return !Double.isInfinite(d) && !Double.isNaN(d) && d == Math.rint(d);
    }

    /** Type rank: 0 undefined, 1 null, 2 boolean, 3 number, 4 string, 5 array, 6 object. */
    static int rank(JsonElement e) {
        if (e == null) {
            return 0;
        }
        if (e.isJsonNull()) {
            return 1;
        }
        if (e.isJsonPrimitive()) {
            JsonPrimitive p = e.getAsJsonPrimitive();
            return p.isBoolean() ? 2 : p.isNumber() ? 3 : 4;
        }
        return e.isJsonArray() ? 5 : 6;
    }

    static String typeName(JsonElement e) {
        return switch (rank(e)) {
            case 0 -> "undefined";
            case 1 -> "null";
            case 2 -> "boolean";
            case 3 -> "number";
            case 4 -> "string";
            case 5 -> "array";
            default -> "object";
        };
    }

    /** Total order used by ORDER BY, MIN/MAX and GROUP BY: by type rank, then by value inside a type. */
    static int compare(JsonElement a, JsonElement b) {
        int ra = rank(a);
        int rb = rank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        switch (ra) {
            case 0, 1:
                return 0;
            case 2:
                return Boolean.compare(a.getAsBoolean(), b.getAsBoolean());
            case 3:
                return Double.compare(dbl(a), dbl(b));
            case 4:
                return compareStrings(a.getAsString(), b.getAsString());
            case 5: {
                JsonArray x = a.getAsJsonArray();
                JsonArray y = b.getAsJsonArray();
                int n = Math.min(x.size(), y.size());
                for (int i = 0; i < n; i++) {
                    int c = compare(x.get(i), y.get(i));
                    if (c != 0) {
                        return c;
                    }
                }
                return Integer.compare(x.size(), y.size());
            }
            default:
                return canon(a).compareTo(canon(b));
        }
    }

    /** Ordinal comparison by Unicode code point (same as UTF-8 byte order). */
    static int compareStrings(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    /** Deep equality of two defined values (numbers as doubles, object keys order-insensitive); undefined never equals anything. */
    static boolean equal(JsonElement a, JsonElement b) {
        if (a == null || b == null) {
            return false;
        }
        int ra = rank(a);
        if (ra != rank(b)) {
            return false;
        }
        switch (ra) {
            case 1:
                return true;
            case 2:
                return a.getAsBoolean() == b.getAsBoolean();
            case 3:
                return dbl(a) == dbl(b);
            case 4:
                return a.getAsString().equals(b.getAsString());
            case 5: {
                JsonArray x = a.getAsJsonArray();
                JsonArray y = b.getAsJsonArray();
                if (x.size() != y.size()) {
                    return false;
                }
                for (int i = 0; i < x.size(); i++) {
                    if (!equal(x.get(i), y.get(i))) {
                        return false;
                    }
                }
                return true;
            }
            default: {
                JsonObject x = a.getAsJsonObject();
                JsonObject y = b.getAsJsonObject();
                if (x.size() != y.size()) {
                    return false;
                }
                for (Map.Entry<String, JsonElement> e : x.entrySet()) {
                    if (!y.has(e.getKey()) || !equal(e.getValue(), y.get(e.getKey()))) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    /** A canonical text of a value (sorted object keys, normalized numbers); "undefined" for a missing value. */
    static String canon(JsonElement e) {
        StringBuilder sb = new StringBuilder();
        canon(e, sb);
        return sb.toString();
    }

    private static void canon(JsonElement e, StringBuilder sb) {
        switch (rank(e)) {
            case 0 -> sb.append("undefined");
            case 1 -> sb.append("null");
            case 2 -> sb.append(e.getAsBoolean());
            case 3 -> {
                double d = dbl(e);
                if (d == Math.rint(d) && Math.abs(d) < 9.007199254740992E15) {
                    sb.append((long) d);
                } else {
                    sb.append(e.getAsJsonPrimitive().getAsBigDecimal().stripTrailingZeros().toString());
                }
            }
            case 4 -> sb.append(new JsonPrimitive(e.getAsString()));
            case 5 -> {
                sb.append('[');
                boolean first = true;
                for (JsonElement x : e.getAsJsonArray()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    canon(x, sb);
                }
                sb.append(']');
            }
            default -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<String, JsonElement> en : new TreeMap<>(toMap(e.getAsJsonObject())).entrySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    sb.append(new JsonPrimitive(en.getKey())).append(':');
                    canon(en.getValue(), sb);
                }
                sb.append('}');
            }
        }
    }

    private static Map<String, JsonElement> toMap(JsonObject o) {
        Map<String, JsonElement> m = new java.util.HashMap<>();
        o.entrySet().forEach(en -> m.put(en.getKey(), en.getValue()));
        return m;
    }

    /** Property {@code name} of an object, or undefined. */
    static JsonElement prop(JsonElement base, String name) {
        if (base == null || !base.isJsonObject()) {
            return null;
        }
        return base.getAsJsonObject().get(name);
    }

    /** Follows a partition-key style path {@code /a/b} (segments are property names). */
    static JsonElement path(JsonElement doc, String path) {
        JsonElement cur = doc;
        for (String seg : pathSegments(path)) {
            cur = prop(cur, seg);
            if (cur == null) {
                return null;
            }
        }
        return cur;
    }

    static List<String> pathSegments(String path) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == '/' && !quoted) {
                if (sb.length() > 0) {
                    out.add(sb.toString());
                    sb.setLength(0);
                }
            } else {
                sb.append(c);
            }
        }
        if (sb.length() > 0) {
            out.add(sb.toString());
        }
        return out;
    }

    static JsonElement deepCopy(JsonElement e) {
        return e == null ? null : e.deepCopy();
    }
}
