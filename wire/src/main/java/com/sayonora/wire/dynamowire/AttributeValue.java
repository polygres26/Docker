package com.sayonora.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A DynamoDB attribute value (the typed-JSON {@code {"S": "x"}} shape). Numbers are normalised on
 * ingest the way DynamoDB does (plain decimal string, no trailing zeros, at most 38 significant
 * digits, magnitude within 1E-130..9.99E+125), binary values are canonical base64, and sets are
 * validated (non-empty, no duplicates) so everything downstream can compare by value.
 */
public final class AttributeValue {

    public enum Type { S, N, B, BOOL, NULL, M, L, SS, NS, BS }

    public static final int MAX_NESTING = 32;

    public final Type type;
    public final String scalar;
    public final Map<String, AttributeValue> map;
    public final List<AttributeValue> list;
    public final Set<String> stringSet;

    private AttributeValue(Type type, String scalar, Map<String, AttributeValue> map,
            List<AttributeValue> list, Set<String> stringSet) {
        this.type = type;
        this.scalar = scalar;
        this.map = map;
        this.list = list;
        this.stringSet = stringSet;
    }

    public static AttributeValue ofS(String s) { return new AttributeValue(Type.S, s, null, null, null); }
    public static AttributeValue ofN(String n) { return new AttributeValue(Type.N, normalizeNumber(n), null, null, null); }
    public static AttributeValue ofB(String base64) { return new AttributeValue(Type.B, canonicalBase64(base64), null, null, null); }
    public static AttributeValue ofBool(boolean b) { return new AttributeValue(Type.BOOL, String.valueOf(b), null, null, null); }
    public static AttributeValue ofNull() { return new AttributeValue(Type.NULL, "true", null, null, null); }
    public static AttributeValue ofM(Map<String, AttributeValue> m) { return new AttributeValue(Type.M, null, m, null, null); }
    public static AttributeValue ofL(List<AttributeValue> l) { return new AttributeValue(Type.L, null, null, l, null); }

    /** Builds an SS/NS/BS from already-normalised members (no validation). */
    public static AttributeValue ofSet(Type setType, Set<String> members) {
        return new AttributeValue(setType, null, null, null, members);
    }

    // ---------------------------------------------------------------------------------- numbers

    /** DynamoDB's canonical number text; throws the DynamoDB ValidationExceptions on bad input. */
    public static String normalizeNumber(String raw) {
        BigDecimal d = parseNumber(raw);
        return format(d);
    }

    public static BigDecimal parseNumber(String raw) {
        if (raw == null) {
            throw new DynamoException("ValidationException", "A value provided cannot be converted into a number");
        }
        BigDecimal d;
        try {
            String t = raw.trim();
            if (t.isEmpty() || !t.equals(raw)) {
                throw new NumberFormatException();
            }
            d = new BigDecimal(t);
        } catch (NumberFormatException e) {
            throw new DynamoException("ValidationException", "A value provided cannot be converted into a number");
        }
        if (d.signum() == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal stripped = d.stripTrailingZeros();
        if (stripped.precision() > 38) {
            throw new DynamoException("ValidationException", "DynamoDB only supports precision up to 38 digits");
        }
        int exponent = stripped.precision() - stripped.scale() - 1;
        if (exponent > 125) {
            throw new DynamoException("ValidationException",
                    "Number overflow. Attempting to store a number with magnitude larger than supported range");
        }
        if (exponent < -130) {
            throw new DynamoException("ValidationException",
                    "Number underflow. Attempting to store a number with magnitude smaller than supported range");
        }
        return stripped;
    }

    public static String format(BigDecimal d) {
        if (d.signum() == 0) {
            return "0";
        }
        return d.stripTrailingZeros().toPlainString();
    }

    /** Result of arithmetic (+/-) rounded/validated to DynamoDB's number rules. */
    public static AttributeValue ofNumber(BigDecimal d) {
        BigDecimal r = d.round(new MathContext(38, java.math.RoundingMode.HALF_EVEN));
        return ofN(format(r));
    }

    public BigDecimal number() {
        return new BigDecimal(scalar);
    }

    // ---------------------------------------------------------------------------------- binary

    private static String canonicalBase64(String s) {
        return Base64.getEncoder().encodeToString(decodeBase64(s));
    }

    static byte[] decodeBase64(String s) {
        if (s.length() % 4 != 0) {
            throw new DynamoException("SerializationException",
                    "Base64 encoded length is expected a multiple of 4 bytes but found: " + s.length());
        }
        try {
            return Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException e) {
            throw new DynamoException("SerializationException", "Unexpected value type in payload");
        }
    }

    public byte[] bytes() {
        return Base64.getDecoder().decode(scalar);
    }

    // ---------------------------------------------------------------------------------- JSON

    public static AttributeValue fromJson(JsonElement el) {
        return fromJson(el, 1);
    }

    private static AttributeValue fromJson(JsonElement el, int depth) {
        if (el == null || !el.isJsonObject()) {
            throw new DynamoException("SerializationException", "Unexpected value type in payload");
        }
        if (depth > MAX_NESTING + 1) {
            throw new DynamoException("ValidationException",
                    "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit");
        }
        JsonObject obj = el.getAsJsonObject();
        if (obj.entrySet().isEmpty()) {
            throw new DynamoException("ValidationException",
                    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes");
        }
        if (obj.entrySet().size() > 1) {
            throw new DynamoException("ValidationException",
                    "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes");
        }
        Map.Entry<String, JsonElement> entry = obj.entrySet().iterator().next();
        String key = entry.getKey();
        JsonElement v = entry.getValue();
        return switch (key) {
            case "S" -> ofS(str(v));
            case "N" -> ofN(str(v));
            case "B" -> ofB(str(v));
            case "BOOL" -> {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isBoolean()) {
                    throw new DynamoException("SerializationException", "Unexpected value type in payload");
                }
                yield ofBool(v.getAsBoolean());
            }
            case "NULL" -> {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isBoolean()) {
                    throw new DynamoException("SerializationException", "Unexpected value type in payload");
                }
                if (!v.getAsBoolean()) {
                    throw new DynamoException("ValidationException",
                            "One or more parameter values were invalid: Null attribute value types must have the value of true");
                }
                yield ofNull();
            }
            case "M" -> {
                if (!v.isJsonObject()) {
                    throw new DynamoException("SerializationException", "Unexpected value type in payload");
                }
                Map<String, AttributeValue> m = new LinkedHashMap<>();
                for (Map.Entry<String, JsonElement> e : v.getAsJsonObject().entrySet()) {
                    m.put(e.getKey(), fromJson(e.getValue(), depth + 1));
                }
                yield ofM(m);
            }
            case "L" -> {
                if (!v.isJsonArray()) {
                    throw new DynamoException("SerializationException", "Unexpected value type in payload");
                }
                List<AttributeValue> l = new ArrayList<>();
                for (JsonElement e : v.getAsJsonArray()) l.add(fromJson(e, depth + 1));
                yield ofL(l);
            }
            case "SS" -> ofSet(Type.SS, toSet(v, Type.SS));
            case "NS" -> ofSet(Type.NS, toSet(v, Type.NS));
            case "BS" -> ofSet(Type.BS, toSet(v, Type.BS));
            default -> throw new DynamoException("SerializationException",
                    "Unexpected value type in payload");
        };
    }

    private static String str(JsonElement v) {
        if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isString()) {
            throw new DynamoException("SerializationException", "Unexpected value type in payload");
        }
        return v.getAsString();
    }

    private static Set<String> toSet(JsonElement v, Type t) {
        if (!v.isJsonArray()) {
            throw new DynamoException("SerializationException", "Unexpected value type in payload");
        }
        JsonArray arr = v.getAsJsonArray();
        if (arr.isEmpty()) {
            String what = t == Type.SS ? "string" : t == Type.NS ? "number" : "binary";
            throw new DynamoException("ValidationException",
                    "One or more parameter values were invalid: An " + what + " set  may not be empty");
        }
        Set<String> s = new LinkedHashSet<>();
        List<String> raw = new ArrayList<>();
        for (JsonElement e : arr) {
            String member = str(e);
            raw.add(member);
            String norm = switch (t) {
                case SS -> member;
                case NS -> normalizeNumber(member);
                default -> canonicalBase64(member);
            };
            if (!s.add(norm)) {
                throw new DynamoException("ValidationException",
                        "One or more parameter values were invalid: Input collection " + raw + " contains duplicates.");
            }
        }
        return s;
    }

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

    /** Deep copy (containers are mutable; scalars and sets are copied too so updates never alias). */
    public AttributeValue deepCopy() {
        return switch (type) {
            case M -> {
                Map<String, AttributeValue> m = new LinkedHashMap<>();
                for (var e : map.entrySet()) m.put(e.getKey(), e.getValue().deepCopy());
                yield ofM(m);
            }
            case L -> {
                List<AttributeValue> l = new ArrayList<>(list.size());
                for (AttributeValue v : list) l.add(v.deepCopy());
                yield ofL(l);
            }
            case SS, NS, BS -> ofSet(type, new LinkedHashSet<>(stringSet));
            default -> this;
        };
    }

    public static Map<String, AttributeValue> copyItem(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        for (var e : item.entrySet()) out.put(e.getKey(), e.getValue().deepCopy());
        return out;
    }

    public boolean isSet() {
        return type == Type.SS || type == Type.NS || type == Type.BS;
    }

    public boolean isScalarKeyType() {
        return type == Type.S || type == Type.N || type == Type.B;
    }

    // ---------------------------------------------------------------------------------- compare

    /** Ordering for S/N/B (UTF-8 byte order for strings, numeric for numbers, unsigned bytes for
     * binary); {@code null} when the two values are of different types or not orderable. */
    public Integer compareOrNull(AttributeValue other) {
        if (other == null || type != other.type) {
            return null;
        }
        return switch (type) {
            case S -> Arrays.compareUnsigned(scalar.getBytes(StandardCharsets.UTF_8),
                    other.scalar.getBytes(StandardCharsets.UTF_8));
            case N -> number().compareTo(other.number());
            case B -> Arrays.compareUnsigned(bytes(), other.bytes());
            default -> null;
        };
    }

    public int compareTo(AttributeValue other) {
        Integer c = compareOrNull(other);
        if (c == null) {
            throw new DynamoException("ValidationException", "Type " + type + " is not comparable");
        }
        return c;
    }

    public boolean deepEquals(AttributeValue other) {
        if (other == null || type != other.type) return false;
        return switch (type) {
            case S, B, BOOL, NULL -> scalar.equals(other.scalar);
            case N -> number().compareTo(other.number()) == 0;
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

    // ---------------------------------------------------------------------------------- size

    /** DynamoDB item-size accounting (attribute-name bytes + value bytes; see the DynamoDB
     * developer guide, "Item sizes"). */
    public int sizeBytes() {
        switch (type) {
            case S:
                return utf8(scalar);
            case N:
                return numberSize(scalar);
            case B:
                return bytes().length;
            case BOOL:
            case NULL:
                return 1;
            case SS: {
                int size = 0;
                for (String e : stringSet) size += utf8(e);
                return size;
            }
            case NS: {
                int size = 0;
                for (String e : stringSet) size += numberSize(e);
                return size;
            }
            case BS: {
                int size = 0;
                for (String e : stringSet) size += Base64.getDecoder().decode(e).length;
                return size;
            }
            case L: {
                int size = 3;
                for (AttributeValue e : list) size += e.sizeBytes() + 1;
                return size;
            }
            case M: {
                int size = 3;
                for (var e : map.entrySet()) size += utf8(e.getKey()) + e.getValue().sizeBytes() + 1;
                return size;
            }
            default:
                return 0;
        }
    }

    /** Numbers are stored as (digits / 2 + 1) bytes, +1 when negative. */
    private static int numberSize(String n) {
        BigDecimal d = new BigDecimal(n);
        if (d.signum() == 0) {
            return 1;
        }
        int digits = d.unscaledValue().abs().toString().length();
        return (digits + 1) / 2 + 1 + (d.signum() < 0 ? 1 : 0);
    }

    static int utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    public static int itemSize(Map<String, AttributeValue> item) {
        int size = 0;
        for (var e : item.entrySet()) size += utf8(e.getKey()) + e.getValue().sizeBytes();
        return size;
    }

    /** DynamoDB's own rendering in some error messages: {@code {N:5}}. */
    public String debug() {
        return "{" + type + ":" + (scalar != null ? scalar : toJson().getAsJsonObject().get(type.name())) + "}";
    }

    /** The type names DynamoDB uses in "operand type: STRING" messages. */
    public String typeName() {
        return switch (type) {
            case S -> "STRING";
            case N -> "NUMBER";
            case B -> "BINARY";
            case BOOL -> "BOOLEAN";
            case NULL -> "NULL";
            case M -> "MAP";
            case L -> "LIST";
            case SS -> "STRING_SET";
            case NS -> "NUMBER_SET";
            case BS -> "BINARY_SET";
        };
    }

    @Override
    public String toString() {
        return toJson().toString();
    }
}
