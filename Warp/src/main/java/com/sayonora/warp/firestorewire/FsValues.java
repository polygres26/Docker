package com.sayonora.warp.firestorewire;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Firestore value semantics: the total order across types (null &lt; boolean &lt; NaN &lt; number &lt; timestamp &lt; string &lt;
 * bytes &lt; reference &lt; geo point &lt; array &lt; vector &lt; map, the order real Firestore's backend and client SDKs use),
 * numeric comparison that is exact between int64 and double, equality, field-path parsing and value inspection.
 */
final class FsValues {

    private FsValues() {
    }

    static final int T_NULL = 0, T_BOOL = 1, T_NAN = 2, T_NUM = 3, T_TS = 4, T_STR = 5, T_BYTES = 6, T_REF = 7, T_GEO = 8,
            T_ARRAY = 9, T_VECTOR = 10, T_MAP = 11, T_OTHER = 12;

    static final Value NULL = Value.newBuilder().setNullValueValue(0).build();

    static boolean isVector(Value v) {
        if (v.getValueTypeCase() != Value.ValueTypeCase.MAP_VALUE) {
            return false;
        }
        Value t = v.getMapValue().getFieldsMap().get("__type__");
        Value val = v.getMapValue().getFieldsMap().get("value");
        return t != null && t.getValueTypeCase() == Value.ValueTypeCase.STRING_VALUE && t.getStringValue().equals("__vector__")
                && val != null && val.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE;
    }

    /** The type bracket used by ordering (and by inequality filters). */
    static int typeOrder(Value v) {
        switch (v.getValueTypeCase()) {
            case NULL_VALUE:
                return T_NULL;
            case BOOLEAN_VALUE:
                return T_BOOL;
            case INTEGER_VALUE:
                return T_NUM;
            case DOUBLE_VALUE:
                return Double.isNaN(v.getDoubleValue()) ? T_NAN : T_NUM;
            case TIMESTAMP_VALUE:
                return T_TS;
            case STRING_VALUE:
                return T_STR;
            case BYTES_VALUE:
                return T_BYTES;
            case REFERENCE_VALUE:
                return T_REF;
            case GEO_POINT_VALUE:
                return T_GEO;
            case ARRAY_VALUE:
                return T_ARRAY;
            case MAP_VALUE:
                return isVector(v) ? T_VECTOR : T_MAP;
            default:
                return T_OTHER;
        }
    }

    static int compare(Value a, Value b) {
        int ta = typeOrder(a);
        int tb = typeOrder(b);
        if (ta != tb) {
            return Integer.compare(ta, tb);
        }
        switch (ta) {
            case T_NULL:
            case T_NAN:
                return 0;
            case T_BOOL:
                return Boolean.compare(a.getBooleanValue(), b.getBooleanValue());
            case T_NUM:
                return compareNumbers(a, b);
            case T_TS:
                return compareTs(a.getTimestampValue(), b.getTimestampValue());
            case T_STR:
                return compareStrings(a.getStringValue(), b.getStringValue());
            case T_BYTES:
                return compareBytes(a.getBytesValue(), b.getBytesValue());
            case T_REF:
                return compareRefs(a.getReferenceValue(), b.getReferenceValue());
            case T_GEO: {
                int c = Double.compare(norm(a.getGeoPointValue().getLatitude()), norm(b.getGeoPointValue().getLatitude()));
                return c != 0 ? c : Double.compare(norm(a.getGeoPointValue().getLongitude()), norm(b.getGeoPointValue().getLongitude()));
            }
            case T_ARRAY:
                return compareArrays(a.getArrayValue(), b.getArrayValue());
            case T_VECTOR: {
                ArrayValue x = a.getMapValue().getFieldsMap().get("value").getArrayValue();
                ArrayValue y = b.getMapValue().getFieldsMap().get("value").getArrayValue();
                int c = Integer.compare(x.getValuesCount(), y.getValuesCount());
                return c != 0 ? c : compareArrays(x, y);
            }
            case T_MAP:
                return compareMaps(a.getMapValue(), b.getMapValue());
            default:
                return 0;
        }
    }

    private static double norm(double d) {
        return d == 0 ? 0.0 : d;
    }

    static boolean isNaN(Value v) {
        return v.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE && Double.isNaN(v.getDoubleValue());
    }

    static boolean isNumber(Value v) {
        int t = typeOrder(v);
        return t == T_NUM || t == T_NAN;
    }

    static int compareNumbers(Value a, Value b) {
        boolean ai = a.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE;
        boolean bi = b.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE;
        if (ai && bi) {
            return Long.compare(a.getIntegerValue(), b.getIntegerValue());
        }
        if (!ai && !bi) {
            return cmpDouble(a.getDoubleValue(), b.getDoubleValue());
        }
        if (ai) {
            return -cmpDoubleLong(b.getDoubleValue(), a.getIntegerValue());
        }
        return cmpDoubleLong(a.getDoubleValue(), b.getIntegerValue());
    }

    static int cmpDouble(double x, double y) {
        if (Double.isNaN(x)) {
            return Double.isNaN(y) ? 0 : -1;
        }
        if (Double.isNaN(y)) {
            return 1;
        }
        return x < y ? -1 : x > y ? 1 : 0;
    }

    /** Exact comparison of a double with a long. */
    static int cmpDoubleLong(double d, long l) {
        if (Double.isNaN(d)) {
            return -1;
        }
        if (d >= 9.223372036854775807E18) {
            return 1;
        }
        if (d < -9.223372036854775808E18) {
            return -1;
        }
        long dl = (long) d;
        if (dl != l) {
            return Long.compare(dl, l);
        }
        double frac = d - (double) dl;
        return frac > 0 ? 1 : frac < 0 ? -1 : 0;
    }

    static int compareTs(Timestamp a, Timestamp b) {
        int c = Long.compare(a.getSeconds(), b.getSeconds());
        return c != 0 ? c : Integer.compare(a.getNanos(), b.getNanos());
    }

    /** UTF-8 byte order == code point order. */
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

    static int compareBytes(ByteString a, ByteString b) {
        return ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    static int compareRefs(String a, String b) {
        String[] x = a.split("/", -1);
        String[] y = b.split("/", -1);
        for (int i = 0; i < Math.min(x.length, y.length); i++) {
            int c = compareStrings(x[i], y[i]);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(x.length, y.length);
    }

    static int compareArrays(ArrayValue a, ArrayValue b) {
        int n = Math.min(a.getValuesCount(), b.getValuesCount());
        for (int i = 0; i < n; i++) {
            int c = compare(a.getValues(i), b.getValues(i));
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.getValuesCount(), b.getValuesCount());
    }

    static int compareMaps(MapValue a, MapValue b) {
        var x = new TreeMap<String, Value>(FsValues::compareStrings);
        x.putAll(a.getFieldsMap());
        var y = new TreeMap<String, Value>(FsValues::compareStrings);
        y.putAll(b.getFieldsMap());
        var i = x.entrySet().iterator();
        var j = y.entrySet().iterator();
        while (i.hasNext() && j.hasNext()) {
            var e = i.next();
            var f = j.next();
            int c = compareStrings(e.getKey(), f.getKey());
            if (c != 0) {
                return c;
            }
            c = compare(e.getValue(), f.getValue());
            if (c != 0) {
                return c;
            }
        }
        return Boolean.compare(i.hasNext(), j.hasNext());
    }

    /** Firestore equality (1 == 1.0, NaN == NaN, -0 == 0). */
    static boolean equal(Value a, Value b) {
        return compare(a, b) == 0;
    }

    static Value ofLong(long l) {
        return Value.newBuilder().setIntegerValue(l).build();
    }

    static Value ofDouble(double d) {
        return Value.newBuilder().setDoubleValue(d).build();
    }

    static Value ofString(String s) {
        return Value.newBuilder().setStringValue(s).build();
    }

    static Value ofRef(String s) {
        return Value.newBuilder().setReferenceValue(s).build();
    }

    static Value ofTs(long micros) {
        return Value.newBuilder().setTimestampValue(FsClock.ts(micros)).build();
    }

    static Value ofMap(Map<String, Value> m) {
        return Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(m)).build();
    }

    /** Firestore stores timestamps with microsecond precision: nanoseconds are truncated on write. Returns the same map if unchanged. */
    static Map<String, Value> truncateTimestamps(Map<String, Value> fields) {
        Map<String, Value> out = null;
        for (var e : fields.entrySet()) {
            Value t = truncate(e.getValue());
            if (t != e.getValue()) {
                if (out == null) {
                    out = new java.util.LinkedHashMap<>(fields);
                }
                out.put(e.getKey(), t);
            }
        }
        return out == null ? fields : out;
    }

    private static Value truncate(Value v) {
        switch (v.getValueTypeCase()) {
            case TIMESTAMP_VALUE: {
                Timestamp t = v.getTimestampValue();
                int n = t.getNanos() / 1000 * 1000;
                return n == t.getNanos() ? v : v.toBuilder().setTimestampValue(t.toBuilder().setNanos(n)).build();
            }
            case ARRAY_VALUE: {
                ArrayValue.Builder b = null;
                List<Value> vs = v.getArrayValue().getValuesList();
                for (int i = 0; i < vs.size(); i++) {
                    Value t = truncate(vs.get(i));
                    if (t != vs.get(i)) {
                        if (b == null) {
                            b = v.getArrayValue().toBuilder();
                        }
                        b.setValues(i, t);
                    }
                }
                return b == null ? v : v.toBuilder().setArrayValue(b).build();
            }
            case MAP_VALUE: {
                Map<String, Value> m = truncateTimestamps(v.getMapValue().getFieldsMap());
                return m == v.getMapValue().getFieldsMap() ? v : ofMap(m);
            }
            default:
                return v;
        }
    }

    // ------------------------------------------------------------------ field paths

    private static final java.util.regex.Pattern SIMPLE = java.util.regex.Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");
    private static final String SEG = "(?:[a-zA-Z_][a-zA-Z_0-9]*|`(?:[^`\\\\]|\\\\.)+`)";
    private static final java.util.regex.Pattern FULL = java.util.regex.Pattern.compile("^" + SEG + "(?:\\." + SEG + ")*$", java.util.regex.Pattern.DOTALL);

    static FsException badPath(String p) {
        return FsException.invalid("Invalid property path \"" + p + "\". Unquoted property paths must match regex ([a-zA-Z_][a-zA-Z_0-9]*), "
                + "and quoted property paths must match regex (`(?:[^`\\\\]|(?:\\\\.))+`)");
    }

    /** Parses a dotted field path with backtick quoting: {@code a.b.`c d`}. */
    static List<String> parseFieldPath(String p) {
        if (p == null || p.isEmpty()) {
            throw FsException.invalid("Invalid empty property path string.");
        }
        if (!FULL.matcher(p).matches()) {
            throw badPath(p);
        }
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = p.length();
        while (i < n) {
            StringBuilder sb = new StringBuilder();
            if (p.charAt(i) == '`') {
                i++;
                while (i < n && p.charAt(i) != '`') {
                    if (p.charAt(i) == '\\' && i + 1 < n) {
                        sb.append(p.charAt(i + 1));
                        i += 2;
                    } else {
                        sb.append(p.charAt(i++));
                    }
                }
                i++; // closing backtick
            } else {
                while (i < n && p.charAt(i) != '.') {
                    sb.append(p.charAt(i++));
                }
            }
            out.add(sb.toString());
            i++; // skip '.'
        }
        return out;
    }

    static String quoteSegment(String s) {
        return SIMPLE.matcher(s).matches() ? s : "`" + s.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    static String joinPath(List<String> segs) {
        StringBuilder sb = new StringBuilder();
        for (String s : segs) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(quoteSegment(s));
        }
        return sb.toString();
    }

    /** The value at a field path, or null. */
    static Value get(Map<String, Value> fields, List<String> path) {
        Map<String, Value> cur = fields;
        for (int i = 0; i < path.size(); i++) {
            Value v = cur.get(path.get(i));
            if (v == null) {
                return null;
            }
            if (i == path.size() - 1) {
                return v;
            }
            if (v.getValueTypeCase() != Value.ValueTypeCase.MAP_VALUE) {
                return null;
            }
            cur = v.getMapValue().getFieldsMap();
        }
        return null;
    }

    /** Returns a copy of {@code fields} with the value at {@code path} set (creating / replacing intermediate maps). */
    static Map<String, Value> set(Map<String, Value> fields, List<String> path, int i, Value v) {
        Map<String, Value> out = new java.util.LinkedHashMap<>(fields);
        String k = path.get(i);
        if (i == path.size() - 1) {
            out.put(k, v);
        } else {
            Value cur = out.get(k);
            Map<String, Value> child = cur != null && cur.getValueTypeCase() == Value.ValueTypeCase.MAP_VALUE
                    ? cur.getMapValue().getFieldsMap() : Map.of();
            out.put(k, ofMap(set(child, path, i + 1, v)));
        }
        return out;
    }

    /** Copy with the value at path removed (no-op when absent). Empty parent maps left by the removal are kept. */
    static Map<String, Value> remove(Map<String, Value> fields, List<String> path, int i) {
        String k = path.get(i);
        Value cur = fields.get(k);
        if (cur == null) {
            return fields;
        }
        Map<String, Value> out = new java.util.LinkedHashMap<>(fields);
        if (i == path.size() - 1) {
            out.remove(k);
        } else {
            if (cur.getValueTypeCase() != Value.ValueTypeCase.MAP_VALUE) {
                return fields;
            }
            out.put(k, ofMap(remove(cur.getMapValue().getFieldsMap(), path, i + 1)));
        }
        return out;
    }

    // ------------------------------------------------------------------ size & validation

    static long docNameSize(String rel) {
        long s = 16;
        for (String seg : rel.split("/")) {
            s += seg.getBytes(StandardCharsets.UTF_8).length + 1;
        }
        return s;
    }

    static long size(Value v) {
        switch (v.getValueTypeCase()) {
            case NULL_VALUE:
            case BOOLEAN_VALUE:
                return 1;
            case INTEGER_VALUE:
            case DOUBLE_VALUE:
                return 8;
            case TIMESTAMP_VALUE:
            case GEO_POINT_VALUE:
                return 16;
            case STRING_VALUE:
                return v.getStringValue().getBytes(StandardCharsets.UTF_8).length + 1;
            case BYTES_VALUE:
                return v.getBytesValue().size();
            case REFERENCE_VALUE: {
                String r = v.getReferenceValue();
                int i = r.indexOf("/documents/");
                return i < 0 ? r.length() + 1 : docNameSize(r.substring(i + 11));
            }
            case ARRAY_VALUE: {
                long s = 0;
                for (Value x : v.getArrayValue().getValuesList()) {
                    s += size(x);
                }
                return s;
            }
            case MAP_VALUE: {
                long s = 0;
                for (var e : v.getMapValue().getFieldsMap().entrySet()) {
                    s += e.getKey().getBytes(StandardCharsets.UTF_8).length + 1 + size(e.getValue());
                }
                return s;
            }
            default:
                return 0;
        }
    }

    static long docSize(String rel, Map<String, Value> fields) {
        long s = docNameSize(rel) + 32;
        for (var e : fields.entrySet()) {
            s += e.getKey().getBytes(StandardCharsets.UTF_8).length + 1 + size(e.getValue());
        }
        return s;
    }

    static final int MAX_DEPTH = 20;
    static final int MAX_VALUE_BYTES = 1_048_487;

    /** Validates field names, nesting depth and value shapes of a document body; throws INVALID_ARGUMENT like Firestore. */
    static void validateFields(Map<String, Value> fields) {
        for (var e : fields.entrySet()) {
            validateFieldName(e.getKey());
            try {
                validateValue(e.getKey(), e.getValue(), 1, false);
            } catch (Deep d) {
                throw FsException.invalid("Property " + e.getKey() + " contains an invalid nested entity.");
            }
        }
    }

    private static final class Deep extends RuntimeException {
        Deep() {
            super(null, null, false, false);
        }
    }

    private static void validateFieldName(String k) {
        if (k.isEmpty()) {
            throw FsException.invalid("The property.name is the empty string.");
        }
        if (k.length() >= 4 && k.startsWith("__") && k.endsWith("__") && !k.equals("__type__")) {
            throw FsException.invalid("field name " + k + " is reserved");
        }
        if (k.getBytes(StandardCharsets.UTF_8).length > 1500) {
            throw FsException.invalid("The property name \"" + k.substring(0, 20) + "...\" is longer than 1500 bytes.");
        }
    }

    private static void validateValue(String name, Value v, int depth, boolean inArray) {
        switch (v.getValueTypeCase()) {
            case VALUETYPE_NOT_SET:
                throw FsException.invalid("Value must have a value type");
            case STRING_VALUE:
                if (v.getStringValue().getBytes(StandardCharsets.UTF_8).length > MAX_VALUE_BYTES) {
                    throw FsException.invalid("The value of property \"" + name + "\" is longer than " + MAX_VALUE_BYTES + " bytes.");
                }
                break;
            case BYTES_VALUE:
                if (v.getBytesValue().size() > MAX_VALUE_BYTES) {
                    throw FsException.invalid("The value of property \"" + name + "\" is longer than " + MAX_VALUE_BYTES + " bytes.");
                }
                break;
            case TIMESTAMP_VALUE: {
                Timestamp t = v.getTimestampValue();
                if (t.getSeconds() < -62135596800L || t.getSeconds() > 253402300799L || t.getNanos() < 0 || t.getNanos() > 999999999) {
                    throw FsException.invalid("Timestamp is out of range: seconds=" + t.getSeconds() + " nanos=" + t.getNanos());
                }
                break;
            }
            case GEO_POINT_VALUE: {
                var g = v.getGeoPointValue();
                if (!(g.getLatitude() >= -90 && g.getLatitude() <= 90)) {
                    throw FsException.invalid("Geo point latitude '" + g.getLatitude() + "' outside permitted range -90.0 to 90.0.");
                }
                if (!(g.getLongitude() >= -180 && g.getLongitude() <= 180)) {
                    throw FsException.invalid("Geo point longitude '" + g.getLongitude() + "' outside permitted range -180.0 to 180.0.");
                }
                break;
            }
            case ARRAY_VALUE:
                if (depth > MAX_DEPTH) {
                    throw new Deep();
                }
                for (Value x : v.getArrayValue().getValuesList()) {
                    if (x.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
                        throw FsException.invalid("Nested arrays are not allowed");
                    }
                    validateValue(name, x, depth + 1, true);
                }
                break;
            case MAP_VALUE:
                if (depth > MAX_DEPTH) {
                    throw new Deep();
                }
                for (var e : v.getMapValue().getFieldsMap().entrySet()) {
                    validateFieldName(e.getKey());
                    validateValue(e.getKey(), e.getValue(), depth + 1, false);
                }
                break;
            default:
                break;
        }
    }
}
