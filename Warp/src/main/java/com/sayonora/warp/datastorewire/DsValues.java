package com.sayonora.warp.datastorewire;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Datastore value semantics as the official emulator (and classic Cloud Datastore) implements them: one total order across types
 * (null &lt; integers and timestamps &lt; booleans &lt; byte strings &lt; unicode strings &lt; doubles &lt; geo points &lt; keys), no type
 * bracketing in inequality filters, NaN equal to itself and greatest among doubles, array properties indexed element by element (an
 * empty array indexes as null), embedded entities addressable by dotted paths, unindexed properties invisible to queries.
 */
final class DsValues {

    private DsValues() {
    }

    static final int R_NULL = 0, R_FIXED = 1, R_BOOL = 2, R_BLOB = 3, R_STRING = 4, R_DOUBLE = 5, R_GEO = 6, R_KEY = 7, R_ENTITY = 8, R_ARRAY = 9;

    static int rank(Value v) {
        switch (v.getValueTypeCase()) {
            case NULL_VALUE:
                return R_NULL;
            case INTEGER_VALUE:
            case TIMESTAMP_VALUE:
                return R_FIXED;
            case BOOLEAN_VALUE:
                return R_BOOL;
            case BLOB_VALUE:
                return R_BLOB;
            case STRING_VALUE:
                return R_STRING;
            case DOUBLE_VALUE:
                return R_DOUBLE;
            case GEO_POINT_VALUE:
                return R_GEO;
            case KEY_VALUE:
                return R_KEY;
            case ENTITY_VALUE:
                return R_ENTITY;
            case ARRAY_VALUE:
                return R_ARRAY;
            default:
                return R_NULL;
        }
    }

    static Value nullValue() {
        return Value.newBuilder().setNullValueValue(0).build();
    }

    static long micros(Timestamp t) {
        return t.getSeconds() * 1_000_000L + t.getNanos() / 1000;
    }

    static int compare(Value a, Value b) {
        int ra = rank(a);
        int rb = rank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        switch (ra) {
            case R_FIXED: {
                long x = a.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? a.getIntegerValue() : micros(a.getTimestampValue());
                long y = b.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? b.getIntegerValue() : micros(b.getTimestampValue());
                return Long.compare(x, y);
            }
            case R_BOOL:
                return Boolean.compare(a.getBooleanValue(), b.getBooleanValue());
            case R_BLOB:
                return ByteString.unsignedLexicographicalComparator().compare(a.getBlobValue(), b.getBlobValue());
            case R_STRING:
                return compareStrings(a.getStringValue(), b.getStringValue());
            case R_DOUBLE: {
                double x = a.getDoubleValue();
                double y = b.getDoubleValue();
                if (Double.isNaN(x)) {
                    return Double.isNaN(y) ? 0 : 1;
                }
                if (Double.isNaN(y)) {
                    return -1;
                }
                return x < y ? -1 : x > y ? 1 : 0;
            }
            case R_GEO: {
                int c = Double.compare(a.getGeoPointValue().getLatitude(), b.getGeoPointValue().getLatitude());
                return c != 0 ? c : Double.compare(a.getGeoPointValue().getLongitude(), b.getGeoPointValue().getLongitude());
            }
            case R_KEY:
                return DsKeys.compare(a.getKeyValue(), b.getKeyValue());
            default:
                return 0;
        }
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

    // ------------------------------------------------------------------ indexed values of a property

    /**
     * The index entries of a property path of an entity: array elements flattened, an empty array as null, unindexed values and embedded
     * entities themselves excluded. Empty = the entity has no such indexed property (it does not appear in queries on it).
     */
    static List<Value> indexed(Entity e, String path) {
        List<Value> out = new ArrayList<>();
        collect(e.getPropertiesMap(), path, out);
        return out;
    }

    private static void collect(Map<String, Value> props, String path, List<Value> out) {
        // property names may contain dots: try the whole remaining path as a name first, then split at each dot
        Value whole = props.get(path);
        if (whole != null) {
            terminal(whole, out);
        }
        int dot = path.indexOf('.');
        while (dot > 0) {
            String head = path.substring(0, dot);
            String rest = path.substring(dot + 1);
            Value v = props.get(head);
            if (v != null && !v.getExcludeFromIndexes()) {
                descend(v, rest, out);
            }
            dot = path.indexOf('.', dot + 1);
        }
    }

    private static void descend(Value v, String rest, List<Value> out) {
        if (v.getValueTypeCase() == Value.ValueTypeCase.ENTITY_VALUE) {
            collect(v.getEntityValue().getPropertiesMap(), rest, out);
        } else if (v.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
            for (Value x : v.getArrayValue().getValuesList()) {
                if (!x.getExcludeFromIndexes()) {
                    descend(x, rest, out);
                }
            }
        }
    }

    private static void terminal(Value v, List<Value> out) {
        if (v.getExcludeFromIndexes()) {
            return;
        }
        switch (v.getValueTypeCase()) {
            case ARRAY_VALUE:
                if (v.getArrayValue().getValuesCount() == 0) {
                    out.add(nullValue());
                }
                for (Value x : v.getArrayValue().getValuesList()) {
                    if (!x.getExcludeFromIndexes() && x.getValueTypeCase() != Value.ValueTypeCase.ENTITY_VALUE) {
                        out.add(x);
                    }
                }
                break;
            case ENTITY_VALUE:
            case VALUETYPE_NOT_SET:
                break;
            default:
                out.add(v);
        }
    }

    // ------------------------------------------------------------------ validation

    private static final Pattern RESERVED = Pattern.compile("^__.*__$", Pattern.DOTALL);
    private static final int MAX_INDEXED_BYTES = 1500;

    /** Validates an entity body (property names, value shapes, index limits) and returns it with timestamps truncated to microseconds. */
    static Entity validateAndNormalize(Entity e, DsKeys.Part part) {
        Map<String, Value> out = new LinkedHashMap<>();
        boolean changed = false;
        for (var p : e.getPropertiesMap().entrySet()) {
            checkName(p.getKey());
            Value v = normalize(p.getKey(), p.getValue(), false, part);
            changed |= v != p.getValue();
            out.put(p.getKey(), v);
        }
        Entity r = changed ? e.toBuilder().clearProperties().putAllProperties(out).build() : e;
        if (r.getSerializedSize() > 1_048_572) {
            throw DsException.invalid("entity is too big: " + r.getSerializedSize() + " bytes; the maximum is 1048572");
        }
        return r;
    }

    private static void checkName(String n) {
        if (n.isEmpty()) {
            throw DsException.invalid("The property.name is the empty string.");
        }
        if (RESERVED.matcher(n).matches()) {
            throw DsException.invalid("The property.name \"" + n + "\" is reserved.");
        }
        if (n.getBytes(StandardCharsets.UTF_8).length > 1500) {
            throw DsException.invalid("The property.name is longer than 1500 bytes.");
        }
    }

    private static Value normalize(String name, Value v, boolean inArray, DsKeys.Part part) {
        switch (v.getValueTypeCase()) {
            case STRING_VALUE:
                if (!v.getExcludeFromIndexes() && v.getStringValue().getBytes(StandardCharsets.UTF_8).length > MAX_INDEXED_BYTES) {
                    throw DsException.invalid("The value of property \"" + name + "\" is longer than " + MAX_INDEXED_BYTES + " bytes.");
                }
                return v;
            case BLOB_VALUE:
                if (!v.getExcludeFromIndexes() && v.getBlobValue().size() > MAX_INDEXED_BYTES) {
                    throw DsException.invalid("The value of property \"" + name + "\" is longer than " + MAX_INDEXED_BYTES + " bytes.");
                }
                return v;
            case TIMESTAMP_VALUE: {
                Timestamp t = v.getTimestampValue();
                int n = t.getNanos() / 1000 * 1000;
                return n == t.getNanos() ? v : v.toBuilder().setTimestampValue(t.toBuilder().setNanos(n)).build();
            }
            case GEO_POINT_VALUE: {
                var g = v.getGeoPointValue();
                if (!(g.getLatitude() >= -90 && g.getLatitude() <= 90)) {
                    throw DsException.invalid("Geo point latitude outside permitted range -90.0 to 90.0.");
                }
                if (!(g.getLongitude() >= -180 && g.getLongitude() <= 180)) {
                    throw DsException.invalid("Geo point longitude outside permitted range -180.0 to 180.0.");
                }
                return v;
            }
            case KEY_VALUE: {
                DsKeys.validate(v.getKeyValue(), false, false);
                DsKeys.Part kp = DsKeys.partOf(part, v.getKeyValue());
                return v.toBuilder().setKeyValue(DsKeys.withPartition(v.getKeyValue(), kp)).build();
            }
            case ARRAY_VALUE: {
                ArrayValue.Builder b = null;
                List<Value> vs = v.getArrayValue().getValuesList();
                for (int i = 0; i < vs.size(); i++) {
                    Value x = vs.get(i);
                    if (x.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
                        throw DsException.invalid("list_value cannot contain a Value containing another list_value.");
                    }
                    Value n = normalize(name, x, true, part);
                    if (n != x) {
                        if (b == null) {
                            b = v.getArrayValue().toBuilder();
                        }
                        b.setValues(i, n);
                    }
                }
                return b == null ? v : v.toBuilder().setArrayValue(b).build();
            }
            case ENTITY_VALUE: {
                Entity in = v.getEntityValue();
                Map<String, Value> out = new LinkedHashMap<>();
                boolean changed = false;
                for (var p : in.getPropertiesMap().entrySet()) {
                    checkName(p.getKey());
                    Value n = normalize(p.getKey(), p.getValue(), false, part);
                    changed |= n != p.getValue();
                    out.put(p.getKey(), n);
                }
                if (in.hasKey() && !in.getKey().equals(Key.getDefaultInstance())) {
                    Key ik = in.getKey();
                    Key nk = DsKeys.withPartition(ik, DsKeys.partOf(part, ik));
                    if (!nk.equals(ik)) {
                        in = in.toBuilder().setKey(nk).build();
                        changed = true;
                    }
                }
                return changed ? v.toBuilder().setEntityValue(in.toBuilder().clearProperties().putAllProperties(out)).build() : v;
            }
            default:
                return v;
        }
    }

    static boolean isNumber(Value v) {
        return v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE || v.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE;
    }
}
