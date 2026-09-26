package com.sayonora.wire.mongowire;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.Collator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.bson.BsonBinary;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

/** MongoDB's BSON comparison order, numeric promotion and canonical equality keys. */
final class BsonCmp {

    private BsonCmp() {
    }

    /** Optional string collation (null = binary comparison, MongoDB's default). */
    record Collation(Collator collator, boolean numeric, BsonValue spec) {
        int compareStrings(String a, String b) {
            if (numeric) {
                int c = compareNumeric(a, b);
                if (c != Integer.MIN_VALUE) {
                    return c;
                }
            }
            return collator.compare(a, b);
        }

        private static int compareNumeric(String a, String b) {
            try {
                return new BigDecimal(a).compareTo(new BigDecimal(b));
            } catch (NumberFormatException e) {
                return Integer.MIN_VALUE;
            }
        }
    }

    static int typeOrder(BsonValue v) {
        switch (v.getBsonType()) {
            case MIN_KEY: return -1;
            case UNDEFINED: return 0;
            case NULL: return 5;
            case INT32: case INT64: case DOUBLE: case DECIMAL128: return 10;
            case STRING: case SYMBOL: return 15;
            case DOCUMENT: return 20;
            case ARRAY: return 25;
            case BINARY: return 30;
            case OBJECT_ID: return 35;
            case BOOLEAN: return 40;
            case DATE_TIME: return 45;
            case TIMESTAMP: return 47;
            case REGULAR_EXPRESSION: return 50;
            case DB_POINTER: return 55;
            case JAVASCRIPT: return 60;
            case JAVASCRIPT_WITH_SCOPE: return 65;
            case MAX_KEY: return 127;
            default: return 100;
        }
    }

    static boolean isUndef(BsonValue v) {
        return v.getBsonType() == BsonType.UNDEFINED;
    }

    static boolean isNumber(BsonValue v) {
        return v.isInt32() || v.isInt64() || v.isDouble() || v.isDecimal128();
    }

    static int compare(BsonValue a, BsonValue b) {
        return compare(a, b, null);
    }

    static int compare(BsonValue a, BsonValue b, Collation coll) {
        int ta = typeOrder(a);
        int tb = typeOrder(b);
        if (ta != tb) {
            return Integer.compare(ta, tb);
        }
        return compareSameBracket(a, b, coll);
    }

    /** Compare two values already known to be in the same type bracket. */
    static int compareSameBracket(BsonValue a, BsonValue b, Collation coll) {
        switch (a.getBsonType()) {
            case MIN_KEY: case MAX_KEY: case NULL: case UNDEFINED:
                return 0;
            case INT32: case INT64: case DOUBLE: case DECIMAL128:
                return compareNumbers(a, b);
            case STRING: case SYMBOL: {
                String x = str(a);
                String y = str(b);
                return coll != null ? coll.compareStrings(x, y) : compareUtf8(x, y);
            }
            case DOCUMENT: {
                Iterator<Map.Entry<String, BsonValue>> i = a.asDocument().entrySet().iterator();
                Iterator<Map.Entry<String, BsonValue>> j = b.asDocument().entrySet().iterator();
                while (i.hasNext() && j.hasNext()) {
                    Map.Entry<String, BsonValue> x = i.next();
                    Map.Entry<String, BsonValue> y = j.next();
                    int tx = typeOrder(x.getValue());
                    int ty = typeOrder(y.getValue());
                    if (tx != ty) {
                        return Integer.compare(tx, ty);
                    }
                    int c = compareUtf8(x.getKey(), y.getKey());
                    if (c != 0) {
                        return c;
                    }
                    c = compareSameBracket(x.getValue(), y.getValue(), coll);
                    if (c != 0) {
                        return c;
                    }
                }
                return i.hasNext() ? 1 : j.hasNext() ? -1 : 0;
            }
            case ARRAY: {
                List<BsonValue> x = a.asArray().getValues();
                List<BsonValue> y = b.asArray().getValues();
                int n = Math.min(x.size(), y.size());
                for (int k = 0; k < n; k++) {
                    int c = compare(x.get(k), y.get(k), coll);
                    if (c != 0) {
                        return c;
                    }
                }
                return Integer.compare(x.size(), y.size());
            }
            case BINARY: {
                BsonBinary x = a.asBinary();
                BsonBinary y = b.asBinary();
                if (x.getData().length != y.getData().length) {
                    return Integer.compare(x.getData().length, y.getData().length);
                }
                if ((x.getType() & 0xFF) != (y.getType() & 0xFF)) {
                    return Integer.compare(x.getType() & 0xFF, y.getType() & 0xFF);
                }
                return java.util.Arrays.compareUnsigned(x.getData(), y.getData());
            }
            case OBJECT_ID:
                return java.util.Arrays.compareUnsigned(a.asObjectId().getValue().toByteArray(),
                        b.asObjectId().getValue().toByteArray());
            case BOOLEAN:
                return Boolean.compare(a.asBoolean().getValue(), b.asBoolean().getValue());
            case DATE_TIME:
                return Long.compare(a.asDateTime().getValue(), b.asDateTime().getValue());
            case TIMESTAMP: {
                long x = a.asTimestamp().getValue();
                long y = b.asTimestamp().getValue();
                return Long.compareUnsigned(x, y);
            }
            case REGULAR_EXPRESSION: {
                int c = compareUtf8(a.asRegularExpression().getPattern(), b.asRegularExpression().getPattern());
                return c != 0 ? c : compareUtf8(a.asRegularExpression().getOptions(), b.asRegularExpression().getOptions());
            }
            case DB_POINTER: {
                int c = compareUtf8(a.asDBPointer().getNamespace(), b.asDBPointer().getNamespace());
                return c != 0 ? c : java.util.Arrays.compareUnsigned(a.asDBPointer().getId().toByteArray(),
                        b.asDBPointer().getId().toByteArray());
            }
            case JAVASCRIPT:
                return compareUtf8(a.asJavaScript().getCode(), b.asJavaScript().getCode());
            case JAVASCRIPT_WITH_SCOPE: {
                int c = compareUtf8(a.asJavaScriptWithScope().getCode(), b.asJavaScriptWithScope().getCode());
                return c != 0 ? c : compareSameBracket(a.asJavaScriptWithScope().getScope(),
                        b.asJavaScriptWithScope().getScope(), coll);
            }
            default:
                return 0;
        }
    }

    static String str(BsonValue v) {
        return v.isString() ? v.asString().getValue() : v.asSymbol().getSymbol();
    }

    static int compareUtf8(String a, String b) {
        // code point order == UTF-8 byte order
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    // ---- numbers ----

    /** -2 NaN, -1 -inf, 0 finite, 1 +inf. */
    private static int special(BsonValue v) {
        if (v.isDouble()) {
            double d = v.asDouble().getValue();
            return Double.isNaN(d) ? -2 : d == Double.NEGATIVE_INFINITY ? -1 : d == Double.POSITIVE_INFINITY ? 1 : 0;
        }
        if (v.isDecimal128()) {
            Decimal128 d = v.asDecimal128().getValue();
            return d.isNaN() ? -2 : d.isInfinite() ? (d.isNegative() ? -1 : 1) : 0;
        }
        return 0;
    }

    static BigDecimal toBigDecimal(BsonValue v) {
        switch (v.getBsonType()) {
            case INT32: return BigDecimal.valueOf(v.asInt32().getValue());
            case INT64: return BigDecimal.valueOf(v.asInt64().getValue());
            case DOUBLE: return new BigDecimal(v.asDouble().getValue());
            default:
                try {
                    return v.asDecimal128().getValue().bigDecimalValue();
                } catch (ArithmeticException e) {
                    return BigDecimal.ZERO;
                }
        }
    }

    static int compareNumbers(BsonValue a, BsonValue b) {
        if ((a.isInt32() || a.isInt64()) && (b.isInt32() || b.isInt64())) {
            return Long.compare(a.isInt32() ? a.asInt32().getValue() : a.asInt64().getValue(),
                    b.isInt32() ? b.asInt32().getValue() : b.asInt64().getValue());
        }
        if (a.isDouble() && b.isDouble()) {
            double x = a.asDouble().getValue();
            double y = b.asDouble().getValue();
            if (!Double.isNaN(x) && !Double.isNaN(y)) {
                return x < y ? -1 : x > y ? 1 : 0;
            }
        }
        int sa = special(a);
        int sb = special(b);
        if (sa != 0 || sb != 0) {
            if (sa != 0 && sb != 0) {
                return Integer.compare(sa, sb);
            }
            return sa != 0 ? (sa == 1 ? 1 : -1) : (sb == 1 ? -1 : 1);
        }
        return toBigDecimal(a).compareTo(toBigDecimal(b));
    }

    static boolean equal(BsonValue a, BsonValue b) {
        return compare(a, b, null) == 0;
    }

    // ---- canonical equality keys (group keys, distinct, unique indexes, _id) ----

    static String key(BsonValue v) {
        StringBuilder sb = new StringBuilder();
        key(v, sb, null);
        return sb.toString();
    }

    static String key(BsonValue v, Collation coll) {
        StringBuilder sb = new StringBuilder();
        key(v, sb, coll);
        return sb.toString();
    }

    private static void key(BsonValue v, StringBuilder sb, Collation coll) {
        switch (v.getBsonType()) {
            case INT32: case INT64: case DOUBLE: case DECIMAL128: {
                int s = special(v);
                if (s != 0) {
                    sb.append("n:").append(s == -2 ? "NaN" : s == -1 ? "-Inf" : "Inf");
                } else {
                    BigDecimal d = toBigDecimal(v);
                    sb.append("n:").append(d.signum() == 0 ? "0" : d.stripTrailingZeros().toPlainString());
                }
                break;
            }
            case STRING: case SYMBOL: {
                String s = str(v);
                if (coll != null) {
                    s = java.util.HexFormat.of().formatHex(coll.collator().getCollationKey(s).toByteArray());
                }
                sb.append("s:").append(s.length()).append(':').append(s);
                break;
            }
            case DOCUMENT:
                sb.append("d{");
                for (Map.Entry<String, BsonValue> e : v.asDocument().entrySet()) {
                    sb.append(e.getKey().length()).append(':').append(e.getKey()).append('=');
                    key(e.getValue(), sb, coll);
                    sb.append(',');
                }
                sb.append('}');
                break;
            case ARRAY:
                sb.append("a[");
                for (BsonValue x : v.asArray()) {
                    key(x, sb, coll);
                    sb.append(',');
                }
                sb.append(']');
                break;
            case BINARY:
                sb.append("b:").append(v.asBinary().getType() & 0xFF).append(':')
                        .append(java.util.HexFormat.of().formatHex(v.asBinary().getData()));
                break;
            case OBJECT_ID:
                sb.append("o:").append(v.asObjectId().getValue().toHexString());
                break;
            case BOOLEAN:
                sb.append(v.asBoolean().getValue() ? "t" : "f");
                break;
            case DATE_TIME:
                sb.append("D:").append(v.asDateTime().getValue());
                break;
            case TIMESTAMP:
                sb.append("T:").append(v.asTimestamp().getValue());
                break;
            case REGULAR_EXPRESSION:
                sb.append("r:").append(v.asRegularExpression().getPattern()).append('/')
                        .append(v.asRegularExpression().getOptions());
                break;
            case NULL: case UNDEFINED:
                sb.append(v.getBsonType() == BsonType.NULL ? "null" : "undef");
                break;
            case MIN_KEY:
                sb.append("min");
                break;
            case MAX_KEY:
                sb.append("max");
                break;
            case JAVASCRIPT:
                sb.append("j:").append(v.asJavaScript().getCode());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                sb.append("J:").append(v.asJavaScriptWithScope().getCode());
                key(v.asJavaScriptWithScope().getScope(), sb, coll);
                break;
            case DB_POINTER:
                sb.append("p:").append(v.asDBPointer().getNamespace()).append(v.asDBPointer().getId().toHexString());
                break;
            default:
                sb.append(v.toString());
        }
    }
}
