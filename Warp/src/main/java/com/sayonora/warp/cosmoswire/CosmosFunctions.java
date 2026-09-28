package com.sayonora.warp.cosmoswire;

import static com.sayonora.warp.cosmoswire.CosmosJson.NULL;
import static com.sayonora.warp.cosmoswire.CosmosJson.bool;
import static com.sayonora.warp.cosmoswire.CosmosJson.dbl;
import static com.sayonora.warp.cosmoswire.CosmosJson.isBool;
import static com.sayonora.warp.cosmoswire.CosmosJson.isNum;
import static com.sayonora.warp.cosmoswire.CosmosJson.isStr;
import static com.sayonora.warp.cosmoswire.CosmosJson.num;
import static com.sayonora.warp.cosmoswire.CosmosJson.str;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

/** System functions of the Cosmos SQL API (strings, math, type checks, arrays, date/time, minimal spatial). */
final class CosmosFunctions {

    private CosmosFunctions() {
    }

    /** name -> {min args, max args} */
    private static final Map<String, int[]> ARITY = new HashMap<>();

    private static void ar(int min, int max, String... names) {
        for (String n : names) {
            ARITY.put(n, new int[] {min, max});
        }
    }

    static {
        ar(1, 1, "IS_ARRAY", "IS_BOOL", "IS_DEFINED", "IS_NULL", "IS_NUMBER", "IS_OBJECT", "IS_PRIMITIVE", "IS_STRING", "IS_FINITE_NUMBER",
                "IS_INTEGER", "LENGTH", "LOWER", "UPPER", "LTRIM", "RTRIM", "TRIM", "REVERSE", "TOSTRING", "STRINGTOARRAY",
                "STRINGTOBOOLEAN", "STRINGTONULL", "STRINGTONUMBER", "STRINGTOOBJECT", "ABS", "ACOS", "ASIN", "ATAN", "CEILING", "COS",
                "COT", "DEGREES", "EXP", "FLOOR", "LOG10", "RADIANS", "ROUND", "SIGN", "SIN", "SQRT", "SQUARE", "TAN", "TRUNC",
                "ARRAY_LENGTH", "INTBITNOT", "DATETIMETOTIMESTAMP", "TIMESTAMPTODATETIME", "DATETIMETOTICKS", "TICKSTODATETIME",
                "ST_ISVALID", "ST_ISVALIDDETAILED");
        ar(0, 0, "PI", "RAND", "GETCURRENTDATETIME", "GETCURRENTTIMESTAMP", "GETCURRENTTICKS");
        ar(1, 2, "LOG");
        ar(2, 3, "CONTAINS", "ENDSWITH", "STARTSWITH", "STRINGEQUALS", "INDEX_OF", "ARRAY_CONTAINS", "REGEXMATCH", "SUBSTRING", "ARRAY_SLICE");
        ar(2, 2, "LEFT", "RIGHT", "REPLICATE", "POWER", "ATN2", "ARRAY_CONCAT", "ST_DISTANCE", "ST_WITHIN", "ST_INTERSECTS", "SETINTERSECT",
                "SETUNION", "INTBITAND", "INTBITOR", "INTBITXOR", "INTBITLEFTSHIFT", "INTBITRIGHTSHIFT", "INTADD", "INTSUB", "INTMUL",
                "INTDIV", "INTMOD", "ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL", "DATETIMEDIFF_");
        ar(3, 3, "REPLACE", "DATETIMEADD", "DATETIMEDIFF");
        ar(2, 2, "DATETIMEPART");
        ar(1, 1000, "CONCAT");
        ar(6, 7, "DATETIMEFROMPARTS");
    }

    static JsonElement call(CosmosEval ev, String name, List<JsonElement> a) {
        int[] ar = ARITY.get(name);
        if (ar == null) {
            throw CosmosException.badRequest("'" + name + "' is not a recognized built-in function name.");
        }
        if (a.size() < ar[0] || a.size() > ar[1]) {
            throw CosmosException.badRequest("The " + name.toLowerCase(Locale.ROOT) + " function requires "
                    + (ar[0] == ar[1] ? ar[0] : ar[0] + " to " + ar[1]) + " argument(s).");
        }
        JsonElement x = a.isEmpty() ? null : a.get(0);
        // type checks accept undefined
        switch (name) {
            case "IS_DEFINED":
                return bool(x != null);
            case "IS_NULL":
                return bool(CosmosJson.isNull(x));
            case "IS_BOOL":
                return bool(isBool(x));
            case "IS_NUMBER":
                return bool(isNum(x));
            case "IS_STRING":
                return bool(isStr(x));
            case "IS_ARRAY":
                return bool(x != null && x.isJsonArray());
            case "IS_OBJECT":
                return bool(x != null && x.isJsonObject());
            case "IS_PRIMITIVE":
                return bool(x != null && (x.isJsonNull() || x.isJsonPrimitive()));
            case "IS_FINITE_NUMBER":
                return bool(isNum(x) && !Double.isInfinite(dbl(x)) && !Double.isNaN(dbl(x)));
            case "IS_INTEGER":
                return bool(CosmosJson.isInteger(x));
            case "GETCURRENTDATETIME":
                return str(iso(Instant.ofEpochMilli(ev.nowMillis)));
            case "GETCURRENTTIMESTAMP":
                return num(ev.nowMillis);
            case "GETCURRENTTICKS":
                return num(ev.nowMillis * 10_000L);
            case "PI":
                return num(Math.PI);
            case "RAND":
                return num(RND.nextDouble());
            case "ARRAY_CONTAINS":
                if (x == null || !x.isJsonArray() || a.get(1) == null) {
                    return null;
                }
                boolean partial = a.size() > 2 && isBool(a.get(2)) && a.get(2).getAsBoolean();
                for (JsonElement e : x.getAsJsonArray()) {
                    if (partial ? partialMatch(e, a.get(1)) : CosmosJson.equal(e, a.get(1))) {
                        return CosmosJson.TRUE;
                    }
                }
                return CosmosJson.FALSE;
            case "ARRAY_CONTAINS_ANY":
            case "ARRAY_CONTAINS_ALL": {
                if (x == null || !x.isJsonArray() || a.get(1) == null || !a.get(1).isJsonArray()) {
                    return null;
                }
                boolean all = name.endsWith("ALL");
                for (JsonElement need : a.get(1).getAsJsonArray()) {
                    boolean has = false;
                    for (JsonElement e : x.getAsJsonArray()) {
                        if (CosmosJson.equal(e, need)) {
                            has = true;
                            break;
                        }
                    }
                    if (all && !has) {
                        return CosmosJson.FALSE;
                    }
                    if (!all && has) {
                        return CosmosJson.TRUE;
                    }
                }
                return bool(all);
            }
            default:
                break;
        }
        for (JsonElement e : a) {
            if (e == null) {
                return null; // every other function is undefined when an argument is undefined
            }
        }
        switch (name) {
            // ---------------------------------------------------------------- strings
            case "CONCAT": {
                StringBuilder sb = new StringBuilder();
                for (JsonElement e : a) {
                    if (!isStr(e)) {
                        return null;
                    }
                    sb.append(e.getAsString());
                }
                return str(sb.toString());
            }
            case "CONTAINS":
            case "ENDSWITH":
            case "STARTSWITH":
            case "STRINGEQUALS": {
                if (!isStr(x) || !isStr(a.get(1))) {
                    return null;
                }
                boolean ic = a.size() > 2 && isBool(a.get(2)) && a.get(2).getAsBoolean();
                if (a.size() > 2 && !isBool(a.get(2))) {
                    return null;
                }
                String s1 = x.getAsString();
                String s2 = a.get(1).getAsString();
                if (ic) {
                    s1 = s1.toLowerCase(Locale.ROOT);
                    s2 = s2.toLowerCase(Locale.ROOT);
                }
                return bool(switch (name) {
                    case "CONTAINS" -> s1.contains(s2);
                    case "ENDSWITH" -> s1.endsWith(s2);
                    case "STARTSWITH" -> s1.startsWith(s2);
                    default -> s1.equals(s2);
                });
            }
            case "INDEX_OF": {
                if (!isStr(x) || !isStr(a.get(1))) {
                    return null;
                }
                int from = 0;
                if (a.size() > 2) {
                    if (!CosmosJson.isInteger(a.get(2))) {
                        return null;
                    }
                    from = Math.max(0, (int) dbl(a.get(2)));
                }
                return num(x.getAsString().indexOf(a.get(1).getAsString(), from));
            }
            case "LEFT":
            case "RIGHT": {
                if (!isStr(x) || !CosmosJson.isInteger(a.get(1))) {
                    return null;
                }
                String s1 = x.getAsString();
                int n = (int) Math.max(0, Math.min(dbl(a.get(1)), s1.length()));
                return str(name.equals("LEFT") ? s1.substring(0, n) : s1.substring(s1.length() - n));
            }
            case "LENGTH":
                return isStr(x) ? num(x.getAsString().codePointCount(0, x.getAsString().length())) : null;
            case "LOWER":
                return isStr(x) ? str(x.getAsString().toLowerCase(Locale.ROOT)) : null;
            case "UPPER":
                return isStr(x) ? str(x.getAsString().toUpperCase(Locale.ROOT)) : null;
            case "LTRIM":
                return isStr(x) ? str(x.getAsString().replaceFirst("^\\s+", "")) : null;
            case "RTRIM":
                return isStr(x) ? str(x.getAsString().replaceFirst("\\s+$", "")) : null;
            case "TRIM":
                return isStr(x) ? str(x.getAsString().strip()) : null;
            case "REPLACE":
                return isStr(x) && isStr(a.get(1)) && isStr(a.get(2)) && !a.get(1).getAsString().isEmpty()
                        ? str(x.getAsString().replace(a.get(1).getAsString(), a.get(2).getAsString())) : null;
            case "REPLICATE": {
                if (!isStr(x) || !CosmosJson.isInteger(a.get(1)) || dbl(a.get(1)) < 0) {
                    return null;
                }
                long n = (long) dbl(a.get(1));
                if (n * x.getAsString().length() > 10_000) {
                    return null; // the service caps the result at 10,000 characters
                }
                return str(x.getAsString().repeat((int) n));
            }
            case "REVERSE":
                return isStr(x) ? str(new StringBuilder(x.getAsString()).reverse().toString()) : null;
            case "SUBSTRING": {
                if (!isStr(x) || !CosmosJson.isInteger(a.get(1)) || a.size() < 3 || !CosmosJson.isInteger(a.get(2))) {
                    return null;
                }
                String s1 = x.getAsString();
                long start = (long) dbl(a.get(1));
                long len = (long) dbl(a.get(2));
                if (start < 0) {
                    start = 0;
                }
                if (start >= s1.length() || len <= 0) {
                    return str("");
                }
                return str(s1.substring((int) start, (int) Math.min(s1.length(), start + len)));
            }
            case "TOSTRING":
                return switch (CosmosJson.rank(x)) {
                    case 4 -> x;
                    case 1 -> str("null");
                    case 2 -> str(String.valueOf(x.getAsBoolean()));
                    case 3 -> str(CosmosJson.canon(x));
                    default -> str(x.toString());
                };
            case "STRINGTOARRAY": {
                JsonElement v = isStr(x) ? CosmosEval.parseJson(x.getAsString()) : null;
                return v != null && v.isJsonArray() ? v : null;
            }
            case "STRINGTOOBJECT": {
                JsonElement v = isStr(x) ? CosmosEval.parseJson(x.getAsString()) : null;
                return v != null && v.isJsonObject() ? v : null;
            }
            case "STRINGTOBOOLEAN": {
                JsonElement v = isStr(x) ? CosmosEval.parseJson(x.getAsString().strip()) : null;
                return isBool(v) ? v : null;
            }
            case "STRINGTONULL": {
                JsonElement v = isStr(x) ? CosmosEval.parseJson(x.getAsString().strip()) : null;
                return CosmosJson.isNull(v) ? v : null;
            }
            case "STRINGTONUMBER": {
                JsonElement v = isStr(x) ? CosmosEval.parseJson(x.getAsString().strip()) : null;
                return isNum(v) ? v : null;
            }
            case "REGEXMATCH": {
                if (!isStr(x) || !isStr(a.get(1)) || a.size() > 2 && !isStr(a.get(2))) {
                    return null;
                }
                Pattern p = ev.regex(a.get(1).getAsString(), a.size() > 2 ? a.get(2).getAsString() : "");
                return p == null ? null : bool(p.matcher(x.getAsString()).find());
            }
            // ---------------------------------------------------------------- math
            case "ABS", "ACOS", "ASIN", "ATAN", "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG10", "RADIANS", "ROUND", "SIGN",
                    "SIN", "SQRT", "SQUARE", "TAN", "TRUNC", "LOG": {
                if (!isNum(x)) {
                    return null;
                }
                double d = dbl(x);
                return switch (name) {
                    case "ABS" -> num(Math.abs(d));
                    case "ACOS" -> num(Math.acos(d));
                    case "ASIN" -> num(Math.asin(d));
                    case "ATAN" -> num(Math.atan(d));
                    case "CEILING" -> num(Math.ceil(d));
                    case "COS" -> num(Math.cos(d));
                    case "COT" -> num(1.0 / Math.tan(d));
                    case "DEGREES" -> num(Math.toDegrees(d));
                    case "EXP" -> num(Math.exp(d));
                    case "FLOOR" -> num(Math.floor(d));
                    case "LOG10" -> num(Math.log10(d));
                    case "RADIANS" -> num(Math.toRadians(d));
                    case "ROUND" -> num(d < 0 ? -Math.floor(-d + 0.5) : Math.floor(d + 0.5));
                    case "SIGN" -> num(Math.signum(d));
                    case "SIN" -> num(Math.sin(d));
                    case "SQRT" -> num(Math.sqrt(d));
                    case "SQUARE" -> num(d * d);
                    case "TAN" -> num(Math.tan(d));
                    case "TRUNC" -> num(d < 0 ? Math.ceil(d) : Math.floor(d));
                    default -> {
                        if (a.size() == 1) {
                            yield num(Math.log(d));
                        }
                        yield isNum(a.get(1)) ? num(Math.log(d) / Math.log(dbl(a.get(1)))) : null;
                    }
                };
            }
            case "POWER":
                return isNum(x) && isNum(a.get(1)) ? num(Math.pow(dbl(x), dbl(a.get(1)))) : null;
            case "ATN2":
                return isNum(x) && isNum(a.get(1)) ? num(Math.atan2(dbl(a.get(1)), dbl(x))) : null;
            case "INTBITNOT":
                return CosmosJson.isInteger(x) ? num(~(long) dbl(x)) : null;
            case "INTBITAND", "INTBITOR", "INTBITXOR", "INTBITLEFTSHIFT", "INTBITRIGHTSHIFT", "INTADD", "INTSUB", "INTMUL", "INTDIV", "INTMOD": {
                if (!CosmosJson.isInteger(x) || !CosmosJson.isInteger(a.get(1))) {
                    return null;
                }
                long p = (long) dbl(x);
                long q = (long) dbl(a.get(1));
                return switch (name) {
                    case "INTBITAND" -> num(p & q);
                    case "INTBITOR" -> num(p | q);
                    case "INTBITXOR" -> num(p ^ q);
                    case "INTBITLEFTSHIFT" -> num(p << q);
                    case "INTBITRIGHTSHIFT" -> num(p >> q);
                    case "INTADD" -> num(p + q);
                    case "INTSUB" -> num(p - q);
                    case "INTMUL" -> num(p * q);
                    case "INTDIV" -> q == 0 ? null : num(p / q);
                    default -> q == 0 ? null : num(p % q);
                };
            }
            // ---------------------------------------------------------------- arrays
            case "ARRAY_LENGTH":
                return x.isJsonArray() ? num(x.getAsJsonArray().size()) : null;
            case "ARRAY_CONCAT": {
                if (!x.isJsonArray() || !a.get(1).isJsonArray()) {
                    return null;
                }
                JsonArray out = new JsonArray();
                x.getAsJsonArray().forEach(out::add);
                a.get(1).getAsJsonArray().forEach(out::add);
                return out;
            }
            case "ARRAY_SLICE": {
                if (!x.isJsonArray() || !CosmosJson.isInteger(a.get(1)) || a.size() > 2 && !CosmosJson.isInteger(a.get(2))) {
                    return null;
                }
                JsonArray src = x.getAsJsonArray();
                int n = src.size();
                long start = (long) dbl(a.get(1));
                if (start < 0) {
                    start = Math.max(0, n + start);
                }
                start = Math.min(start, n);
                long len = a.size() > 2 ? (long) dbl(a.get(2)) : n;
                long end = len < 0 ? Math.max(start, n + len) : Math.min(n, start + len);
                JsonArray out = new JsonArray();
                for (long i = start; i < end; i++) {
                    out.add(src.get((int) i));
                }
                return out;
            }
            case "SETINTERSECT":
            case "SETUNION": {
                if (!x.isJsonArray() || !a.get(1).isJsonArray()) {
                    return null;
                }
                JsonArray out = new JsonArray();
                java.util.Set<String> seen = new java.util.HashSet<>();
                boolean inter = name.equals("SETINTERSECT");
                java.util.Set<String> other = new java.util.HashSet<>();
                a.get(1).getAsJsonArray().forEach(e -> other.add(CosmosJson.canon(e)));
                for (JsonElement e : x.getAsJsonArray()) {
                    String k = CosmosJson.canon(e);
                    if ((!inter || other.contains(k)) && seen.add(k)) {
                        out.add(e);
                    }
                }
                if (!inter) {
                    for (JsonElement e : a.get(1).getAsJsonArray()) {
                        if (seen.add(CosmosJson.canon(e))) {
                            out.add(e);
                        }
                    }
                }
                return out;
            }
            // ---------------------------------------------------------------- date and time
            case "DATETIMEADD": {
                Instant t = parseIso(a.get(2));
                if (!isStr(x) || !CosmosJson.isInteger(a.get(1)) || t == null) {
                    return null;
                }
                ZonedDateTime z = t.atZone(ZoneOffset.UTC);
                long n = (long) dbl(a.get(1));
                String part = x.getAsString().toLowerCase(Locale.ROOT);
                z = switch (part) {
                    case "year", "yyyy", "yy" -> z.plusYears(n);
                    case "month", "mm", "m" -> z.plusMonths(n);
                    case "day", "dd", "d" -> z.plusDays(n);
                    case "hour", "hh" -> z.plusHours(n);
                    case "minute", "mi", "n" -> z.plusMinutes(n);
                    case "second", "ss", "s" -> z.plusSeconds(n);
                    case "millisecond", "ms" -> z.plusNanos(n * 1_000_000L);
                    case "microsecond", "mcs" -> z.plusNanos(n * 1_000L);
                    case "nanosecond", "ns" -> z.plusNanos(n);
                    default -> null;
                };
                return z == null ? null : str(iso(z.toInstant()));
            }
            case "DATETIMEDIFF": {
                Instant t1 = parseIso(a.get(1));
                Instant t2 = parseIso(a.get(2));
                if (!isStr(x) || t1 == null || t2 == null) {
                    return null;
                }
                ZonedDateTime z1 = t1.atZone(ZoneOffset.UTC);
                ZonedDateTime z2 = t2.atZone(ZoneOffset.UTC);
                String part = x.getAsString().toLowerCase(Locale.ROOT);
                // count of datepart boundaries crossed (like SQL Server DATEDIFF)
                return switch (part) {
                    case "year", "yyyy", "yy" -> num(z2.getYear() - z1.getYear());
                    case "month", "mm", "m" -> num((z2.getYear() - z1.getYear()) * 12L + z2.getMonthValue() - z1.getMonthValue());
                    case "day", "dd", "d" -> num(ChronoUnit.DAYS.between(z1.truncatedTo(ChronoUnit.DAYS), z2.truncatedTo(ChronoUnit.DAYS)));
                    case "hour", "hh" -> num(ChronoUnit.HOURS.between(z1.truncatedTo(ChronoUnit.HOURS), z2.truncatedTo(ChronoUnit.HOURS)));
                    case "minute", "mi", "n" -> num(ChronoUnit.MINUTES.between(z1.truncatedTo(ChronoUnit.MINUTES), z2.truncatedTo(ChronoUnit.MINUTES)));
                    case "second", "ss", "s" -> num(ChronoUnit.SECONDS.between(z1.truncatedTo(ChronoUnit.SECONDS), z2.truncatedTo(ChronoUnit.SECONDS)));
                    case "millisecond", "ms" -> num(ChronoUnit.MILLIS.between(z1.truncatedTo(ChronoUnit.MILLIS), z2.truncatedTo(ChronoUnit.MILLIS)));
                    case "microsecond", "mcs" -> num(ChronoUnit.MICROS.between(z1.truncatedTo(ChronoUnit.MICROS), z2.truncatedTo(ChronoUnit.MICROS)));
                    case "nanosecond", "ns" -> num(ChronoUnit.NANOS.between(z1, z2));
                    default -> null;
                };
            }
            case "DATETIMEPART": {
                Instant t = parseIso(a.get(1));
                if (!isStr(x) || t == null) {
                    return null;
                }
                ZonedDateTime z = t.atZone(ZoneOffset.UTC);
                return switch (x.getAsString().toLowerCase(Locale.ROOT)) {
                    case "year", "yyyy", "yy" -> num(z.getYear());
                    case "month", "mm", "m" -> num(z.getMonthValue());
                    case "day", "dd", "d" -> num(z.getDayOfMonth());
                    case "hour", "hh" -> num(z.getHour());
                    case "minute", "mi", "n" -> num(z.getMinute());
                    case "second", "ss", "s" -> num(z.getSecond());
                    case "millisecond", "ms" -> num(z.getNano() / 1_000_000);
                    case "microsecond", "mcs" -> num(z.getNano() / 1_000 % 1_000_000);
                    case "nanosecond", "ns" -> num(z.getNano());
                    default -> null;
                };
            }
            case "DATETIMEFROMPARTS": {
                long[] v = new long[7];
                for (int i = 0; i < a.size(); i++) {
                    if (!CosmosJson.isInteger(a.get(i))) {
                        return null;
                    }
                    v[i] = (long) dbl(a.get(i));
                }
                try {
                    LocalDateTime l = LocalDateTime.of((int) v[0], (int) v[1], (int) v[2], (int) v[3], (int) v[4], (int) v[5])
                            .plusNanos(v[6] * 100L);
                    return str(iso(l.toInstant(ZoneOffset.UTC)));
                } catch (RuntimeException e) {
                    return null;
                }
            }
            case "DATETIMETOTIMESTAMP": {
                Instant t = parseIso(x);
                return t == null ? null : num(t.toEpochMilli());
            }
            case "DATETIMETOTICKS": {
                Instant t = parseIso(x);
                return t == null ? null : num(t.getEpochSecond() * 10_000_000L + t.getNano() / 100);
            }
            case "TIMESTAMPTODATETIME":
                return CosmosJson.isInteger(x) ? str(iso(Instant.ofEpochMilli((long) dbl(x)))) : null;
            case "TICKSTODATETIME": {
                if (!CosmosJson.isInteger(x)) {
                    return null;
                }
                long t = (long) dbl(x);
                return str(iso(Instant.ofEpochSecond(Math.floorDiv(t, 10_000_000L), Math.floorMod(t, 10_000_000L) * 100)));
            }
            // ---------------------------------------------------------------- spatial (minimal: Points and Polygons on a sphere / plane)
            case "ST_DISTANCE": {
                double[] p = point(x);
                double[] q = point(a.get(1));
                return p == null || q == null ? null : num(haversine(p, q));
            }
            case "ST_WITHIN":
            case "ST_INTERSECTS": {
                double[] p = point(x);
                JsonElement poly = a.get(1);
                if (p == null && a.get(1) != null && point(a.get(1)) != null && polygon(x) != null) {
                    p = point(a.get(1));
                    poly = x;
                }
                if (p == null) {
                    return null;
                }
                JsonArray ring = polygon(poly);
                if (ring == null) {
                    double[] q = point(poly);
                    return q != null && name.equals("ST_INTERSECTS") ? bool(p[0] == q[0] && p[1] == q[1]) : null;
                }
                return bool(inRing(p, ring));
            }
            case "ST_ISVALID":
                return bool(validGeo(x) == null);
            case "ST_ISVALIDDETAILED": {
                String why = validGeo(x);
                JsonObject o = new JsonObject();
                o.addProperty("valid", why == null);
                if (why != null) {
                    o.addProperty("reason", why);
                }
                return o;
            }
            default:
                throw CosmosException.badRequest("'" + name + "' is not a recognized built-in function name.");
        }
    }

    private static final Random RND = new Random();

    static boolean partialMatch(JsonElement item, JsonElement pattern) {
        if (pattern.isJsonObject() && item.isJsonObject()) {
            for (Map.Entry<String, JsonElement> e : pattern.getAsJsonObject().entrySet()) {
                JsonElement v = item.getAsJsonObject().get(e.getKey());
                if (v == null || !partialMatch(v, e.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return CosmosJson.equal(item, pattern);
    }

    static String iso(Instant t) {
        ZonedDateTime z = t.atZone(ZoneOffset.UTC);
        return String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d.%07dZ", z.getYear(), z.getMonthValue(), z.getDayOfMonth(), z.getHour(),
                z.getMinute(), z.getSecond(), z.getNano() / 100);
    }

    static Instant parseIso(JsonElement e) {
        if (!isStr(e)) {
            return null;
        }
        try {
            String s = e.getAsString();
            if (s.length() == 10) {
                return java.time.LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC);
            }
            return java.time.OffsetDateTime.parse(s.endsWith("Z") || s.contains("+") || s.lastIndexOf('-') > 10 ? s : s + "Z").toInstant();
        } catch (RuntimeException ex) {
            return null;
        }
    }

    // ---------------------------------------------------------------- geo helpers (GeoJSON, [longitude, latitude])

    private static double[] point(JsonElement e) {
        if (e == null || !e.isJsonObject() || !"Point".equals(strOf(CosmosJson.prop(e, "type")))) {
            return null;
        }
        JsonElement c = CosmosJson.prop(e, "coordinates");
        if (c == null || !c.isJsonArray() || c.getAsJsonArray().size() < 2 || !isNum(c.getAsJsonArray().get(0)) || !isNum(c.getAsJsonArray().get(1))) {
            return null;
        }
        return new double[] {dbl(c.getAsJsonArray().get(0)), dbl(c.getAsJsonArray().get(1))};
    }

    private static String strOf(JsonElement e) {
        return isStr(e) ? e.getAsString() : null;
    }

    private static JsonArray polygon(JsonElement e) {
        if (e == null || !e.isJsonObject() || !"Polygon".equals(strOf(CosmosJson.prop(e, "type")))) {
            return null;
        }
        JsonElement c = CosmosJson.prop(e, "coordinates");
        if (c == null || !c.isJsonArray() || c.getAsJsonArray().isEmpty() || !c.getAsJsonArray().get(0).isJsonArray()) {
            return null;
        }
        return c.getAsJsonArray().get(0).getAsJsonArray();
    }

    private static double haversine(double[] p, double[] q) {
        double r = 6_378_137.0;
        double la1 = Math.toRadians(p[1]);
        double la2 = Math.toRadians(q[1]);
        double dla = la2 - la1;
        double dlo = Math.toRadians(q[0] - p[0]);
        double h = Math.sin(dla / 2) * Math.sin(dla / 2) + Math.cos(la1) * Math.cos(la2) * Math.sin(dlo / 2) * Math.sin(dlo / 2);
        return 2 * r * Math.asin(Math.min(1, Math.sqrt(h)));
    }

    private static boolean inRing(double[] p, JsonArray ring) {
        boolean in = false;
        for (int i = 0, j = ring.size() - 1; i < ring.size(); j = i++) {
            double xi = dbl(ring.get(i).getAsJsonArray().get(0));
            double yi = dbl(ring.get(i).getAsJsonArray().get(1));
            double xj = dbl(ring.get(j).getAsJsonArray().get(0));
            double yj = dbl(ring.get(j).getAsJsonArray().get(1));
            if ((yi > p[1]) != (yj > p[1]) && p[0] < (xj - xi) * (p[1] - yi) / (yj - yi) + xi) {
                in = !in;
            }
        }
        return in;
    }

    private static String validGeo(JsonElement g) {
        if (g == null || !g.isJsonObject()) {
            return "The GeoJSON object is not valid.";
        }
        double[] p = point(g);
        if (p != null) {
            return p[0] < -180 || p[0] > 180 || p[1] < -90 || p[1] > 90 ? "Latitude values must be between -90 and 90 degrees." : null;
        }
        JsonArray ring = polygon(g);
        if (ring != null) {
            if (ring.size() < 4) {
                return "Polygon rings must have at least 4 points.";
            }
            if (!CosmosJson.equal(ring.get(0), ring.get(ring.size() - 1))) {
                return "Polygon rings must be closed.";
            }
            return null;
        }
        return "The GeoJSON type is not supported.";
    }
}
