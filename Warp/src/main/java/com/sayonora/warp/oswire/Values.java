package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Typed doc values of a field: Long (integer types, dates as epoch ms), Double (float types), Boolean, String (keyword/ip). */
final class Values {

    private Values() {
    }

    static Object parse(Mappings.Field f, JsonElement el, JsonObject settings, ZoneId zone) {
        if (el == null || el.isJsonNull() || el.isJsonObject() || el.isJsonArray()) {
            return null;
        }
        JsonPrimitive p = el.getAsJsonPrimitive();
        try {
            switch (f.type) {
                case "long", "integer", "short", "byte" -> {
                    if (p.isBoolean()) {
                        return null;
                    }
                    BigDecimal bd = p.isNumber() ? p.getAsBigDecimal() : new BigDecimal(p.getAsString().trim());
                    return bd.longValue();
                }
                case "unsigned_long" -> {
                    return new BigDecimal(p.getAsString()).doubleValue();
                }
                case "double", "scaled_float" -> {
                    return p.isBoolean() ? null : new BigDecimal(p.getAsString().trim()).doubleValue();
                }
                case "float", "half_float" -> {
                    return p.isBoolean() ? null : (double) Float.parseFloat(p.getAsString().trim());
                }
                case "boolean" -> {
                    if (p.isBoolean()) {
                        return p.getAsBoolean();
                    }
                    String s = p.getAsString();
                    return s.equals("true") ? Boolean.TRUE : s.isEmpty() || s.equals("false") ? Boolean.FALSE : null;
                }
                case "date", "date_nanos" -> {
                    if (p.isNumber() && (f.format == null || f.format.contains("epoch_millis") || f.format.contains("strict_date_optional_time"))) {
                        return p.getAsLong();
                    }
                    return Dates.parse(p.getAsString(), f.format, zone == null ? ZoneOffset.UTC : zone);
                }
                case "keyword", "constant_keyword", "wildcard" -> {
                    String s = p.getAsString();
                    if (f.def != null && f.def.has("ignore_above") && s.length() > f.def.get("ignore_above").getAsInt()) {
                        return null;
                    }
                    return normalize(f, s, settings);
                }
                case "ip" -> {
                    return p.getAsString();
                }
                default -> {
                    return p.getAsString();
                }
            }
        } catch (RuntimeException e) {
            return null;
        }
    }

    static String normalize(Mappings.Field f, String s, JsonObject settings) {
        if (f.def != null && f.def.has("normalizer")) {
            JsonObject an = Analysis.analysisSettings(settings);
            String name = f.def.get("normalizer").getAsString();
            if (an != null && an.has("normalizer") && an.getAsJsonObject("normalizer").has(name)) {
                JsonObject n = an.getAsJsonObject("normalizer").getAsJsonObject(name);
                String out = s;
                if (n.has("filter")) {
                    for (JsonElement fe : n.getAsJsonArray("filter")) {
                        switch (fe.getAsString()) {
                            case "lowercase" -> out = out.toLowerCase(Locale.ROOT);
                            case "uppercase" -> out = out.toUpperCase(Locale.ROOT);
                            case "trim" -> out = out.trim();
                            case "asciifolding" -> out = java.text.Normalizer.normalize(out, java.text.Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
                            default -> {
                            }
                        }
                    }
                }
                return out;
            }
        }
        return s;
    }

    /** All typed values of {@code f} in the (sub)document; text fields yield their raw strings. */
    static List<Object> of(Mappings.Field f, JsonObject obj, String relPath, JsonObject settings, ZoneId zone) {
        List<JsonElement> raw = Mappings.values(obj, relPath);
        List<Object> out = new ArrayList<>(raw.size());
        for (JsonElement e : raw) {
            if (f.isText()) {
                if (e.isJsonPrimitive()) {
                    out.add(e.getAsString());
                }
                continue;
            }
            Object v = parse(f, e, settings, zone);
            if (v != null) {
                out.add(v);
            }
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compare(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            if (x instanceof Long lx && y instanceof Long ly) {
                return Long.compare(lx, ly);
            }
            return Double.compare(x.doubleValue(), y.doubleValue());
        }
        if (a instanceof String s && b instanceof String t) {
            return compareCodePoints(s, t);
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        return ((Comparable) String.valueOf(a)).compareTo(String.valueOf(b));
    }

    static int compareCodePoints(String s, String t) {
        int i = 0, j = 0;
        while (i < s.length() && j < t.length()) {
            int a = s.codePointAt(i), b = t.codePointAt(j);
            if (a != b) {
                return Integer.compare(a, b);
            }
            i += Character.charCount(a);
            j += Character.charCount(b);
        }
        return Integer.compare(s.length() - i, t.length() - j);
    }

    static double toDouble(Object o) {
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        if (o instanceof Boolean b) {
            return b ? 1 : 0;
        }
        return Double.NaN;
    }
}
