package com.nexagres.wire.influxwire;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Parses InfluxDB line protocol -- the real wire format every real InfluxDB client SDK sends to
 * {@code POST /write}, independent of whether the server speaks the v1 or v2 HTTP API (both use
 * this same point-encoding). One line is one point:
 * <pre>
 *   measurement[,tag_key=tag_value[,tag_key2=tag_value2...]] field_key=field_value[,field_key2=field_value2...] [timestamp]
 * </pre>
 * e.g. {@code weather,station=NYC temperature=72.5,humidity=45i 1717171717000000000}.
 *
 * <p><b>Field value types</b> (real line protocol's own suffix rules, not something this codebase
 * invented): a bare number is a float ({@code Double}); a number with a trailing {@code i} is a
 * signed integer ({@code Long}, the {@code i} stripped); {@code t}/{@code T}/{@code true}/
 * {@code True}/{@code TRUE} or {@code f}/{@code F}/{@code false}/{@code False}/{@code FALSE} is a
 * boolean; anything double-quoted is a string. Unsigned integers ({@code u} suffix) are parsed as
 * {@code Long} too -- V1 doesn't distinguish signed/unsigned, matching every other real InfluxDB-
 * compatible reimplementation's usual first-pass scope.
 *
 * <p><b>Escaping</b> handled: backslash-escaped commas, spaces, and equals signs in measurement
 * names, tag keys/values, and field keys (real line protocol's own escaping rules), and
 * backslash-escaped double quotes/backslashes inside a double-quoted string field value. Not
 * handled (V1 scope): line-protocol comments, or malformed input recovery -- a bad line throws
 * {@link InfluxException} with the offending line included, matching {@code OpenSearchAdapter}'s
 * "unrecognized clause fails loudly" policy rather than silently dropping or guessing.
 */
public final class LineProtocolParser {

    private LineProtocolParser() {
    }

    /**
     * @param precision the {@code /write?precision=} query param ({@code ns}/{@code us}/
     *     {@code ms}/{@code s}, real InfluxDB's own accepted values; {@code null} or anything else
     *     defaults to {@code ns}, matching real InfluxDB's own default) -- every point's timestamp
     *     is scaled up to nanoseconds so {@link InfluxPoint#timestampNanos()} is always
     *     precision-agnostic for callers.
     */
    public static List<InfluxPoint> parse(String body, String precision) {
        long nanosPerUnit = nanosPerUnit(precision);
        List<InfluxPoint> points = new java.util.ArrayList<>();
        for (String rawLine : body.split("\n")) {
            String line = rawLine.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            points.add(parseLine(line, nanosPerUnit));
        }
        return points;
    }

    private static long nanosPerUnit(String precision) {
        if (precision == null) {
            return 1;
        }
        return switch (precision) {
            case "us" -> TimeUnit.MICROSECONDS.toNanos(1);
            case "ms" -> TimeUnit.MILLISECONDS.toNanos(1);
            case "s" -> TimeUnit.SECONDS.toNanos(1);
            default -> 1; // "ns" or unrecognized -- real InfluxDB's own default is ns.
        };
    }

    private static InfluxPoint parseLine(String line, long nanosPerUnit) {
        // Three whitespace-separated sections, but only the boundaries OUTSIDE a quoted string
        // count -- a string field value can itself contain a space ("hello world"). Walk the line
        // once, tracking quote state, to find the two real (unquoted) space boundaries.
        int firstSpace = unquotedIndexOf(line, ' ', 0);
        if (firstSpace < 0) {
            throw new InfluxException("line protocol point has no field set: \"" + line + "\"");
        }
        int secondSpace = unquotedIndexOf(line, ' ', firstSpace + 1);
        String measurementAndTags = line.substring(0, firstSpace);
        String fieldSet = secondSpace < 0 ? line.substring(firstSpace + 1) : line.substring(firstSpace + 1, secondSpace);
        String timestampPart = secondSpace < 0 ? null : line.substring(secondSpace + 1).strip();

        List<String> mtParts = splitUnescaped(measurementAndTags, ',');
        String measurement = unescape(mtParts.get(0));
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 1; i < mtParts.size(); i++) {
            String[] kv = splitOneUnescaped(mtParts.get(i), '=');
            tags.put(unescape(kv[0]), unescape(kv[1]));
        }

        Map<String, Object> fields = new LinkedHashMap<>();
        for (String fieldPair : splitUnescaped(fieldSet, ',')) {
            String[] kv = splitOneUnescaped(fieldPair, '=');
            fields.put(unescape(kv[0]), parseFieldValue(kv[1]));
        }
        if (fields.isEmpty()) {
            throw new InfluxException("line protocol point has an empty field set: \"" + line + "\"");
        }

        long timestampNanos = timestampPart == null || timestampPart.isEmpty()
                ? TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis())
                : Long.parseLong(timestampPart) * nanosPerUnit;
        return new InfluxPoint(measurement, tags, fields, timestampNanos);
    }

    private static Object parseFieldValue(String raw) {
        if (raw.length() >= 2 && raw.charAt(0) == '"' && raw.charAt(raw.length() - 1) == '"') {
            return unescapeQuoted(raw.substring(1, raw.length() - 1));
        }
        if (raw.equalsIgnoreCase("true") || raw.equals("t") || raw.equals("T")) {
            return Boolean.TRUE;
        }
        if (raw.equalsIgnoreCase("false") || raw.equals("f") || raw.equals("F")) {
            return Boolean.FALSE;
        }
        if (raw.endsWith("i") || raw.endsWith("u")) {
            return Long.parseLong(raw.substring(0, raw.length() - 1));
        }
        return Double.parseDouble(raw);
    }

    /** Splits on an unescaped delimiter, leaving {@code \<delim>} sequences intact for
     * {@link #unescape} to resolve afterward -- so a tag value like {@code a\,b} survives as one
     * token, not two. */
    private static List<String> splitUnescaped(String s, char delim) {
        List<String> parts = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
                cur.append(c);
            } else if (c == '\\' && i + 1 < s.length()) {
                cur.append(c).append(s.charAt(++i));
            } else if (c == delim && !inQuotes) {
                parts.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        parts.add(cur.toString());
        return parts;
    }

    private static String[] splitOneUnescaped(String s, char delim) {
        List<String> parts = splitUnescaped(s, delim);
        if (parts.size() < 2) {
            throw new InfluxException("expected \"key" + delim + "value\", got \"" + s + "\"");
        }
        // A field/tag value may itself validly contain '=' once unescaped is applied elsewhere
        // (e.g. a quoted string field), so only the FIRST delimiter splits key from value; any
        // remaining delimiters belong to the value.
        return new String[] {parts.get(0), s.substring(parts.get(0).length() + 1)};
    }

    private static String unescape(String s) {
        return s.replace("\\,", ",").replace("\\ ", " ").replace("\\=", "=");
    }

    private static String unescapeQuoted(String s) {
        return s.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static int unquotedIndexOf(String s, char target, int from) {
        boolean inQuotes = false;
        for (int i = from; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == '\\' && i + 1 < s.length()) {
                i++;
            } else if (c == target && !inQuotes) {
                return i;
            }
        }
        return -1;
    }
}
