package com.sayonora.wire.influxwire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parses InfluxDB line protocol. A faithful port of the scanning rules of InfluxDB 1.x's
 * {@code models.ParsePointsWithPrecision} (same accepted syntax, same error texts, e.g.
 * {@code unable to parse 'm v=1u 1': invalid number}), so a client sees the same accept/reject behaviour and
 * error messages as against real InfluxDB.
 *
 * <p>Field types: bare number = float; {@code i} suffix = integer; {@code t T true True TRUE f F false False FALSE}
 * = boolean; double-quoted = string. The unsigned {@code u} suffix is rejected ({@code invalid number}), exactly as
 * the stock InfluxDB 1.x binary does. CRLF is not accepted (the {@code \r} makes the timestamp bad), as in InfluxDB.
 */
public final class LineProtocolParser {

    static final long MIN_TIME = Long.MIN_VALUE + 2;
    static final long MAX_TIME = Long.MAX_VALUE - 1;

    private LineProtocolParser() {
    }

    /** Result of parsing a whole body: the valid points and one message per rejected line. */
    public record Parsed(List<InfluxPoint> points, List<String> errors) {
    }

    /** Whether {@code precision} is one InfluxDB's /write accepts. */
    public static boolean validPrecision(String precision) {
        return precision == null || switch (precision) {
            case "", "n", "ns", "u", "ms", "s", "m", "h" -> true;
            default -> false;
        };
    }

    static long multiplier(String precision) {
        if (precision == null) {
            return 1;
        }
        return switch (precision) {
            case "u" -> 1_000L;
            case "ms" -> 1_000_000L;
            case "s" -> 1_000_000_000L;
            case "m" -> 60_000_000_000L;
            case "h" -> 3_600_000_000_000L;
            default -> 1L;
        };
    }

    /** Strict variant for in-process callers (MCP): any bad line throws. */
    public static List<InfluxPoint> parse(String body, String precision) {
        Parsed p = parseLenient(body, precision);
        if (!p.errors().isEmpty()) {
            throw new InfluxException(String.join("\n", p.errors()));
        }
        return p.points();
    }

    public static Parsed parseLenient(String body, String precision) {
        long mult = multiplier(precision);
        long now = System.currentTimeMillis() * 1_000_000L + (System.nanoTime() % 1_000_000L + 1_000_000L) % 1_000_000L;
        now = now - Math.floorMod(now, mult);
        List<InfluxPoint> points = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        int n = body.length();
        int pos = 0;
        while (pos < n) {
            int end = scanLine(body, pos);
            String block = body.substring(pos, end);
            pos = end + 1;
            if (block.isEmpty()) {
                continue;
            }
            int start = skipWs(block, 0);
            if (start >= block.length() || block.charAt(start) == '#') {
                continue;
            }
            String line = block.substring(start);
            try {
                points.add(parsePoint(line, now, mult));
            } catch (LineError e) {
                failed.add("unable to parse '" + line + "': " + e.getMessage());
            }
        }
        return new Parsed(points, failed);
    }

    private static final class LineError extends RuntimeException {
        LineError(String m) {
            super(m, null, false, false);
        }
    }

    private static int scanLine(String s, int i) {
        boolean quoted = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\n' && !quoted) {
                break;
            }
            if (c == '\\' && i + 1 < s.length()) {
                i++;
                continue;
            }
            if (c == '"') {
                quoted = !quoted;
            }
        }
        return i;
    }

    private static int skipWs(String s, int i) {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c != ' ' && c != '\t' && c != 0) {
                break;
            }
            i++;
        }
        return i;
    }

    private static InfluxPoint parsePoint(String buf, long now, long mult) {
        int[] pos = {0};
        String key = scanKey(buf, pos);
        if (key.isEmpty()) {
            throw new LineError("missing measurement");
        }
        if (key.length() > 65535) {
            throw new LineError("max key length exceeded: " + key.length() + " > 65535");
        }
        if (pos[0] >= buf.length()) {
            throw new LineError("missing fields");
        }
        String fieldBlock = scanFields(buf, pos);
        String ts = scanTime(buf, pos);
        int rest = skipWs(buf, pos[0]);
        if (rest < buf.length()) {
            throw new LineError("point is invalid");
        }
        // measurement + tags
        List<String> parts = splitUnescaped(key, ',');
        String measurement = unescapeMeasurement(parts.get(0));
        if (measurement.isEmpty()) {
            throw new LineError("missing measurement");
        }
        TreeMap<String, String> tags = new TreeMap<>();
        for (int i = 1; i < parts.size(); i++) {
            String kv = parts.get(i);
            int eq = indexOfUnescaped(kv, '=');
            if (eq < 0) {
                throw new LineError("missing tag value");
            }
            String k = unescapeTag(kv.substring(0, eq));
            String v = unescapeTag(kv.substring(eq + 1));
            if (tags.containsKey(k)) {
                throw new LineError("duplicate tags");
            }
            tags.put(k, v);
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        for (String f : splitFields(fieldBlock)) {
            int eq = indexOfUnescaped(f, '=');
            String k = unescapeTag(f.substring(0, eq));
            fields.put(k, fieldValue(f.substring(eq + 1)));
        }
        long time;
        if (ts.isEmpty()) {
            time = now;
        } else {
            long v;
            try {
                v = Long.parseLong(ts);
            } catch (NumberFormatException e) {
                throw new LineError("strconv.ParseInt: parsing \"" + ts + "\": "
                        + (ts.equals("-") ? "invalid syntax" : "value out of range"));
            }
            time = v * mult;
        }
        if (time < MIN_TIME || time > MAX_TIME) {
            throw new LineError("time outside range " + MIN_TIME + " - " + MAX_TIME);
        }
        return new InfluxPoint(measurement, tags, fields, time);
    }

    private static String scanKey(String buf, int[] posRef) {
        int start = skipWs(buf, posRef[0]);
        int i = start;
        int commas = 0;
        int equals = 0;
        while (i < buf.length()) {
            char c = buf.charAt(i);
            if (c == '=' && commas > 0) {
                if (i - 1 < 0 || i - 2 < 0) {
                    throw new LineError("missing tag key");
                }
                if (buf.charAt(i - 1) == ',' && buf.charAt(i - 2) != '\\') {
                    throw new LineError("missing tag key");
                }
                if (buf.charAt(i - 1) == ' ' && buf.charAt(i - 2) != '\\') {
                    throw new LineError("missing tag key");
                }
                i++;
                equals++;
                if (i < buf.length() && (buf.charAt(i) == ',' || buf.charAt(i) == ' ') && buf.charAt(i - 1) != '\\') {
                    throw new LineError("missing tag value");
                }
                continue;
            }
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == ' ' && i > 0 && buf.charAt(i - 1) != '\\') {
                break;
            }
            if (c == ',' && i > 0 && buf.charAt(i - 1) != '\\') {
                commas++;
            }
            i++;
        }
        if (i > buf.length()) {
            i = buf.length();
        }
        if (commas != equals) {
            throw new LineError("missing tag value");
        }
        posRef[0] = i;
        return buf.substring(start, i);
    }

    private static String scanFields(String buf, int[] posRef) {
        int start = skipWs(buf, posRef[0]);
        int i = start;
        boolean quoted = false;
        int equals = 0;
        int commas = 0;
        while (i < buf.length()) {
            char c = buf.charAt(i);
            if (c == '\\' && i + 1 < buf.length()) {
                i += 2;
                continue;
            }
            if (c == '"' && equals > commas) {
                quoted = !quoted;
                i++;
                continue;
            }
            if (c == '=' && !quoted) {
                equals++;
                if (i - 1 >= 0 && buf.charAt(i - 1) == ' ' && (i - 2 < 0 || buf.charAt(i - 2) != '\\')) {
                    throw new LineError("missing field key");
                }
                if (i - 1 >= 0 && buf.charAt(i - 1) == ',' && (i - 2 < 0 || buf.charAt(i - 2) != '\\')) {
                    throw new LineError("missing field key");
                }
                if (i + 1 >= buf.length()) {
                    throw new LineError("missing field value");
                }
                char nx = buf.charAt(i + 1);
                if (nx == ',' || nx == ' ') {
                    throw new LineError("missing field value");
                }
                if ((nx >= '0' && nx <= '9') || nx == '.' || nx == '-' || nx == 'N' || nx == 'n') {
                    i = scanNumber(buf, i + 1);
                    continue;
                }
                if (nx != '"') {
                    i = scanBoolean(buf, i + 1);
                    continue;
                }
            }
            if (c == ',' && !quoted) {
                commas++;
            }
            if (c == ' ' && !quoted) {
                break;
            }
            i++;
        }
        if (quoted) {
            throw new LineError("unbalanced quotes");
        }
        if (equals == 0 || commas != equals - 1) {
            throw new LineError("invalid field format");
        }
        posRef[0] = Math.min(i, buf.length());
        return buf.substring(start, Math.min(i, buf.length()));
    }

    private static int scanNumber(String buf, int i) {
        int start = i;
        boolean isInt = false;
        boolean isUnsigned = false;
        if (i < buf.length() && buf.charAt(i) == '-') {
            i++;
            if (i == buf.length()) {
                throw new LineError("invalid number");
            }
        }
        boolean decimal = false;
        boolean scientific = false;
        while (i < buf.length()) {
            char c = buf.charAt(i);
            if (c == ',' || c == ' ') {
                break;
            }
            if (c == 'i' && i > start && !(isInt || isUnsigned)) {
                isInt = true;
                i++;
                continue;
            } else if (c == 'u' && i > start && !(isInt || isUnsigned)) {
                isUnsigned = true;
                i++;
                continue;
            }
            if (c == '.') {
                if (decimal) {
                    throw new LineError("invalid number");
                }
                decimal = true;
            }
            if (i > start && (c == 'e' || c == 'E')) {
                scientific = true;
                i++;
                continue;
            }
            if ((c == '+' || c == '-') && (buf.charAt(i - 1) == 'e' || buf.charAt(i - 1) == 'E')) {
                i++;
                continue;
            }
            if (i + 2 < buf.length() && (c == 'N' || c == 'n')) {
                throw new LineError("invalid number");
            }
            if (!((c >= '0' && c <= '9') || c == '.')) {
                throw new LineError("invalid number");
            }
            i++;
        }
        if ((isInt || isUnsigned) && (decimal || scientific)) {
            throw new LineError("invalid number");
        }
        if (isUnsigned) {
            throw new LineError("invalid number");
        }
        String txt = buf.substring(start, i);
        int digits = 0;
        for (int k = 0; k < txt.length(); k++) {
            if (Character.isDigit(txt.charAt(k))) {
                digits++;
            }
        }
        if (digits == 0) {
            throw new LineError("invalid number");
        }
        if (isInt) {
            String num = txt.substring(0, txt.length() - 1);
            try {
                Long.parseLong(num);
            } catch (NumberFormatException e) {
                throw new LineError("unable to parse integer " + num + ": strconv.ParseInt: parsing \"" + num + "\": value out of range");
            }
        } else {
            try {
                double d = Double.parseDouble(txt);
                if (Double.isInfinite(d) || Double.isNaN(d)) {
                    throw new LineError("invalid float");
                }
            } catch (NumberFormatException e) {
                throw new LineError("invalid float");
            }
        }
        return i;
    }

    private static int scanBoolean(String buf, int i) {
        int start = i;
        while (i < buf.length() && buf.charAt(i) != ',' && buf.charAt(i) != ' ') {
            i++;
        }
        switch (buf.substring(start, i)) {
            case "t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE":
                return i;
            default:
                throw new LineError("invalid boolean");
        }
    }

    private static String scanTime(String buf, int[] posRef) {
        int start = skipWs(buf, posRef[0]);
        int i = start;
        while (i < buf.length()) {
            char c = buf.charAt(i);
            if (c == '\n' || c == ' ') {
                break;
            }
            if (c < '0' || c > '9') {
                if (i == start && c == '-') {
                    i++;
                    continue;
                }
                throw new LineError("bad timestamp");
            }
            i++;
        }
        posRef[0] = i;
        return buf.substring(start, i);
    }

    private static Object fieldValue(String raw) {
        if (raw.length() >= 2 && raw.charAt(0) == '"') {
            // like InfluxDB: strip the first and last byte without checking the closing quote
            return unescapeQuoted(raw.substring(1, raw.length() - 1));
        }
        switch (raw) {
            case "t", "T", "true", "True", "TRUE":
                return Boolean.TRUE;
            case "f", "F", "false", "False", "FALSE":
                return Boolean.FALSE;
            default:
        }
        if (raw.endsWith("i")) {
            return Long.parseLong(raw.substring(0, raw.length() - 1));
        }
        return Double.parseDouble(raw);
    }

    private static List<String> splitUnescaped(String s, char delim) {
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                cur.append(c).append(s.charAt(++i));
            } else if (c == delim) {
                parts.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        parts.add(cur.toString());
        return parts;
    }

    /** Field pairs split on commas outside double-quoted values. */
    private static List<String> splitFields(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        int equals = 0;
        int commas = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                cur.append(c).append(s.charAt(++i));
                continue;
            }
            if (c == '"' && equals > commas) {
                quoted = !quoted;
            } else if (c == '=' && !quoted) {
                equals++;
            }
            if (c == ',' && !quoted) {
                commas++;
                parts.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        parts.add(cur.toString());
        return parts;
    }

    private static int indexOfUnescaped(String s, char target) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                i++;
            } else if (c == target) {
                return i;
            }
        }
        return -1;
    }

    private static String unescapeMeasurement(String s) {
        return s.replace("\\,", ",").replace("\\ ", " ");
    }

    private static String unescapeTag(String s) {
        return s.replace("\\,", ",").replace("\\=", "=").replace("\\ ", " ");
    }

    private static String unescapeQuoted(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length() && (s.charAt(i + 1) == '"' || s.charAt(i + 1) == '\\')) {
                sb.append(s.charAt(++i));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
