package com.sayonora.wire.influxwire;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/** Result model and its JSON / CSV renderings, matching InfluxDB 1.x's HTTP output byte for byte where feasible. */
final class InfluxFmt {

    private InfluxFmt() {
    }

    /** A time-column value (epoch nanoseconds). */
    record TimeV(long nanos) {
    }

    static final class Series {
        String name;
        Map<String, String> tags;
        List<String> columns;
        List<Object[]> values = new java.util.ArrayList<>();
        /** Source tags of each row of a plain (non-aggregated) SELECT, so an outer query can group by them. */
        List<Map<String, String>> rowTags;
        boolean partial;

        Series(String name, Map<String, String> tags, List<String> columns) {
            this.name = name;
            this.tags = tags;
            this.columns = columns;
        }
    }

    static final class StmtResult {
        int id;
        String error;
        List<String[]> messages;
        List<Series> series = new java.util.ArrayList<>();
        boolean partial;
    }

    // ------------------------------------------------------------------ scalars

    /** Go's {@code strconv.FormatFloat(f, 'f'|'e', -1, 64)} as {@code encoding/json} emits it. */
    static String jsonFloat(double d) {
        if (d == 0) {
            return (1 / d < 0) ? "-0" : "0";
        }
        double abs = Math.abs(d);
        String digitsExp = Double.toString(abs);
        // shortest round-trip digits and decimal exponent from Java's (JDK 19+) shortest representation
        BigDecimal bd = new BigDecimal(digitsExp);
        String unscaled = bd.unscaledValue().toString();
        int exp10 = unscaled.length() - 1 - bd.scale(); // exponent of the leading digit
        String digits = unscaled.replaceAll("0+$", "");
        if (digits.isEmpty()) {
            digits = "0";
        }
        StringBuilder sb = new StringBuilder();
        if (d < 0) {
            sb.append('-');
        }
        if (abs < 1e-6 || abs >= 1e21) {
            sb.append(digits.charAt(0));
            if (digits.length() > 1) {
                sb.append('.').append(digits, 1, digits.length());
            }
            sb.append('e');
            sb.append(exp10 < 0 ? '-' : '+');
            int e = Math.abs(exp10);
            sb.append(e < 10 ? "0" + e : String.valueOf(e));
            // Go trims "e-09" to "e-9" but keeps two digits for >= 10
            String s = sb.toString();
            return s.replaceFirst("e([+-])0(\\d)$", "e$1$2");
        }
        if (exp10 >= 0) {
            if (digits.length() <= exp10 + 1) {
                sb.append(digits);
                for (int i = digits.length(); i < exp10 + 1; i++) {
                    sb.append('0');
                }
            } else {
                sb.append(digits, 0, exp10 + 1).append('.').append(digits, exp10 + 1, digits.length());
            }
        } else {
            sb.append("0.");
            for (int i = 0; i < -exp10 - 1; i++) {
                sb.append('0');
            }
            sb.append(digits);
        }
        return sb.toString();
    }

    private static final DateTimeFormatter SECS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    static String rfc3339Nano(long nanos) {
        long sec = Math.floorDiv(nanos, 1_000_000_000L);
        long frac = Math.floorMod(nanos, 1_000_000_000L);
        String base = SECS.format(Instant.ofEpochSecond(sec));
        if (frac == 0) {
            return base + "Z";
        }
        String f = String.format("%09d", frac).replaceAll("0+$", "");
        return base + "." + f + "Z";
    }

    /** epoch: null = RFC3339; ns, u, ms, s, m, h. */
    static String timeValue(long nanos, String epoch) {
        if (epoch == null) {
            return "\"" + rfc3339Nano(nanos) + "\"";
        }
        return Long.toString(epochInt(nanos, epoch));
    }

    static long epochInt(long nanos, String epoch) {
        return switch (epoch) {
            case "u" -> nanos / 1_000L;
            case "ms" -> nanos / 1_000_000L;
            case "s" -> nanos / 1_000_000_000L;
            case "m" -> nanos / 60_000_000_000L;
            case "h" -> nanos / 3_600_000_000_000L;
            default -> nanos;
        };
    }

    static void jsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '<' -> sb.append("\\u003c");
                case '>' -> sb.append("\\u003e");
                case '&' -> sb.append("\\u0026");
                case ' ' -> sb.append("\\u2028");
                case ' ' -> sb.append("\\u2029");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    static void jsonValue(StringBuilder sb, Object v, String epoch) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof TimeV t) {
            sb.append(timeValue(t.nanos(), epoch));
        } else if (v instanceof Long l) {
            sb.append(l);
        } else if (v instanceof Double d) {
            if (d.isNaN() || d.isInfinite()) {
                sb.append("null");
            } else {
                sb.append(jsonFloat(d));
            }
        } else if (v instanceof Boolean b) {
            sb.append(b);
        } else {
            jsonString(sb, String.valueOf(v));
        }
    }

    // ------------------------------------------------------------------ JSON

    static String json(List<StmtResult> results, String epoch, boolean pretty) {
        if (results.isEmpty()) {
            return "{}";
        }
        for (StmtResult r : results) {
            for (Series s : r.series) {
                for (Object[] row : s.values) {
                    for (Object v : row) {
                        if (v instanceof Double d && d.isInfinite()) {
                            return "{\"error\":\"json: unsupported value: " + (d > 0 ? "+Inf" : "-Inf") + "\"}";
                        }
                    }
                }
            }
        }
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"results\":[");
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendResult(sb, results.get(i), epoch);
        }
        sb.append("]}");
        return pretty ? indent(sb.toString()) : sb.toString();
    }

    static void appendResult(StringBuilder sb, StmtResult r, String epoch) {
        sb.append("{\"statement_id\":").append(r.id);
        if (!r.series.isEmpty()) {
            sb.append(",\"series\":[");
            for (int i = 0; i < r.series.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                appendSeries(sb, r.series.get(i), epoch);
            }
            sb.append(']');
        }
        if (r.partial) {
            sb.append(",\"partial\":true");
        }
        if (r.messages != null && !r.messages.isEmpty()) {
            sb.append(",\"messages\":[");
            for (int i = 0; i < r.messages.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("{\"level\":");
                jsonString(sb, r.messages.get(i)[0]);
                sb.append(",\"text\":");
                jsonString(sb, r.messages.get(i)[1]);
                sb.append('}');
            }
            sb.append(']');
        }
        if (r.error != null) {
            sb.append(",\"error\":");
            jsonString(sb, r.error);
        }
        sb.append('}');
    }

    static void appendSeries(StringBuilder sb, Series s, String epoch) {
        sb.append('{');
        boolean first = true;
        if (s.name != null) {
            sb.append("\"name\":");
            jsonString(sb, s.name);
            first = false;
        }
        if (s.tags != null && !s.tags.isEmpty()) {
            sb.append(first ? "" : ",").append("\"tags\":{");
            boolean f2 = true;
            for (var e : new java.util.TreeMap<>(s.tags).entrySet()) {
                if (!f2) {
                    sb.append(',');
                }
                f2 = false;
                jsonString(sb, e.getKey());
                sb.append(':');
                jsonString(sb, e.getValue());
            }
            sb.append('}');
            first = false;
        }
        sb.append(first ? "" : ",").append("\"columns\":[");
        for (int i = 0; i < s.columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            jsonString(sb, s.columns.get(i));
        }
        sb.append(']');
        if (!s.values.isEmpty()) {
        sb.append(",\"values\":[");
        for (int i = 0; i < s.values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('[');
            Object[] row = s.values.get(i);
            for (int j = 0; j < row.length; j++) {
                if (j > 0) {
                    sb.append(',');
                }
                jsonValue(sb, row[j], epoch);
            }
            sb.append(']');
        }
        sb.append(']');
        }
        if (s.partial) {
            sb.append(",\"partial\":true");
        }
        sb.append('}');
    }

    /** Re-indents compact JSON the way Go's MarshalIndent(v, "", "    ") does. */
    static String indent(String compact) {
        StringBuilder out = new StringBuilder(compact.length() * 2);
        int depth = 0;
        boolean inStr = false;
        for (int i = 0; i < compact.length(); i++) {
            char c = compact.charAt(i);
            if (inStr) {
                out.append(c);
                if (c == '\\') {
                    out.append(compact.charAt(++i));
                } else if (c == '"') {
                    inStr = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> {
                    inStr = true;
                    out.append(c);
                }
                case '{', '[' -> {
                    char close = c == '{' ? '}' : ']';
                    if (i + 1 < compact.length() && compact.charAt(i + 1) == close) {
                        out.append(c).append(close);
                        i++;
                    } else {
                        depth++;
                        out.append(c).append('\n').append("    ".repeat(depth));
                    }
                }
                case '}', ']' -> {
                    depth--;
                    out.append('\n').append("    ".repeat(depth)).append(c);
                }
                case ',' -> out.append(",\n").append("    ".repeat(depth));
                case ':' -> out.append(": ");
                default -> out.append(c);
            }
        }
        return out.toString();
    }

    // ------------------------------------------------------------------ CSV

    static String csv(List<StmtResult> results, String epoch) {
        StringBuilder sb = new StringBuilder();
        boolean anyOutput = false;
        for (StmtResult r : results) {
            List<String> lastCols = null;
            for (Series s : r.series) {
                if (lastCols == null || !lastCols.equals(s.columns)) {
                    if (anyOutput && lastCols == null) {
                        sb.append('\n');
                    }
                    sb.append("name,tags,");
                    boolean first = true;
                    for (String c : s.columns) {
                        sb.append(first ? "" : ",").append(csvField(c));
                        first = false;
                    }
                    sb.append('\n');
                    lastCols = s.columns;
                }
                anyOutput = true;
                String tags = "";
                if (s.tags != null && !s.tags.isEmpty()) {
                    StringBuilder t = new StringBuilder();
                    for (var e : new java.util.TreeMap<>(s.tags).entrySet()) {
                        t.append(t.length() > 0 ? "," : "").append(e.getKey()).append('=').append(e.getValue());
                    }
                    tags = t.toString();
                }
                for (Object[] row : s.values) {
                    sb.append(csvField(s.name == null ? "" : s.name)).append(',').append(csvField(tags));
                    for (Object v : row) {
                        sb.append(',');
                        if (v == null) {
                            continue;
                        }
                        if (v instanceof TimeV t) {
                            sb.append(epoch == null ? rfc3339Nano(t.nanos()) : Long.toString(epochInt(t.nanos(), epoch)));
                        } else if (v instanceof Double d) {
                            sb.append(d.isNaN() || d.isInfinite() ? "" : jsonFloat(d));
                        } else {
                            sb.append(csvField(String.valueOf(v)));
                        }
                    }
                    sb.append('\n');
                }
            }
            if (r.error != null) {
                sb.append("error,").append(csvField(r.error)).append('\n');
            }
        }
        return sb.toString();
    }

    private static String csvField(String s) {
        if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    // ------------------------------------------------------------------ durations (SHOW RETENTION POLICIES)

    /** Go's {@code time.Duration.String()}. */
    static String goDuration(long nanos) {
        if (nanos == 0) {
            return "0s";
        }
        boolean neg = nanos < 0;
        long u = Math.abs(nanos);
        String out;
        if (u < 1_000L) {
            out = u + "ns";
        } else if (u < 1_000_000L) {
            out = trimFrac(0, u % 1_000, 3, u / 1_000) + "µs";
        } else if (u < 1_000_000_000L) {
            out = trimFrac(0, u % 1_000_000, 6, u / 1_000_000) + "ms";
        } else {
            long h = u / 3_600_000_000_000L;
            long rem = u % 3_600_000_000_000L;
            long m = rem / 60_000_000_000L;
            rem = rem % 60_000_000_000L;
            String secs = trimFrac(0, rem % 1_000_000_000L, 9, rem / 1_000_000_000L) + "s";
            out = (h > 0 ? h + "h" : "") + ((h > 0 || m > 0) ? m + "m" : "") + secs;
        }
        return neg ? "-" + out : out;
    }

    private static String trimFrac(double ignored, long frac, int digits, long whole) {
        if (frac == 0) {
            return Long.toString(whole);
        }
        String f = String.format("%0" + digits + "d", frac).replaceAll("0+$", "");
        return whole + "." + f;
    }
}
