package com.sayonora.warp.oswire;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Date parsing/formatting/date-math for oswire's {@code date} fields ({@code format}s, {@code now-1d/d}, ...). */
final class Dates {

    static final String DEFAULT_FORMAT = "strict_date_optional_time||epoch_millis";

    private static final Pattern ISO = Pattern.compile(
            "^(\\d{4})(?:-(\\d{1,2})(?:-(\\d{1,2}))?)?(?:T(\\d{1,2})(?::(\\d{1,2})(?::(\\d{1,2})(?:[.,](\\d{1,9}))?)?)?(Z|[+-]\\d{2}(?::?\\d{2})?)?)?$");
    private static final Pattern MATH = Pattern.compile("([+-]\\d*[yMwdhHms]|/[yMwdhHms])");

    private Dates() {
    }

    /** Parses one value (string, or a number of epoch millis) into epoch millis. Throws IllegalArgumentException. */
    static long parse(String s, String format, ZoneId zone) {
        String fmt = format == null || format.isBlank() ? DEFAULT_FORMAT : format;
        for (String f : fmt.split("\\|\\|")) {
            Long v = tryParse(s.trim(), f.startsWith("8") && f.length() > 1 && !f.startsWith("8_") ? f.substring(1) : f, zone);
            if (v != null) {
                return v;
            }
        }
        throw new IllegalArgumentException("failed to parse date field [" + s + "] with format [" + fmt + "]");
    }

    private static Long tryParse(String s, String f, ZoneId zone) {
        try {
            switch (f) {
                case "epoch_millis":
                    return Long.parseLong(s);
                case "epoch_second":
                    return Long.parseLong(s) * 1000L;
                case "strict_date_optional_time", "date_optional_time", "strict_date_optional_time_nanos",
                        "date_time", "strict_date_time", "date_time_no_millis", "strict_date_time_no_millis",
                        "date", "strict_date", "year", "strict_year", "year_month", "strict_year_month",
                        "date_hour_minute_second", "strict_date_hour_minute_second", "date_hour_minute_second_millis",
                        "strict_date_hour_minute_second_millis", "date_hour_minute", "strict_date_hour_minute",
                        "date_hour", "strict_date_hour", "date_hour_minute_second_fraction":
                    return iso(s, zone, f.startsWith("strict"));
                case "basic_date":
                    return LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(zone).toInstant().toEpochMilli();
                case "basic_date_time", "basic_date_time_no_millis":
                    return java.time.OffsetDateTime.parse(s, DateTimeFormatter.ofPattern(
                            f.endsWith("millis") ? "yyyyMMdd'T'HHmmssXX" : "yyyyMMdd'T'HHmmss.SSSXX")).toInstant().toEpochMilli();
                default:
                    return custom(s, f, zone);
            }
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Long iso(String s, ZoneId zone, boolean strict) {
        Matcher m = ISO.matcher(s);
        if (!m.matches()) {
            return null;
        }
        if (strict && ((m.group(2) != null && m.group(2).length() != 2) || (m.group(3) != null && m.group(3).length() != 2)
                || (m.group(4) != null && m.group(4).length() != 2))) {
            return null;
        }
        int y = Integer.parseInt(m.group(1));
        int mo = m.group(2) == null ? 1 : Integer.parseInt(m.group(2));
        int d = m.group(3) == null ? 1 : Integer.parseInt(m.group(3));
        int h = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
        int mi = m.group(5) == null ? 0 : Integer.parseInt(m.group(5));
        int sec = m.group(6) == null ? 0 : Integer.parseInt(m.group(6));
        int nanos = 0;
        if (m.group(7) != null) {
            nanos = Integer.parseInt((m.group(7) + "000000000").substring(0, 9));
        }
        LocalDateTime ldt = LocalDateTime.of(y, mo, d, h, mi, sec, nanos);
        ZoneId z = zone;
        if (m.group(8) != null) {
            String tz = m.group(8);
            z = tz.equals("Z") ? ZoneOffset.UTC : ZoneOffset.of(tz.length() == 5 ? tz.substring(0, 3) + ":" + tz.substring(3) : tz);
        }
        return ldt.atZone(z).toInstant().toEpochMilli();
    }

    private static Long custom(String s, String pattern, ZoneId zone) {
        String p = pattern.replace("yyyy", "uuuu").replace("YYYY", "uuuu");
        DateTimeFormatter f = DateTimeFormatter.ofPattern(p, Locale.ROOT);
        TemporalAccessor t = f.parseBest(s, ZonedDateTime::from, LocalDateTime::from, LocalDate::from, LocalTime::from);
        if (t instanceof ZonedDateTime z) {
            return z.toInstant().toEpochMilli();
        }
        if (t instanceof LocalDateTime l) {
            return l.atZone(zone).toInstant().toEpochMilli();
        }
        if (t instanceof LocalDate l) {
            return l.atStartOfDay(zone).toInstant().toEpochMilli();
        }
        return ((LocalTime) t).atDate(LocalDate.of(1970, 1, 1)).atZone(zone).toInstant().toEpochMilli();
    }

    /** Parses a range-bound / date-math expression: {@code now-1d/d}, {@code 2020-01-01||+1M/d}, or plain date. */
    static long parseMath(String expr, String format, ZoneId zone, boolean roundUp, long now) {
        String base;
        String math;
        if (expr.startsWith("now")) {
            base = null;
            math = expr.substring(3);
        } else {
            int i = expr.indexOf("||");
            if (i >= 0) {
                base = expr.substring(0, i);
                math = expr.substring(i + 2);
            } else {
                base = expr;
                math = "";
            }
        }
        long millis = base == null ? now : parse(base, format, zone);
        if (math.isEmpty()) {
            return millis;
        }
        ZonedDateTime t = Instant.ofEpochMilli(millis).atZone(zone);
        Matcher m = MATH.matcher(math);
        int pos = 0;
        while (m.find()) {
            if (m.start() != pos) {
                throw new IllegalArgumentException("operator not supported for date math [" + expr + "]");
            }
            pos = m.end();
            String op = m.group(1);
            if (op.charAt(0) == '/') {
                t = round(t, op.charAt(1), roundUp);
            } else {
                int sign = op.charAt(0) == '-' ? -1 : 1;
                String digits = op.substring(1, op.length() - 1);
                long amount = digits.isEmpty() ? 1 : Long.parseLong(digits);
                t = t.plus(sign * amount, unit(op.charAt(op.length() - 1)));
                if (op.charAt(op.length() - 1) == 'w') {
                    // plus() on ChronoUnit.WEEKS is fine; kept explicit for clarity
                }
            }
        }
        if (pos != math.length()) {
            throw new IllegalArgumentException("unit [" + math.substring(pos) + "] not supported for date math [" + expr + "]");
        }
        return t.toInstant().toEpochMilli();
    }

    private static ChronoUnit unit(char c) {
        return switch (c) {
            case 'y' -> ChronoUnit.YEARS;
            case 'M' -> ChronoUnit.MONTHS;
            case 'w' -> ChronoUnit.WEEKS;
            case 'd' -> ChronoUnit.DAYS;
            case 'h', 'H' -> ChronoUnit.HOURS;
            case 'm' -> ChronoUnit.MINUTES;
            default -> ChronoUnit.SECONDS;
        };
    }

    private static ZonedDateTime round(ZonedDateTime t, char u, boolean up) {
        ZonedDateTime floor = switch (u) {
            case 'y' -> t.with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS);
            case 'M' -> t.with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS);
            case 'w' -> t.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).truncatedTo(ChronoUnit.DAYS);
            case 'd' -> t.truncatedTo(ChronoUnit.DAYS);
            case 'h', 'H' -> t.truncatedTo(ChronoUnit.HOURS);
            case 'm' -> t.truncatedTo(ChronoUnit.MINUTES);
            default -> t.truncatedTo(ChronoUnit.SECONDS);
        };
        return up ? floor.plus(1, unit(u)).minus(1, ChronoUnit.MILLIS) : floor;
    }

    /** Default {@code strict_date_optional_time} rendering ({@code 2020-01-01T00:00:00.000Z}) or the given format. */
    static String format(long millis, String format, ZoneId zone) {
        ZonedDateTime t = Instant.ofEpochMilli(millis).atZone(zone);
        if (format == null || format.isBlank() || format.startsWith("strict_date_optional_time") || format.equals("date_optional_time")
                || format.startsWith("date_time")) {
            return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.ROOT).format(t).replace("+00:00", "Z");
        }
        String first = format.split("\\|\\|")[0];
        return switch (first) {
            case "epoch_millis" -> Long.toString(millis);
            case "epoch_second" -> Long.toString(Math.floorDiv(millis, 1000L));
            case "date", "strict_date" -> DateTimeFormatter.ISO_LOCAL_DATE.format(t);
            case "basic_date" -> DateTimeFormatter.BASIC_ISO_DATE.format(t);
            case "year" -> String.format("%04d", t.getYear());
            case "year_month" -> DateTimeFormatter.ofPattern("yyyy-MM", Locale.ROOT).format(t);
            default -> {
                try {
                    yield DateTimeFormatter.ofPattern(first.replace("yyyy", "uuuu"), Locale.ROOT).format(t);
                } catch (RuntimeException e) {
                    yield DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.ROOT).format(t).replace("+00:00", "Z");
                }
            }
        };
    }

    /** Resolves a date-math index name: {@code <logstash-{now/d}>}, {@code <idx-{now-1d/d{yyyy.MM.dd|+02:00}}>}. */
    static String indexNameMath(String expr) {
        String inner = expr.substring(1, expr.length() - 1);
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < inner.length()) {
            char c = inner.charAt(i);
            if (c == '\\' && i + 1 < inner.length()) {
                out.append(inner.charAt(i + 1));
                i += 2;
            } else if (c == '{') {
                int depth = 1, j = i + 1;
                while (j < inner.length() && depth > 0) {
                    if (inner.charAt(j) == '{') {
                        depth++;
                    } else if (inner.charAt(j) == '}') {
                        depth--;
                    }
                    j++;
                }
                if (depth != 0) {
                    throw OpenSearchException.illegalArgument("invalid dynamic name expression [" + expr + "]. date math expression must be closed");
                }
                String body = inner.substring(i + 1, j - 1);
                String math = body, fmt = "yyyy.MM.dd", tz = null;
                int brace = body.indexOf('{');
                if (brace >= 0) {
                    math = body.substring(0, brace);
                    String f = body.substring(brace + 1, body.lastIndexOf('}'));
                    int bar = f.indexOf('|');
                    fmt = bar >= 0 ? f.substring(0, bar) : f;
                    tz = bar >= 0 ? f.substring(bar + 1) : null;
                }
                ZoneId z = zone(tz);
                long millis = parseMath(math, null, z, false, System.currentTimeMillis());
                out.append(DateTimeFormatter.ofPattern(fmt.replace("yyyy", "uuuu"), Locale.ROOT).format(Instant.ofEpochMilli(millis).atZone(z)));
                i = j;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    static ZoneId zone(String tz) {
        if (tz == null || tz.isBlank() || tz.equals("Z")) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneId.of(tz.startsWith("+") || tz.startsWith("-") ? tz : tz);
        } catch (java.time.DateTimeException e) {
            throw OpenSearchException.illegalArgument("unknown time zone [" + tz + "]");
        }
    }
}
