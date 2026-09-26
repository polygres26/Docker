package com.sayonora.wire.boltwire;

import com.sayonora.wire.boltwire.Values.DurationV;
import com.sayonora.wire.boltwire.Values.PointV;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Temporal (date / time / datetime / duration) and spatial (point) functions, arithmetic, parsing and formatting. */
final class Temporal {

    private Temporal() {
    }

    static final Object NOT_TEMPORAL = new Object();

    private static final long AVG_MONTH_SECONDS = 2_629_746L; // 30.436875 days
    private static final BigDecimal NANOS = BigDecimal.valueOf(1_000_000_000L);

    // ------------------------------------------------------------------------------------------ registry

    private static final List<String> KINDS = List.of("date", "localtime", "time", "localdatetime", "datetime");

    static int[] arity(String n) {
        if (KINDS.contains(n) || n.equals("duration")) {
            return new int[] {0, 1};
        }
        int dot = n.indexOf('.');
        if (dot > 0) {
            String base = n.substring(0, dot), fn = n.substring(dot + 1);
            if (KINDS.contains(base)) {
                return switch (fn) {
                    case "realtime", "statement", "transaction" -> new int[] {0, 1};
                    case "truncate" -> new int[] {2, 3};
                    case "fromepoch" -> base.equals("datetime") ? new int[] {2, 2} : null;
                    case "fromepochmillis" -> base.equals("datetime") ? new int[] {1, 1} : null;
                    default -> null;
                };
            }
            if (base.equals("duration") && (fn.equals("between") || fn.equals("inmonths") || fn.equals("indays")
                    || fn.equals("inseconds"))) {
                return new int[] {2, 2};
            }
        }
        return null;
    }

    static boolean handlesNull(String n) {
        return arity(n) != null;
    }

    static Object call(Eval ev, String n, List<Object> a) {
        int[] ar = arity(n);
        if (ar == null) {
            return NOT_TEMPORAL;
        }
        int dot = n.indexOf('.');
        String base = dot < 0 ? n : n.substring(0, dot);
        String fn = dot < 0 ? "" : n.substring(dot + 1);
        try {
            if (base.equals("duration")) {
                if (fn.isEmpty()) {
                    return a.isEmpty() ? null : durationCtor(a.get(0));
                }
                return between(fn, a.get(0), a.get(1));
            }
            switch (fn) {
                case "" -> {
                    if (a.isEmpty()) {
                        return now(ev, base, null);
                    }
                    return construct(ev, base, a.get(0));
                }
                case "realtime", "statement", "transaction" -> {
                    if (!a.isEmpty() && a.get(0) == null) {
                        return null;
                    }
                    return now(ev, base, a.isEmpty() ? null : a.get(0));
                }
                case "truncate" -> {
                    return truncate(ev, base, a.get(0), a.get(1), a.size() > 2 ? a.get(2) : null);
                }
                case "fromepoch" -> {
                    if (a.get(0) == null || a.get(1) == null) {
                        return null;
                    }
                    long s = Eval.toLong(a.get(0)), ns = Eval.toLong(a.get(1));
                    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(s, ns), ZoneOffset.UTC);
                }
                default -> {
                    if (a.get(0) == null) {
                        return null;
                    }
                    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Eval.toLong(a.get(0))), ZoneOffset.UTC);
                }
            }
        } catch (DateTimeException | ArithmeticException e) {
            throw CypherException.syntax(String.valueOf(e.getMessage()));
        }
    }

    // ------------------------------------------------------------------------------------------ helpers: fields

    private static long intField(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v instanceof Long l) {
            return l;
        }
        if (v instanceof Double d && d == Math.rint(d)) {
            return d.longValue();
        }
        throw CypherException.type("Invalid input for temporal field '" + key + "': expected an integer, got " + Values.typeName(v));
    }

    private static boolean has(Map<?, ?> m, String k) {
        return m.containsKey(k) && m.get(k) != null;
    }

    private static Map<?, ?> asMap(Object o) {
        if (o instanceof Map<?, ?> m) {
            return m;
        }
        throw CypherException.type("Type mismatch: expected Map but was " + Values.typeName(o));
    }

    private static final List<String> DATE_KEYS = List.of("year", "quarter", "month", "week", "dayOfWeek", "ordinalDay",
            "dayOfQuarter", "day");
    private static final List<String> TIME_KEYS = List.of("hour", "minute", "second", "millisecond", "microsecond",
            "nanosecond");

    // ------------------------------------------------------------------------------------------ now

    private static ZoneId zoneArg(Object tz) {
        if (tz == null) {
            return ZoneOffset.UTC;
        }
        if (tz instanceof Map<?, ?> m) {
            if (m.get("timezone") == null) {
                return ZoneOffset.UTC;
            }
            return parseZone(String.valueOf(m.get("timezone")));
        }
        if (tz instanceof String s) {
            return parseZone(s);
        }
        throw CypherException.type("Type mismatch: expected Map or String but was " + Values.typeName(tz));
    }

    private static Object now(Eval ev, String kind, Object tz) {
        ZoneId zone = zoneArg(tz);
        ZonedDateTime z = ev.x.startedAt.atZone(zone);
        return switch (kind) {
            case "date" -> z.toLocalDate();
            case "localtime" -> z.toLocalTime();
            case "time" -> OffsetTime.of(z.toLocalTime(), z.getOffset());
            case "localdatetime" -> z.toLocalDateTime();
            default -> z;
        };
    }

    // ------------------------------------------------------------------------------------------ construction

    private static Object construct(Eval ev, String kind, Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof String s) {
            return parseString(kind, s);
        }
        if (arg instanceof Map<?, ?> m) {
            return fromMap(ev, kind, m);
        }
        return convert(kind, arg);
    }

    /** date(x) / time(x) ... applied to another temporal value. */
    private static Object convert(String kind, Object v) {
        LocalDate d = dateOf(v);
        LocalTime t = timeOf(v);
        ZoneId z = zoneOf(v);
        switch (kind) {
            case "date" -> {
                if (d == null) {
                    throw badArg(kind, v);
                }
                return d;
            }
            case "localtime" -> {
                if (t == null) {
                    throw badArg(kind, v);
                }
                return t;
            }
            case "time" -> {
                if (t == null) {
                    throw badArg(kind, v);
                }
                if (v instanceof OffsetTime) {
                    return v;
                }
                ZoneOffset off = z == null ? ZoneOffset.UTC : (z instanceof ZoneOffset zo ? zo
                        : ((ZonedDateTime) v).getOffset());
                return OffsetTime.of(t, off);
            }
            case "localdatetime" -> {
                if (d == null) {
                    throw badArg(kind, v);
                }
                return LocalDateTime.of(d, t == null ? LocalTime.MIDNIGHT : t);
            }
            default -> {
                if (d == null) {
                    throw badArg(kind, v);
                }
                if (v instanceof ZonedDateTime) {
                    return v;
                }
                return ZonedDateTime.of(d, t == null ? LocalTime.MIDNIGHT : t, ZoneOffset.UTC);
            }
        }
    }

    private static CypherException badArg(String kind, Object v) {
        return CypherException.type("Invalid input for " + kind + "(): cannot convert " + Values.typeName(v));
    }

    static LocalDate dateOf(Object v) {
        if (v instanceof LocalDate d) {
            return d;
        }
        if (v instanceof LocalDateTime t) {
            return t.toLocalDate();
        }
        if (v instanceof ZonedDateTime t) {
            return t.toLocalDate();
        }
        return null;
    }

    static LocalTime timeOf(Object v) {
        if (v instanceof LocalTime t) {
            return t;
        }
        if (v instanceof OffsetTime t) {
            return t.toLocalTime();
        }
        if (v instanceof LocalDateTime t) {
            return t.toLocalTime();
        }
        if (v instanceof ZonedDateTime t) {
            return t.toLocalTime();
        }
        return null;
    }

    /** The zone (ZoneOffset or region) carried by the value, or null when it has none. */
    static ZoneId zoneOf(Object v) {
        if (v instanceof OffsetTime t) {
            return t.getOffset();
        }
        if (v instanceof ZonedDateTime t) {
            return t.getZone();
        }
        return null;
    }

    private static LocalDate buildDate(Map<?, ?> m, LocalDate base) {
        Long year = has(m, "year") ? intField(m, "year") : (base != null ? (long) base.getYear() : null);
        boolean weekBased = has(m, "week") || has(m, "dayOfWeek");
        boolean quarterBased = has(m, "quarter") || has(m, "dayOfQuarter");
        boolean ordinal = has(m, "ordinalDay");
        if (year == null) {
            throw CypherException.argument("Cannot construct a date without a year");
        }
        if (weekBased) {
            long week = has(m, "week") ? intField(m, "week") : (base != null ? base.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) : 1);
            long dow = has(m, "dayOfWeek") ? intField(m, "dayOfWeek") : (base != null ? base.getDayOfWeek().getValue() : 1);
            long wy = has(m, "year") ? year : (base != null ? base.get(IsoFields.WEEK_BASED_YEAR) : year);
            return weekDate(wy, week, dow);
        }
        if (quarterBased) {
            long q = has(m, "quarter") ? intField(m, "quarter") : (base != null ? base.get(IsoFields.QUARTER_OF_YEAR) : 1);
            long doq = has(m, "dayOfQuarter") ? intField(m, "dayOfQuarter")
                    : (base != null ? base.get(IsoFields.DAY_OF_QUARTER) : 1);
            if (q < 1 || q > 4) {
                throw CypherException.argument("Invalid value for quarter: " + q);
            }
            return LocalDate.of((int) (long) year, (int) ((q - 1) * 3 + 1), 1).plusDays(doq - 1);
        }
        if (ordinal) {
            return LocalDate.ofYearDay((int) (long) year, (int) intField(m, "ordinalDay"));
        }
        long month = has(m, "month") ? intField(m, "month") : (base != null ? base.getMonthValue() : 1);
        long day = has(m, "day") ? intField(m, "day") : (base != null ? base.getDayOfMonth() : 1);
        return LocalDate.of((int) (long) year, (int) month, (int) day);
    }

    private static LocalDate weekDate(long weekYear, long week, long dow) {
        LocalDate jan4 = LocalDate.of((int) weekYear, 1, 4);
        LocalDate monday1 = jan4.minusDays(jan4.getDayOfWeek().getValue() - 1);
        return monday1.plusWeeks(week - 1).plusDays(dow - 1);
    }

    private static LocalTime buildTime(Map<?, ?> m, LocalTime base) {
        if (!hasAnyTimeField(m)) {
            return base == null ? LocalTime.MIDNIGHT : base;
        }
        long hour = has(m, "hour") ? intField(m, "hour") : (base != null ? base.getHour() : 0);
        long minute = has(m, "minute") ? intField(m, "minute") : (base != null ? base.getMinute() : 0);
        long second = has(m, "second") ? intField(m, "second") : (base != null ? base.getSecond() : 0);
        long nanos;
        if (has(m, "millisecond") || has(m, "microsecond") || has(m, "nanosecond")) {
            nanos = (has(m, "millisecond") ? intField(m, "millisecond") * 1_000_000L : 0)
                    + (has(m, "microsecond") ? intField(m, "microsecond") * 1_000L : 0)
                    + (has(m, "nanosecond") ? intField(m, "nanosecond") : 0);
        } else {
            nanos = base != null ? base.getNano() : 0;
        }
        return LocalTime.of((int) hour, (int) minute, (int) second, (int) nanos);
    }

    private static Object fromMap(Eval ev, String kind, Map<?, ?> m) {
        for (Object k : m.keySet()) {
            String key = (String) k;
            if (!DATE_KEYS.contains(key) && !TIME_KEYS.contains(key) && !List.of("date", "time", "datetime", "timezone",
                    "epochSeconds", "epochMillis").contains(key)) {
                throw CypherException.argument("Unknown field '" + key + "' for " + kind + "()");
            }
        }
        Object dtSrc = m.get("datetime"), dateSrc = m.get("date"), timeSrc = m.get("time");
        LocalDate baseDate = null;
        LocalTime baseTime = null;
        ZoneId baseZone = null;
        Object primary = dtSrc != null ? dtSrc : null;
        if (primary != null) {
            baseDate = dateOf(primary);
            baseTime = timeOf(primary);
            baseZone = zoneOf(primary);
        }
        if (dateSrc != null) {
            baseDate = dateOf(dateSrc);
        }
        if (timeSrc != null) {
            baseTime = timeOf(timeSrc);
            if (zoneOf(timeSrc) != null) {
                baseZone = zoneOf(timeSrc);
            }
        }
        ZoneId zone = has(m, "timezone") ? parseZone(String.valueOf(m.get("timezone"))) : null;
        switch (kind) {
            case "date" -> {
                return buildDate(m, baseDate);
            }
            case "localtime" -> {
                return buildTime(m, baseTime);
            }
            case "time" -> {
                LocalTime t = buildTime(m, baseTime);
                ZoneId srcZone = baseZone;
                if (zone == null) {
                    ZoneOffset off = srcZone == null ? ZoneOffset.UTC : offsetOf(srcZone, baseDate, t);
                    return OffsetTime.of(t, off);
                }
                ZoneOffset target = offsetOf(zone, baseDate, t);
                if (srcZone != null) {
                    ZoneOffset from = offsetOf(srcZone, baseDate, t);
                    long shift = target.getTotalSeconds() - from.getTotalSeconds();
                    return OffsetTime.of(t.plusSeconds(shift), target);
                }
                return OffsetTime.of(t, target);
            }
            case "localdatetime" -> {
                LocalDate d = buildDate(m, baseDate);
                LocalTime t = buildTime(m, baseTime);
                return LocalDateTime.of(d, t);
            }
            default -> {
                if (has(m, "epochSeconds") || has(m, "epochMillis")) {
                    long secs = has(m, "epochSeconds") ? intField(m, "epochSeconds") : intField(m, "epochMillis") / 1000;
                    long nanos = has(m, "epochMillis") ? (intField(m, "epochMillis") % 1000) * 1_000_000L : 0;
                    nanos += (has(m, "nanosecond") ? intField(m, "nanosecond") : 0);
                    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(secs, nanos), zone == null ? ZoneOffset.UTC : zone);
                }
                LocalDate d = buildDate(m, baseDate);
                LocalTime t = buildTime(m, baseTime);
                LocalDateTime ldt = LocalDateTime.of(d, t);
                if (zone == null) {
                    return baseZone == null ? ZonedDateTime.of(ldt, ZoneOffset.UTC) : ZonedDateTime.of(ldt, baseZone);
                }
                if (baseZone != null) {
                    // a zoned source keeps its instant when the result is placed in another zone
                    return ZonedDateTime.of(ldt, baseZone).withZoneSameInstant(zone);
                }
                return ZonedDateTime.of(ldt, zone);
            }
        }
    }

    private static boolean hasAnyTimeOrDate(Map<?, ?> m) {
        for (String k : DATE_KEYS) {
            if (has(m, k)) {
                return true;
            }
        }
        return hasAnyTimeField(m);
    }

    private static boolean hasAnyTimeField(Map<?, ?> m) {
        for (String k : TIME_KEYS) {
            if (has(m, k)) {
                return true;
            }
        }
        return false;
    }

    private static ZoneOffset offsetOf(ZoneId z, LocalDate date, LocalTime t) {
        if (z instanceof ZoneOffset o) {
            return o;
        }
        LocalDate d = date == null ? LocalDate.of(1970, 1, 1) : date;
        return z.getRules().getOffset(LocalDateTime.of(d, t));
    }

    // ------------------------------------------------------------------------------------------ parsing

    private static final Pattern ZONE_TAIL = Pattern.compile("(Z|[+-]\\d{2}(?::?\\d{2}(?::?\\d{2})?)?)?(?:\\[([^\\]]+)\\])?");

    static ZoneId parseZone(String s) {
        if (s.equals("Z") || s.equals("z")) {
            return ZoneOffset.UTC;
        }
        if (s.startsWith("+") || s.startsWith("-")) {
            return parseOffset(s);
        }
        try {
            return ZoneId.of(s);
        } catch (DateTimeException e) {
            throw CypherException.argument("Unknown time zone: " + s);
        }
    }

    private static ZoneOffset parseOffset(String s) {
        Matcher m = Pattern.compile("([+-])(\\d{2})(?::?(\\d{2})(?::?(\\d{2}))?)?").matcher(s);
        if (!m.matches()) {
            throw CypherException.argument("Cannot parse time zone offset: " + s);
        }
        int sign = m.group(1).equals("-") ? -1 : 1;
        int h = Integer.parseInt(m.group(2)), mi = m.group(3) == null ? 0 : Integer.parseInt(m.group(3)),
                se = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
        return ZoneOffset.ofTotalSeconds(sign * (h * 3600 + mi * 60 + se));
    }

    private static final Pattern P_DATE_EXT = Pattern.compile("([+-]?\\d{4,9})-(\\d{2})-(\\d{2})");
    private static final Pattern P_DATE_YM = Pattern.compile("([+-]?\\d{4,9})-(\\d{2})");
    private static final Pattern P_DATE_ORD_EXT = Pattern.compile("([+-]?\\d{4,9})-(\\d{3})");
    private static final Pattern P_DATE_WEEK = Pattern.compile("([+-]?\\d{4,9})-?W(\\d{2})(?:-?(\\d))?");
    private static final Pattern P_DATE_BASIC = Pattern.compile("([+-]?\\d{4})(\\d{2})(\\d{2})");
    private static final Pattern P_DATE_BASIC_YM = Pattern.compile("(\\d{4})(\\d{2})");
    private static final Pattern P_DATE_BASIC_ORD = Pattern.compile("(\\d{4})(\\d{3})");
    private static final Pattern P_DATE_Y = Pattern.compile("([+-]?\\d{4,9})");

    static LocalDate parseDate(String s) {
        Matcher m;
        if ((m = P_DATE_WEEK.matcher(s)).matches()) {
            return weekDate(Long.parseLong(m.group(1)), Long.parseLong(m.group(2)),
                    m.group(3) == null ? 1 : Long.parseLong(m.group(3)));
        }
        if ((m = P_DATE_EXT.matcher(s)).matches()) {
            return LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)));
        }
        if ((m = P_DATE_ORD_EXT.matcher(s)).matches()) {
            return LocalDate.ofYearDay(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
        }
        if ((m = P_DATE_YM.matcher(s)).matches()) {
            return LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), 1);
        }
        if (s.matches("\\d{8}")) {
            m = P_DATE_BASIC.matcher(s);
            m.matches();
            return LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)));
        }
        if (s.matches("\\d{7}")) {
            m = P_DATE_BASIC_ORD.matcher(s);
            m.matches();
            return LocalDate.ofYearDay(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
        }
        if (s.matches("\\d{6}")) {
            m = P_DATE_BASIC_YM.matcher(s);
            m.matches();
            return LocalDate.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), 1);
        }
        if ((m = P_DATE_Y.matcher(s)).matches()) {
            return LocalDate.of(Integer.parseInt(m.group(1)), 1, 1);
        }
        throw CypherException.syntax("Text cannot be parsed to a Date: " + s);
    }

    private static final Pattern P_TIME_EXT = Pattern.compile("(\\d{2})(?::(\\d{2})(?::(\\d{2})(?:[.,](\\d{1,9}))?)?)?");
    private static final Pattern P_TIME_BASIC = Pattern.compile("(\\d{2})(?:(\\d{2})(?:(\\d{2})(?:[.,](\\d{1,9}))?)?)?");

    /** Parses the clock part (no zone); returns null when the text is not a clock time. */
    private static LocalTime parseClock(String s) {
        Matcher m = (s.contains(":") ? P_TIME_EXT : P_TIME_BASIC).matcher(s);
        if (!m.matches()) {
            return null;
        }
        int h = Integer.parseInt(m.group(1));
        int mi = m.group(2) == null ? 0 : Integer.parseInt(m.group(2));
        int se = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
        int ns = 0;
        if (m.group(4) != null) {
            String f = (m.group(4) + "000000000").substring(0, 9);
            ns = Integer.parseInt(f);
        }
        return LocalTime.of(h, mi, se, ns);
    }

    private static Object[] splitZone(String timePart) {
        // returns {clock string, offset string or null, region or null}
        String region = null;
        int b = timePart.indexOf('[');
        if (b >= 0) {
            int e = timePart.indexOf(']', b);
            region = timePart.substring(b + 1, e);
            timePart = timePart.substring(0, b);
        }
        String off = null;
        if (timePart.endsWith("Z")) {
            off = "Z";
            timePart = timePart.substring(0, timePart.length() - 1);
        } else {
            int idx = Math.max(timePart.lastIndexOf('+'), timePart.lastIndexOf('-'));
            if (idx > 0) {
                off = timePart.substring(idx);
                timePart = timePart.substring(0, idx);
            }
        }
        return new Object[] {timePart, off, region};
    }

    private static Object parseString(String kind, String s) {
        try {
            switch (kind) {
                case "date" -> {
                    return parseDate(s);
                }
                case "localtime" -> {
                    LocalTime t = parseClock(s);
                    if (t == null) {
                        throw CypherException.syntax("Text cannot be parsed to a LocalTime: " + s);
                    }
                    return t;
                }
                case "time" -> {
                    Object[] p = splitZone(s);
                    LocalTime t = parseClock((String) p[0]);
                    if (t == null) {
                        throw CypherException.syntax("Text cannot be parsed to a Time: " + s);
                    }
                    ZoneOffset off = p[1] == null ? ZoneOffset.UTC : (ZoneOffset) parseZone((String) p[1]);
                    return OffsetTime.of(t, off);
                }
                default -> {
                    int ti = s.indexOf('T');
                    String datePart = ti < 0 ? s : s.substring(0, ti);
                    LocalDate d = parseDate(datePart);
                    LocalTime t = LocalTime.MIDNIGHT;
                    String zoneOff = null, region = null;
                    if (ti >= 0) {
                        Object[] p = splitZone(s.substring(ti + 1));
                        LocalTime pt = parseClock((String) p[0]);
                        if (pt == null) {
                            throw CypherException.syntax("Text cannot be parsed to a DateTime: " + s);
                        }
                        t = pt;
                        zoneOff = (String) p[1];
                        region = (String) p[2];
                    }
                    if (kind.equals("localdatetime")) {
                        if (zoneOff != null || region != null) {
                            throw CypherException.syntax("Text cannot be parsed to a LocalDateTime: " + s);
                        }
                        return LocalDateTime.of(d, t);
                    }
                    LocalDateTime ldt = LocalDateTime.of(d, t);
                    if (region != null) {
                        ZoneId z = ZoneId.of(region);
                        if (zoneOff != null) {
                            ZoneOffset pref = (ZoneOffset) parseZone(zoneOff);
                            return ZonedDateTime.ofLocal(ldt, z, pref);
                        }
                        return ZonedDateTime.of(ldt, z);
                    }
                    return ZonedDateTime.of(ldt, zoneOff == null ? ZoneOffset.UTC : parseZone(zoneOff));
                }
            }
        } catch (DateTimeException | NumberFormatException e) {
            throw CypherException.syntax("Text cannot be parsed to a temporal value (" + kind + "): " + s + " -- "
                    + e.getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------ duration construction

    private static DurationV durationCtor(Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof String s) {
            return parseDuration(s);
        }
        if (arg instanceof DurationV d) {
            return d;
        }
        Map<?, ?> m = asMap(arg);
        for (Object k : m.keySet()) {
            if (!List.of("years", "quarters", "months", "weeks", "days", "hours", "minutes", "seconds", "milliseconds",
                    "microseconds", "nanoseconds").contains(k)) {
                throw CypherException.argument("Unknown field '" + k + "' for duration()");
            }
        }
        BigDecimal months = num(m, "years").multiply(BigDecimal.valueOf(12)).add(num(m, "quarters").multiply(BigDecimal.valueOf(3)))
                .add(num(m, "months"));
        BigDecimal days = num(m, "weeks").multiply(BigDecimal.valueOf(7)).add(num(m, "days"));
        BigDecimal nanos = num(m, "hours").multiply(BigDecimal.valueOf(3_600_000_000_000L))
                .add(num(m, "minutes").multiply(BigDecimal.valueOf(60_000_000_000L)))
                .add(num(m, "seconds").multiply(NANOS))
                .add(num(m, "milliseconds").multiply(BigDecimal.valueOf(1_000_000L)))
                .add(num(m, "microseconds").multiply(BigDecimal.valueOf(1_000L))).add(num(m, "nanoseconds"));
        return normalize(months, days, nanos);
    }

    private static BigDecimal num(Map<?, ?> m, String k) {
        Object v = m.get(k);
        if (v == null) {
            return BigDecimal.ZERO;
        }
        if (v instanceof Long l) {
            return BigDecimal.valueOf(l);
        }
        if (v instanceof Double d) {
            return new BigDecimal(d);
        }
        throw CypherException.type("Invalid input for duration field '" + k + "': " + Values.typeName(v));
    }

    /** Cascades fractional months into days and fractional days into seconds, truncating toward zero. */
    private static DurationV normalize(BigDecimal months, BigDecimal days, BigDecimal nanos) {
        BigDecimal wholeMonths = months.setScale(0, RoundingMode.DOWN);
        BigDecimal fracMonths = months.subtract(wholeMonths);
        BigDecimal daysTotal = days.add(fracMonths.multiply(BigDecimal.valueOf(AVG_MONTH_SECONDS))
                .divide(BigDecimal.valueOf(86400), 30, RoundingMode.HALF_EVEN));
        BigDecimal wholeDays = daysTotal.setScale(0, RoundingMode.DOWN);
        BigDecimal fracDays = daysTotal.subtract(wholeDays);
        BigDecimal totalNanos = nanos.add(fracDays.multiply(BigDecimal.valueOf(86400L * 1_000_000_000L)))
                .setScale(0, RoundingMode.DOWN);
        return of(wholeMonths.longValueExact(), wholeDays.longValueExact(), totalNanos.toBigInteger());
    }

    /** Seconds are floor-divided so the nanosecond part is always non-negative (Neo4j's DurationValue layout). */
    private static DurationV of(long months, long days, java.math.BigInteger totalNanos) {
        java.math.BigInteger[] qr = totalNanos.divideAndRemainder(java.math.BigInteger.valueOf(1_000_000_000L));
        long secs = qr[0].longValueExact();
        int nanos = qr[1].intValue();
        if (nanos < 0) {
            secs -= 1;
            nanos += 1_000_000_000;
        }
        return new DurationV(months, days, secs, nanos);
    }

    private static final Pattern P_DUR = Pattern.compile(
            "([+-]?)P(?:(-?[\\d.,]+)Y)?(?:(-?[\\d.,]+)M)?(?:(-?[\\d.,]+)W)?(?:(-?[\\d.,]+)D)?(?:T(?:(-?[\\d.,]+)H)?(?:(-?[\\d.,]+)M)?(?:(-?[\\d.,]+)S)?)?");
    private static final Pattern P_DUR_DATE = Pattern.compile(
            "P(\\d{4})-?(\\d{2})-?(\\d{2})(?:T(\\d{2}):?(\\d{2}):?(\\d{2})(?:[.,](\\d{1,9}))?)?");

    static DurationV parseDuration(String s) {
        Matcher dm = P_DUR_DATE.matcher(s);
        if (dm.matches()) {
            long months = Long.parseLong(dm.group(1)) * 12 + Long.parseLong(dm.group(2));
            long days = Long.parseLong(dm.group(3));
            long h = dm.group(4) == null ? 0 : Long.parseLong(dm.group(4));
            long mi = dm.group(5) == null ? 0 : Long.parseLong(dm.group(5));
            long se = dm.group(6) == null ? 0 : Long.parseLong(dm.group(6));
            long ns = dm.group(7) == null ? 0 : Long.parseLong((dm.group(7) + "000000000").substring(0, 9));
            return of(months, days, java.math.BigInteger.valueOf(h * 3600 + mi * 60 + se)
                    .multiply(java.math.BigInteger.valueOf(1_000_000_000L)).add(java.math.BigInteger.valueOf(ns)));
        }
        Matcher m = P_DUR.matcher(s);
        if (!m.matches() || s.equals("P") || s.endsWith("T")) {
            throw CypherException.syntax("Text cannot be parsed to a Duration: " + s);
        }
        BigDecimal sign = m.group(1).equals("-") ? BigDecimal.ONE.negate() : BigDecimal.ONE;
        BigDecimal y = dec(m.group(2)), mo = dec(m.group(3)), w = dec(m.group(4)), d = dec(m.group(5)), h = dec(m.group(6)),
                mi = dec(m.group(7)), se = dec(m.group(8));
        BigDecimal months = y.multiply(BigDecimal.valueOf(12)).add(mo).multiply(sign);
        BigDecimal days = w.multiply(BigDecimal.valueOf(7)).add(d).multiply(sign);
        BigDecimal nanos = h.multiply(BigDecimal.valueOf(3_600_000_000_000L)).add(mi.multiply(BigDecimal.valueOf(60_000_000_000L)))
                .add(se.multiply(NANOS)).multiply(sign);
        return normalize(months, days, nanos);
    }

    private static BigDecimal dec(String s) {
        return s == null ? BigDecimal.ZERO : new BigDecimal(s.replace(',', '.'));
    }

    // ------------------------------------------------------------------------------------------ duration arithmetic

    static DurationV negate(DurationV d) {
        return of(-d.months(), -d.days(), java.math.BigInteger.valueOf(d.seconds()).multiply(java.math.BigInteger.valueOf(1_000_000_000L))
                .add(java.math.BigInteger.valueOf(d.nanos())).negate());
    }

    static DurationV addDurations(DurationV a, DurationV b) {
        java.math.BigInteger n = java.math.BigInteger.valueOf(a.seconds()).multiply(java.math.BigInteger.valueOf(1_000_000_000L))
                .add(java.math.BigInteger.valueOf(a.nanos()))
                .add(java.math.BigInteger.valueOf(b.seconds()).multiply(java.math.BigInteger.valueOf(1_000_000_000L)))
                .add(java.math.BigInteger.valueOf(b.nanos()));
        return of(Math.addExact(a.months(), b.months()), Math.addExact(a.days(), b.days()), n);
    }

    static DurationV divideDuration(DurationV a, long n) {
        return scale(a, BigDecimal.ONE, BigDecimal.valueOf(n));
    }

    private static DurationV scale(DurationV a, BigDecimal mul, BigDecimal div) {
        BigDecimal months = BigDecimal.valueOf(a.months()).multiply(mul).divide(div, 30, RoundingMode.HALF_EVEN);
        BigDecimal days = BigDecimal.valueOf(a.days()).multiply(mul).divide(div, 30, RoundingMode.HALF_EVEN);
        BigDecimal nanos = BigDecimal.valueOf(a.seconds()).multiply(NANOS).add(BigDecimal.valueOf(a.nanos())).multiply(mul)
                .divide(div, 30, RoundingMode.HALF_EVEN);
        return normalize(months, days, nanos);
    }

    static Object multiply(Object a, Object b, boolean divide) {
        if (a instanceof DurationV d && Values.isNumber(b)) {
            BigDecimal f = b instanceof Long l ? BigDecimal.valueOf(l) : new BigDecimal((Double) b);
            if (divide) {
                if (f.signum() == 0) {
                    throw CypherException.arithmetic("/ by zero");
                }
                return scale(d, BigDecimal.ONE, f);
            }
            return scale(d, f, BigDecimal.ONE);
        }
        if (!divide && Values.isNumber(a) && b instanceof DurationV d) {
            BigDecimal f = a instanceof Long l ? BigDecimal.valueOf(l) : new BigDecimal((Double) a);
            return scale(d, f, BigDecimal.ONE);
        }
        return NOT_TEMPORAL;
    }

    /** temporal +/- duration, duration +/- duration. */
    static Object add(Object a, Object b, boolean subtract) {
        if (a instanceof DurationV x && b instanceof DurationV y) {
            return addDurations(x, subtract ? negate(y) : y);
        }
        if (b instanceof DurationV d && isTemporalValue(a)) {
            return plus(a, subtract ? negate(d) : d);
        }
        if (a instanceof DurationV d && isTemporalValue(b) && !subtract) {
            return plus(b, d);
        }
        return NOT_TEMPORAL;
    }

    private static boolean isTemporalValue(Object v) {
        return v instanceof LocalDate || v instanceof LocalTime || v instanceof OffsetTime || v instanceof LocalDateTime
                || v instanceof ZonedDateTime;
    }

    private static Object plus(Object t, DurationV d) {
        long secs = d.seconds();
        int nanos = d.nanos();
        if (t instanceof LocalDate x) {
            return x.plusMonths(d.months()).plusDays(d.days() + secs / 86400);
        }
        if (t instanceof LocalTime x) {
            return x.plusSeconds(secs % 86400).plusNanos(nanos);
        }
        if (t instanceof OffsetTime x) {
            return x.plusSeconds(secs % 86400).plusNanos(nanos);
        }
        if (t instanceof LocalDateTime x) {
            return x.plusMonths(d.months()).plusDays(d.days()).plusSeconds(secs).plusNanos(nanos);
        }
        ZonedDateTime x = (ZonedDateTime) t;
        return x.plusMonths(d.months()).plusDays(d.days()).plusSeconds(secs).plusNanos(nanos);
    }

    // ------------------------------------------------------------------------------------------ duration.between

    private static Object between(String fn, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (!isTemporalValue(a) || !isTemporalValue(b)) {
            throw CypherException.type("Type mismatch: expected a temporal value but was "
                    + Values.typeName(isTemporalValue(a) ? b : a));
        }
        LocalDate da = dateOf(a), db = dateOf(b);
        ZoneId za = zoneOf(a), zb = zoneOf(b);
        if (da == null && db == null) {
            da = db = LocalDate.of(1970, 1, 1);
        } else if (da == null) {
            da = db;
        } else if (db == null) {
            db = da;
        }
        LocalTime ta = timeOf(a) == null ? LocalTime.MIDNIGHT : timeOf(a);
        LocalTime tb = timeOf(b) == null ? LocalTime.MIDNIGHT : timeOf(b);
        ZoneId zone = za != null ? za : zb;
        java.time.temporal.Temporal from, to;
        if (zone != null) {
            ZonedDateTime fa = a instanceof ZonedDateTime azd ? azd
                    : ZonedDateTime.of(LocalDateTime.of(da, ta), a instanceof OffsetTime oa ? oa.getOffset() : zone);
            ZonedDateTime fb = b instanceof ZonedDateTime bzd ? bzd.withZoneSameInstant(fa.getZone())
                    : ZonedDateTime.of(LocalDateTime.of(db, tb), b instanceof OffsetTime ob ? ob.getOffset() : fa.getZone());
            from = fa;
            to = fb;
        } else {
            from = LocalDateTime.of(da, ta);
            to = LocalDateTime.of(db, tb);
        }
        switch (fn) {
            case "inmonths" -> {
                return new DurationV(ChronoUnit.MONTHS.between(from, to), 0, 0, 0);
            }
            case "indays" -> {
                return new DurationV(0, ChronoUnit.DAYS.between(from, to), 0, 0);
            }
            case "inseconds" -> {
                java.time.Duration du = java.time.Duration.between(from, to);
                return of(0, 0, java.math.BigInteger.valueOf(du.getSeconds()).multiply(java.math.BigInteger.valueOf(1_000_000_000L))
                        .add(java.math.BigInteger.valueOf(du.getNano())));
            }
            default -> {
                long months = ChronoUnit.MONTHS.between(from, to);
                java.time.temporal.Temporal mid = from.plus(months, ChronoUnit.MONTHS);
                long days = ChronoUnit.DAYS.between(mid, to);
                java.time.temporal.Temporal mid2 = mid.plus(days, ChronoUnit.DAYS);
                java.time.Duration rest = java.time.Duration.between(mid2, to);
                return of(months, days, java.math.BigInteger.valueOf(rest.getSeconds())
                        .multiply(java.math.BigInteger.valueOf(1_000_000_000L)).add(java.math.BigInteger.valueOf(rest.getNano())));
            }
        }
    }

    // ------------------------------------------------------------------------------------------ truncation

    private static Object truncate(Eval ev, String kind, Object unitArg, Object src, Object mapArg) {
        if (unitArg == null || src == null) {
            return null;
        }
        if (!(unitArg instanceof String)) {
            throw CypherException.type("Type mismatch: expected String but was " + Values.typeName(unitArg));
        }
        String unit = ((String) unitArg).toLowerCase(Locale.ROOT);
        if (!isTemporalValue(src)) {
            throw CypherException.type("Type mismatch: expected a temporal value but was " + Values.typeName(src));
        }
        Map<?, ?> m = mapArg == null ? Map.of() : asMap(mapArg);
        LocalDate d = dateOf(src);
        LocalTime t = timeOf(src);
        ZoneId z = zoneOf(src);
        boolean dateUnit = List.of("millennium", "century", "decade", "year", "weekyear", "quarter", "month", "week", "day")
                .contains(unit);
        boolean timeUnit = List.of("hour", "minute", "second", "millisecond", "microsecond").contains(unit);
        if (!dateUnit && !timeUnit) {
            throw CypherException.argument("Unsupported truncation unit: " + unitArg);
        }
        LocalDate td = d;
        LocalTime tt = t;
        if (dateUnit) {
            if (kind.equals("localtime") || kind.equals("time")) {
                if (!unit.equals("day")) {
                    throw CypherException.argument("Cannot truncate a time to " + unit);
                }
            } else if (td == null) {
                throw CypherException.argument("Cannot truncate a time value to " + unit);
            }
            if (td != null) {
                td = switch (unit) {
                    case "millennium" -> LocalDate.of(Math.floorDiv(td.getYear(), 1000) * 1000, 1, 1);
                    case "century" -> LocalDate.of(Math.floorDiv(td.getYear(), 100) * 100, 1, 1);
                    case "decade" -> LocalDate.of(Math.floorDiv(td.getYear(), 10) * 10, 1, 1);
                    case "year" -> LocalDate.of(td.getYear(), 1, 1);
                    case "weekyear" -> weekDate(td.get(IsoFields.WEEK_BASED_YEAR), 1, 1);
                    case "quarter" -> LocalDate.of(td.getYear(), (td.get(IsoFields.QUARTER_OF_YEAR) - 1) * 3 + 1, 1);
                    case "month" -> td.withDayOfMonth(1);
                    case "week" -> td.minusDays(td.getDayOfWeek().getValue() - 1);
                    default -> td;
                };
            }
            tt = LocalTime.MIDNIGHT;
        } else {
            if (kind.equals("date")) {
                throw CypherException.argument("Cannot truncate a date to " + unit);
            }
            if (tt == null) {
                tt = LocalTime.MIDNIGHT;
            }
            tt = switch (unit) {
                case "hour" -> tt.truncatedTo(ChronoUnit.HOURS);
                case "minute" -> tt.truncatedTo(ChronoUnit.MINUTES);
                case "second" -> tt.truncatedTo(ChronoUnit.SECONDS);
                case "millisecond" -> tt.truncatedTo(ChronoUnit.MILLIS);
                default -> tt.truncatedTo(ChronoUnit.MICROS);
            };
        }
        // apply the override map on the truncated pieces
        LocalDate fd = td == null ? null : (hasAnyDateField(m) ? buildDate(m, td) : td);
        LocalTime ft = hasAnyTimeField(m) ? buildTimeOverlay(m, tt) : tt;
        switch (kind) {
            case "date" -> {
                return fd;
            }
            case "localtime" -> {
                return ft;
            }
            case "time" -> {
                ZoneId zz = has(m, "timezone") ? parseZone(String.valueOf(m.get("timezone"))) : (z == null ? ZoneOffset.UTC : z);
                return OffsetTime.of(ft, offsetOf(zz, d, ft));
            }
            case "localdatetime" -> {
                return LocalDateTime.of(fd == null ? LocalDate.of(1970, 1, 1) : fd, ft);
            }
            default -> {
                ZoneId zz = has(m, "timezone") ? parseZone(String.valueOf(m.get("timezone"))) : (z == null ? ZoneOffset.UTC : z);
                return ZonedDateTime.of(LocalDateTime.of(fd == null ? LocalDate.of(1970, 1, 1) : fd, ft), zz);
            }
        }
    }

    private static boolean hasAnyDateField(Map<?, ?> m) {
        for (String k : DATE_KEYS) {
            if (has(m, k)) {
                return true;
            }
        }
        return false;
    }

    /** Time fields of a truncate() override: the given components replace the truncated ones. */
    private static LocalTime buildTimeOverlay(Map<?, ?> m, LocalTime base) {
        long hour = has(m, "hour") ? intField(m, "hour") : base.getHour();
        long minute = has(m, "minute") ? intField(m, "minute") : base.getMinute();
        long second = has(m, "second") ? intField(m, "second") : base.getSecond();
        long nanos = base.getNano();
        if (has(m, "millisecond") || has(m, "microsecond") || has(m, "nanosecond")) {
            nanos = base.getNano() + (has(m, "millisecond") ? intField(m, "millisecond") * 1_000_000L : 0)
                    + (has(m, "microsecond") ? intField(m, "microsecond") * 1_000L : 0)
                    + (has(m, "nanosecond") ? intField(m, "nanosecond") : 0);
        }
        return LocalTime.of((int) hour, (int) minute, (int) second, (int) nanos);
    }

    // ------------------------------------------------------------------------------------------ accessors

    private static String offsetString(int total) {
        if (total == 0) {
            return "Z";
        }
        int a = Math.abs(total);
        String s = String.format("%s%02d:%02d", total < 0 ? "-" : "+", a / 3600, (a % 3600) / 60);
        if (a % 60 != 0) {
            s += String.format(":%02d", a % 60);
        }
        return s;
    }

    static Object component(Object t, String key) {
        if (t instanceof PointV p) {
            return switch (key) {
                case "x", "longitude" -> p.x();
                case "y", "latitude" -> p.y();
                case "z", "height" -> p.z();
                case "srid" -> (long) p.srid();
                case "crs" -> crsName(p);
                default -> null;
            };
        }
        if (t instanceof DurationV d) {
            long secs = d.seconds();
            return switch (key) {
                case "years" -> d.months() / 12;
                case "quarters" -> d.months() / 3;
                case "months" -> d.months();
                case "weeks" -> d.days() / 7;
                case "days" -> d.days();
                case "hours" -> secs / 3600;
                case "minutes" -> secs / 60;
                case "seconds" -> secs;
                case "milliseconds" -> secs * 1000 + d.nanos() / 1_000_000;
                case "microseconds" -> secs * 1_000_000 + d.nanos() / 1_000;
                case "nanoseconds" -> secs * 1_000_000_000L + d.nanos();
                case "quartersOfYear" -> (d.months() / 3) % 4;
                case "monthsOfQuarter" -> d.months() % 3;
                case "monthsOfYear" -> d.months() % 12;
                case "daysOfWeek" -> d.days() % 7;
                case "minutesOfHour" -> (secs / 60) % 60;
                case "secondsOfMinute" -> secs % 60;
                case "millisecondsOfSecond" -> (long) (d.nanos() / 1_000_000);
                case "microsecondsOfSecond" -> (long) (d.nanos() / 1_000);
                case "nanosecondsOfSecond" -> (long) d.nanos();
                default -> null;
            };
        }
        if (!isTemporalValue(t)) {
            return NOT_TEMPORAL;
        }
        LocalDate d = dateOf(t);
        LocalTime tm = timeOf(t);
        if (d != null) {
            switch (key) {
                case "year" -> {
                    return (long) d.getYear();
                }
                case "quarter" -> {
                    return (long) d.get(IsoFields.QUARTER_OF_YEAR);
                }
                case "month" -> {
                    return (long) d.getMonthValue();
                }
                case "week" -> {
                    return (long) d.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
                }
                case "weekYear" -> {
                    return (long) d.get(IsoFields.WEEK_BASED_YEAR);
                }
                case "day" -> {
                    return (long) d.getDayOfMonth();
                }
                case "ordinalDay" -> {
                    return (long) d.getDayOfYear();
                }
                case "weekDay" -> {
                    return (long) d.getDayOfWeek().getValue();
                }
                case "dayOfQuarter" -> {
                    return (long) d.get(IsoFields.DAY_OF_QUARTER);
                }
                default -> {
                }
            }
        }
        if (tm != null) {
            switch (key) {
                case "hour" -> {
                    return (long) tm.getHour();
                }
                case "minute" -> {
                    return (long) tm.getMinute();
                }
                case "second" -> {
                    return (long) tm.getSecond();
                }
                case "millisecond" -> {
                    return (long) (tm.getNano() / 1_000_000);
                }
                case "microsecond" -> {
                    return (long) (tm.getNano() / 1_000);
                }
                case "nanosecond" -> {
                    return (long) tm.getNano();
                }
                default -> {
                }
            }
        }
        ZoneId z = zoneOf(t);
        if (z != null) {
            int off = t instanceof OffsetTime ot ? ot.getOffset().getTotalSeconds() : ((ZonedDateTime) t).getOffset().getTotalSeconds();
            switch (key) {
                case "timezone" -> {
                    return z instanceof ZoneOffset zo ? offsetString(zo.getTotalSeconds()) : z.getId();
                }
                case "offset" -> {
                    return offsetString(off);
                }
                case "offsetMinutes" -> {
                    return (long) (off / 60);
                }
                case "offsetSeconds" -> {
                    return (long) off;
                }
                default -> {
                }
            }
            if (t instanceof ZonedDateTime zdt) {
                if (key.equals("epochSeconds")) {
                    return zdt.toEpochSecond();
                }
                if (key.equals("epochMillis")) {
                    return zdt.toInstant().toEpochMilli();
                }
            }
        }
        return null;
    }

    // ------------------------------------------------------------------------------------------ formatting

    private static String frac(int nanos) {
        if (nanos == 0) {
            return "";
        }
        if (nanos % 1_000_000 == 0) {
            return String.format(".%03d", nanos / 1_000_000);
        }
        if (nanos % 1_000 == 0) {
            return String.format(".%06d", nanos / 1_000);
        }
        return String.format(".%09d", nanos);
    }

    private static String clock(LocalTime t) {
        return String.format("%02d:%02d:%02d", t.getHour(), t.getMinute(), t.getSecond()) + frac(t.getNano());
    }

    private static String dateStr(LocalDate d) {
        int y = d.getYear();
        return (y < 0 ? String.format("-%04d", -y) : (y > 9999 ? "+" + y : String.format("%04d", y))) + String.format("-%02d-%02d",
                d.getMonthValue(), d.getDayOfMonth());
    }

    static String toStringValue(Object v) {
        if (v instanceof LocalDate d) {
            return dateStr(d);
        }
        if (v instanceof LocalTime t) {
            return clock(t);
        }
        if (v instanceof OffsetTime t) {
            return clock(t.toLocalTime()) + offsetString(t.getOffset().getTotalSeconds());
        }
        if (v instanceof LocalDateTime t) {
            return dateStr(t.toLocalDate()) + "T" + clock(t.toLocalTime());
        }
        if (v instanceof ZonedDateTime t) {
            String s = dateStr(t.toLocalDate()) + "T" + clock(t.toLocalTime()) + offsetString(t.getOffset().getTotalSeconds());
            if (!(t.getZone() instanceof ZoneOffset)) {
                s += "[" + t.getZone().getId() + "]";
            }
            return s;
        }
        if (v instanceof DurationV d) {
            return durationString(d);
        }
        if (v instanceof PointV p) {
            String c = "{x: " + Funcs.doubleToString(p.x()) + ", y: " + Funcs.doubleToString(p.y())
                    + (p.z() != null ? ", z: " + Funcs.doubleToString(p.z()) : "") + ", crs: '" + crsName(p) + "'}";
            return "point(" + c + ")";
        }
        return null;
    }

    static String durationString(DurationV d) {
        StringBuilder sb = new StringBuilder("P");
        long months = d.months();
        long years = months / 12, rem = months % 12;
        if (years != 0) {
            sb.append(years).append('Y');
        }
        if (rem != 0) {
            sb.append(rem).append('M');
        }
        if (d.days() != 0) {
            sb.append(d.days()).append('D');
        }
        java.math.BigInteger total = java.math.BigInteger.valueOf(d.seconds()).multiply(java.math.BigInteger.valueOf(1_000_000_000L))
                .add(java.math.BigInteger.valueOf(d.nanos()));
        if (total.signum() != 0) {
            boolean neg = total.signum() < 0;
            java.math.BigInteger a = total.abs();
            java.math.BigInteger[] hr = a.divideAndRemainder(java.math.BigInteger.valueOf(3_600_000_000_000L));
            java.math.BigInteger[] mi = hr[1].divideAndRemainder(java.math.BigInteger.valueOf(60_000_000_000L));
            java.math.BigInteger[] se = mi[1].divideAndRemainder(java.math.BigInteger.valueOf(1_000_000_000L));
            String sg = neg ? "-" : "";
            StringBuilder t = new StringBuilder();
            if (hr[0].signum() != 0) {
                t.append(sg).append(hr[0]).append('H');
            }
            if (mi[0].signum() != 0) {
                t.append(sg).append(mi[0]).append('M');
            }
            if (se[0].signum() != 0 || se[1].signum() != 0) {
                t.append(sg).append(se[0]);
                if (se[1].signum() != 0) {
                    t.append('.').append(String.format("%09d", se[1].intValue()).replaceAll("0+$", ""));
                }
                t.append('S');
            }
            sb.append('T').append(t);
        }
        if (sb.length() == 1) {
            return "PT0S";
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------------ points

    static String crsName(PointV p) {
        return switch (p.srid()) {
            case 7203 -> "cartesian";
            case 9157 -> "cartesian-3d";
            case 4326 -> "wgs-84";
            case 4979 -> "wgs-84-3d";
            default -> "unknown";
        };
    }

    static Object point(Object arg) {
        if (arg == null) {
            return null;
        }
        if (!(arg instanceof Map<?, ?> m)) {
            throw Eval.typeMismatch("Map", arg);
        }
        Object x = m.get("x"), y = m.get("y"), z = m.get("z"), lon = m.get("longitude"), lat = m.get("latitude"),
                h = m.get("height");
        Object crs = m.get("crs"), sridArg = m.get("srid");
        boolean geo = lon != null || lat != null || (crs instanceof String c && c.toLowerCase(Locale.ROOT).startsWith("wgs"))
                || (sridArg instanceof Long s && (s == 4326 || s == 4979));
        Object px = geo ? (lon != null ? lon : x) : x, py = geo ? (lat != null ? lat : y) : y, pz = geo ? (h != null ? h : z) : z;
        if (px == null || py == null) {
            if (m.values().stream().anyMatch(v -> v == null)) {
                return null;
            }
            throw CypherException.syntax("A " + (geo ? "WGS84" : "cartesian") + " point must contain "
                    + (geo ? "'latitude' and 'longitude'" : "'x' and 'y'"));
        }
        double dx = ((Number) px).doubleValue(), dy = ((Number) py).doubleValue();
        Double dz = pz == null ? null : ((Number) pz).doubleValue();
        int srid;
        if (sridArg instanceof Long s) {
            srid = s.intValue();
        } else if (geo) {
            srid = dz == null ? 4326 : 4979;
        } else {
            srid = dz == null ? 7203 : 9157;
        }
        if (geo && (dy < -90 || dy > 90)) {
            throw CypherException.argument("Invalid latitude: " + dy);
        }
        return new PointV(srid, dx, dy, dz);
    }

    static Object distance(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (!(a instanceof PointV p) || !(b instanceof PointV q)) {
            return null;
        }
        if (p.srid() != q.srid()) {
            return null;
        }
        if (p.geographic()) {
            double r = 6378140.0;
            double phi1 = Math.toRadians(p.y()), phi2 = Math.toRadians(q.y());
            double dphi = phi2 - phi1, dl = Math.toRadians(q.x() - p.x());
            double h = Math.sin(dphi / 2) * Math.sin(dphi / 2) + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dl / 2) * Math.sin(dl / 2);
            return 2 * r * Math.asin(Math.sqrt(h));
        }
        double dz = p.z() != null && q.z() != null ? p.z() - q.z() : 0;
        return Math.sqrt((p.x() - q.x()) * (p.x() - q.x()) + (p.y() - q.y()) * (p.y() - q.y()) + dz * dz);
    }

    static DayOfWeek unusedDow() {
        return DayOfWeek.MONDAY;
    }
}
