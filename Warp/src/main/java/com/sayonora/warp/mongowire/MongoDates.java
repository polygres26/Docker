package com.sayonora.warp.mongowire;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/** Date expression operators. */
final class MongoDates {

    private static final BsonValue MISSING = MongoMatcher.MISSING;

    private MongoDates() {
    }

    static String isoString(long ms, ZoneId zone) {
        ZonedDateTime z = Instant.ofEpochMilli(ms).atZone(zone);
        return String.format("%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", z.getYear(), z.getMonthValue(), z.getDayOfMonth(),
                z.getHour(), z.getMinute(), z.getSecond(), z.getNano() / 1_000_000);
    }

    static ZoneId zone(BsonValue tz) {
        if (tz == null || MongoExpr.nullish(tz)) {
            return ZoneOffset.UTC;
        }
        if (!tz.isString()) {
            throw new MongoCmdException(40517, "timezone must evaluate to a string, found " + MongoMatcher.typeName(tz));
        }
        String s = tz.asString().getValue();
        try {
            if (s.equals("Z") || s.equalsIgnoreCase("utc") && false) {
                return ZoneOffset.UTC;
            }
            if (s.startsWith("+") || s.startsWith("-")) {
                return ZoneOffset.of(s);
            }
            return ZoneId.of(s);
        } catch (RuntimeException e) {
            throw new MongoCmdException(40485, "unrecognized time zone identifier: \"" + s + "\"");
        }
    }

    static long dateMs(BsonValue v) {
        if (v.isDateTime()) {
            return v.asDateTime().getValue();
        }
        if (v.isTimestamp()) {
            return v.asTimestamp().getTime() * 1000L;
        }
        if (v.isObjectId()) {
            return v.asObjectId().getValue().getDate().getTime();
        }
        throw new MongoCmdException(16006, "can't convert from BSON type " + MongoMatcher.typeName(v) + " to Date");
    }

    private static ZonedDateTime zdt(long ms, ZoneId z) {
        return Instant.ofEpochMilli(ms).atZone(z);
    }

    /** Argument forms: a bare date, or {date:, timezone:}. Returns null for null input. */
    private static ZonedDateTime part(BsonValue arg) {
        BsonValue date = arg;
        BsonValue tz = null;
        if (arg.isDocument()) {
            BsonDocument d = arg.asDocument();
            for (String k : d.keySet()) {
                if (!k.equals("date") && !k.equals("timezone")) {
                    throw new MongoCmdException(40535, "unrecognized option to $dayOfMonth: \"" + k + "\"");
                }
            }
            if (!d.containsKey("date")) {
                throw new MongoCmdException(40539, "missing 'date' argument to $dayOfMonth, provided: " + d.toJson());
            }
            date = d.get("date");
            tz = d.get("timezone");
        }
        if (MongoExpr.nullish(date)) {
            return null;
        }
        ZoneId z = zone(tz);
        return zdt(dateMs(date), z);
    }

    private static void reg1(MongoExpr.Reg reg, String name, java.util.function.ToIntFunction<ZonedDateTime> f) {
        reg.reg(name, 1, 1, a -> {
            ZonedDateTime z = part(a[0]);
            return z == null ? BsonNull.VALUE : new BsonInt32(f.applyAsInt(z));
        });
    }

    static void register(MongoExpr.Reg reg, Map<String, BiFunction<BsonValue, String, MongoExpr.Expr>> special) {
        reg1(reg, "$year", ZonedDateTime::getYear);
        reg1(reg, "$month", ZonedDateTime::getMonthValue);
        reg1(reg, "$dayOfMonth", ZonedDateTime::getDayOfMonth);
        reg1(reg, "$hour", ZonedDateTime::getHour);
        reg1(reg, "$minute", ZonedDateTime::getMinute);
        reg1(reg, "$second", ZonedDateTime::getSecond);
        reg1(reg, "$millisecond", z -> z.getNano() / 1_000_000);
        reg1(reg, "$dayOfYear", ZonedDateTime::getDayOfYear);
        reg1(reg, "$dayOfWeek", z -> z.getDayOfWeek().getValue() % 7 + 1);
        reg1(reg, "$isoDayOfWeek", z -> z.getDayOfWeek().getValue());
        reg1(reg, "$isoWeek", z -> z.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
        reg.reg("$isoWeekYear", 1, 1, a -> {
            ZonedDateTime z = part(a[0]);
            return z == null ? BsonNull.VALUE : new BsonInt64(z.get(IsoFields.WEEK_BASED_YEAR));
        });
        reg1(reg, "$week", z -> {
            // US week: weeks start on Sunday; days before the first Sunday are week 0
            int doy = z.getDayOfYear();
            int dow = z.getDayOfWeek().getValue() % 7; // Sunday=0
            return (doy + 6 - dow) / 7;
        });
        reg.reg("$dateToString", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateToString");
            BsonValue date = d.get("date");
            if (date == null) {
                throw new MongoCmdException(18628, "Missing 'date' parameter to $dateToString");
            }
            if (MongoExpr.nullish(date)) {
                return d.containsKey("onNull") ? d.get("onNull") : BsonNull.VALUE;
            }
            BsonValue fmt = d.get("format");
            if (fmt != null && MongoExpr.nullish(fmt)) {
                return BsonNull.VALUE;
            }
            ZoneId z = zone(d.get("timezone"));
            long ms = dateMs(date);
            String f = fmt == null ? null : fmt.asString().getValue();
            if (f == null) {
                boolean utc = d.get("timezone") == null;
                ZonedDateTime zz = zdt(ms, z);
                String base = String.format("%04d-%02d-%02dT%02d:%02d:%02d.%03d", zz.getYear(), zz.getMonthValue(), zz.getDayOfMonth(),
                        zz.getHour(), zz.getMinute(), zz.getSecond(), zz.getNano() / 1_000_000);
                return new BsonString(base + "Z");
            }
            return new BsonString(formatDate(f, zdt(ms, z)));
        });
        reg.reg("$dateFromString", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateFromString");
            BsonValue s = d.get("dateString");
            if (s == null) {
                throw new MongoCmdException(40542, "Missing 'dateString' parameter to $dateFromString");
            }
            if (MongoExpr.nullish(s)) {
                return d.containsKey("onNull") ? d.get("onNull") : BsonNull.VALUE;
            }
            try {
                if (!s.isString()) {
                    throw new MongoCmdException(40543, "$dateFromString requires that 'dateString' be a string, found: "
                            + MongoMatcher.typeName(s) + " with value " + s);
                }
                BsonValue fmt = d.get("format");
                return new BsonDateTime(parseDate(s.asString().getValue(), fmt == null || MongoExpr.nullish(fmt) ? null
                        : fmt.asString().getValue(), d.get("timezone")));
            } catch (MongoCmdException e) {
                if (d.containsKey("onError")) {
                    return d.get("onError");
                }
                throw e;
            }
        });
        reg.reg("$dateFromParts", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateFromParts");
            ZoneId z = zone(d.get("timezone"));
            for (Map.Entry<String, BsonValue> e : d.entrySet()) {
                if (MongoExpr.nullish(e.getValue()) && !e.getKey().equals("timezone")) {
                    return BsonNull.VALUE;
                }
            }
            boolean iso = d.containsKey("isoWeekYear");
            if (!iso && !d.containsKey("year")) {
                throw new MongoCmdException(40516, "$dateFromParts requires either 'year' or 'isoWeekYear' to be present");
            }
            long hour = num(d, "hour", 0);
            long minute = num(d, "minute", 0);
            long second = num(d, "second", 0);
            long milli = num(d, "millisecond", 0);
            LocalDateTime base;
            if (iso) {
                long y = num(d, "isoWeekYear", 1970);
                long w = num(d, "isoWeek", 1);
                long dw = num(d, "isoDayOfWeek", 1);
                LocalDate jan4 = LocalDate.of((int) y, 1, 4);
                LocalDate week1 = jan4.with(java.time.temporal.TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
                base = week1.plusWeeks(w - 1).plusDays(dw - 1).atStartOfDay();
            } else {
                long y = num(d, "year", 1970);
                if (y < 1 || y > 9999) {
                    throw new MongoCmdException(40523, "'year' must evaluate to an integer in the range 1 to 9999, found " + y);
                }
                base = LocalDate.of((int) y, 1, 1).plusMonths(num(d, "month", 1) - 1).plusDays(num(d, "day", 1) - 1).atStartOfDay();
            }
            base = base.plusHours(hour).plusMinutes(minute).plusSeconds(second).plusNanos(milli * 1_000_000);
            return new BsonDateTime(base.atZone(z).toInstant().toEpochMilli());
        });
        reg.reg("$dateToParts", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateToParts");
            BsonValue date = d.get("date");
            if (date == null) {
                throw new MongoCmdException(40522, "Missing 'date' parameter to $dateToParts");
            }
            if (MongoExpr.nullish(date)) {
                return BsonNull.VALUE;
            }
            ZonedDateTime z = zdt(dateMs(date), zone(d.get("timezone")));
            boolean iso = d.containsKey("iso8601") && MongoExpr.truthy(d.get("iso8601"));
            BsonDocument out = new BsonDocument();
            if (iso) {
                out.put("isoWeekYear", new BsonInt32(z.get(IsoFields.WEEK_BASED_YEAR)));
                out.put("isoWeek", new BsonInt32(z.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)));
                out.put("isoDayOfWeek", new BsonInt32(z.getDayOfWeek().getValue()));
            } else {
                out.put("year", new BsonInt32(z.getYear()));
                out.put("month", new BsonInt32(z.getMonthValue()));
                out.put("day", new BsonInt32(z.getDayOfMonth()));
            }
            out.put("hour", new BsonInt32(z.getHour()));
            out.put("minute", new BsonInt32(z.getMinute()));
            out.put("second", new BsonInt32(z.getSecond()));
            out.put("millisecond", new BsonInt32(z.getNano() / 1_000_000));
            return out;
        });
        reg.reg("$dateAdd", 1, 1, a -> dateAdd(arg(a[0], "$dateAdd"), 1, "$dateAdd"));
        reg.reg("$dateSubtract", 1, 1, a -> dateAdd(arg(a[0], "$dateSubtract"), -1, "$dateSubtract"));
        reg.reg("$dateDiff", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateDiff");
            for (String k : List.of("startDate", "endDate", "unit")) {
                if (!d.containsKey(k)) {
                    throw new MongoCmdException(5166302, "Missing '" + k + "' parameter to $dateDiff");
                }
            }
            if (MongoExpr.nullish(d.get("startDate")) || MongoExpr.nullish(d.get("endDate")) || MongoExpr.nullish(d.get("unit"))) {
                return BsonNull.VALUE;
            }
            ZoneId z = zone(d.get("timezone"));
            String unit = unit(d.get("unit"), "$dateDiff");
            ZonedDateTime s = zdt(dateMs(d.get("startDate")), z);
            ZonedDateTime e = zdt(dateMs(d.get("endDate")), z);
            long r;
            switch (unit) {
                case "millisecond": r = e.toInstant().toEpochMilli() - s.toInstant().toEpochMilli(); break;
                case "second": r = Math.floorDiv(e.toInstant().toEpochMilli(), 1000) - Math.floorDiv(s.toInstant().toEpochMilli(), 1000); break;
                case "minute": r = Math.floorDiv(e.toInstant().toEpochMilli(), 60000) - Math.floorDiv(s.toInstant().toEpochMilli(), 60000); break;
                case "hour": r = Math.floorDiv(e.toInstant().toEpochMilli(), 3600000) - Math.floorDiv(s.toInstant().toEpochMilli(), 3600000); break;
                case "day": r = ChronoUnit.DAYS.between(s.toLocalDate(), e.toLocalDate()); break;
                case "week": {
                    DayOfWeek sow = startOfWeek(d.get("startOfWeek"));
                    LocalDate sd = s.toLocalDate().with(java.time.temporal.TemporalAdjusters.previousOrSame(sow));
                    LocalDate ed = e.toLocalDate().with(java.time.temporal.TemporalAdjusters.previousOrSame(sow));
                    r = ChronoUnit.DAYS.between(sd, ed) / 7;
                    break;
                }
                case "month": r = (e.getYear() - s.getYear()) * 12L + e.getMonthValue() - s.getMonthValue(); break;
                case "quarter": r = (e.getYear() * 4L + (e.getMonthValue() - 1) / 3) - (s.getYear() * 4L + (s.getMonthValue() - 1) / 3); break;
                default: r = e.getYear() - s.getYear();
            }
            return new BsonInt64(r);
        });
        reg.reg("$dateTrunc", 1, 1, a -> {
            BsonDocument d = arg(a[0], "$dateTrunc");
            if (!d.containsKey("date")) {
                throw new MongoCmdException(5439012, "Missing 'date' parameter to $dateTrunc");
            }
            if (!d.containsKey("unit")) {
                throw new MongoCmdException(5439013, "Missing 'unit' parameter to $dateTrunc");
            }
            if (MongoExpr.nullish(d.get("date")) || MongoExpr.nullish(d.get("unit"))) {
                return BsonNull.VALUE;
            }
            ZoneId z = zone(d.get("timezone"));
            String unit = unit(d.get("unit"), "$dateTrunc");
            long bin = d.containsKey("binSize") ? MongoNum.truncLong(d.get("binSize")) : 1;
            ZonedDateTime t = zdt(dateMs(d.get("date")), z);
            ZonedDateTime r;
            switch (unit) {
                case "millisecond": r = t.withNano((int) (t.getNano() / 1_000_000 / bin * bin * 1_000_000)); break;
                case "second": r = t.withNano(0).withSecond((int) (t.getSecond() / bin * bin)); break;
                case "minute": r = t.truncatedTo(ChronoUnit.MINUTES).withMinute((int) (t.getMinute() / bin * bin)); break;
                case "hour": r = t.truncatedTo(ChronoUnit.HOURS).withHour((int) (t.getHour() / bin * bin)); break;
                case "day": r = t.truncatedTo(ChronoUnit.DAYS); break;
                case "week": r = t.truncatedTo(ChronoUnit.DAYS).with(java.time.temporal.TemporalAdjusters.previousOrSame(startOfWeek(d.get("startOfWeek")))); break;
                case "month": r = t.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1); break;
                case "quarter": r = t.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth((t.getMonthValue() - 1) / 3 * 3 + 1); break;
                default: r = t.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
            }
            return new BsonDateTime(r.toInstant().toEpochMilli());
        });
    }

    private static DayOfWeek startOfWeek(BsonValue v) {
        if (v == null || MongoExpr.nullish(v)) {
            return DayOfWeek.SUNDAY;
        }
        String s = v.asString().getValue().toLowerCase(Locale.ROOT);
        for (DayOfWeek d : DayOfWeek.values()) {
            String n = d.name().toLowerCase(Locale.ROOT);
            if (n.equals(s) || n.substring(0, 3).equals(s)) {
                return d;
            }
        }
        throw new MongoCmdException(5439015, "unrecognized startOfWeek '" + s + "'");
    }

    private static String unit(BsonValue u, String op) {
        if (!u.isString()) {
            throw new MongoCmdException(5439017, op + " requires 'unit' to be a string, but got " + MongoMatcher.typeName(u));
        }
        String s = u.asString().getValue();
        if (!List.of("year", "quarter", "week", "month", "day", "hour", "minute", "second", "millisecond").contains(s)) {
            throw new MongoCmdException(9, "unknown time unit value: " + s);
        }
        return s;
    }

    private static BsonValue dateAdd(BsonDocument d, int sign, String op) {
        for (String k : List.of("startDate", "unit", "amount")) {
            if (!d.containsKey(k)) {
                throw new MongoCmdException(5166402, "Missing '" + k + "' parameter to " + op);
            }
        }
        if (MongoExpr.nullish(d.get("startDate")) || MongoExpr.nullish(d.get("unit")) || MongoExpr.nullish(d.get("amount"))) {
            return BsonNull.VALUE;
        }
        String unit = unit(d.get("unit"), op);
        BsonValue amt = d.get("amount");
        if (!BsonCmp.isNumber(amt) || MongoNum.toDouble(amt) != Math.floor(MongoNum.toDouble(amt))) {
            throw new MongoCmdException(5166405, "invalid amount for " + op + ": must be an integer");
        }
        long n = MongoNum.truncLong(amt) * sign;
        ZoneId z = zone(d.get("timezone"));
        ZonedDateTime t = zdt(dateMs(d.get("startDate")), z);
        ZonedDateTime r = switch (unit) {
            case "year" -> t.plusYears(n);
            case "quarter" -> t.plusMonths(3 * n);
            case "month" -> t.plusMonths(n);
            case "week" -> t.plusWeeks(n);
            case "day" -> t.plusDays(n);
            case "hour" -> t.plusHours(n);
            case "minute" -> t.plusMinutes(n);
            case "second" -> t.plusSeconds(n);
            default -> t.plus(n, ChronoUnit.MILLIS);
        };
        return new BsonDateTime(r.toInstant().toEpochMilli());
    }

    private static long num(BsonDocument d, String k, long def) {
        BsonValue v = d.get(k);
        if (v == null) {
            return def;
        }
        if (!BsonCmp.isNumber(v) || MongoNum.toDouble(v) != Math.floor(MongoNum.toDouble(v))) {
            throw new MongoCmdException(40515, "'" + k + "' must evaluate to an integer, found " + MongoMatcher.typeName(v) + " with value " + v);
        }
        return MongoNum.truncLong(v);
    }

    private static BsonDocument arg(BsonValue v, String op) {
        if (!v.isDocument()) {
            throw new MongoCmdException(40540, op + " only supports an object as its argument");
        }
        return v.asDocument();
    }

    private static String offsetString(ZonedDateTime z, boolean colon) {
        int s = z.getOffset().getTotalSeconds();
        int h = Math.abs(s) / 3600;
        int m = Math.abs(s) / 60 % 60;
        return (s < 0 ? "-" : "+") + String.format("%02d", h) + (colon ? ":" : "") + String.format("%02d", m);
    }

    static String formatDate(String f, ZonedDateTime z) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < f.length(); i++) {
            char c = f.charAt(i);
            if (c != '%') {
                sb.append(c);
                continue;
            }
            if (i + 1 >= f.length()) {
                throw new MongoCmdException(18535, "Unmatched '%' at end of format string");
            }
            char k = f.charAt(++i);
            switch (k) {
                case 'Y' -> sb.append(String.format("%04d", z.getYear()));
                case 'm' -> sb.append(String.format("%02d", z.getMonthValue()));
                case 'd' -> sb.append(String.format("%02d", z.getDayOfMonth()));
                case 'H' -> sb.append(String.format("%02d", z.getHour()));
                case 'M' -> sb.append(String.format("%02d", z.getMinute()));
                case 'S' -> sb.append(String.format("%02d", z.getSecond()));
                case 'L' -> sb.append(String.format("%03d", z.getNano() / 1_000_000));
                case 'j' -> sb.append(String.format("%03d", z.getDayOfYear()));
                case 'w' -> sb.append(z.getDayOfWeek().getValue() % 7 + 1);
                case 'u' -> sb.append(z.getDayOfWeek().getValue());
                case 'U' -> {
                    int doy = z.getDayOfYear();
                    int dow = z.getDayOfWeek().getValue() % 7;
                    sb.append(String.format("%02d", (doy + 6 - dow) / 7));
                }
                case 'V' -> sb.append(String.format("%02d", z.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)));
                case 'G' -> sb.append(String.format("%04d", z.get(IsoFields.WEEK_BASED_YEAR)));
                case 'z' -> sb.append(offsetString(z, false));
                case 'Z' -> sb.append(z.getOffset().getTotalSeconds() / 60);
                case 'F' -> sb.append(String.format("%04d-%02d-%02d", z.getYear(), z.getMonthValue(), z.getDayOfMonth()));
                case 'T' -> sb.append(String.format("%02d:%02d:%02d", z.getHour(), z.getMinute(), z.getSecond()));
                case '%' -> sb.append('%');
                default -> throw new MongoCmdException(18536, "Invalid format character '%" + k + "' in format string");
            }
        }
        return sb.toString();
    }

    private static final java.util.regex.Pattern ISO = java.util.regex.Pattern.compile(
            "^(\\d{4})-(\\d{2})-(\\d{2})(?:[T ](\\d{2}):(\\d{2})(?::(\\d{2})(?:[.,](\\d{1,9}))?)?)?\\s*(Z|[+-]\\d{2}(?::?\\d{2})?)?$");

    static long parseDate(String s, String format, BsonValue tz) {
        ZoneId zone = zone(tz);
        try {
            if (format != null) {
                return parseWithFormat(s, format, zone);
            }
            java.util.regex.Matcher m = ISO.matcher(s.trim());
            if (!m.matches()) {
                String t = s.trim();
                // year-only, date-only alternatives
                if (t.matches("^\\d{4}$")) {
                    return LocalDate.of(Integer.parseInt(t), 1, 1).atStartOfDay(zone).toInstant().toEpochMilli();
                }
                throw new MongoCmdException(241, "Error parsing date string '" + s + "'");
            }
            int nano = 0;
            if (m.group(7) != null) {
                String frac = (m.group(7) + "000000000").substring(0, 9);
                nano = Integer.parseInt(frac) / 1_000_000 * 1_000_000;
            }
            LocalDateTime ldt = LocalDateTime.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
                    m.group(4) == null ? 0 : Integer.parseInt(m.group(4)), m.group(5) == null ? 0 : Integer.parseInt(m.group(5)),
                    m.group(6) == null ? 0 : Integer.parseInt(m.group(6)), nano);
            String off = m.group(8);
            ZoneId z = zone;
            if (off != null) {
                if (tz != null && !MongoExpr.nullish(tz)) {
                    throw new MongoCmdException(40551, "you cannot pass in a date/time string with time zone information ('" + off
                            + "') together with a timezone argument");
                }
                z = off.equals("Z") ? ZoneOffset.UTC : ZoneOffset.of(off.length() == 3 ? off + ":00" : off.length() == 5 ? off.substring(0, 3) + ":" + off.substring(3) : off);
            }
            return ldt.atZone(z).toInstant().toEpochMilli();
        } catch (java.time.DateTimeException e) {
            throw new MongoCmdException(241, "Error parsing date string '" + s + "'; " + e.getMessage());
        }
    }

    private static long parseWithFormat(String s, String f, ZoneId zone) {
        StringBuilder rx = new StringBuilder();
        List<Character> keys = new java.util.ArrayList<>();
        for (int i = 0; i < f.length(); i++) {
            char c = f.charAt(i);
            if (c == '%' && i + 1 < f.length()) {
                char k = f.charAt(++i);
                switch (k) {
                    case 'Y' -> rx.append("(\\d{4})");
                    case 'm', 'd', 'H', 'M', 'S' -> rx.append("(\\d{2})");
                    case 'L' -> rx.append("(\\d{3})");
                    case 'z' -> rx.append("([+-]\\d{2}:?\\d{2}|Z)");
                    case '%' -> {
                        rx.append("%");
                        continue;
                    }
                    default -> throw new MongoCmdException(18536, "Invalid format character '%" + k + "' in format string");
                }
                keys.add(k);
            } else {
                rx.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(rx.toString()).matcher(s);
        if (!m.matches()) {
            throw new MongoCmdException(241, "Error parsing date string '" + s + "'; Failed to parse with format");
        }
        int y = 1970;
        int mo = 1;
        int d = 1;
        int h = 0;
        int mi = 0;
        int se = 0;
        int ms = 0;
        ZoneId z = zone;
        for (int i = 0; i < keys.size(); i++) {
            String g = m.group(i + 1);
            switch (keys.get(i)) {
                case 'Y' -> y = Integer.parseInt(g);
                case 'm' -> mo = Integer.parseInt(g);
                case 'd' -> d = Integer.parseInt(g);
                case 'H' -> h = Integer.parseInt(g);
                case 'M' -> mi = Integer.parseInt(g);
                case 'S' -> se = Integer.parseInt(g);
                case 'L' -> ms = Integer.parseInt(g);
                default -> z = g.equals("Z") ? ZoneOffset.UTC : ZoneOffset.of(g.length() == 5 ? g.substring(0, 3) + ":" + g.substring(3) : g);
            }
        }
        return LocalDateTime.of(y, mo, d, h, mi, se, ms * 1_000_000).atZone(z).toInstant().toEpochMilli();
    }
}
