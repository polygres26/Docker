package com.sayonora.wire.orawire.ttc;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public final class OracleDateCodec {

    public static byte[] encode(LocalDateTime dt) {
        int year = dt.getYear();
        byte[] out = new byte[7];
        out[0] = (byte) (year / 100 + 100);
        out[1] = (byte) (year % 100 + 100);
        out[2] = (byte) dt.getMonthValue();
        out[3] = (byte) dt.getDayOfMonth();
        out[4] = (byte) (dt.getHour() + 1);
        out[5] = (byte) (dt.getMinute() + 1);
        out[6] = (byte) (dt.getSecond() + 1);
        return out;
    }

    public static LocalDateTime decode(byte[] bytes) {
        int year = ((bytes[0] & 0xFF) - 100) * 100 + (bytes[1] & 0xFF) - 100;
        int month = bytes[2] & 0xFF;
        int day = bytes[3] & 0xFF;
        int hour = (bytes[4] & 0xFF) - 1;
        int minute = (bytes[5] & 0xFF) - 1;
        int second = (bytes[6] & 0xFF) - 1;
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    /**
     * TIMESTAMP WITH TIME ZONE (13 real bytes) -- confirmed live (a real ojdbc {@code
     * PreparedStatement.setObject(1, OffsetDateTime.now())} call): the SAME 7-byte
     * century/year/month/day/hour/minute/second fields {@link #decode} already reads, then 4
     * bytes of fractional-second nanoseconds (big-endian unsigned), then 2 bytes of timezone: an
     * hour offset biased by +20 and a minute offset biased by +60. {@code 2026-09-04
     * 21:00:31.735436160-07:00} arrived as {@code 78 7E 09 04 16 01 20 2B DC 45 80 0D 3C} -- byte
     * 11 ({@code 0x0D} = 13) minus 20 is -7 (the real UTC offset in this environment at capture
     * time), byte 12 ({@code 0x3C} = 60) minus 60 is 0 minutes, both confirmed against the real
     * wall-clock time this was captured at.
     */
    public static OffsetDateTime decodeWithTimeZone(byte[] bytes) {
        LocalDateTime dateTime = decode(bytes);
        long nanos = ((bytes[7] & 0xFFL) << 24) | ((bytes[8] & 0xFFL) << 16)
                | ((bytes[9] & 0xFFL) << 8) | (bytes[10] & 0xFFL);
        dateTime = dateTime.withNano((int) nanos);
        int offsetHours = (bytes[11] & 0xFF) - 20;
        int offsetMinutes = (bytes[12] & 0xFF) - 60;
        ZoneOffset offset = ZoneOffset.ofHoursMinutes(offsetHours, offsetMinutes);
        return OffsetDateTime.of(dateTime, offset);
    }

    private OracleDateCodec() {
    }
}
