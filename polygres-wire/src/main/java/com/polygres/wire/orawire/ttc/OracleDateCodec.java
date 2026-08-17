package com.polygres.wire.orawire.ttc;

import java.time.LocalDateTime;

/**
 * Encodes/decodes Oracle's 7-byte DATE wire format, per
 * reference/ttc_execute_fetch_spec.md §5.3. Fractional-second/timezone
 * variants (TIMESTAMP, length 11+) are out of narrow scope.
 */
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

    private OracleDateCodec() {
    }
}
