package com.polygres.wire.orawire.ttc;

import java.math.BigDecimal;

/**
 * Encodes/decodes Oracle's proprietary NUMBER wire format, per
 * reference/ttc_execute_fetch_spec.md §5.2 (itself derived byte-for-byte
 * from python-oracledb's encode_number/decode_number).
 *
 * Does not implement the "max negative value" single-byte special case
 * (spec §7 item 6, not traced downstream in the reference) or scale==-127
 * FLOAT-style describe-info semantics (§7 item 5) — plain fixed-precision
 * decimals only.
 */
public final class OracleNumberCodec {

    private static final int NUMBER_MAX_DIGITS = 40;

    public static byte[] encode(BigDecimal value) {
        if (value.signum() == 0) {
            return new byte[] {(byte) 128};
        }
        boolean negative = value.signum() < 0;
        BigDecimal abs = value.abs().stripTrailingZeros();
        String digits = abs.unscaledValue().toString();
        // number of digits that fall to the left of the decimal point
        int decimalPointIndex = digits.length() - abs.scale();

        // digit pairs are taken from the front of `digits`, aligned so that
        // decimalPointIndex/2 lands on a pair boundary; pad a leading zero
        // digit if decimalPointIndex is odd, then a trailing zero digit if
        // the (possibly now longer) digit string has odd length.
        if (Math.floorMod(decimalPointIndex, 2) != 0) {
            digits = "0" + digits;
            decimalPointIndex += 1;
        }
        if (digits.length() % 2 != 0) {
            digits = digits + "0";
        }
        int numPairs = digits.length() / 2;

        if (decimalPointIndex > 126 * 2 || decimalPointIndex < -129 * 2 || numPairs > NUMBER_MAX_DIGITS / 2) {
            throw new IllegalArgumentException("NUMBER value out of representable range: " + value);
        }
        int exponentField = decimalPointIndex / 2 + 192; // spec §5.2: exponent_on_wire = decimal_point_index/2 + 192

        byte[] out = new byte[1 + numPairs + (negative ? 1 : 0)];
        out[0] = (byte) (negative ? (~exponentField & 0xFF) : (exponentField | 0x80));
        for (int i = 0; i < numPairs; i++) {
            int hi = digits.charAt(2 * i) - '0';
            int lo = digits.charAt(2 * i + 1) - '0';
            int pairValue = hi * 10 + lo;
            out[1 + i] = (byte) (negative ? (101 - pairValue) : (pairValue + 1));
        }
        if (negative && numPairs < NUMBER_MAX_DIGITS / 2) {
            out[out.length - 1] = 102;
        }
        return out;
    }

    public static BigDecimal decode(byte[] bytes) {
        if (bytes.length == 1 && (bytes[0] & 0xFF) == 128) {
            return BigDecimal.ZERO;
        }
        int firstByte = bytes[0] & 0xFF;
        boolean positive = (firstByte & 0x80) != 0;
        int exponentByte = positive ? firstByte : (~firstByte) & 0xFF;
        int exponent = exponentByte - 193;

        int numBytes = bytes.length;
        if (!positive && (bytes[numBytes - 1] & 0xFF) == 102) {
            numBytes--;
        }
        StringBuilder digits = new StringBuilder();
        for (int i = 1; i < numBytes; i++) {
            int wireByte = bytes[i] & 0xFF;
            int pairValue = positive ? (wireByte - 1) : (101 - wireByte);
            digits.append(pairValue / 10).append(pairValue % 10);
        }
        int decimalPointIndex = exponent * 2 + 2;
        BigDecimal unscaled = new BigDecimal(digits.toString());
        BigDecimal result = unscaled.movePointLeft(digits.length()).movePointRight(decimalPointIndex);
        return positive ? result : result.negate();
    }

    private OracleNumberCodec() {
    }
}
