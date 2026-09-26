package com.sayonora.wire.rediswire;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/** Number parsing and formatting with Redis' exact rules. */
final class Num {

    private Num() {
    }

    /** string2ll: optional '-', no '+', no leading zeros (except "0"), no spaces; {@code null} when invalid. */
    static Long parseLong(byte[] b) {
        int n = b.length;
        if (n == 0 || n > 20) {
            return null;
        }
        int i = 0;
        boolean neg = false;
        if (b[0] == '-') {
            neg = true;
            i = 1;
            if (n == 1) {
                return null;
            }
        }
        if (b[i] == '0') {
            return n == 1 ? Long.valueOf(0) : null;
        }
        long v = 0;
        for (; i < n; i++) {
            int c = b[i] - '0';
            if (c < 0 || c > 9) {
                return null;
            }
            if (v > (Long.MAX_VALUE - c) / 10) {
                if (neg && v == Long.MAX_VALUE / 10 && c == 8 && i == n - 1) {
                    return Long.MIN_VALUE;
                }
                return null;
            }
            v = v * 10 + c;
        }
        return neg ? -v : v;
    }

    static Long parseLong(String s) {
        return parseLong(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    /** strtod as Redis uses it for scores/floats: allows inf/-inf/+inf/infinity, exponents; rejects NaN, spaces, garbage. */
    static Double parseDouble(byte[] b) {
        if (b.length == 0 || b.length > 512) {
            return null;
        }
        String s = new String(b, StandardCharsets.ISO_8859_1);
        char c0 = s.charAt(0);
        if (Character.isWhitespace(c0) || Character.isWhitespace(s.charAt(s.length() - 1))) {
            return null;
        }
        String l = s.toLowerCase(java.util.Locale.ROOT);
        switch (l) {
            case "inf", "+inf", "infinity", "+infinity":
                return Double.POSITIVE_INFINITY;
            case "-inf", "-infinity":
                return Double.NEGATIVE_INFINITY;
            default:
                break;
        }
        // reject java-only syntax ("1d", "0x1p3", "NaN")
        for (int i = 0; i < l.length(); i++) {
            char c = l.charAt(i);
            if (!((c >= '0' && c <= '9') || c == '.' || c == 'e' || c == '+' || c == '-')) {
                return null;
            }
        }
        try {
            double d = Double.parseDouble(s);
            return Double.isNaN(d) ? null : d;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Shortest round-trip digits laid out exactly like Redis' fpconv_dtoa (plain integers up to ndigits+7, scientific without padding). */
    static String fmtDouble(double d) {
        if (Double.isNaN(d)) {
            return "nan";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "inf" : "-inf";
        }
        if (d == 0) {
            return "0";
        }
        // Redis prints integral doubles up to +-LLONG_MAX/2 as integers (double2ll), everything else via fpconv
        if (d == Math.rint(d) && Math.abs(d) <= 4.611686018427388E18) {
            return Long.toString((long) d);
        }
        boolean neg = d < 0;
        BigDecimal bd = new BigDecimal(Double.toString(Math.abs(d))).stripTrailingZeros();
        String digits = bd.unscaledValue().toString();
        int nd = digits.length();
        int k = -bd.scale();
        int exp = Math.abs(k + nd - 1);
        StringBuilder sb = new StringBuilder(neg ? "-" : "");
        if (k >= 0 && exp < nd + 7) {
            sb.append(digits);
            for (int i = 0; i < k; i++) {
                sb.append('0');
            }
            return sb.toString();
        }
        if (k < 0 && (k > -7 || exp < 4)) {
            int offset = nd - Math.abs(k);
            if (offset <= 0) {
                sb.append("0.");
                for (int i = 0; i < -offset; i++) {
                    sb.append('0');
                }
                sb.append(digits);
            } else {
                sb.append(digits, 0, offset).append('.').append(digits, offset, nd);
            }
            return sb.toString();
        }
        sb.append(digits.charAt(0));
        if (nd > 1) {
            sb.append('.').append(digits, 1, nd);
        }
        sb.append('e').append(k + nd - 1 < 0 ? '-' : '+').append(exp);
        return sb.toString();
    }

    static String fmtDoubleResp3(double d) {
        return fmtDouble(d);
    }

    /** INCRBYFLOAT / HINCRBYFLOAT result: Redis prints "%.17Lf" and trims trailing zeros (never scientific). */
    static String fmtLongDouble(BigDecimal v) {
        BigDecimal r = v.setScale(17, java.math.RoundingMode.HALF_EVEN).stripTrailingZeros();
        if (r.signum() == 0) {
            return "0";
        }
        return r.toPlainString();
    }

    /** Parses a plain (non-inf) decimal for INCRBYFLOAT; {@code null} when not a valid float or inf/nan. */
    static BigDecimal parseDecimal(byte[] b) {
        Double d = parseDouble(b);
        if (d == null || d.isInfinite()) {
            return null;
        }
        try {
            return new BigDecimal(new String(b, StandardCharsets.ISO_8859_1));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] bytes(long v) {
        return Long.toString(v).getBytes(StandardCharsets.ISO_8859_1);
    }

    static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    static boolean eq(byte[] a, String upper) {
        if (a.length != upper.length()) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            int c = a[i];
            if (c >= 'a' && c <= 'z') {
                c -= 32;
            }
            if (c != upper.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    static String upper(byte[] a) {
        return new String(a, StandardCharsets.ISO_8859_1).toUpperCase(java.util.Locale.ROOT);
    }
}
