package com.sayonora.wire.gremlinwire;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/** Numeric tower, equality and ordering rules of Gremlin values (TinkerPop's NumberHelper / Compare / Order). */
final class Cmp {

    private Cmp() {
    }

    static int rank(Number n) {
        if (n instanceof Byte) {
            return 1;
        }
        if (n instanceof Short) {
            return 2;
        }
        if (n instanceof Integer) {
            return 3;
        }
        if (n instanceof Long) {
            return 4;
        }
        if (n instanceof BigInteger) {
            return 5;
        }
        if (n instanceof Float) {
            return 6;
        }
        if (n instanceof Double) {
            return 7;
        }
        return 8;
    }

    static BigDecimal big(Number n) {
        if (n instanceof BigDecimal b) {
            return b;
        }
        if (n instanceof BigInteger b) {
            return new BigDecimal(b);
        }
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            return Double.isNaN(d) || Double.isInfinite(d) ? BigDecimal.ZERO : new BigDecimal(n.toString());
        }
        return BigDecimal.valueOf(n.longValue());
    }

    static int numCompare(Number a, Number b) {
        if (a instanceof Double || a instanceof Float || b instanceof Double || b instanceof Float) {
            double x = a.doubleValue();
            double y = b.doubleValue();
            if (Double.isNaN(x) || Double.isNaN(y) || Double.isInfinite(x) || Double.isInfinite(y)) {
                return Double.compare(x, y);
            }
            if (!(a instanceof BigDecimal) && !(b instanceof BigDecimal)) {
                return Double.compare(x, y);
            }
        }
        if (rank(a) <= 4 && rank(b) <= 4) {
            return Long.compare(a.longValue(), b.longValue());
        }
        return big(a).compareTo(big(b));
    }

    static boolean eq(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Number x && b instanceof Number y) {
            return numCompare(x, y) == 0;
        }
        if (a instanceof G.Element ea && b instanceof G.Element eb) {
            return ea.equals(eb);
        }
        return a.equals(b);
    }

    /** Comparison of two comparable values of the same kind; {@code null} when they are not comparable. */
    static Integer compare(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            return numCompare(x, y);
        }
        if (a instanceof String x && b instanceof String y) {
            return Integer.signum(x.compareTo(y));
        }
        if (a instanceof Boolean x && b instanceof Boolean y) {
            return Boolean.compare(x, y);
        }
        if (a instanceof Date x && b instanceof Date y) {
            return x.compareTo(y);
        }
        if (a instanceof UUID x && b instanceof UUID y) {
            return x.compareTo(y);
        }
        if (a instanceof Character x && b instanceof Character y) {
            return x.compareTo(y);
        }
        return null;
    }

    private static int typeRank(Object o) {
        if (o == null) {
            return 0;
        }
        if (o instanceof Boolean) {
            return 1;
        }
        if (o instanceof Number) {
            return 2;
        }
        if (o instanceof String) {
            return 4;
        }
        if (o instanceof Date) {
            return 3;
        }
        if (o instanceof UUID) {
            return 5;
        }
        return 9;
    }

    /** Total order used by order() / min() / max() / sort: null < boolean < number < date < string < uuid < other. */
    static int order(Object a, Object b) {
        Integer c = compare(a, b);
        if (c != null) {
            return c;
        }
        int ra = typeRank(a);
        int rb = typeRank(b);
        if (ra != rb) {
            return Integer.compare(ra, rb);
        }
        if (a instanceof Collection<?> x && b instanceof Collection<?> y) {
            java.util.Iterator<?> i = x.iterator();
            java.util.Iterator<?> j = y.iterator();
            while (i.hasNext() && j.hasNext()) {
                int r = order(i.next(), j.next());
                if (r != 0) {
                    return r;
                }
            }
            return Boolean.compare(i.hasNext(), j.hasNext());
        }
        if (a instanceof G.Element x && b instanceof G.Element y) {
            return order(x.id(), y.id());
        }
        if (a instanceof Map<?, ?> || b instanceof Map<?, ?>) {
            return String.valueOf(a).compareTo(String.valueOf(b));
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    // ------------------------------------------------------------------ arithmetic

    static Number add(Number a, Number b) {
        int r = Math.max(Math.max(rank(a), rank(b)), 3);
        switch (r) {
            case 3:
                try {
                    return Math.addExact(a.intValue(), b.intValue());
                } catch (ArithmeticException e) {
                    return a.longValue() + b.longValue();
                }
            case 4:
                try {
                    return Math.addExact(a.longValue(), b.longValue());
                } catch (ArithmeticException e) {
                    return big(a).toBigInteger().add(big(b).toBigInteger());
                }
            case 5:
                return big(a).toBigInteger().add(big(b).toBigInteger());
            case 6:
                return a.floatValue() + b.floatValue();
            case 7:
                return a.doubleValue() + b.doubleValue();
            default:
                return big(a).add(big(b));
        }
    }

    static Number sub(Number a, Number b) {
        return add(a, negate(b));
    }

    static Number negate(Number a) {
        if (a instanceof Integer i) {
            return i == Integer.MIN_VALUE ? (Number) (-(long) i) : (Number) (-i);
        }
        if (a instanceof Long l) {
            return -l;
        }
        if (a instanceof Double d) {
            return -d;
        }
        if (a instanceof Float f) {
            return -f;
        }
        if (a instanceof BigDecimal b) {
            return b.negate();
        }
        if (a instanceof BigInteger b) {
            return b.negate();
        }
        return -a.intValue();
    }

    static Number mul(Number a, Number b) {
        int r = Math.max(Math.max(rank(a), rank(b)), 3);
        switch (r) {
            case 3:
                try {
                    return Math.multiplyExact(a.intValue(), b.intValue());
                } catch (ArithmeticException e) {
                    return a.longValue() * b.longValue();
                }
            case 4:
                try {
                    return Math.multiplyExact(a.longValue(), b.longValue());
                } catch (ArithmeticException e) {
                    return big(a).toBigInteger().multiply(big(b).toBigInteger());
                }
            case 5:
                return big(a).toBigInteger().multiply(big(b).toBigInteger());
            case 6:
                return a.floatValue() * b.floatValue();
            case 7:
                return a.doubleValue() * b.doubleValue();
            default:
                return big(a).multiply(big(b));
        }
    }

    static Number div(Number a, Number b) {
        int r = Math.max(Math.max(rank(a), rank(b)), 3);
        if (r <= 5 && b.longValue() == 0 && !(b instanceof BigInteger bi && bi.signum() != 0)) {
            throw G.GremlinError.script("java.lang.ArithmeticException: / by zero");
        }
        switch (r) {
            case 3:
                return a.intValue() / b.intValue();
            case 4:
                return a.longValue() / b.longValue();
            case 5:
                return big(a).toBigInteger().divide(big(b).toBigInteger());
            case 6:
                return a.floatValue() / b.floatValue();
            case 7:
                return a.doubleValue() / b.doubleValue();
            default:
                return big(a).divide(big(b), MathContext.DECIMAL128);
        }
    }

    static Number mod(Number a, Number b) {
        int r = Math.max(Math.max(rank(a), rank(b)), 3);
        switch (r) {
            case 3:
                return a.intValue() % b.intValue();
            case 4:
                return a.longValue() % b.longValue();
            case 6:
                return a.floatValue() % b.floatValue();
            case 7:
                return a.doubleValue() % b.doubleValue();
            default:
                return big(a).remainder(big(b));
        }
    }

    /** Groovy's {@code /}: exact integer division stays integral, otherwise a BigDecimal. */
    static Number groovyDiv(Number a, Number b) {
        if (rank(a) <= 5 && rank(b) <= 5) {
            if (b.longValue() == 0) {
                throw G.GremlinError.script("java.lang.ArithmeticException: Division by zero");
            }
            if (a.longValue() % b.longValue() == 0) {
                return div(a, b);
            }
            return big(a).divide(big(b), 16, java.math.RoundingMode.HALF_UP).stripTrailingZeros();
        }
        if (rank(a) >= 6 && rank(a) <= 7 || rank(b) >= 6 && rank(b) <= 7) {
            return a.doubleValue() / b.doubleValue();
        }
        return big(a).divide(big(b), MathContext.DECIMAL128);
    }

    static double dbl(Object o) {
        return ((Number) o).doubleValue();
    }
}
