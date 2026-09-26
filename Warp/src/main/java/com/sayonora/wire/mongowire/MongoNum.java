package com.sayonora.wire.mongowire;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

/** Numeric type promotion and arithmetic with MongoDB semantics (int32 -> int64 -> double -> decimal). */
final class MongoNum {

    private MongoNum() {
    }

    static boolean isInfinite(BsonValue v) {
        return v.isDouble() && Double.isInfinite(v.asDouble().getValue())
                || v.isDecimal128() && v.asDecimal128().getValue().isInfinite();
    }

    static boolean isNaN(BsonValue v) {
        return v.isDouble() && Double.isNaN(v.asDouble().getValue())
                || v.isDecimal128() && v.asDecimal128().getValue().isNaN();
    }

    static double toDouble(BsonValue v) {
        switch (v.getBsonType()) {
            case INT32: return v.asInt32().getValue();
            case INT64: return v.asInt64().getValue();
            case DOUBLE: return v.asDouble().getValue();
            default: {
                Decimal128 d = v.asDecimal128().getValue();
                if (d.isNaN()) {
                    return Double.NaN;
                }
                if (d.isInfinite()) {
                    return d.isNegative() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                }
                return BsonCmp.toBigDecimal(v).doubleValue();
            }
        }
    }

    static long truncLong(BsonValue v) {
        switch (v.getBsonType()) {
            case INT32: return v.asInt32().getValue();
            case INT64: return v.asInt64().getValue();
            case DOUBLE: return (long) v.asDouble().getValue();
            default: return BsonCmp.toBigDecimal(v).longValue();
        }
    }

    static BsonValue fromLong(long l) {
        return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? new BsonInt32((int) l) : new BsonInt64(l);
    }

    static Decimal128 toDecimal(BsonValue v) {
        switch (v.getBsonType()) {
            case DECIMAL128: return v.asDecimal128().getValue();
            case INT32: return new Decimal128(v.asInt32().getValue());
            case INT64: return new Decimal128(v.asInt64().getValue());
            default: {
                double d = v.asDouble().getValue();
                if (Double.isNaN(d)) {
                    return Decimal128.NaN;
                }
                if (Double.isInfinite(d)) {
                    return d > 0 ? Decimal128.POSITIVE_INFINITY : Decimal128.NEGATIVE_INFINITY;
                }
                BigDecimal r = new BigDecimal(d).round(new MathContext(15, RoundingMode.HALF_EVEN));
                int intDigits = r.precision() - r.scale();
                if (r.signum() != 0 && r.precision() < 15) {
                    r = r.setScale(Math.max(r.scale(), 15 - intDigits), RoundingMode.UNNECESSARY);
                }
                return new Decimal128(r);
            }
        }
    }

    private static Decimal128 dec(BigDecimal b) {
        return new Decimal128(b.round(MathContext.DECIMAL128));
    }

    private static boolean anySpecialDec(BsonValue a, BsonValue b) {
        return a.isDecimal128() && (a.asDecimal128().getValue().isNaN() || a.asDecimal128().getValue().isInfinite())
                || b.isDecimal128() && (b.asDecimal128().getValue().isNaN() || b.asDecimal128().getValue().isInfinite());
    }

    static BsonValue add(BsonValue a, BsonValue b) {
        if (a.isDecimal128() || b.isDecimal128()) {
            if (anySpecialDec(a, b)) {
                return new BsonDecimal128(dToDec(toDouble(a) + toDouble(b)));
            }
            return new BsonDecimal128(dec(BsonCmp.toBigDecimal(a).add(BsonCmp.toBigDecimal(b))));
        }
        if (a.isDouble() || b.isDouble()) {
            return new BsonDouble(toDouble(a) + toDouble(b));
        }
        long x = truncLong(a);
        long y = truncLong(b);
        if (a.isInt32() && b.isInt32()) {
            long r = x + y;
            return fromLong(r);
        }
        long r = x + y;
        if (((x ^ r) & (y ^ r)) < 0) {
            return new BsonDouble((double) x + (double) y);
        }
        return new BsonInt64(r);
    }

    private static Decimal128 dToDec(double d) {
        return Double.isNaN(d) ? Decimal128.NaN : Double.isInfinite(d)
                ? (d > 0 ? Decimal128.POSITIVE_INFINITY : Decimal128.NEGATIVE_INFINITY) : new Decimal128(new BigDecimal(d));
    }

    static BsonValue negate(BsonValue a) {
        return multiply(a, new BsonInt32(-1));
    }

    static BsonValue subtract(BsonValue a, BsonValue b) {
        if (a.isDecimal128() || b.isDecimal128()) {
            if (anySpecialDec(a, b)) {
                return new BsonDecimal128(dToDec(toDouble(a) - toDouble(b)));
            }
            return new BsonDecimal128(dec(BsonCmp.toBigDecimal(a).subtract(BsonCmp.toBigDecimal(b))));
        }
        if (a.isDouble() || b.isDouble()) {
            return new BsonDouble(toDouble(a) - toDouble(b));
        }
        long x = truncLong(a);
        long y = truncLong(b);
        long r = x - y;
        if (a.isInt32() && b.isInt32()) {
            return fromLong(r);
        }
        if (((x ^ y) & (x ^ r)) < 0) {
            return new BsonDouble((double) x - (double) y);
        }
        return new BsonInt64(r);
    }

    static BsonValue multiply(BsonValue a, BsonValue b) {
        if (a.isDecimal128() || b.isDecimal128()) {
            if (anySpecialDec(a, b)) {
                return new BsonDecimal128(dToDec(toDouble(a) * toDouble(b)));
            }
            return new BsonDecimal128(dec(BsonCmp.toBigDecimal(a).multiply(BsonCmp.toBigDecimal(b))));
        }
        if (a.isDouble() || b.isDouble()) {
            return new BsonDouble(toDouble(a) * toDouble(b));
        }
        long x = truncLong(a);
        long y = truncLong(b);
        long hi = Math.multiplyHigh(x, y);
        long lo = x * y;
        boolean overflow = !((hi == 0 && lo >= 0) || (hi == -1 && lo < 0));
        if (a.isInt32() && b.isInt32()) {
            if (overflow) {
                return new BsonInt64(lo);
            }
            return fromLong(lo);
        }
        if (overflow) {
            return new BsonDouble((double) x * (double) y);
        }
        return new BsonInt64(lo);
    }

    static BsonValue divide(BsonValue a, BsonValue b) {
        if (a.isDecimal128() || b.isDecimal128()) {
            BigDecimal d = BsonCmp.toBigDecimal(b);
            if (d.signum() == 0) {
                throw new MongoCmdException(2, "can't $divide by zero");
            }
            return new BsonDecimal128(dec(BsonCmp.toBigDecimal(a).divide(d, MathContext.DECIMAL128)));
        }
        double y = toDouble(b);
        if (y == 0) {
            throw new MongoCmdException(2, "can't $divide by zero");
        }
        return new BsonDouble(toDouble(a) / y);
    }

    static BsonValue mod(BsonValue a, BsonValue b) {
        if (a.isDecimal128() || b.isDecimal128()) {
            BigDecimal d = BsonCmp.toBigDecimal(b);
            if (d.signum() == 0) {
                throw new MongoCmdException(16610, "can't $mod by zero");
            }
            return new BsonDecimal128(dec(BsonCmp.toBigDecimal(a).remainder(d)));
        }
        if (a.isDouble() || b.isDouble()) {
            double y = toDouble(b);
            if (y == 0) {
                throw new MongoCmdException(16610, "can't $mod by zero");
            }
            return new BsonDouble(toDouble(a) % y);
        }
        long y = truncLong(b);
        if (y == 0) {
            throw new MongoCmdException(16610, "can't $mod by zero");
        }
        long r = y == -1 ? 0 : truncLong(a) % y;
        return a.isInt32() && b.isInt32() ? new BsonInt32((int) r) : fromLongKeep(r, a, b);
    }

    private static BsonValue fromLongKeep(long r, BsonValue a, BsonValue b) {
        return new BsonInt64(r);
    }
}
