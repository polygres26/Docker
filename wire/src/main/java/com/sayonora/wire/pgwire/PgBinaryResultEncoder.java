package com.sayonora.wire.pgwire;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * The inverse of {@link PgBinaryParamDecoder}: encodes a result-column value into pgwire's
 * BINARY DataRow wire format when the client's own Bind message asked for it. Real gap found
 * live fixing {@link PgBinaryParamDecoder} itself: {@code PgWireSessionHandler} used to discard
 * the client's requested result format codes entirely and always send TEXT, declaring as much
 * honestly in {@code RowDescription} -- fine on its own, except pgjdbc (once it's negotiated
 * binary format for a column, typically after a statement is server-side prepared) decodes
 * whatever bytes arrive as binary regardless of what {@code RowDescription} itself declared,
 * producing real, silently-wrong values (confirmed live: a real {@code bigint} column came back
 * as garbage once pgjdbc started requesting binary results). Same type-coverage scope as {@link
 * PgBinaryParamDecoder} -- the common scalar types a real client actually round-trips through a
 * plain {@code PreparedStatement}, not the full Postgres binary type catalog.
 */
final class PgBinaryResultEncoder {

    private static final int OID_BOOL = 16;
    private static final int OID_INT8 = 20;
    private static final int OID_INT2 = 21;
    private static final int OID_TEXT = 25;
    private static final int OID_INT4 = 23;
    private static final int OID_FLOAT4 = 700;
    private static final int OID_FLOAT8 = 701;
    private static final int OID_BPCHAR = 1042;
    private static final int OID_VARCHAR = 1043;
    private static final int OID_DATE = 1082;
    private static final int OID_TIMESTAMP = 1114;
    private static final int OID_TIMESTAMPTZ = 1184;

    private static final long PG_EPOCH_MILLIS = Date.valueOf("2000-01-01").getTime();
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private PgBinaryResultEncoder() {
    }

    /** @return true if {@link #encode} has a real binary encoding for this OID -- callers should
     *      fall back to text encoding for everything else (numeric, arrays, json/jsonb, uuid,
     *      bytea), same disclosed scope {@link PgBinaryParamDecoder#supports} has on the bind
     *      side. Decided purely by column type/OID, not by any one row's actual value -- every
     *      row in a result set shares the same column type, so the format-code flag written once
     *      in RowDescription and the encoding actually used per DataRow must agree, or a real
     *      client (correctly) treats that as a protocol violation. TEXT/VARCHAR/BPCHAR count as
     *      "binary-supported" too: Postgres's own binary and text wire representations for those
     *      types are byte-for-byte identical (raw UTF-8, no numeric header), so {@link #encode}'s
     *      default case already produces a valid binary encoding for them. */
    static boolean supports(int oid) {
        return switch (oid) {
            case OID_BOOL, OID_INT8, OID_INT2, OID_INT4, OID_FLOAT4, OID_FLOAT8, OID_DATE,
                    OID_TIMESTAMP, OID_TIMESTAMPTZ, OID_TEXT, OID_VARCHAR, OID_BPCHAR -> true;
            default -> false;
        };
    }

    static byte[] encode(int oid, Object value) {
        return switch (oid) {
            case OID_BOOL -> new byte[] { (Boolean.TRUE.equals(value) || "true".equalsIgnoreCase(String.valueOf(value))
                    || "t".equalsIgnoreCase(String.valueOf(value))) ? (byte) 1 : (byte) 0 };
            case OID_INT2 -> writeInt(toLong(value), 2);
            case OID_INT4 -> writeInt(toLong(value), 4);
            case OID_INT8 -> writeInt(toLong(value), 8);
            case OID_FLOAT4 -> writeInt(Float.floatToIntBits(toDouble(value).floatValue()) & 0xFFFFFFFFL, 4);
            case OID_FLOAT8 -> writeInt(Double.doubleToLongBits(toDouble(value)), 8);
            case OID_DATE -> {
                long millis = value instanceof Date d ? d.getTime() : Date.valueOf(String.valueOf(value)).getTime();
                yield writeInt(Math.floorDiv(millis - PG_EPOCH_MILLIS, MILLIS_PER_DAY) & 0xFFFFFFFFL, 4);
            }
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> {
                long millis = value instanceof Timestamp t ? t.getTime() : Timestamp.valueOf(String.valueOf(value)).getTime();
                yield writeInt((millis - PG_EPOCH_MILLIS) * 1000L, 8);
            }
            default -> String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        };
    }

    private static long toLong(Object value) {
        return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
    }

    private static Double toDouble(Object value) {
        return value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
    }

    private static byte[] writeInt(long v, int len) {
        byte[] out = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            out[i] = (byte) (v & 0xFF);
            v >>>= 8;
        }
        return out;
    }
}
