package com.sayonora.wire.pgwire;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decodes a pgwire Bind message's binary-format ({@code format=1}) parameter values into real
 * Java objects, keyed by the parameter's real Postgres type OID -- the OID a real client sent in
 * its own Parse message (see {@code PgWireSessionHandler#handleParse}, which used to discard
 * these entirely; {@code handleBind} then had no way to know a binary value's actual shape and
 * simply refused every one of them).
 *
 * <p>Covers the types a real client library actually sends in binary form for an ordinary
 * {@code PreparedStatement} -- confirmed live via pgjdbc, which negotiates binary format for
 * plain integers by default once a statement has been executed enough times to be server-side
 * prepared (see {@code JdbcBackendExecutor}'s own reuse-a-PreparedStatement comment for the
 * matching threshold on Warp's OWN backend side). Numeric ({@code NUMERIC}/{@code DECIMAL}),
 * arrays, and the JSON/UUID/network types use Postgres's own more elaborate binary encodings
 * (a variable-length digit-group format for numeric, a length-prefixed element list for arrays)
 * not implemented here -- those still fail with a clear, per-OID error naming exactly what's
 * missing, not a blanket "no binary support at all" the way every OID used to.
 */
final class PgBinaryParamDecoder {

    // Real Postgres type OIDs (pg_type.oid) for the scalar types actually reachable here --
    // see https://www.postgresql.org/docs/current/catalog-pg-type.html.
    private static final int OID_BOOL = 16;
    private static final int OID_BYTEA = 17;
    private static final int OID_INT8 = 20;
    private static final int OID_INT2 = 21;
    private static final int OID_INT4 = 23;
    private static final int OID_TEXT = 25;
    private static final int OID_FLOAT4 = 700;
    private static final int OID_FLOAT8 = 701;
    private static final int OID_BPCHAR = 1042;
    private static final int OID_VARCHAR = 1043;
    private static final int OID_DATE = 1082;
    private static final int OID_TIMESTAMP = 1114;
    private static final int OID_TIMESTAMPTZ = 1184;
    private static final int OID_JSON = 114;
    private static final int OID_UUID = 2950;
    private static final int OID_JSONB = 3802;

    // Postgres's own binary date/timestamp epoch -- 2000-01-01, NOT the Unix epoch -- see
    // https://www.postgresql.org/docs/current/protocol-message-formats.html's own notes on the
    // Binary Format of date/timestamp values.
    private static final long PG_EPOCH_MILLIS = Date.valueOf("2000-01-01").getTime();
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private PgBinaryParamDecoder() {
    }

    /** @return true if this OID has a real decoder below -- callers should still call {@link
     *      #decode} even when this returns false, purely to get its clear per-OID error message
     *      instead of writing their own. */
    static boolean supports(int oid) {
        return switch (oid) {
            case OID_BOOL, OID_BYTEA, OID_INT8, OID_INT2, OID_INT4, OID_TEXT, OID_FLOAT4, OID_FLOAT8,
                    OID_BPCHAR, OID_VARCHAR, OID_DATE, OID_TIMESTAMP, OID_TIMESTAMPTZ,
                    OID_UUID, OID_JSON, OID_JSONB -> true;
            default -> false;
        };
    }

    // pgjdbc leaves a placeholder's type OID as 0 ("unspecified, you figure it out") whenever it
    // doesn't know the target column's type ahead of time -- confirmed live: setTimestamp() on a
    // plain "?" placeholder sends oid=0, not OID_TIMESTAMP, even though the value itself is
    // unambiguously a timestamp. Real Postgres resolves oid=0 by parsing the query and looking at
    // what the placeholder is compared/assigned to; Warp has no such analysis here, so this falls
    // back to shape-sniffing the text itself -- good enough to cover the actual common case
    // (date/timestamp values), not a substitute for real type inference.
    private static final Pattern TIMESTAMP_SHAPE = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)([+-]\\d{2}(:?\\d{2})?)?$");
    private static final Pattern DATE_SHAPE = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

    /**
     * As {@link #decode}, but for a TEXT-format ({@code format=0}) value -- a real gap found
     * fixing the binary-format one above: a text-format {@code boolean} bind (pgjdbc sends
     * {@code "true"}/{@code "false"}) reached {@code JdbcBackendExecutor}'s generic string
     * {@code coerce()} (int, then decimal, then give up and bind the raw string), which has no
     * boolean case at all -- binding the literal string {@code "true"} against a real
     * {@code boolean} column fails with a genuine {@code column "x" is of type boolean but
     * expression is of type character varying}. Now that the client's declared OID is available
     * for every parameter regardless of format (see {@code PgWireSessionHandler#handleParse}),
     * text-format values get the same OID-driven typing binary ones do, instead of guessing.
     * Returns {@code text} unchanged for an OID with no specific text-format handling here (which
     * includes every OID {@link #supports} doesn't cover) -- {@code coerce()}'s own numeric-string
     * sniffing remains the fallback for those, unlike {@link #decode}'s hard failure, since a
     * text-format value that isn't specially handled here is still perfectly valid SQL text.
     *
     * <p>A trailing {@code [+-]HH[:MM]} timezone offset (pgjdbc appends the session timezone to
     * ANY timestamp-shaped text value, tz column or not, whenever it doesn't know which) is
     * stripped before parsing -- Warp doesn't yet apply timezone conversion itself, so this binds
     * the same wall-clock value the client typed, same as every other pgwire test's existing
     * literal-SQL convention already assumes.
     */
    static Object decodeText(int oid, String text) {
        return switch (oid) {
            case OID_BOOL -> switch (text) {
                case "t", "true", "TRUE", "1" -> Boolean.TRUE;
                case "f", "false", "FALSE", "0" -> Boolean.FALSE;
                default -> text;
            };
            case OID_INT2, OID_INT4, OID_INT8 -> {
                try {
                    yield Long.valueOf(text);
                } catch (NumberFormatException e) {
                    yield text;
                }
            }
            case OID_FLOAT4, OID_FLOAT8 -> {
                try {
                    yield Double.valueOf(text);
                } catch (NumberFormatException e) {
                    yield text;
                }
            }
            case OID_DATE -> {
                try {
                    yield Date.valueOf(text);
                } catch (IllegalArgumentException e) {
                    yield text;
                }
            }
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> parseTimestampOrElseText(text, text);
            // A plain String bound with no type hint negotiates as Postgres's "unknown"
            // pseudo-type, which fails to implicitly cast to uuid/json/jsonb for a column
            // comparison or insert (the same "column x is of type Y but expression is of type
            // character varying" class of error boolean/timestamp already had -- see this
            // method's own javadoc). java.util.UUID binds as a real uuid parameter automatically
            // (pgjdbc's own behavior); PGobject with its type set is the equivalent for json/
            // jsonb, since plain JDBC has no built-in Java type for either.
            case OID_UUID -> {
                try {
                    yield java.util.UUID.fromString(text);
                } catch (IllegalArgumentException e) {
                    yield text;
                }
            }
            case OID_JSON, OID_JSONB -> {
                try {
                    org.postgresql.util.PGobject pgObject = new org.postgresql.util.PGobject();
                    pgObject.setType(oid == OID_JSONB ? "jsonb" : "json");
                    pgObject.setValue(text);
                    yield pgObject;
                } catch (java.sql.SQLException e) {
                    yield text;
                }
            }
            // oid=0: the common real case for date/time binds (see this method's own javadoc) --
            // everything else left as plain text, same as before.
            default -> {
                if (DATE_SHAPE.matcher(text).matches()) {
                    try {
                        yield Date.valueOf(text);
                    } catch (IllegalArgumentException e) {
                        yield text;
                    }
                } else if (TIMESTAMP_SHAPE.matcher(text).matches()) {
                    yield parseTimestampOrElseText(text, text);
                } else {
                    yield text;
                }
            }
        };
    }

    private static Object parseTimestampOrElseText(String text, String fallback) {
        Matcher m = TIMESTAMP_SHAPE.matcher(text);
        String withoutOffset = m.matches() ? m.group(1) + " " + m.group(2) : text.replace('T', ' ');
        try {
            return Timestamp.valueOf(withoutOffset);
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }

    static Object decode(int oid, byte[] bytes, int paramIndex) throws IOException {
        return switch (oid) {
            case OID_BOOL -> {
                requireLength(oid, bytes, 1, paramIndex);
                yield bytes[0] != 0;
            }
            case OID_INT2 -> {
                requireLength(oid, bytes, 2, paramIndex);
                yield (short) readInt(bytes, 2);
            }
            case OID_INT4 -> {
                requireLength(oid, bytes, 4, paramIndex);
                yield (int) readInt(bytes, 4);
            }
            case OID_INT8 -> {
                requireLength(oid, bytes, 8, paramIndex);
                yield readInt(bytes, 8);
            }
            case OID_FLOAT4 -> {
                requireLength(oid, bytes, 4, paramIndex);
                yield Float.intBitsToFloat((int) readInt(bytes, 4));
            }
            case OID_FLOAT8 -> {
                requireLength(oid, bytes, 8, paramIndex);
                yield Double.longBitsToDouble(readInt(bytes, 8));
            }
            case OID_TEXT, OID_VARCHAR, OID_BPCHAR -> new String(bytes, StandardCharsets.UTF_8);
            case OID_BYTEA -> bytes;
            case OID_DATE -> {
                requireLength(oid, bytes, 4, paramIndex);
                int daysSincePgEpoch = (int) readInt(bytes, 4);
                yield new Date(PG_EPOCH_MILLIS + daysSincePgEpoch * MILLIS_PER_DAY);
            }
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> {
                requireLength(oid, bytes, 8, paramIndex);
                long microsSincePgEpoch = readInt(bytes, 8);
                yield new Timestamp(PG_EPOCH_MILLIS + Math.floorDiv(microsSincePgEpoch, 1000L));
            }
            // 16 raw bytes, standard RFC 4122 layout -- the same shape java.util.UUID's own
            // most/least-significant-bits pair uses, so no byte reordering is needed.
            case OID_UUID -> {
                requireLength(oid, bytes, 16, paramIndex);
                long mostSigBits = readInt(bytes, 8);
                long leastSigBits = 0;
                for (int i = 8; i < 16; i++) {
                    leastSigBits = (leastSigBits << 8) | (bytes[i] & 0xFFL);
                }
                yield new java.util.UUID(mostSigBits, leastSigBits);
            }
            // Plain UTF-8 text, no extra framing.
            case OID_JSON -> wrapAsPgJson(bytes, "json", paramIndex);
            // One leading version byte (always 1 in every Postgres version that defines a binary
            // jsonb format), then the same plain UTF-8 text json uses.
            case OID_JSONB -> {
                if (bytes.length < 1) {
                    throw new IOException("binary-format bind parameter " + (paramIndex + 1)
                            + " (type OID " + oid + ") is empty, expected a leading version byte");
                }
                yield wrapAsPgJson(java.util.Arrays.copyOfRange(bytes, 1, bytes.length), "jsonb", paramIndex);
            }
            default -> throw new IOException("binary-format bind parameter " + (paramIndex + 1)
                    + " uses type OID " + oid + ", which has no binary decoder yet (numeric and "
                    + "arrays aren't implemented) -- send it as a text-format parameter instead");
        };
    }

    private static Object wrapAsPgJson(byte[] utf8Bytes, String pgType, int paramIndex) throws IOException {
        try {
            org.postgresql.util.PGobject pgObject = new org.postgresql.util.PGobject();
            pgObject.setType(pgType);
            pgObject.setValue(new String(utf8Bytes, StandardCharsets.UTF_8));
            return pgObject;
        } catch (java.sql.SQLException e) {
            throw new IOException("binary-format bind parameter " + (paramIndex + 1)
                    + " (type " + pgType + ") could not be wrapped: " + e.getMessage(), e);
        }
    }

    private static long readInt(byte[] bytes, int len) {
        long v = 0;
        for (int i = 0; i < len; i++) {
            v = (v << 8) | (bytes[i] & 0xFFL);
        }
        return v;
    }

    private static void requireLength(int oid, byte[] bytes, int expected, int paramIndex) throws IOException {
        if (bytes.length != expected) {
            throw new IOException("binary-format bind parameter " + (paramIndex + 1) + " (type OID " + oid
                    + ") should be " + expected + " byte(s), got " + bytes.length);
        }
    }
}
