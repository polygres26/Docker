package com.sayonora.wire.mssqlwire.frontend;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes a TDS {@code RPCRequest} batch -- specifically the {@code sp_executesql(@stmt, @params,
 * @P0, @P1, ...)} shape, the path essentially every modern SQL Server driver/ORM uses for a
 * parameterized query (JDBC's {@code PreparedStatement}, .NET's {@code SqlCommand} with
 * parameters, etc). This closes a real gap: {@code MssqlWireSessionHandler} used to reject every
 * RPC outright, so a client sending bind values instead of literal SQL got a hard error rather
 * than a working (if degraded to literal-scanning) shard-routing outcome -- see
 * {@code RouterStage.ValueShardRule}, which needs real {@code bindParams()} to route by bound
 * value, not just by literal.
 *
 * <p>Scope, deliberately: only the parameter types a real ORM commonly binds --
 * {@code NVARCHAR}/{@code VARCHAR}/{@code NCHAR}/{@code CHAR} (including the PLP/MAX-length
 * encoding, which JDBC uses for any moderately long string, including {@code @stmt} itself),
 * {@code INTN} (nullable int/bigint/etc), {@code FLTN} (float/real), and {@code BITN} (boolean).
 * {@code DATETIME}/{@code NUMERIC}/{@code DECIMAL} and every other TYPE_INFO code are explicitly
 * refused with an {@link IOException} rather than guessed at -- those have variable-length or
 * precision-dependent encodings where a wrong guess desyncs every parameter that follows in the
 * same call, silently. Also refused: anything other than a single RPC call per message (real
 * drivers send exactly one). OUTPUT/BY_REF parameters (status flag 0x01) are decoded, not
 * refused -- {@code sp_prepare}'s {@code @handle OUTPUT} is exactly this shape, and a real client
 * still sends real (if usually NULL) value bytes for one that have to be consumed in step with
 * everything else, same as for a plain input parameter -- see {@link RpcParam#output}.
 */
public final class RpcRequestReader {

    /** @param output true if the client marked this parameter OUTPUT/BY_REF (status flag 0x01)
     *      -- {@code sp_prepare}'s {@code @handle} is the one real shape that carries this;
     *      {@code value} is still decoded (a real client sends a real, if usually NULL, value
     *      even for an output param, and the byte stream desyncs for every param after it if
     *      those bytes aren't consumed the same way an input param's would be). */
    public record RpcParam(String name, Object value, boolean output) {
        public RpcParam(String name, Object value) {
            this(name, value, false);
        }
    }

    public record RpcRequest(int procId, String procName, List<RpcParam> params) {
    }

    private static final int TYPE_NVARCHAR = 0xE7;
    private static final int TYPE_BIGVARCHAR = 0xA7;
    private static final int TYPE_NCHAR = 0xEF;
    private static final int TYPE_BIGCHAR = 0xAF;
    private static final int TYPE_INTN = 0x26;
    private static final int TYPE_FLTN = 0x6D;
    private static final int TYPE_BITN = 0x68;
    private static final int TYPE_DECIMALN = 0x6A;
    private static final int TYPE_NUMERICN = 0x6C;
    private static final int TYPE_BIGVARBINARY = 0xA5;
    private static final int TYPE_BIGBINARY = 0xAD;
    private static final int TYPE_DATEN = 0x28;
    private static final int TYPE_DATETIME2N = 0x2A;
    private static final int TYPE_GUIDN = 0x24;

    private static final long PLP_NULL = 0xFFFFFFFFFFFFFFFFL;

    public static RpcRequest read(byte[] payload) throws IOException {
        Cursor c = new Cursor(payload);
        long totalHeadersLength = c.readU32();
        int headersEnd = (int) totalHeadersLength;
        if (headersEnd < 4 || headersEnd > payload.length) {
            headersEnd = 4; // no/malformed ALL_HEADERS block -- treat as absent, same as SqlBatchReader
        }
        c.pos = headersEnd;

        int nameLen = c.readU16();
        int procId = 0;
        String procName = null;
        if (nameLen == 0xFFFF) {
            procId = c.readU16();
        } else {
            procName = c.readUtf16(nameLen);
        }
        c.readU16(); // OptionFlags -- not needed for this scope

        List<RpcParam> params = new ArrayList<>();
        while (c.pos < payload.length) {
            params.add(readParam(c));
        }
        return new RpcRequest(procId, procName, params);
    }

    private static RpcParam readParam(Cursor c) throws IOException {
        int nameLen = c.readU8();
        String name = nameLen == 0 ? null : c.readUtf16(nameLen);
        int statusFlags = c.readU8();
        boolean output = (statusFlags & 0x01) != 0;
        int type = c.readU8();
        Object value = switch (type) {
            case TYPE_NVARCHAR, TYPE_BIGVARCHAR, TYPE_NCHAR, TYPE_BIGCHAR -> readCharValue(c, type == TYPE_NVARCHAR || type == TYPE_NCHAR);
            case TYPE_INTN -> readIntNValue(c);
            case TYPE_FLTN -> readFltNValue(c);
            case TYPE_BITN -> readBitNValue(c);
            case TYPE_DECIMALN, TYPE_NUMERICN -> readDecimalNValue(c);
            case TYPE_BIGVARBINARY, TYPE_BIGBINARY -> readBinaryValue(c);
            case TYPE_DATEN -> readDateNValue(c);
            case TYPE_DATETIME2N -> readDateTime2NValue(c);
            case TYPE_GUIDN -> readGuidNValue(c);
            default -> throw new IOException("RPC parameter \"" + name + "\" has unsupported TDS type 0x"
                    + Integer.toHexString(type) + " -- only NVARCHAR/VARCHAR/NCHAR/CHAR/INT/FLOAT/BIT/DECIMAL/"
                    + "NUMERIC/BINARY/VARBINARY/DATE/DATETIME2/GUID parameters are supported (refusing rather than "
                    + "risking a mis-decode that would desync every parameter after it -- TIME/DATETIMEOFFSET/"
                    + "legacy DATETIME are a separate, not-yet-covered gap: confirmed live that mssql-jdbc's own "
                    + "PreparedStatement.setTime() sends the LEGACY DATETIMN type 0x6f, not TIMEN, with a "
                    + "different low-precision tick encoding this class doesn't decode yet)");
        };
        return new RpcParam(name, value, output);
    }

    private static Object readCharValue(Cursor c, boolean wide) throws IOException {
        int maxLen = c.readU16();
        c.skip(5); // collation
        if (maxLen == 0xFFFF) {
            return readPlpString(c, wide);
        }
        int actualLen = c.readU16();
        if (actualLen == 0xFFFF) {
            return null;
        }
        byte[] bytes = c.readBytes(actualLen);
        return new String(bytes, wide ? StandardCharsets.UTF_16LE : StandardCharsets.ISO_8859_1);
    }

    private static String readPlpString(Cursor c, boolean wide) throws IOException {
        long plpLen = c.readU64();
        if (plpLen == PLP_NULL) {
            return null;
        }
        java.io.ByteArrayOutputStream data = new java.io.ByteArrayOutputStream();
        while (true) {
            long chunkLen = c.readU32();
            if (chunkLen == 0) {
                break;
            }
            data.writeBytes(c.readBytes((int) chunkLen));
        }
        return new String(data.toByteArray(), wide ? StandardCharsets.UTF_16LE : StandardCharsets.ISO_8859_1);
    }

    private static Object readIntNValue(Cursor c) throws IOException {
        c.readU8(); // declared max length (1/2/4/8) -- the actual length below is authoritative
        int len = c.readU8();
        return switch (len) {
            case 0 -> null;
            case 1 -> (long) c.readU8();
            case 2 -> (long) c.readS16LE();
            case 4 -> (long) c.readS32LE();
            case 8 -> c.readS64LE();
            default -> throw new IOException("INTN parameter has invalid length " + len);
        };
    }

    private static Object readFltNValue(Cursor c) throws IOException {
        c.readU8(); // declared max length (4 or 8)
        int len = c.readU8();
        return switch (len) {
            case 0 -> null;
            case 4 -> (double) Float.intBitsToFloat(c.readS32LE());
            case 8 -> Double.longBitsToDouble(c.readS64LE());
            default -> throw new IOException("FLTN parameter has invalid length " + len);
        };
    }

    /**
     * DECIMALN/NUMERICN -- confirmed live (mssql-jdbc {@code PreparedStatement.setBigDecimal},
     * captured via a temporary debug probe, since removed) rather than guessed: TYPE_INFO is
     * {@code MaxLen(1) Precision(1) Scale(1)}, then the value is {@code Length(1) Sign(1)
     * UnscaledValueLE((Length-1) bytes)} -- Sign is 1 for positive, 0 for negative, and the
     * unscaled magnitude is little-endian. {@code 12.34} arrived on the wire as TYPE_INFO
     * {@code 11 26 02} (MaxLen=17, Precision=38, Scale=2) and value {@code 03 01 D2 04} (length 3
     * = 1 sign byte + 2 data bytes, sign=positive, unscaled=0x04D2=1234, scale 2 -> 12.34).
     */
    private static Object readDecimalNValue(Cursor c) throws IOException {
        c.readU8(); // declared MaxLen -- not needed, the value's own length below is authoritative
        c.readU8(); // Precision -- not needed either; BigDecimal's own scale below round-trips fine
        int scale = c.readU8();
        int len = c.readU8();
        if (len == 0) {
            return null;
        }
        int sign = c.readU8();
        byte[] magnitude = c.readBytes(len - 1);
        // BigInteger wants big-endian, most-significant byte first; the wire is little-endian.
        byte[] bigEndian = new byte[magnitude.length];
        for (int i = 0; i < magnitude.length; i++) {
            bigEndian[i] = magnitude[magnitude.length - 1 - i];
        }
        java.math.BigInteger unscaled = new java.math.BigInteger(1, bigEndian);
        if (sign == 0) {
            unscaled = unscaled.negate();
        }
        return new java.math.BigDecimal(unscaled, scale);
    }

    /**
     * BIGVARBINARY/BIGBINARY -- confirmed live (mssql-jdbc {@code PreparedStatement.setBytes},
     * captured via a temporary debug probe, since removed): unlike the char types, TYPE_INFO has
     * no collation field at all (collation is meaningless for raw binary), and MaxLen/ActualLen
     * are both plain U16 (not the PLP/1-byte shapes some other types use for their short form).
     * An 8-byte array arrived as TYPE_INFO {@code 40 1F} (MaxLen=8000) then value {@code 08 00}
     * (ActualLen=8) followed by the 8 raw bytes, byte-for-byte identical to the bound array.
     */
    private static Object readBinaryValue(Cursor c) throws IOException {
        int maxLen = c.readU16();
        if (maxLen == 0xFFFF) {
            return readPlpBytes(c);
        }
        int actualLen = c.readU16();
        if (actualLen == 0xFFFF) {
            return null;
        }
        return c.readBytes(actualLen);
    }

    private static byte[] readPlpBytes(Cursor c) throws IOException {
        long plpLen = c.readU64();
        if (plpLen == PLP_NULL) {
            return null;
        }
        java.io.ByteArrayOutputStream data = new java.io.ByteArrayOutputStream();
        while (true) {
            long chunkLen = c.readU32();
            if (chunkLen == 0) {
                break;
            }
            data.writeBytes(c.readBytes((int) chunkLen));
        }
        return data.toByteArray();
    }

    private static final java.time.LocalDate TDS_DATE_EPOCH = java.time.LocalDate.of(1, 1, 1);

    /**
     * DATEN -- confirmed live (mssql-jdbc {@code PreparedStatement.setDate}, captured via a
     * temporary debug probe, since removed): unlike every other date/time-family type, DATEN has
     * NO TYPE_INFO byte at all (no scale, no length) -- the type byte is immediately followed by
     * the value's own Length(1)/3-byte-date shape. {@code 2026-09-04} arrived as {@code 03 16 4A
     * 0B}: length 3, then the date as a little-endian day count from {@code 0001-01-01} (proleptic
     * Gregorian) -- {@code 0x0B4A16 = 739862} days after {@code 0001-01-01} is genuinely
     * {@code 2026-09-04}, confirmed by direct calculation, not assumed from the spec alone.
     */
    private static Object readDateNValue(Cursor c) throws IOException {
        int len = c.readU8();
        if (len == 0) {
            return null;
        }
        if (len != 3) {
            throw new IOException("DATEN parameter has unexpected length " + len + " (expected 3)");
        }
        long days = c.readU8() | (c.readU8() << 8) | (c.readU8() << 16);
        return java.sql.Date.valueOf(TDS_DATE_EPOCH.plusDays(days));
    }

    /**
     * DATETIME2N -- confirmed live (mssql-jdbc {@code PreparedStatement.setTimestamp}, captured
     * via a temporary debug probe, since removed): TYPE_INFO is a single Scale byte (0-7, unlike
     * DECIMALN's 3-byte MaxLen/Precision/Scale). The value is Length(1) then a scale-dependent-
     * width little-endian TIME component (3 bytes for scale 0-2, 4 bytes for scale 3-4, 5 bytes
     * for scale 5-7 -- confirmed live at scale 7, a 5-byte time field) immediately followed by the
     * SAME 3-byte DATEN-shaped date field {@link #readDateNValue} decodes. The time component is
     * an integer count of {@code 10^-scale} second units since midnight.
     */
    private static Object readDateTime2NValue(Cursor c) throws IOException {
        int scale = c.readU8();
        int len = c.readU8();
        if (len == 0) {
            return null;
        }
        int timeByteCount = scale <= 2 ? 3 : scale <= 4 ? 4 : 5;
        if (len != timeByteCount + 3) {
            throw new IOException("DATETIME2N parameter has length " + len + " inconsistent with its own "
                    + "scale " + scale + " (expected " + (timeByteCount + 3) + ")");
        }
        long timeUnits = 0;
        for (int i = 0; i < timeByteCount; i++) {
            timeUnits |= (long) c.readU8() << (8 * i);
        }
        long days = c.readU8() | ((long) c.readU8() << 8) | ((long) c.readU8() << 16);
        long nanosPerUnit = (long) Math.pow(10, 9 - scale);
        java.time.LocalTime time = java.time.LocalTime.ofNanoOfDay(timeUnits * nanosPerUnit);
        java.time.LocalDateTime dateTime = java.time.LocalDateTime.of(TDS_DATE_EPOCH.plusDays(days), time);
        return java.sql.Timestamp.valueOf(dateTime);
    }

    /**
     * GUIDN -- confirmed live (mssql-jdbc {@code PreparedStatement.setObject(1, UUID...)},
     * captured via a temporary debug probe, since removed): TYPE_INFO is a single MaxLen byte
     * (always 16), value is Length(1, always 16) then 16 raw bytes in real .NET/SQL Server GUID
     * layout -- NOT the same byte order as {@link java.util.UUID}'s own {@code toString()}. A
     * .NET {@code Guid}'s first three fields (Data1 4 bytes, Data2 2 bytes, Data3 2 bytes) are
     * each individually little-endian on the wire, then the last 8 bytes (Data4) are sent as-is,
     * big-endian, matching {@code UUID}'s own least-significant-bits layout directly. Confirmed by
     * direct calculation against a real bound value: UUID
     * {@code 12345678-90ab-cdef-1234-567890abcdef} arrived as bytes
     * {@code 78563412 AB90 EFCD 1234567890ABCDEF} -- each of the first three groups is the
     * corresponding UUID field's bytes reversed; the last group (Data4) is untouched.
     */
    private static Object readGuidNValue(Cursor c) throws IOException {
        c.readU8(); // declared MaxLen -- always 16, the value's own length below is authoritative
        int len = c.readU8();
        if (len == 0) {
            return null;
        }
        if (len != 16) {
            throw new IOException("GUIDN parameter has unexpected length " + len + " (expected 16)");
        }
        byte[] b = c.readBytes(16);
        long mostSigBits = ((long) (b[3] & 0xFF) << 56) | ((long) (b[2] & 0xFF) << 48)
                | ((long) (b[1] & 0xFF) << 40) | ((long) (b[0] & 0xFF) << 32)
                | ((long) (b[5] & 0xFF) << 24) | ((long) (b[4] & 0xFF) << 16)
                | ((long) (b[7] & 0xFF) << 8) | (b[6] & 0xFF);
        long leastSigBits = 0;
        for (int i = 8; i < 16; i++) {
            leastSigBits = (leastSigBits << 8) | (b[i] & 0xFF);
        }
        return new java.util.UUID(mostSigBits, leastSigBits);
    }

    private static Object readBitNValue(Cursor c) throws IOException {
        c.readU8(); // declared max length (always 1)
        int len = c.readU8();
        if (len == 0) {
            return null;
        }
        return c.readU8() != 0;
    }

    private static final class Cursor {
        final byte[] data;
        int pos;

        Cursor(byte[] data) {
            this.data = data;
        }

        int readU8() throws IOException {
            require(1);
            return data[pos++] & 0xFF;
        }

        int readU16() throws IOException {
            require(2);
            int v = (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8);
            pos += 2;
            return v;
        }

        int readS16LE() throws IOException {
            return (short) readU16();
        }

        long readU32() throws IOException {
            require(4);
            long v = (data[pos] & 0xFFL) | ((data[pos + 1] & 0xFFL) << 8)
                    | ((data[pos + 2] & 0xFFL) << 16) | ((data[pos + 3] & 0xFFL) << 24);
            pos += 4;
            return v;
        }

        int readS32LE() throws IOException {
            return (int) readU32();
        }

        long readU64() throws IOException {
            require(8);
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v |= (data[pos + i] & 0xFFL) << (8 * i);
            }
            pos += 8;
            return v;
        }

        long readS64LE() throws IOException {
            return readU64();
        }

        byte[] readBytes(int n) throws IOException {
            require(n);
            byte[] out = new byte[n];
            System.arraycopy(data, pos, out, 0, n);
            pos += n;
            return out;
        }

        String readUtf16(int charCount) throws IOException {
            return new String(readBytes(charCount * 2), StandardCharsets.UTF_16LE);
        }

        void skip(int n) throws IOException {
            require(n);
            pos += n;
        }

        void require(int n) throws IOException {
            if (pos + n > data.length) {
                throw new IOException("RPC request payload truncated at offset " + pos
                        + " (needed " + n + " more byte(s), " + (data.length - pos) + " available)");
            }
        }
    }

    private RpcRequestReader() {
    }
}
