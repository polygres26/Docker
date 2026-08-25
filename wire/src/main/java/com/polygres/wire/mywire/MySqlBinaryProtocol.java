package com.polygres.wire.mywire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes {@code COM_STMT_EXECUTE}'s binary bind-parameter values and encodes binary result rows
 * for {@code COM_STMT_PREPARE}/{@code COM_STMT_EXECUTE} -- MySQL's server-side prepared-statement
 * protocol, which {@link MySqlWireSessionHandler} previously rejected outright (only plain
 * {@code COM_QUERY} was handled), so {@code RouterStage.ValueShardRule} (routing by a bound
 * sharding value) could never fire for mywire clients using prepared statements at all.
 *
 * <p>Result-row encoding scope, deliberately: every result column and prepared-statement
 * parameter is declared to the client as {@code VAR_STRING} (0xfd) regardless of its real type --
 * see {@code MySqlWireSessionHandler}'s PREPARE/EXECUTE response building. This means every
 * non-null result value is written as one length-encoded string, never a native binary
 * int/double/date encoding. That's a real, disclosed trade-off (a client sees every column as a
 * string rather than its native MySQL type), chosen deliberately to avoid the sharpest risk in
 * this whole feature: per-type binary row encoding where a wrong-length write (e.g. a variable-
 * length DATETIME) silently corrupts every column after it in the same row, with no error. The
 * null-bitmap itself is still real, protocol-correct binary encoding (see {@link #encodeRow}) --
 * only the value bytes are simplified.
 *
 * <p>Bind-*parameter* decoding (the input side) has no such shortcut available -- the client
 * chooses the wire type, not this server -- so every type a real client commonly binds is decoded
 * per its real MySQL binary-protocol encoding. {@code COM_STMT_SEND_LONG_DATA} (streamed
 * BLOB/CLOB parameters) is refused rather than attempted; see {@code MySqlWireSessionHandler}.
 */
final class MySqlBinaryProtocol {

    // MySQL binary protocol type codes (subset -- see class doc for what's deliberately not here).
    static final int TYPE_NULL = 0x06;
    static final int TYPE_TINY = 0x01;
    static final int TYPE_SHORT = 0x02;
    static final int TYPE_LONG = 0x03;
    static final int TYPE_INT24 = 0x09;
    static final int TYPE_LONGLONG = 0x08;
    static final int TYPE_YEAR = 0x0d;
    static final int TYPE_FLOAT = 0x04;
    static final int TYPE_DOUBLE = 0x05;
    static final int TYPE_DATE = 0x0a;
    static final int TYPE_DATETIME = 0x0c;
    static final int TYPE_TIMESTAMP = 0x07;
    static final int TYPE_STRING = 0xfe;
    static final int TYPE_VAR_STRING = 0xfd;
    static final int TYPE_VARCHAR = 0x0f;
    static final int TYPE_BLOB = 0xfc;
    static final int TYPE_DECIMAL = 0xf6;
    static final int TYPE_NEWDECIMAL = 0x00;

    private static final java.util.Set<Integer> SUPPORTED_TYPES = java.util.Set.of(
            TYPE_NULL, TYPE_TINY, TYPE_SHORT, TYPE_LONG, TYPE_INT24, TYPE_LONGLONG, TYPE_YEAR,
            TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE, TYPE_DATETIME, TYPE_TIMESTAMP,
            TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR, TYPE_BLOB, TYPE_DECIMAL, TYPE_NEWDECIMAL);

    static boolean isSupportedType(int type) {
        return SUPPORTED_TYPES.contains(type);
    }

    /** Reads the {@code (num_params+7)/8}-byte null-bitmap that precedes
     * {@code COM_STMT_EXECUTE}'s parameter values -- plain, 0-based, NOT the +2-bit-offset shape
     * {@link #nullBitmapLength}/binary result rows use (that offset is specific to result rows,
     * reserving bits 0-1 for a header; the parameter null-bitmap has no such reservation). */
    static boolean[] readParamNullBitmap(byte[] data, int[] pos, int numParams) {
        int len = (numParams + 7) / 8;
        boolean[] isNull = new boolean[numParams];
        for (int i = 0; i < numParams; i++) {
            int b = data[pos[0] + (i / 8)] & 0xFF;
            isNull[i] = (b & (1 << (i % 8))) != 0;
        }
        pos[0] += len;
        return isNull;
    }

    /** Decodes one bind-parameter value per its real MySQL binary-protocol type encoding. Throws
     * {@link IOException} for any type outside {@link #isSupportedType} rather than guessing --
     * several of the excluded types (esp. DATE/DATETIME/TIMESTAMP variants at other precisions,
     * and DECIMAL's string-of-digits encoding) are handled here for the common case, but anything
     * this method doesn't recognize is refused, not misread. */
    static Object readValue(byte[] data, int[] pos, int type) throws IOException {
        return switch (type) {
            case TYPE_TINY -> (long) data[pos[0]++];
            case TYPE_SHORT, TYPE_YEAR -> {
                int v = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8);
                pos[0] += 2;
                yield (long) (short) v;
            }
            case TYPE_LONG, TYPE_INT24 -> {
                long v = (data[pos[0]] & 0xFFL) | ((data[pos[0] + 1] & 0xFFL) << 8)
                        | ((data[pos[0] + 2] & 0xFFL) << 16) | ((data[pos[0] + 3] & 0xFFL) << 24);
                pos[0] += 4;
                yield (long) (int) v;
            }
            case TYPE_LONGLONG -> {
                long v = 0;
                for (int i = 0; i < 8; i++) {
                    v |= (data[pos[0] + i] & 0xFFL) << (8 * i);
                }
                pos[0] += 8;
                yield v;
            }
            case TYPE_FLOAT -> {
                int bits = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8)
                        | ((data[pos[0] + 2] & 0xFF) << 16) | ((data[pos[0] + 3] & 0xFF) << 24);
                pos[0] += 4;
                yield (double) Float.intBitsToFloat(bits);
            }
            case TYPE_DOUBLE -> {
                long bits = 0;
                for (int i = 0; i < 8; i++) {
                    bits |= (data[pos[0] + i] & 0xFFL) << (8 * i);
                }
                pos[0] += 8;
                yield Double.longBitsToDouble(bits);
            }
            case TYPE_DATE, TYPE_DATETIME, TYPE_TIMESTAMP -> readDateTime(data, pos);
            case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR, TYPE_BLOB, TYPE_DECIMAL, TYPE_NEWDECIMAL ->
                    MySqlPacket.readLenEncString(data, pos);
            default -> throw new IOException("unsupported COM_STMT_EXECUTE parameter type 0x"
                    + Integer.toHexString(type) + " -- refusing rather than risking a mis-decode "
                    + "that would desync every parameter after it in the same call");
        };
    }

    /** DATE/DATETIME/TIMESTAMP share one variable-length encoding: a 1-byte length prefix (0, 4,
     * 7, or 11) determines how much follows -- year(u16)/month/day, then optionally
     * hour/minute/second, then optionally microseconds(u32). Getting this length dispatch wrong is
     * exactly the "every later parameter desyncs" risk flagged in this class's javadoc, so it's
     * handled explicitly for every length the wire actually uses rather than assumed. */
    private static String readDateTime(byte[] data, int[] pos) {
        int len = data[pos[0]++] & 0xFF;
        if (len == 0) {
            return "0000-00-00";
        }
        int year = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8);
        int month = data[pos[0] + 2] & 0xFF;
        int day = data[pos[0] + 3] & 0xFF;
        pos[0] += 4;
        if (len == 4) {
            return String.format("%04d-%02d-%02d", year, month, day);
        }
        int hour = data[pos[0]] & 0xFF;
        int minute = data[pos[0] + 1] & 0xFF;
        int second = data[pos[0] + 2] & 0xFF;
        pos[0] += 3;
        if (len == 7) {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        }
        long micros = (data[pos[0]] & 0xFFL) | ((data[pos[0] + 1] & 0xFFL) << 8)
                | ((data[pos[0] + 2] & 0xFFL) << 16) | ((data[pos[0] + 3] & 0xFFL) << 24);
        pos[0] += 4;
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, micros);
    }

    /** {@code (numColumns+9)/8} bytes, each column's presence bit at index {@code i+2} (not
     * {@code i}) -- the offset reserves the first 2 bits of the bitmap for the packet's own
     * header/OK-status per the MySQL binary protocol spec. Getting this offset wrong produces a
     * well-formed packet with every column's null-ness shifted by one, silently. */
    private static int nullBitmapLength(int numColumns) {
        return (numColumns + 7 + 2) / 8;
    }

    static byte[] encodeRow(List<Object> values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        byte[] bitmap = new byte[nullBitmapLength(values.size())];
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                int bitIndex = i + 2;
                bitmap[bitIndex / 8] |= (1 << (bitIndex % 8));
            }
        }
        out.writeBytes(bitmap);
        for (Object v : values) {
            if (v != null) {
                MySqlPacket.writeLenEncString(out, String.valueOf(v));
            }
        }
        return out.toByteArray();
    }

    /** Counts {@code ?} placeholders outside string literals (single- or double-quoted, with
     * doubled-quote escaping) -- used to size {@code COM_STMT_PREPARE_OK}'s parameter count. Not
     * paren/comment-aware beyond that; matches the regex-on-raw-SQL scope this project already
     * uses for RouterStage's schema/shard-table rules, not a full parser. */
    static int countPlaceholders(String sql) {
        int count = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inSingle = !inSingle;
            } else if (c == '"' && !inSingle) {
                if (inDouble && i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
                    i++;
                    continue;
                }
                inDouble = !inDouble;
            } else if (c == '?' && !inSingle && !inDouble) {
                count++;
            }
        }
        return count;
    }

    static List<Object> decodeExecuteParams(byte[] payload, int[] pos, int numParams,
            int[] cachedTypes) throws IOException {
        boolean[] isNull = readParamNullBitmap(payload, pos, numParams);
        int newParamsBound = payload[pos[0]++] & 0xFF;
        int[] types = cachedTypes;
        if (newParamsBound == 1) {
            types = new int[numParams];
            for (int i = 0; i < numParams; i++) {
                types[i] = payload[pos[0]] & 0xFF;
                pos[0] += 2; // type (1 byte) + unsigned flag (1 byte, ignored -- see class doc scope)
                if (!isSupportedType(types[i]) && !isNull[i]) {
                    throw new IOException("unsupported COM_STMT_EXECUTE parameter type 0x"
                            + Integer.toHexString(types[i]) + " for parameter " + i);
                }
            }
            System.arraycopy(types, 0, cachedTypes, 0, numParams);
        } else if (cachedTypes[0] == -1 && numParams > 0) {
            throw new IOException("COM_STMT_EXECUTE: new_params_bound_flag was 0 on the first "
                    + "EXECUTE for this statement -- no cached parameter types to reuse");
        }
        List<Object> values = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            if (isNull[i]) {
                values.add(null);
            } else {
                values.add(readValue(payload, pos, types[i]));
            }
        }
        return values;
    }

    private MySqlBinaryProtocol() {
    }
}
