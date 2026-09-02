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
 * drivers send exactly one), and binary-format/output/BY_REF parameters.
 */
public final class RpcRequestReader {

    public record RpcParam(String name, Object value) {
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
        if ((statusFlags & 0x01) != 0) {
            throw new IOException("RPC parameter \"" + name + "\" is BY_REF (output parameter) -- "
                    + "not supported, only plain input parameters");
        }
        int type = c.readU8();
        Object value = switch (type) {
            case TYPE_NVARCHAR, TYPE_BIGVARCHAR, TYPE_NCHAR, TYPE_BIGCHAR -> readCharValue(c, type == TYPE_NVARCHAR || type == TYPE_NCHAR);
            case TYPE_INTN -> readIntNValue(c);
            case TYPE_FLTN -> readFltNValue(c);
            case TYPE_BITN -> readBitNValue(c);
            default -> throw new IOException("RPC parameter \"" + name + "\" has unsupported TDS type 0x"
                    + Integer.toHexString(type) + " -- only NVARCHAR/VARCHAR/NCHAR/CHAR/INT/FLOAT/BIT "
                    + "parameters are supported (refusing rather than risking a mis-decode that would "
                    + "desync every parameter after it)");
        };
        return new RpcParam(name, value);
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
