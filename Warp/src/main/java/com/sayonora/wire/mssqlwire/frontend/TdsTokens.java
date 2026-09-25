package com.sayonora.wire.mssqlwire.frontend;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class TdsTokens {

    private static final byte TOKEN_LOGINACK = (byte) 0xAD;
    private static final byte TOKEN_ENVCHANGE = (byte) 0xE3;
    private static final byte TOKEN_ERROR = (byte) 0xAA;
    private static final byte TOKEN_COLMETADATA = (byte) 0x81;
    private static final byte TOKEN_ROW = (byte) 0xD1;
    private static final byte TOKEN_DONE = (byte) 0xFD;
    // Sent instead of plain DONE at the end of an RPC (sp_executesql) response -- some clients
    // count DONEPROC to know an RPC call specifically has finished, distinct from a DONE inside a
    // batch. Same body shape as DONE otherwise.
    private static final byte TOKEN_DONEPROC = (byte) 0xFE;
    // The response to sp_prepare's @handle OUTPUT parameter -- see writeReturnValueInt below.
    private static final byte TOKEN_RETURNVALUE = (byte) 0xAC;
    // Wraps a raw NTLM Type-2 (Challenge) blob when replying to a Windows/SSPI LOGIN7 -- confirmed
    // live (mssql-jdbc authenticationScheme=NTLM): sending the raw blob as a TABULAR_RESULT
    // packet's payload with no token wrapper at all gets rejected client-side ("unexpected token
    // unknown token (0x4E)" -- 'N', the first byte of "NTLMSSP\0" -- because the client parses the
    // packet as a token stream and expects a real token type byte first, same as every other
    // TABULAR_RESULT response this class builds).
    private static final byte TOKEN_SSPI = (byte) 0xED;

    private static final int ENVCHANGE_DATABASE = 1;

    private static final int DONE_FINAL = 0x00;
    private static final int DONE_COUNT = 0x10;
    private static final int DONE_ERROR = 0x02;
    // Per the TDS spec, a DONE token in response to an ATTENTION MUST have this bit set, or the
    // client has no way to know its cancel/reset was actually acknowledged. Found live: two
    // independent TDS client libraries (pymssql/FreeTDS, pytds) each send an ATTENTION packet
    // before reusing a connection for a second query -- entirely standard, spec-correct client
    // behavior -- and both hung forever waiting for an ack this server was never sending. Proven
    // by capturing raw socket bytes: the plain DONE_FINAL response this handler used to send
    // parses as perfectly valid TDS, just semantically wrong for this one case, so a byte-level
    // sanity check alone wouldn't have caught it -- only watching what a real client actually does
    // with it did.
    private static final int DONE_ATTN = 0x20;

    private TdsTokens() {
    }

    public static byte[] loginAck(String database) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeLoginAck(out);
        if (database != null && !database.isBlank()) {
            writeEnvChangeDatabase(out, database);
        }
        writeEnvChangeCollation(out);
        writeDone(out, DONE_FINAL, 0, 0);
        return out.toByteArray();
    }

    private static void writeLoginAck(ByteArrayOutputStream out) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(1);
        
        body.write(0x74); body.write(0x00); body.write(0x00); body.write(0x04);
        writeBVarChar(body, "Warp mssqlwire");
        body.write(15); body.write(0); body.write(0x07); body.write(0x00);

        out.write(TOKEN_LOGINACK);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    private static final int ENVCHANGE_COLLATION = 7;

    // The same 5-byte collation value writeColMetaData already sends for every column's
    // TYPE_INFO -- reusing it here (rather than picking a fresh encoding) keeps this file
    // internally consistent with a value already proven to round-trip through a real client.
    private static final byte[] DEFAULT_COLLATION = {0x09, 0x04, 0x00, 0x00, 0x00};

    private static void writeEnvChangeDatabase(ByteArrayOutputStream out, String database) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ENVCHANGE_DATABASE);
        writeBVarChar(body, database);
        writeBVarChar(body, "");

        out.write(TOKEN_ENVCHANGE);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    // Without this, a real client (mssql-jdbc confirmed live) has no server collation to encode
    // string RPC parameters with and throws a NullPointerException client-side before a single
    // byte reaches Warp -- not a decode bug in RpcRequestReader, a missing piece of the LOGIN7
    // response every client needs regardless of RPC support at all. Found via
    // MssqlJdbcIntegrationTest, a real client -- exactly the kind of gap a hand-constructed test
    // payload can't surface, since nothing about RpcRequestReader's own decoding was wrong.
    private static void writeEnvChangeCollation(ByteArrayOutputStream out) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ENVCHANGE_COLLATION);
        body.write(DEFAULT_COLLATION.length);
        body.writeBytes(DEFAULT_COLLATION);
        body.write(0); // old value length -- none, this is the initial login response

        out.write(TOKEN_ENVCHANGE);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    /** Wraps a raw NTLM Type-2 blob as a real TOKEN_SSPI -- see that constant's own javadoc. */
    public static byte[] sspiToken(byte[] blob) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(TOKEN_SSPI);
        writeU16LE(out, blob.length);
        out.writeBytes(blob);
        return out.toByteArray();
    }

    public static byte[] errorMessage(int number, String message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeU32LE(body, number);
        body.write(1);
        body.write(16);
        
        writeUsVarChar(body, message == null ? "backend error" : message);
        writeBVarChar(body, "warp-mssqlwire");
        writeBVarChar(body, "");
        writeU32LE(body, 0);

        out.write(TOKEN_ERROR);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
        writeDone(out, DONE_ERROR, 0, 0);
        return out.toByteArray();
    }

    // java.sql.Types values for the binary family -- kept as plain ints (not an import of
    // java.sql.Types) since this is the frontend/wire-encoding layer and the only thing it needs
    // from that class is these four numeric constants.
    private static final int JDBC_TYPE_BINARY = -2;
    private static final int JDBC_TYPE_VARBINARY = -3;
    private static final int JDBC_TYPE_LONGVARBINARY = -4;
    private static final int JDBC_TYPE_BLOB = 2004;
    private static final int TDS_TYPE_BIGVARBINARY = 0xA5;

    private static boolean isBinaryColumn(com.sayonora.wire.core.ColumnInfo column) {
        return switch (column.jdbcType()) {
            case JDBC_TYPE_BINARY, JDBC_TYPE_VARBINARY, JDBC_TYPE_LONGVARBINARY, JDBC_TYPE_BLOB -> true;
            default -> false;
        };
    }

    /**
     * Declares each column's real TDS type instead of blanket NVARCHAR(8000) -- found live
     * auditing this frontend for GA transparency: a BINARY/VARBINARY/BLOB column was declared
     * NVARCHAR and its value encoded via {@code String.valueOf(byte[])} (Java's default
     * {@code Object.toString()}, e.g. {@code "[B@1a2b3c4d"}), silently corrupting every binary
     * column's data end to end rather than merely mis-declaring its type. Everything else still
     * goes out as NVARCHAR(8000) text (unchanged from before) -- real SQL Server clients decode a
     * declared-NVARCHAR numeric/date value from its string form without issue, so that part of
     * the original design is kept; only the specifically corrupting binary case is fixed here.
     */
    public static void writeColMetaData(ByteArrayOutputStream out, List<com.sayonora.wire.core.ColumnInfo> columns) {
        out.write(TOKEN_COLMETADATA);
        writeU16LE(out, columns.size());
        for (com.sayonora.wire.core.ColumnInfo column : columns) {
            writeU32LE(out, 0);
            writeU16LE(out, 0);
            if (isBinaryColumn(column)) {
                out.write(TDS_TYPE_BIGVARBINARY);
                writeU16LE(out, 8000);
                // No collation field for a binary TYPE_INFO -- that's a char-type-only field.
            } else {
                out.write(0xE7);
                writeU16LE(out, 8000);
                out.write(0x09); out.write(0x04); out.write(0x00); out.write(0x00); out.write(0x00);
            }
            writeBVarChar(out, column.name());
        }
    }

    /** Encodes each value per its real column type -- a binary column's {@code byte[]} value goes
     * out as its actual raw bytes (matching the BIGVARBINARY TYPE_INFO {@link
     * #writeColMetaData(ByteArrayOutputStream, List)} declares for it above), not
     * {@code String.valueOf(byte[])}'s garbage {@code Object.toString()} form. */
    public static void writeRow(ByteArrayOutputStream out, List<Object> values, List<com.sayonora.wire.core.ColumnInfo> columns) {
        out.write(TOKEN_ROW);
        for (int i = 0; i < values.size(); i++) {
            Object v = values.get(i);
            if (v == null) {
                writeU16LE(out, 0xFFFF);
            } else if (i < columns.size() && isBinaryColumn(columns.get(i)) && v instanceof byte[] bytes) {
                writeU16LE(out, bytes.length);
                out.writeBytes(bytes);
            } else {
                byte[] chars = String.valueOf(v).getBytes(StandardCharsets.UTF_16LE);
                writeU16LE(out, chars.length);
                out.writeBytes(chars);
            }
        }
    }

    /**
     * A RETURNVALUE token carrying a single {@code int} (TDS {@code INTN}) value for an OUTPUT
     * parameter -- specifically {@code sp_prepare}'s {@code @handle OUTPUT}, per MS-TDS 2.2.7.17.
     * Sent BEFORE the final DONEPROC, same ordering real SQL Server uses. {@code ordinal} is the
     * parameter's 1-based position in the original RPC call (2 for {@code sp_prepare}'s
     * {@code @handle}, since {@code @params}/{@code @stmt} occupy 0 and 1... actually {@code
     * @handle} is param 1 -- see the caller for the exact position used).
     */
    public static void writeReturnValueInt(ByteArrayOutputStream out, int ordinal, int value) {
        out.write(TOKEN_RETURNVALUE);
        writeU16LE(out, ordinal);
        writeBVarChar(out, ""); // ParamName -- real SQL Server also sends this empty for a
                                 // positional (unnamed) parameter, matching the request shape.
        out.write(0x01); // Status: 1 = the value came from an OUTPUT parameter.
        writeU32LE(out, 0); // UserType
        writeU16LE(out, 0); // Flags
        out.write(0x26); // TYPE_INFO: INTN
        out.write(4);    // MaxLen: 4 bytes (a real int, not smallint/tinyint/bigint)
        out.write(4);    // Value's actual length: 4 (never NULL -- Warp always hands back a
                          // real handle, never fails this specific step silently)
        writeU32LE(out, value & 0xFFFFFFFFL);
    }

    public static void writeDone(ByteArrayOutputStream out, int status, int curCmd, long rowCount) {
        writeDoneToken(out, TOKEN_DONE, status, curCmd, rowCount);
    }

    public static void writeDoneProc(ByteArrayOutputStream out, int status, int curCmd, long rowCount) {
        writeDoneToken(out, TOKEN_DONEPROC, status, curCmd, rowCount);
    }

    private static void writeDoneToken(ByteArrayOutputStream out, byte token, int status, int curCmd, long rowCount) {
        out.write(token);
        writeU16LE(out, status);
        writeU16LE(out, curCmd);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((rowCount >>> (8 * i)) & 0xFF));
        }
    }

    private static final int CMD_SELECT = 193;
    private static final int CMD_INSERT = 195;
    private static final int CMD_DELETE = 196;
    private static final int CMD_UPDATE = 197;

    public static int curCmdFor(String sql) {
        String trimmed = sql.stripLeading();
        int space = 0;
        while (space < trimmed.length() && !Character.isWhitespace(trimmed.charAt(space))) {
            space++;
        }
        String keyword = trimmed.substring(0, space).toUpperCase(java.util.Locale.ROOT);
        return switch (keyword) {
            case "INSERT" -> CMD_INSERT;
            case "DELETE" -> CMD_DELETE;
            case "UPDATE" -> CMD_UPDATE;
            default -> CMD_INSERT;
        };
    }

    public static int curCmdSelect() {
        return CMD_SELECT;
    }

    public static int doneCountStatus() {
        return DONE_COUNT;
    }

    public static int doneFinalStatus() {
        return DONE_FINAL;
    }

    public static int doneAttnStatus() {
        return DONE_ATTN;
    }

    private static void writeU16LE(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void writeU32LE(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    private static void writeBVarChar(ByteArrayOutputStream out, String s) {
        byte[] chars = s.getBytes(StandardCharsets.UTF_16LE);
        out.write(s.length() & 0xFF);
        out.writeBytes(chars);
    }

    private static void writeUsVarChar(ByteArrayOutputStream out, String s) {
        byte[] chars = s.getBytes(StandardCharsets.UTF_16LE);
        writeU16LE(out, s.length());
        out.writeBytes(chars);
    }
}
