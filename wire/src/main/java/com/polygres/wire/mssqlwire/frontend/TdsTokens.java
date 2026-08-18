package com.polygres.wire.mssqlwire.frontend;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builders for the TDS response token stream (MS-TDS §2.2.7) this pass emits: LOGINACK,
 * ENVCHANGE, ERROR, COLMETADATA, ROW, and DONE. All multi-byte fields are little-endian (the TDS
 * packet header itself is the one big-endian exception — see {@link
 * com.polygres.wire.mssqlwire.wireformat.TdsPacket}).
 *
 * <p>Every result column is rendered as {@code NVARCHAR} via each JDBC value's {@code
 * String.valueOf} — the same "text protocol only" simplification {@code mywire}'s {@code
 * MySqlMessages#textRow} makes, appropriate for this pass's "basic connectivity" scope (typed
 * TDS columns — INTN/DECIMALN/DATETIME2 etc. — are follow-up work, not attempted here).
 */
public final class TdsTokens {

    private static final byte TOKEN_LOGINACK = (byte) 0xAD;
    private static final byte TOKEN_ENVCHANGE = (byte) 0xE3;
    private static final byte TOKEN_ERROR = (byte) 0xAA;
    private static final byte TOKEN_COLMETADATA = (byte) 0x81;
    private static final byte TOKEN_ROW = (byte) 0xD1;
    private static final byte TOKEN_DONE = (byte) 0xFD;

    private static final int ENVCHANGE_DATABASE = 1;

    // DONE token Status flags (MS-TDS 2.2.7.6)
    private static final int DONE_FINAL = 0x00;
    private static final int DONE_COUNT = 0x10;
    private static final int DONE_ERROR = 0x02;

    private TdsTokens() {
    }

    /** LOGINACK + ENVCHANGE(database) + DONE, the standard successful-login response body. */
    public static byte[] loginAck(String database) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeLoginAck(out);
        if (database != null && !database.isBlank()) {
            writeEnvChangeDatabase(out, database);
        }
        writeDone(out, DONE_FINAL, 0, 0);
        return out.toByteArray();
    }

    private static void writeLoginAck(ByteArrayOutputStream out) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(1); // Interface: SQL_TDS
        // TDSVersion: 0x74000004 == TDS 7.4, big-endian-looking constant per spec (written MSB-first here)
        body.write(0x74); body.write(0x00); body.write(0x00); body.write(0x04);
        writeBVarChar(body, "PolyWire mssqlwire"); // ProgName
        body.write(15); body.write(0); body.write(0x07); body.write(0x00); // MajorVer.MinorVer.Build (arbitrary, well-formed)

        out.write(TOKEN_LOGINACK);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    private static void writeEnvChangeDatabase(ByteArrayOutputStream out, String database) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ENVCHANGE_DATABASE);
        writeBVarChar(body, database); // new value
        writeBVarChar(body, "");       // old value

        out.write(TOKEN_ENVCHANGE);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    /** ERROR token + DONE(error) — sent instead of a query's normal result on a backend SQLException. */
    public static byte[] errorMessage(int number, String message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeU32LE(body, number);
        body.write(1);  // State
        body.write(16); // Class/severity (16 = generic user-correctable error)
        // MsgText is US_VARCHAR (2-byte char-count prefix) but ServerName/ProcName are both
        // B_VARCHAR (1-byte prefix) -- found live: using US_VARCHAR for all three (an easy typo
        // given LOGINACK/ENVCHANGE/COLMETADATA all use B_VARCHAR uniformly) put every following
        // byte 3 positions off, which mssql-jdbc's TDSParser then read as a bogus token type.
        writeUsVarChar(body, message == null ? "backend error" : message);
        writeBVarChar(body, "polywire-mssqlwire"); // ServerName
        writeBVarChar(body, "");                   // ProcName
        writeU32LE(body, 0); // LineNumber

        out.write(TOKEN_ERROR);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
        writeDone(out, DONE_ERROR, 0, 0);
        return out.toByteArray();
    }

    /** COLMETADATA token: column count + one TYPE_INFO/name entry per column, all typed NVARCHAR(4000). */
    public static void writeColMetaData(ByteArrayOutputStream out, List<String> columnNames) {
        out.write(TOKEN_COLMETADATA);
        writeU16LE(out, columnNames.size());
        for (String name : columnNames) {
            writeU32LE(out, 0);      // UserType
            writeU16LE(out, 0);      // Flags
            out.write(0xE7);         // TYPE_INFO: NVARCHARTYPE
            writeU16LE(out, 8000);   // MaxLength in bytes (4000 chars UCS-2)
            // Collation (5 bytes): LCID 0x0409 (en-US) LE + flags(0) + sortId(0) --
            // SQL_Latin1_General_CP1_CI_AS. Found live: an all-zero collation makes mssql-jdbc
            // fail with "Windows collation 0 is not supported by this driver" (LCID 0 isn't a
            // real locale) even though the byte count/shape was otherwise correct.
            out.write(0x09); out.write(0x04); out.write(0x00); out.write(0x00); out.write(0x00);
            writeBVarChar(out, name);
        }
    }

    /** ROW token: one length-prefixed UCS-2 value per column (0xFFFF length == SQL NULL). */
    public static void writeRow(ByteArrayOutputStream out, List<Object> values) {
        out.write(TOKEN_ROW);
        for (Object v : values) {
            if (v == null) {
                writeU16LE(out, 0xFFFF);
            } else {
                byte[] chars = String.valueOf(v).getBytes(StandardCharsets.UTF_16LE);
                writeU16LE(out, chars.length);
                out.writeBytes(chars);
            }
        }
    }

    /** DONE token: Status(2) CurCmd(2) DoneRowCount(8), all little-endian. */
    public static void writeDone(ByteArrayOutputStream out, int status, int curCmd, long rowCount) {
        out.write(TOKEN_DONE);
        writeU16LE(out, status);
        writeU16LE(out, curCmd);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((rowCount >>> (8 * i)) & 0xFF));
        }
    }

    // DONE token CurCmd values a real server sends (MS-TDS §2.2.7.6 lists these as an opaque
    // "current command" the client may ignore -- in practice mssql-jdbc's StreamDone.
    // getUpdateCount() (found live via bytecode inspection of StreamDone.class, no source
    // available) hard-asserts CurCmd is one of exactly this whitelist before it will read
    // DoneRowCount at all; anything else -- including the all-zero value this pass originally
    // sent -- makes a real, successful row-count come back as a silent -1 from executeUpdate(),
    // even with DONE_COUNT correctly set. SELECT's own CurCmd (193) isn't in that whitelist
    // either, but SELECT doesn't call getUpdateCount() so it doesn't matter there.
    private static final int CMD_SELECT = 193;
    private static final int CMD_INSERT = 195;
    private static final int CMD_DELETE = 196;
    private static final int CMD_UPDATE = 197;

    /** Best-effort CurCmd for a DONE token following a non-query statement's result, sniffed from the leading keyword. */
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
            default -> CMD_INSERT; // any other whitelisted value works equally well as a safe default
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

    // ---- primitive writers --------------------------------------------------

    private static void writeU16LE(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void writeU32LE(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** B_VARCHAR: 1-byte character count + UCS-2 data (used by LOGINACK/ENVCHANGE/COLMETADATA names). */
    private static void writeBVarChar(ByteArrayOutputStream out, String s) {
        byte[] chars = s.getBytes(StandardCharsets.UTF_16LE);
        out.write(s.length() & 0xFF);
        out.writeBytes(chars);
    }

    /** US_VARCHAR: 2-byte LE character count + UCS-2 data (used by ERROR's message/server/proc fields). */
    private static void writeUsVarChar(ByteArrayOutputStream out, String s) {
        byte[] chars = s.getBytes(StandardCharsets.UTF_16LE);
        writeU16LE(out, s.length());
        out.writeBytes(chars);
    }
}
