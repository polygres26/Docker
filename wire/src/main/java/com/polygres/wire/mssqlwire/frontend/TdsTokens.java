package com.polygres.wire.mssqlwire.frontend;

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

    private static final int ENVCHANGE_DATABASE = 1;

    private static final int DONE_FINAL = 0x00;
    private static final int DONE_COUNT = 0x10;
    private static final int DONE_ERROR = 0x02;

    private TdsTokens() {
    }

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
        body.write(1);
        
        body.write(0x74); body.write(0x00); body.write(0x00); body.write(0x04);
        writeBVarChar(body, "PolyWire mssqlwire");
        body.write(15); body.write(0); body.write(0x07); body.write(0x00);

        out.write(TOKEN_LOGINACK);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    private static void writeEnvChangeDatabase(ByteArrayOutputStream out, String database) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ENVCHANGE_DATABASE);
        writeBVarChar(body, database);
        writeBVarChar(body, "");

        out.write(TOKEN_ENVCHANGE);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
    }

    public static byte[] errorMessage(int number, String message) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeU32LE(body, number);
        body.write(1);
        body.write(16);
        
        writeUsVarChar(body, message == null ? "backend error" : message);
        writeBVarChar(body, "polywire-mssqlwire");
        writeBVarChar(body, "");
        writeU32LE(body, 0);

        out.write(TOKEN_ERROR);
        writeU16LE(out, body.size());
        out.writeBytes(body.toByteArray());
        writeDone(out, DONE_ERROR, 0, 0);
        return out.toByteArray();
    }

    public static void writeColMetaData(ByteArrayOutputStream out, List<String> columnNames) {
        out.write(TOKEN_COLMETADATA);
        writeU16LE(out, columnNames.size());
        for (String name : columnNames) {
            writeU32LE(out, 0);
            writeU16LE(out, 0);
            out.write(0xE7);
            writeU16LE(out, 8000);
            
            out.write(0x09); out.write(0x04); out.write(0x00); out.write(0x00); out.write(0x00);
            writeBVarChar(out, name);
        }
    }

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

    public static void writeDone(ByteArrayOutputStream out, int status, int curCmd, long rowCount) {
        out.write(TOKEN_DONE);
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
