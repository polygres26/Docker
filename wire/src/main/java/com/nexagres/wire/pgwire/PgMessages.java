package com.nexagres.wire.pgwire;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;

final class PgMessages {

    static final int SSL_REQUEST_CODE = 80877103;
    static final int GSSENC_REQUEST_CODE = 80877104;
    static final int PROTOCOL_VERSION_3_0 = 196608;

    static void writeAuthCleartextPassword(DataOutputStream out) throws IOException {
        out.writeByte('R');
        out.writeInt(8);
        out.writeInt(3);
        out.flush();
    }

    static void writeAuthOk(DataOutputStream out) throws IOException {
        out.writeByte('R');
        out.writeInt(8);
        out.writeInt(0);
    }

    static void writeParameterStatus(DataOutputStream out, String name, String value) throws IOException {
        byte[] nameB = cstring(name);
        byte[] valueB = cstring(value);
        out.writeByte('S');
        out.writeInt(4 + nameB.length + valueB.length);
        out.write(nameB);
        out.write(valueB);
    }

    static void writeBackendKeyData(DataOutputStream out) throws IOException {
        out.writeByte('K');
        out.writeInt(12);
        out.writeInt(0);
        out.writeInt(0);
    }

    static void writeReadyForQuery(DataOutputStream out, char status) throws IOException {
        out.writeByte('Z');
        out.writeInt(5);
        out.writeByte(status);
    }

    static void writeErrorAndReady(DataOutputStream out, String sqlState, String message) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write('S'); body.write(cstring("ERROR"));
        body.write('C'); body.write(cstring(sqlState));
        body.write('M'); body.write(cstring(message));
        body.write(0);
        out.writeByte('E');
        out.writeInt(4 + body.size());
        out.write(body.toByteArray());
        writeReadyForQuery(out, 'I');
    }

    static void writeRowDescription(DataOutputStream out, java.util.List<String> columnNames,
            java.util.List<Integer> columnJdbcTypes) throws IOException {
        int n = columnNames.size();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeShort(body, n);
        for (int i = 0; i < n; i++) {
            body.write(cstring(columnNames.get(i)));
            writeInt(body, 0);
            writeShort(body, 0);
            writeInt(body, oidFor(columnJdbcTypes.get(i)));
            writeShort(body, -1);
            writeInt(body, -1);
            writeShort(body, 0);
        }
        out.writeByte('T');
        out.writeInt(4 + body.size());
        out.write(body.toByteArray());
    }

    static void writeDataRow(DataOutputStream out, java.util.List<Object> row) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeShort(body, row.size());
        for (Object value : row) {
            if (value == null) {
                writeInt(body, -1);
            } else {
                byte[] bytes = String.valueOf(value).getBytes(StandardCharsets.UTF_8);
                writeInt(body, bytes.length);
                body.write(bytes);
            }
        }
        out.writeByte('D');
        out.writeInt(4 + body.size());
        out.write(body.toByteArray());
    }

    static void writeCommandComplete(DataOutputStream out, String tag) throws IOException {
        byte[] tagB = cstring(tag);
        out.writeByte('C');
        out.writeInt(4 + tagB.length);
        out.write(tagB);
    }

    static void writeParseComplete(DataOutputStream out) throws IOException {
        out.writeByte('1');
        out.writeInt(4);
    }

    static void writeBindComplete(DataOutputStream out) throws IOException {
        out.writeByte('2');
        out.writeInt(4);
    }

    static void writeCloseComplete(DataOutputStream out) throws IOException {
        out.writeByte('3');
        out.writeInt(4);
    }

    static void writeParameterDescription(DataOutputStream out, int paramCount) throws IOException {
        out.writeByte('t');
        out.writeInt(4 + 2 + paramCount * 4);
        writeShortDirect(out, paramCount);
    }

    static void writeNoData(DataOutputStream out) throws IOException {
        out.writeByte('n');
        out.writeInt(4);
    }

    static void writePortalSuspended(DataOutputStream out) throws IOException {
        out.writeByte('s');
        out.writeInt(4);
    }

    static void writeErrorResponse(DataOutputStream out, String sqlState, String message) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write('S'); body.write(cstring("ERROR"));
        body.write('C'); body.write(cstring(sqlState));
        body.write('M'); body.write(cstring(message));
        body.write(0);
        out.writeByte('E');
        out.writeInt(4 + body.size());
        out.write(body.toByteArray());
    }

    private static void writeShortDirect(DataOutputStream out, int v) throws IOException {
        out.writeShort(v);
    }

    private static int oidFor(int jdbcType) {
        return switch (jdbcType) {
            case Types.INTEGER, Types.SMALLINT -> 23;
            case Types.BIGINT -> 20;
            case Types.NUMERIC, Types.DECIMAL -> 1700;
            case Types.DOUBLE, Types.FLOAT, Types.REAL -> 701;
            case Types.BOOLEAN, Types.BIT -> 16;
            case Types.DATE -> 1082;
            case Types.TIMESTAMP -> 1114;
            default -> 25;
        };
    }

    private static byte[] cstring(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[b.length + 1];
        System.arraycopy(b, 0, out, 0, b.length);
        return out;
    }

    private static void writeShort(ByteArrayOutputStream out, int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeInt(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private PgMessages() {
    }
}
