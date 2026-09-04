package com.sayonora.wire.pgwire;

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
        writeRowDescription(out, columnNames, columnJdbcTypes, null);
    }

    /** @param binaryColumns per-column "send this one in binary" decision -- {@code null} or a
     *      shorter array means text (format code 0) for the columns it doesn't cover, same
     *      "unspecified defaults to text" rule the client's own Bind-message result-format array
     *      uses. Must already have folded in {@link PgBinaryResultEncoder#supports} (a column
     *      whose OID has no real binary encoder stays text here even if the client asked for
     *      binary) -- this method trusts the array as the final decision, it doesn't re-check. */
    static void writeRowDescription(DataOutputStream out, java.util.List<String> columnNames,
            java.util.List<Integer> columnJdbcTypes, boolean[] binaryColumns) throws IOException {
        int n = columnNames.size();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeShort(body, n);
        for (int i = 0; i < n; i++) {
            body.write(cstring(columnNames.get(i)));
            writeInt(body, 0);
            writeShort(body, 0);
            writeInt(body, oidForJdbcType(columnJdbcTypes.get(i)));
            writeShort(body, -1);
            writeInt(body, -1);
            writeShort(body, binaryColumns != null && i < binaryColumns.length && binaryColumns[i] ? 1 : 0);
        }
        out.writeByte('T');
        out.writeInt(4 + body.size());
        out.write(body.toByteArray());
    }

    static void writeDataRow(DataOutputStream out, java.util.List<Object> row) throws IOException {
        writeDataRow(out, row, null, null);
    }

    /** @param columnJdbcTypes required (non-null) whenever {@code binaryColumns} names ANY
     *      binary column -- used to resolve that column's real Postgres OID for
     *      {@link PgBinaryResultEncoder#encode}. See {@link #writeRowDescription}'s own javadoc
     *      on {@code binaryColumns}' semantics -- this must be called with the EXACT SAME array
     *      that produced that row's own RowDescription, or the two messages disagree about which
     *      columns are binary. */
    static void writeDataRow(DataOutputStream out, java.util.List<Object> row,
            java.util.List<Integer> columnJdbcTypes, boolean[] binaryColumns) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeShort(body, row.size());
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            if (value == null) {
                writeInt(body, -1);
                continue;
            }
            boolean binary = binaryColumns != null && i < binaryColumns.length && binaryColumns[i];
            byte[] bytes = binary
                    ? PgBinaryResultEncoder.encode(oidForJdbcType(columnJdbcTypes.get(i)), value)
                    : textFormat(value).getBytes(StandardCharsets.UTF_8);
            writeInt(body, bytes.length);
            body.write(bytes);
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

    /** Real Postgres's own canonical TEXT-format representation, not Java's -- a genuine, live
     * bug found fixing binary-format results: {@code Boolean.toString()} gives {@code "true"}/
     * {@code "false"}, but a real Postgres server always sends a boolean as a single character,
     * {@code "t"}/{@code "f"}, in text format, and pgjdbc's own text-boolean parser only
     * recognizes that canonical form -- it silently treated {@code "true"} as false rather than
     * throwing, which is what made this so easy to miss without a real end-to-end round trip. */
    private static String textFormat(Object value) {
        if (value instanceof Boolean b) {
            return b ? "t" : "f";
        }
        return String.valueOf(value);
    }

    static int oidForJdbcType(int jdbcType) {
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
