package com.sayonora.wire.mssqlwire.frontend;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes a real BCP (Bulk Copy) data packet -- the {@code TdsPacketType.BULK_LOAD_BCP} payload a
 * client sends after an {@code INSERT BULK} statement. Confirmed live (a real SQLServerBulkCopy
 * writing into a table whose destination-column metadata this server itself described, via the
 * FMTONLY probe {@code MssqlWireSessionHandler} answers): the payload reuses EXACTLY the same
 * token shapes {@link TdsTokens} already writes for an ordinary query result going the other way
 * (server -> client) -- a COLMETADATA token (0x81), then one ROW token (0xD1) per row with
 * length-prefixed values, terminated by a DONE token (0xFD) whose own body is not otherwise
 * interpreted here (its presence is only used to know where the row data ends).
 *
 * <p>Column TYPE_INFO decoding is deliberately narrow: only NVARCHAR (0xE7, MaxLen + a 5-byte
 * collation) and BIGVARBINARY (0xA5, MaxLen only) are recognized, refusing anything else. That's
 * not a shortcut -- {@link TdsTokens#writeColMetaData} itself only ever declares one of those two
 * TYPE_INFO shapes for any column (see its own javadoc), and a BCP client's declared destination
 * column types always come from whatever THIS server described in the preceding FMTONLY probe, so
 * this decoder's input space is fully bounded by its own encoder's output -- there is no other
 * TYPE_INFO shape a real client sending data back here can produce.
 */
public final class BcpDataReader {

    private static final int TOKEN_COLMETADATA = 0x81;
    private static final int TOKEN_ROW = 0xD1;
    private static final int TOKEN_DONE = 0xFD;
    private static final int TYPE_NVARCHAR = 0xE7;
    private static final int TYPE_BIGVARBINARY = 0xA5;

    public record BcpColumn(String name, boolean binary) {
    }

    public record BcpResult(List<BcpColumn> columns, List<List<Object>> rows) {
    }

    public static BcpResult read(byte[] payload) throws IOException {
        int[] pos = {0};
        int first = readU8(payload, pos);
        if (first != TOKEN_COLMETADATA) {
            throw new IOException("BCP data packet must start with a COLMETADATA token (0x81), got 0x"
                    + Integer.toHexString(first));
        }
        int numCols = readU16LE(payload, pos);
        List<BcpColumn> columns = new ArrayList<>(numCols);
        for (int i = 0; i < numCols; i++) {
            pos[0] += 4; // UserType -- not used, same as TdsTokens' own writer never sets it
            pos[0] += 2; // Flags -- not used
            int typeCode = readU8(payload, pos);
            boolean binary;
            if (typeCode == TYPE_BIGVARBINARY) {
                pos[0] += 2; // MaxLen
                binary = true;
            } else if (typeCode == TYPE_NVARCHAR) {
                pos[0] += 2; // MaxLen
                pos[0] += 5; // collation
                binary = false;
            } else {
                throw new IOException("unsupported BCP column TYPE_INFO 0x" + Integer.toHexString(typeCode)
                        + " for column " + i + " -- refusing rather than risking a mis-decode");
            }
            String name = readBVarChar(payload, pos);
            columns.add(new BcpColumn(name, binary));
        }

        List<List<Object>> rows = new ArrayList<>();
        while (pos[0] < payload.length) {
            int token = payload[pos[0]] & 0xFF;
            if (token == TOKEN_DONE) {
                break; // row data ends here -- DONE's own body isn't otherwise interpreted
            }
            if (token != TOKEN_ROW) {
                throw new IOException("expected a ROW token (0xD1) or DONE (0xFD) in BCP row data, got 0x"
                        + Integer.toHexString(token));
            }
            pos[0]++;
            List<Object> row = new ArrayList<>(columns.size());
            for (BcpColumn column : columns) {
                int len = readU16LE(payload, pos);
                if (len == 0xFFFF) {
                    row.add(null);
                    continue;
                }
                byte[] valueBytes = java.util.Arrays.copyOfRange(payload, pos[0], pos[0] + len);
                pos[0] += len;
                row.add(column.binary() ? valueBytes : new String(valueBytes, StandardCharsets.UTF_16LE));
            }
            rows.add(row);
        }
        return new BcpResult(columns, rows);
    }

    private static int readU8(byte[] data, int[] pos) {
        return data[pos[0]++] & 0xFF;
    }

    private static int readU16LE(byte[] data, int[] pos) {
        int v = (data[pos[0]] & 0xFF) | ((data[pos[0] + 1] & 0xFF) << 8);
        pos[0] += 2;
        return v;
    }

    private static String readBVarChar(byte[] data, int[] pos) {
        int charLen = readU8(data, pos);
        int byteLen = charLen * 2;
        String s = new String(data, pos[0], byteLen, StandardCharsets.UTF_16LE);
        pos[0] += byteLen;
        return s;
    }

    private BcpDataReader() {
    }
}
