package com.polygres.wire.orawire.ttc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

public final class ResponseWriter {

    private static final byte[] DESCRIBE_INFO_BLOB_FILLER = new byte[23];
    private static final int DESCRIBE_INFO_MAX_ROW_SIZE = 22;
    private static final int DESCRIBE_INFO_TRAILING_BYTE = 130;

    public static void writeDescribeInfo(TtcWriter w, List<ColumnMetadata> columns) {
        writeDescribeInfo(w, columns, false);
    }

    /**
     * @param nativeOciColumnFormat Use {@link #writeColumnMetadataNativeOci} instead of the normal
     * (correct for, and unchanged for, JDBC/sqlplus/SQLcl) {@link #writeColumnMetadata} -- see that
     * method's javadoc. Only {@code RequestLoop}'s native-OCI fallback path should ever pass
     * {@code true} here; every other caller uses the default, two-arg overload.
     */
    public static void writeDescribeInfo(TtcWriter w, List<ColumnMetadata> columns,
            boolean nativeOciColumnFormat) {
        w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
        w.writeBytesWithLength(DESCRIBE_INFO_BLOB_FILLER);
        w.writeUb4(DESCRIBE_INFO_MAX_ROW_SIZE);
        w.writeUb4(columns.size());
        if (!columns.isEmpty()) {
            w.writeUint8(DESCRIBE_INFO_TRAILING_BYTE);
        }
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            if (nativeOciColumnFormat && col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER) {
                writeColumnMetadataNativeOci(w, col, i);
            } else {
                writeColumnMetadata(w, col, i);
            }
        }

        byte[] currentDate = OracleDateCodec.encode(java.time.LocalDateTime.now());
        w.writeUb4(currentDate.length);
        w.writeBytesWithLength(currentDate);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeBytesWithLength(new byte[0]);
    }

    private static void writeColumnMetadata(TtcWriter w, ColumnMetadata col, int columnIndex) {
        w.writeUint8(col.oraTypeNum);
        w.writeUint8(0);
        
        int wireScale = col.scale > 0 ? col.scale - 1 : col.scale;
        w.writeSb1(col.precision);
        w.writeSb1(wireScale);
        long wireBufferSize = col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER && col.scale > 0
                ? 278 : col.bufferSize;
        w.writeUb4(wireBufferSize);
        w.writeUb4(0);
        w.writeUb8(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb2(0);
        
        w.writeUint8(col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_VARCHAR ? 1 : 0);
        
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUint8(col.nullsAllowed ? 1 : 0);
        
        w.writeUint8(col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        w.writeStrWithTwoLengths(col.name);
        w.writeStrWithTwoLengths(null);
        w.writeStrWithTwoLengths(null);
        
        w.writeUb2(columnIndex);
        w.writeUb4(0);
        
        w.writeStrWithTwoLengths(null);
        w.writeStrWithTwoLengths(null);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(0);
    }

    // A real Oracle server's per-column DESCRIBE_INFO block for a real distributed-database-link
    // connection's native OCI client is 90 bytes for a NUMBER column, not the 31 bytes
    // writeColumnMetadata above produces (that method's own format is correct for, and confirmed
    // live unaffected by this method's existence, JDBC/sqlplus/SQLcl). Confirmed byte-for-byte
    // against a real Oracle 23c self-loop capture: diffing that capture's own two NUMBER-column
    // blocks (for differently-named columns) against each other isolates exactly which of the 90
    // bytes are fixed/structural (80 of them, identical in both) versus column-specific (name
    // length written twice, a 7-byte fixed-width zero-padded name field, and the 0-based column
    // index) -- this template encodes that structure directly rather than guessing at Oracle's
    // internal field semantics for the other 80 bytes. Column names longer than 7 bytes are
    // truncated (not encoded elsewhere in this real capture's 90-byte block) -- untested beyond
    // that since no real capture with a longer name was available; a byte-accurate fix for that
    // case needs one.
    private static final byte[] NATIVE_OCI_NUMBER_COLUMN_TEMPLATE = java.util.Base64.getDecoder().decode(
        "AQIAAIEWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAECAgAAAAJJRAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    // The column-name length appears three separate times (all three confirmed to move together,
    // in lockstep, between the two real per-column blocks this template was diffed from).
    private static final int[] NATIVE_OCI_NAME_LENGTH_OFFSETS = { 50, 51, 55 };
    private static final int NATIVE_OCI_NAME_FIELD_OFFSET = 56;
    private static final int NATIVE_OCI_NAME_FIELD_WIDTH = 6;
    private static final int NATIVE_OCI_COLUMN_INDEX_OFFSET = 70;

    private static void writeColumnMetadataNativeOci(TtcWriter w, ColumnMetadata col, int columnIndex) {
        byte[] block = NATIVE_OCI_NUMBER_COLUMN_TEMPLATE.clone();
        byte[] nameBytes = col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int nameLen = Math.min(nameBytes.length, NATIVE_OCI_NAME_FIELD_WIDTH);
        for (int offset : NATIVE_OCI_NAME_LENGTH_OFFSETS) {
            block[offset] = (byte) nameLen;
        }
        java.util.Arrays.fill(block, NATIVE_OCI_NAME_FIELD_OFFSET,
                NATIVE_OCI_NAME_FIELD_OFFSET + NATIVE_OCI_NAME_FIELD_WIDTH, (byte) 0);
        System.arraycopy(nameBytes, 0, block, NATIVE_OCI_NAME_FIELD_OFFSET, nameLen);
        block[NATIVE_OCI_COLUMN_INDEX_OFFSET] = (byte) columnIndex;
        w.writeRaw(block);
    }

    public static void writeRow(TtcWriter w, List<ColumnMetadata> columns, Object[] values) {
        w.writeUint8(TtcConstants.MSG_TYPE_ROW_DATA);
        for (int i = 0; i < columns.size(); i++) {
            writeColumnValue(w, columns.get(i), values[i]);
        }
    }

    private static void writeColumnValue(TtcWriter w, ColumnMetadata col, Object value) {
        if (value == null) {
            w.writeUint8(0);
            return;
        }
        switch (col.oraTypeNum) {
            case TtcConstants.ORA_TYPE_NUM_VARCHAR -> w.writeStrWithLength(value.toString());
            case TtcConstants.ORA_TYPE_NUM_NUMBER -> {
                BigDecimal bd = value instanceof BigDecimal b ? b : new BigDecimal(value.toString());
                w.writeBytesWithLength(OracleNumberCodec.encode(bd));
            }
            case TtcConstants.ORA_TYPE_NUM_DATE -> {
                
                LocalDateTime dt;
                if (value instanceof LocalDateTime d) {
                    dt = d;
                } else if (value instanceof java.sql.Timestamp ts) {
                    dt = ts.toLocalDateTime();
                } else if (value instanceof java.sql.Date d) {
                    dt = d.toLocalDate().atStartOfDay();
                } else {
                    throw new IllegalArgumentException("unsupported DATE value type: " + value.getClass());
                }
                w.writeBytesWithLength(OracleDateCodec.encode(dt));
            }
            default -> throw new UnsupportedOperationException("unsupported column type: " + col.oraTypeNum);
        }
    }

    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId) {
        writeSuccessEnd(w, rowcount, cursorId, 0);
    }

    private static final byte[] O5LOGON_TERMINATOR_TAIL = HexFormat.of().parseHex(
            "0401010204df000000000000000000000000000000000000000100000000000000000000");
    
    private static final int O5LOGON_TERMINATOR_CALLNUMBER_OFFSET = 27;

    public static void writeO5LogonSuccessEnd(TtcWriter w, int cursorId, int callNumber) {
        byte[] tail = O5LOGON_TERMINATOR_TAIL.clone();
        tail[O5LOGON_TERMINATOR_CALLNUMBER_OFFSET] = (byte) callNumber;
        w.writeRaw(tail);
    }

    public static void writeSuccessEnd(TtcWriter w, long rowcount, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb2(0);
        w.writeUb2(0);
        w.writeUb2(cursorId);
        w.writeSb1(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        writeZeroRowid(w);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb8(rowcount);
        
        w.writeUb4(0);
        w.writeUb4(0);
    }

    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId) {
        writeErrorEnd(w, errorNum, message, cursorId, 0);
    }

    public static void writeErrorEnd(TtcWriter w, int errorNum, String message, int cursorId, int callNumber) {
        w.writeUint8(TtcConstants.MSG_TYPE_ERROR);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(errorNum);
        w.writeUb2(0);
        w.writeUb2(0);
        w.writeUb2(cursorId);
        w.writeSb1(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        writeZeroRowid(w);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(callNumber);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeBytesWithLength(null);
        w.writeUb2(0);
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUb4(errorNum);
        w.writeUb8(0);
        
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeStrWithLength(message);
    }

    private static void writeZeroRowid(TtcWriter w) {
        w.writeUb4(0);
        w.writeUb2(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUb2(0);
    }

    private ResponseWriter() {
    }
}
