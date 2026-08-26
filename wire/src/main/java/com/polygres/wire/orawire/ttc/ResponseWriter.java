package com.polygres.wire.orawire.ttc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;

public final class ResponseWriter {

    private static final byte[] DESCRIBE_INFO_BLOB_FILLER = new byte[23];
    private static final int DESCRIBE_INFO_MAX_ROW_SIZE = 22;
    private static final int DESCRIBE_INFO_TRAILING_BYTE = 130;

    // The DESCRIBE_INFO header (msgtype through the trailing byte right before the first column)
    // a real Oracle server sends the same real dblink native-OCI client this whole fallback series
    // is for -- 7 bytes longer than the plain all-zero-filler version above (this codebase's own,
    // correct for and unchanged for JDBC/sqlplus/SQLcl), confirmed via a real Oracle 23c self-loop
    // capture. Used verbatim (its own filler content isn't understood field-by-field, only that
    // its LENGTH matters) with just the column-count byte patched -- confirmed at the same offset
    // in a second real capture with a different column count.
    private static final byte[] NATIVE_OCI_DESCRIBE_INFO_HEADER = java.util.Base64.getDecoder().decode(
        "EBcAAADAxMIZMJdKtcUM0E2zdjOGeH4IGgQbKCwAAAACAAAAgg==");
    private static final int NATIVE_OCI_DESCRIBE_INFO_HEADER_COLUMN_COUNT_OFFSET = 32;

    private static void writeDescribeInfoHeaderNativeOci(TtcWriter w, int columnCount) {
        byte[] header = NATIVE_OCI_DESCRIBE_INFO_HEADER.clone();
        header[NATIVE_OCI_DESCRIBE_INFO_HEADER_COLUMN_COUNT_OFFSET] = (byte) columnCount;
        w.writeRaw(header);
    }

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
        if (nativeOciColumnFormat) {
            writeDescribeInfoHeaderNativeOci(w, columns.size());
        } else {
            w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
            w.writeBytesWithLength(DESCRIBE_INFO_BLOB_FILLER);
            w.writeUb4(DESCRIBE_INFO_MAX_ROW_SIZE);
            w.writeUb4(columns.size());
            if (!columns.isEmpty()) {
                w.writeUint8(DESCRIBE_INFO_TRAILING_BYTE);
            }
        }
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            if (nativeOciColumnFormat && col.oraTypeNum == TtcConstants.ORA_TYPE_NUM_NUMBER) {
                writeColumnMetadataNativeOci(w, col, i);
            } else {
                writeColumnMetadata(w, col, i);
            }
        }

        // The real dblink native-OCI client needs a completely different (and much larger) tail
        // here than the plain currentDate+zeros this codebase's own JDBC/sqlplus/SQLcl-correct
        // format writes -- see RequestLoop.writeNativeOciExecuteTail's javadoc, which writes that
        // tail (and skips this codebase's own row-data/success-end writing entirely) instead, right
        // after this method returns, only for that same fallback path.
        if (nativeOciColumnFormat) {
            return;
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
    // connection's native OCI client is a fixed 56-byte prefix, then the column name written at
    // its own real length (NOT padded or truncated to a fixed width -- an earlier version of this
    // fix assumed a fixed 6-byte name field, confirmed WRONG live: a real capture of a 5-char
    // column name ("PRICE") produced a block exactly 3 bytes longer than a 2-char one ("ID"), and
    // a 3-char one ("QTY") 1 byte longer again -- then a fixed 32-byte suffix (which holds the
    // 0-based column index, always at the same offset relative to the suffix's own start,
    // regardless of the name's length). writeColumnMetadata above produces a much shorter, 31-byte
    // block instead -- correct for, and confirmed live unaffected by this method's existence,
    // JDBC/sqlplus/SQLcl. Confirmed byte-for-byte against two real Oracle 23c self-loop captures
    // (a 2-column and a 3-column SELECT, three differently-named columns total): diffing the
    // fixed-width prefix/suffix portions against each other across all three real columns isolates
    // exactly which bytes are fixed/structural versus column-specific (the name length, written
    // three times in lockstep, and the column index) -- this template encodes that structure
    // directly rather than guessing at Oracle's internal field semantics for the rest.
    private static final byte[] NATIVE_OCI_COLUMN_PREFIX = java.util.Base64.getDecoder().decode(
        "AQIAAIEWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAECAgAAAAI=");
    private static final byte[] NATIVE_OCI_COLUMN_SUFFIX = new byte[32];
    private static final int[] NATIVE_OCI_NAME_LENGTH_OFFSETS = { 50, 51, 55 };
    private static final int NATIVE_OCI_COLUMN_INDEX_SUFFIX_OFFSET = 12;

    private static void writeColumnMetadataNativeOci(TtcWriter w, ColumnMetadata col, int columnIndex) {
        byte[] prefix = NATIVE_OCI_COLUMN_PREFIX.clone();
        byte[] nameBytes = col.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int offset : NATIVE_OCI_NAME_LENGTH_OFFSETS) {
            prefix[offset] = (byte) nameBytes.length;
        }
        byte[] suffix = NATIVE_OCI_COLUMN_SUFFIX.clone();
        suffix[NATIVE_OCI_COLUMN_INDEX_SUFFIX_OFFSET] = (byte) columnIndex;
        w.writeRaw(prefix);
        w.writeRaw(nameBytes);
        w.writeRaw(suffix);
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
