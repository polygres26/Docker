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
        w.writeUint8(TtcConstants.MSG_TYPE_DESCRIBE_INFO);
        w.writeBytesWithLength(DESCRIBE_INFO_BLOB_FILLER);
        w.writeUb4(DESCRIBE_INFO_MAX_ROW_SIZE);
        w.writeUb4(columns.size());
        if (!columns.isEmpty()) {
            w.writeUint8(DESCRIBE_INFO_TRAILING_BYTE);
        }
        for (int i = 0; i < columns.size(); i++) {
            writeColumnMetadata(w, columns.get(i), i);
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
