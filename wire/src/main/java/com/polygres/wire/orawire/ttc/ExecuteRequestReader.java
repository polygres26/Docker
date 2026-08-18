package com.polygres.wire.orawire.ttc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ExecuteRequestReader {

    public static ExecuteRequest read(TtcReader r) {
        long options = r.readUb4();
        long cursorId = r.readUb4();

        String sqlText = null;
        int sqlPointer = r.readUint8();
        long sqlLength = r.readUb4();
        boolean freshParse = sqlPointer != 0;

        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUint8();
        r.readUb4();
        long numIters = r.readUb4();
        r.readUb4();

        int bindsPointer = r.readUint8();
        long numParams = r.readUb4();

        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUint8();

        int definesPointer = r.readUint8();
        long numDefines = r.readUb4();
        if (definesPointer != 0 || numDefines != 0) {
            throw new UnsupportedOperationException("column defines not supported in narrow slice");
        }

        r.readUb4();
        r.readUint8();
        r.readUint8();
        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUb4();
        r.readUb4();

        r.readUint8();
        r.readUb4();
        
        r.readUint8();

        r.readUint8();
        r.readUb4();
        r.readUint8();
        r.readUb4();
        r.readUint8();
        
        r.readUint8();
        r.readUb4();

        if (freshParse) {
            sqlText = readSqlText(r, (int) sqlLength);
            r.readUb4();
        } else {
            r.readUb4();
        }
        r.readUb4();
        for (int i = 0; i < 4; i++) {
            r.readUb4();
        }
        r.readUb4();
        long isQueryField = r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();
        r.readUb4();

        List<BindParam> bindParams = Collections.emptyList();
        if (bindsPointer != 0 && numParams > 0) {
            bindParams = readBindParams(r, (int) numParams);
        }

        return new ExecuteRequest(cursorId, sqlText, options, numIters, bindParams);
    }

    private static List<BindParam> readBindParams(TtcReader r, int numParams) {
        int[] oraTypeNums = new int[numParams];
        for (int i = 0; i < numParams; i++) {
            oraTypeNums[i] = r.readUint8();
            r.readUint8();
            r.readUint8();
            r.readUint8();
            r.readUb4();
            r.readUb4();
            r.readUb8();
            r.readUb4();
            r.readUb2();
            r.readUb2();
            r.readUint8();
            r.readUb4();
            r.readUb4();
        }

        List<BindParam> params = readBindValueRow(r, oraTypeNums);
        return params;
    }

    public static List<BindParam> readBindValueRow(TtcReader r, int[] oraTypeNums) {
        int rowDataTag = r.readUint8();
        if (rowDataTag != TtcConstants.MSG_TYPE_ROW_DATA) {
            throw new UnsupportedOperationException(
                    "expected ROW_DATA tag for bind values, got " + rowDataTag
                            + " (multi-execution/array binds not supported)");
        }

        List<BindParam> params = new ArrayList<>(oraTypeNums.length);
        for (int oraTypeNum : oraTypeNums) {
            byte[] bytes = r.readBytesWithLength();
            Object value = decodeBindValue(oraTypeNum, bytes);
            params.add(new BindParam(oraTypeNum, value));
        }
        return params;
    }

    private static Object decodeBindValue(int oraTypeNum, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return switch (oraTypeNum) {
            case TtcConstants.ORA_TYPE_NUM_VARCHAR -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            case TtcConstants.ORA_TYPE_NUM_NUMBER -> OracleNumberCodec.decode(bytes);
            case TtcConstants.ORA_TYPE_NUM_DATE, TtcConstants.ORA_TYPE_NUM_TIMESTAMP -> OracleDateCodec.decode(bytes);
            
            case TtcConstants.ORA_TYPE_NUM_RAW -> bytes;
            default -> throw new UnsupportedOperationException("unsupported bind variable type: " + oraTypeNum);
        };
    }

    private static String readSqlText(TtcReader r, int sqlLength) {
        byte[] bytes = r.readRawOrLengthPrefixedBytes(sqlLength);
        return bytes == null ? "" : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
