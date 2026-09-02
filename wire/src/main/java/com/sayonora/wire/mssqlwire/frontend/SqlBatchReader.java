package com.sayonora.wire.mssqlwire.frontend;

import java.nio.charset.StandardCharsets;

public final class SqlBatchReader {

    public static String readSqlText(byte[] payload) {
        if (payload.length < 4) {
            return "";
        }
        long totalHeadersLength = readU32LE(payload, 0);
        int start = (int) totalHeadersLength;
        if (start < 4 || start > payload.length) {
            
            start = 0;
        }
        return new String(payload, start, payload.length - start, StandardCharsets.UTF_16LE);
    }

    private static long readU32LE(byte[] data, int pos) {
        return (data[pos] & 0xFFL) | ((data[pos + 1] & 0xFFL) << 8)
                | ((data[pos + 2] & 0xFFL) << 16) | ((data[pos + 3] & 0xFFL) << 24);
    }

    private SqlBatchReader() {
    }
}
