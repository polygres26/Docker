package com.polygres.wire.mssqlwire.frontend;

import java.nio.charset.StandardCharsets;

/**
 * SQL_BATCH (MS-TDS §2.2.6.7): plain-text SQL, no bind/RPC support — the TDS equivalent of the
 * "Simple Query protocol only" scope every other PolyWire frontend started with. Payload shape:
 *
 * <pre>
 * ALL_HEADERS: TotalLength(4 LE) [+ headers, each Length(4 LE) Type(2 LE) Data...]
 * SQLText:     remaining bytes, UCS-2 (UTF-16LE)
 * </pre>
 *
 * A real client (verified live against {@code mssql-jdbc}) always sends one header — a
 * Transaction Descriptor (Type 0x0002: an 8-byte transaction descriptor + a 4-byte outstanding
 * request count) — but the exact header contents aren't needed for this pass (no cross-batch
 * transaction/MARS tracking); only {@code TotalLength} is used, to find where the SQL text
 * starts.
 */
public final class SqlBatchReader {

    public static String readSqlText(byte[] payload) {
        if (payload.length < 4) {
            return "";
        }
        long totalHeadersLength = readU32LE(payload, 0);
        int start = (int) totalHeadersLength;
        if (start < 4 || start > payload.length) {
            // Malformed/unexpected ALL_HEADERS (or a client that omits it entirely, against spec)
            // -- fall back to treating the whole payload as SQL text rather than failing the batch.
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
