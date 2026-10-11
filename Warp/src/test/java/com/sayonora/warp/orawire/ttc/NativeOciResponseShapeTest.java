package com.sayonora.warp.orawire.ttc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Shapes of the native-OCI responses that were wrong against a real SQL*Plus 23ai, each compared with bytes captured from a real Oracle. */
class NativeOciResponseShapeTest {

    private static final ColumnMetadata NUM = new ColumnMetadata("A", TtcConstants.ORA_TYPE_NUM_NUMBER, 0, 0, 22, true);
    private static final ColumnMetadata STR = new ColumnMetadata("B", TtcConstants.ORA_TYPE_NUM_VARCHAR, 0, 0, 10, true);
    private static final ColumnMetadata DATE = new ColumnMetadata("D", TtcConstants.ORA_TYPE_NUM_DATE, 0, 0, 7, true);

    private static byte[] lastRow(List<ColumnMetadata> cols, Object... values) {
        TtcWriter w = new TtcWriter();
        ResponseWriter.writeFetchLastRowResponseNativeOci(w, cols, values);
        return w.toByteArray();
    }

    private static int indexOfRowMarker(byte[] b) {
        // the row follows the fixed 50-byte prefix; MSG_TYPE_ROW_DATA is 7
        return 50;
    }

    @Test
    void theRowPrefixLengthFollowsTheRowLikeARealOraclesDid() {
        // real Oracle: (2, 'y') -> prefix length byte 8, (2, 'two') -> 10
        byte[] y = lastRow(List.of(NUM, STR), 2, "y");
        byte[] two = lastRow(List.of(NUM, STR), 2, "two");
        assertEquals(8, y[indexOfRowMarker(y) + 1]);
        assertEquals(10, two[indexOfRowMarker(two) + 1]);
    }

    @Test
    void aSingleColumnLastRowStartsLikeARealOraclesSingleColumnResponse() {
        byte[] b = lastRow(List.of(NUM), 2);
        assertEquals(0x06, b[0]);
        assertEquals(0x01, b[1]);
        assertEquals(0x02, b[2]);
        assertEquals((byte) 0x94, b[3]);
    }

    @Test
    void aDateColumnIsDescribedAsTypeTwelve() {
        TtcWriter w = new TtcWriter();
        ResponseWriter.writeDescribeInfo(w, List.of(DATE), true);
        byte[] b = w.toByteArray();
        // the column block starts after the fixed describe header; type is its second byte
        int typeIndex = -1;
        for (int i = 0; i < b.length - 4; i++) {
            if (b[i] == 1 && b[i + 1] == 12 && b[i + 2] == 0 && b[i + 3] == 0 && b[i + 4] == 0 && b[i + 5] == 1) {
                typeIndex = i;
                break;
            }
        }
        assertEquals(true, typeIndex >= 0, "a DATE column block (01 0c 00 00 00 01) is present");
    }
}
