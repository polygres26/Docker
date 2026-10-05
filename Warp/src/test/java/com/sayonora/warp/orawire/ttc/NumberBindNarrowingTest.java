package com.sayonora.warp.orawire.ttc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A whole-number NUMBER bind must reach the backend as a Long so a bigint key lookup can use its index. */
class NumberBindNarrowingTest {

    private static Object bind(BigDecimal oracleNumber) {
        byte[] n = OracleNumberCodec.encode(oracleNumber);
        byte[] wire = new byte[n.length + 2];
        wire[0] = (byte) TtcConstants.MSG_TYPE_ROW_DATA;
        wire[1] = (byte) n.length;
        System.arraycopy(n, 0, wire, 2, n.length);
        List<BindParam> row = ExecuteRequestReader.readBindValueRow(new TtcReader(wire),
                new int[] {TtcConstants.ORA_TYPE_NUM_NUMBER});
        return row.get(0).value;
    }

    @Test
    void wholeNumbersBecomeLongs() {
        assertEquals(150_000L, assertInstanceOf(Long.class, bind(new BigDecimal("150000"))));
        assertEquals(0L, assertInstanceOf(Long.class, bind(BigDecimal.ZERO)));
        assertEquals(-7L, assertInstanceOf(Long.class, bind(new BigDecimal("-7"))));
        assertEquals(130_000_005L, assertInstanceOf(Long.class, bind(new BigDecimal("130000005"))));
        assertEquals(1_000L, assertInstanceOf(Long.class, bind(new BigDecimal("1E+3"))));
    }

    @Test
    void fractionsAndHugeNumbersStayBigDecimal() {
        // compareTo: the codec may return the same value at a different scale (1.50)
        assertEquals(0, new BigDecimal("1.5").compareTo(assertInstanceOf(BigDecimal.class, bind(new BigDecimal("1.5")))));
        assertEquals(0, new BigDecimal("-0.25").compareTo(assertInstanceOf(BigDecimal.class, bind(new BigDecimal("-0.25")))));
        assertInstanceOf(BigDecimal.class, bind(new BigDecimal("123456789012345678901234")));
    }
}
