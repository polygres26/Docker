package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DerTest {

    @Test
    void shortAndLongFormLengthsRoundTrip() {
        byte[] small = Der.tlv(0x04, new byte[10]);
        assertEquals(2 + 10, small.length);
        assertEquals((byte) 0x04, small[0]);
        assertEquals((byte) 10, small[1]);
        byte[] big = Der.tlv(0x04, new byte[200]);
        assertEquals((byte) 0x81, big[1]);   // long form, 1 length byte
        assertEquals((byte) 200, big[2]);
        byte[] huge = Der.tlv(0x04, new byte[70000]);
        assertEquals((byte) 0x83, huge[1]);   // 3 length bytes
    }

    @Test
    void oidEncodesRfc4514StyleArcs() {
        // 2.5.4.3 (commonName): first byte = 40*2+5 = 85 = 0x55
        byte[] cn = Der.oid("2.5.4.3");
        assertEquals(0x06, cn[0] & 0xff);
        assertEquals(3, cn[1]);
        assertArrayEquals(new byte[] {0x55, 0x04, 0x03}, java.util.Arrays.copyOfRange(cn, 2, cn.length));
    }

    @Test
    void integerEncodingAvoidsAndAddsSignPaddingCorrectly() {
        byte[] i1 = Der.integer(1);
        assertArrayEquals(new byte[] {0x02, 0x01, 0x01}, i1);
        byte[] i0 = Der.integer(0);
        assertArrayEquals(new byte[] {0x02, 0x01, 0x00}, i0);
    }
}
