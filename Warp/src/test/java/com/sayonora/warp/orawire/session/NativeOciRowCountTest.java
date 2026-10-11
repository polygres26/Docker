package com.sayonora.warp.orawire.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class NativeOciRowCountTest {

    @Test
    void rowsAffectedIsWrittenInBothPlacesAsAFourByteLittleEndianNumber() {
        byte[] template = new byte[204];
        byte[] patched = RequestLoop.withRowCount(template, 70000);
        // 70000 = 0x00011170, the encoding a real Oracle used (seen at both offsets)
        assertEquals(0x70, patched[43] & 0xFF);
        assertEquals(0x11, patched[44] & 0xFF);
        assertEquals(0x01, patched[45] & 0xFF);
        assertEquals(0x00, patched[46] & 0xFF);
        assertEquals(0x70, patched[171] & 0xFF);
        assertEquals(0, template[43], "the template itself is not changed");
    }

    @Test
    void threeRowsAndZeroRows() {
        assertEquals(3, RequestLoop.withRowCount(new byte[204], 3)[43]);
        assertEquals(0, RequestLoop.withRowCount(new byte[204], 0)[171]);
    }
}
