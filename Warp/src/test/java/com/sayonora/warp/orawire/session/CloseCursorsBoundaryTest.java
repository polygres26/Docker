package com.sayonora.warp.orawire.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/** Where the call after a close-cursors piggyback starts, including the cursor ids that look like function calls. */
class CloseCursorsBoundaryTest {

    private static byte[] hex(String s) {
        return HexFormat.of().parseHex(s.replace(" ", ""));
    }

    /** The bytes after piggyback tag, function code (0x69), sequence number and UB8 -- what skipPiggyback hands over. */
    private static byte[] afterPreamble(String payloadHex) {
        byte[] p = hex(payloadHex);
        byte[] rest = new byte[p.length - 4];
        System.arraycopy(p, 4, rest, 0, rest.length);
        return rest;
    }

    @Test
    void cursorId773IsNotMistakenForAFetchCall() {
        // captured from ojdbc11 on a connection that had run ~770 statements: close cursor 0x0305, then Execute (03 5e)
        byte[] rest = afterPreamble("11691800010101020305035e19000280210001012e01010d000004ffffffff010a047fffffff0000000000"
                + "0000000000000100000000000000000000000000000073656c65637420636f756e74282a292066726f6d207420776865726520"
                + "70726f746f203d20276f726177697265270101000000000000010100028000000000");
        int b = RequestLoop.closeCursorsBoundary(rest);
        assertEquals(0x03, rest[b] & 0xFF);
        assertEquals(0x5e, rest[b + 1] & 0xFF, "the real call is Execute (0x5e), not the Fetch (0x05) hidden in the id");
        assertEquals(6, b, "pointer, count (2 bytes), id (3 bytes) precede it");
    }

    @Test
    void anIdThatSpellsCommitIsNotMistakenForOneEither() {
        // id 0x030e = bytes 03 0e = FUNCTION tag + FUNC_COMMIT, then a real Execute
        byte[] rest = hex("01 01 01 02 03 0e 03 5e 19 00");
        assertEquals(6, RequestLoop.closeCursorsBoundary(rest));
    }

    @Test
    void severalIdsAreAllSkipped() {
        // pointer, count 3, ids 4, 0x0305 and 7, then Execute
        byte[] rest = hex("01 01 03 01 04 02 03 05 01 07 03 5e 19 00");
        assertEquals(10, RequestLoop.closeCursorsBoundary(rest));
    }

    @Test
    void aStandaloneCloseCursorsCallEndsWithThePacket() {
        byte[] rest = hex("01 01 01 01 02");
        assertEquals(rest.length, RequestLoop.closeCursorsBoundary(rest));
    }

    @Test
    void aBundledShapeTheExactParseDoesNotModelStillFallsBackToTheScan() {
        // exact parse stops on marker bytes (00 00 00 00 00), not a boundary; the scan then finds the chained Commit
        byte[] rest = hex("01 01 01 01 02 00 00 00 00 00 03 0e 19 00");
        assertEquals(10, RequestLoop.closeCursorsBoundary(rest));
    }
}
