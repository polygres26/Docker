package com.polygres.wire.orawire.ttc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Regression coverage for TWO real, opposite-direction bugs found live at this exact field,
 * against two genuine Oracle clients:
 *
 * <ul>
 *   <li>A real SQLcl client sends NO redundant length-prefix byte before SQL text. The original
 *       reader ({@code TtcReader#readRawOrLengthPrefixedBytes}) guessed one was present whenever
 *       the *next* byte's value equaled the already-known {@code sqlLength} -- for a 67-byte
 *       {@code "CREATE TABLE ..."} statement, {@code 'C'} (67) coincidentally collided with the
 *       length itself, the guess fired wrongly, and the real leading {@code 'C'} got skipped,
 *       corrupting the text and misaligning every field parsed after it.
 *   <li>A real {@code python-oracledb} (thin-mode) client DOES send a genuine redundant
 *       length-prefix byte before SQL text -- confirmed live via byte-level tracing: the byte
 *       immediately before a real {@code "SELECT ..."} statement was {@code 0x26}, exactly
 *       {@code sqlLength}, a real echo of the length just parsed, not a first-character
 *       coincidence. The first fix for the SQLcl bug above (readRawBytes, no skip, ever) broke
 *       this client instead -- the marker byte got read as the SQL text's own first byte.
 * </ul>
 *
 * <p>Neither client's own identity is available to key off here, and using one would be the
 * wrong signal anyway -- Oracle's wire protocol is opcode/content-driven, not client-driven.
 * {@link ExecuteRequestReader#read} instead reads BOTH candidate windows (marker skipped and
 * not) and keeps whichever one actually decodes to a real SQL statement (starts with a real SQL
 * keyword) -- a signal that's genuinely unambiguous regardless of which client sent it.
 */
class ExecuteRequestReaderTest {

    /** Builds a minimal, valid Execute-request payload (matching {@link ExecuteRequestReader#read}'s
     * exact field order) around one SQL statement -- no binds, no defines, a fresh parse.
     * {@code redundantLengthPrefix} reproduces the real python-oracledb wire shape (a byte equal
     * to the SQL's own length, immediately before the SQL bytes) when {@code true}; {@code false}
     * reproduces the real SQLcl shape (no such byte at all). */
    private static byte[] buildExecuteRequestPayload(String sql, boolean redundantLengthPrefix) {
        TtcWriter w = new TtcWriter();
        byte[] sqlBytes = sql.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        w.writeUb4(0);                 // options
        w.writeUb4(0);                 // cursorId
        w.writeUint8(1);               // sqlPointer (non-zero -> freshParse)
        w.writeUb4(sqlBytes.length);   // sqlLength

        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUb4(0);                 // numIters
        w.writeUb4(0);

        w.writeUint8(0);               // bindsPointer
        w.writeUb4(0);                 // numParams

        for (int i = 0; i < 5; i++) w.writeUint8(0);

        w.writeUint8(0);               // definesPointer
        w.writeUb4(0);                 // numDefines

        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUb4(0);

        w.writeUint8(0);
        w.writeUb4(0);

        w.writeUint8(0);

        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUint8(0);
        w.writeUb4(0);
        w.writeUint8(0);

        w.writeUint8(0);
        w.writeUb4(0);

        // freshParse branch: (real python-oracledb only) a redundant length-echo byte, then the
        // raw SQL bytes, then a trailing ub4.
        if (redundantLengthPrefix) {
            w.writeUint8(sqlBytes.length);
        }
        w.writeRaw(sqlBytes);
        w.writeUb4(0);

        w.writeUb4(0);
        for (int i = 0; i < 4; i++) w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);                 // isQueryField
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);
        w.writeUb4(0);

        return w.toByteArray();
    }

    @Test
    void sqlTextIsReadCorrectlyWhenItsByteLengthCollidesWithItsOwnFirstCharacter() {
        // Real SQLcl shape: no redundant length-prefix byte. "CREATE TABLE sqlcl_demo4 (id NUMBER
        // PRIMARY KEY, name VARCHAR2(50))" is 69 bytes and starts with 'C' (67), same shape as the
        // real statement that triggered this live. Use a statement engineered to hit the exact
        // collision: length == first char's ASCII value -- proves the fix doesn't fall back to
        // "always skip" the way the pre-regression code did.
        String sql = buildCollidingStatement();
        TtcReader r = new TtcReader(buildExecuteRequestPayload(sql, false));

        ExecuteRequest request = ExecuteRequestReader.read(r);

        assertEquals(sql, request.sqlText,
                "SQL text must round-trip exactly even when its length collides with its own first character's ASCII value");
    }

    @Test
    void sqlTextIsReadCorrectlyWhenNoCollisionOccurs() {
        String sql = "SELECT 1 FROM dual";
        TtcReader r = new TtcReader(buildExecuteRequestPayload(sql, false));

        ExecuteRequest request = ExecuteRequestReader.read(r);

        assertEquals(sql, request.sqlText);
    }

    @Test
    void sqlTextIsReadCorrectlyWithARealRedundantLengthPrefixByte() {
        // Real python-oracledb (thin-mode) shape, confirmed live via byte-level tracing: a genuine
        // redundant length-prefix byte DOES precede the SQL text -- reading it as the SQL text's
        // own first byte (the pre-existing regression this test guards against) corrupts every
        // statement from this client, not just a rare collision.
        String sql = "SELECT val FROM rtt_bench WHERE id = 1";
        TtcReader r = new TtcReader(buildExecuteRequestPayload(sql, true));

        ExecuteRequest request = ExecuteRequestReader.read(r);

        assertEquals(sql, request.sqlText,
                "SQL text must round-trip exactly when a real redundant length-prefix byte precedes it");
    }

    /** Builds a "CREATE TABLE ..." statement padded to exactly 67 bytes -- 'C' is ASCII 67, the
     * exact collision that broke this live. */
    private static String buildCollidingStatement() {
        String prefix = "CREATE TABLE t (id NUMBER PRIMARY KEY, name VARCHAR2";
        StringBuilder sql = new StringBuilder(prefix);
        while (sql.length() < 63) {
            sql.append('x');
        }
        sql.append("(1))");
        String result = sql.toString();
        assertEquals('C', result.charAt(0));
        assertEquals((int) 'C', result.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
                "test fixture must actually hit the collision (length == first char's ASCII value) to be meaningful");
        return result;
    }
}
