package com.polygres.wire.orawire.ttc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Regression coverage for a real bug found live testing orawire against a genuine Oracle SQLcl
 * client: a fresh-parse Execute request's SQL text was misread whenever the statement's byte
 * length happened to equal the ASCII value of its own first character -- e.g. a 67-byte
 * "CREATE TABLE ..." statement, since {@code 'C'} is 67. {@link ExecuteRequestReader#read}'s old
 * SQL-text reader called {@code TtcReader#readRawOrLengthPrefixedBytes}, which guesses there's a
 * redundant length-prefix byte whenever the *next* byte equals the expected length -- a coincidence
 * with real SQL text's first character, not an actual protocol marker. The guess skipped a real
 * content byte, corrupting the text and misaligning every field parsed after it (surfacing several
 * fields later as an unrelated-looking ArrayIndexOutOfBoundsException). Fixed by reading exactly
 * {@code sqlLength} raw bytes with no guessing.
 */
class ExecuteRequestReaderTest {

    /** Builds a minimal, valid Execute-request payload (matching {@link ExecuteRequestReader#read}'s
     * exact field order) around one SQL statement -- no binds, no defines, a fresh parse. */
    private static byte[] buildExecuteRequestPayload(String sql) {
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

        // freshParse branch: raw SQL bytes, then a trailing ub4.
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
        // "CREATE TABLE sqlcl_demo4 (id NUMBER PRIMARY KEY, name VARCHAR2(50))" is 69 bytes and
        // starts with 'C' (67), same shape as the real statement that triggered this live. Use a
        // statement engineered to hit the exact collision: length == first char's ASCII value.
        String sql = buildCollidingStatement();
        TtcReader r = new TtcReader(buildExecuteRequestPayload(sql));

        ExecuteRequest request = ExecuteRequestReader.read(r);

        assertEquals(sql, request.sqlText,
                "SQL text must round-trip exactly even when its length collides with its own first character's ASCII value");
    }

    @Test
    void sqlTextIsReadCorrectlyWhenNoCollisionOccurs() {
        String sql = "SELECT 1 FROM dual";
        TtcReader r = new TtcReader(buildExecuteRequestPayload(sql));

        ExecuteRequest request = ExecuteRequestReader.read(r);

        assertEquals(sql, request.sqlText);
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
