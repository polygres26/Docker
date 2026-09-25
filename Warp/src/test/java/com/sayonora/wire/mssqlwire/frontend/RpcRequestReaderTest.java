package com.sayonora.wire.mssqlwire.frontend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Golden-byte coverage for {@link RpcRequestReader}, hand-building synthetic TDS RPCRequest
 * payloads field-by-field to the exact wire shape, same discipline as
 * {@code orawire.ttc.ExecuteRequestReaderTest} -- this decoder is exactly the class of byte-level
 * protocol code where a hand-written unit test only validates the decoder against the author's own
 * understanding of the spec (see {@code com.sayonora.wire.mssqlwire.MssqlJdbcIntegrationTest} for
 * the real-client complement).
 */
class RpcRequestReaderTest {

    // --- byte-builder helpers, matching the wire shape one field at a time ---

    private static void u16(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void u32(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    private static void u64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    private static void noAllHeaders(ByteArrayOutputStream out) {
        u32(out, 4); // TotalLength == 4 means "no headers, just the length field itself"
    }

    private static void procIdSpExecuteSql(ByteArrayOutputStream out) {
        u16(out, 0xFFFF);
        u16(out, 10); // sp_executesql
        u16(out, 0); // OptionFlags
    }

    private static void shortVarcharParam(ByteArrayOutputStream out, String name, boolean wide, String value) {
        writeParamHeader(out, name);
        out.write(wide ? 0xE7 : 0xA7);
        u16(out, 4000); // MaxLen (not MAX)
        out.writeBytes(new byte[5]); // collation
        if (value == null) {
            u16(out, 0xFFFF);
        } else {
            byte[] bytes = value.getBytes(wide ? StandardCharsets.UTF_16LE : StandardCharsets.ISO_8859_1);
            u16(out, bytes.length);
            out.writeBytes(bytes);
        }
    }

    private static void plpVarcharParam(ByteArrayOutputStream out, String name, String value) {
        writeParamHeader(out, name);
        out.write(0xE7);
        u16(out, 0xFFFF); // MaxLen == MAX -> PLP encoding
        out.writeBytes(new byte[5]);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_16LE);
        u64(out, bytes.length); // "known" PLP total length -- informational only, we re-derive from chunks
        u32(out, bytes.length); // one chunk containing everything
        out.writeBytes(bytes);
        u32(out, 0); // terminator chunk
    }

    private static void intNParam(ByteArrayOutputStream out, String name, Long value) {
        writeParamHeader(out, name);
        out.write(0x26);
        out.write(4); // declared max length
        if (value == null) {
            out.write(0);
        } else {
            out.write(4);
            u32(out, value & 0xFFFFFFFFL);
        }
    }

    private static void writeParamHeader(ByteArrayOutputStream out, String name) {
        if (name == null) {
            out.write(0);
        } else {
            out.write(name.length());
            out.writeBytes(name.getBytes(StandardCharsets.UTF_16LE));
        }
        out.write(0); // StatusFlags -- plain input parameter
    }

    // --- tests ---

    @Test
    void decodesANamedProcCallViaTheNumericSpExecutesqlProcId() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        shortVarcharParam(out, null, true, "SELECT * FROM t WHERE id = @P0");
        shortVarcharParam(out, null, true, "@P0 int");
        intNParam(out, "@P0", 42L);

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(10, request.procId());
        assertEquals(3, request.params().size());
        assertEquals("SELECT * FROM t WHERE id = @P0", request.params().get(0).value());
        assertEquals(42L, request.params().get(2).value());
        assertEquals("@P0", request.params().get(2).name());
    }

    @Test
    void plpMaxLengthStringIsDecodedAcrossMultipleChunksCorrectly() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);

        // Build the @stmt param as PLP but with TWO chunks, not one -- the shape a real long
        // statement string is likely to actually arrive in.
        writeParamHeader(out, null);
        out.write(0xE7);
        u16(out, 0xFFFF);
        out.writeBytes(new byte[5]);
        u64(out, 0xFFFFFFFFFFFFFFFEL); // "unknown length" marker -- must not be misread as the value
        byte[] first = "SELECT * FROM ".getBytes(StandardCharsets.UTF_16LE);
        byte[] second = "orders".getBytes(StandardCharsets.UTF_16LE);
        u32(out, first.length);
        out.writeBytes(first);
        u32(out, second.length);
        out.writeBytes(second);
        u32(out, 0); // terminator

        shortVarcharParam(out, null, true, "");

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals("SELECT * FROM orders", request.params().get(0).value());
    }

    @Test
    void plpNullIsDistinctFromAnEmptyString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        plpVarcharParam(out, null, "SELECT 1");
        shortVarcharParam(out, null, true, "@P0 nvarchar(50)");
        writeParamHeader(out, "@P0");
        out.write(0xE7);
        u16(out, 0xFFFF);
        out.writeBytes(new byte[5]);
        u64(out, 0xFFFFFFFFFFFFFFFFL); // PLP_NULL

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(2).value());
    }

    @Test
    void intNNullIsDecodedAsNullNotZero() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        shortVarcharParam(out, null, true, "SELECT 1");
        shortVarcharParam(out, null, true, "@P0 int");
        intNParam(out, "@P0", null);

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(2).value());
    }

    @Test
    void unsupportedParamTypeIsRefusedRatherThanMisdecoded() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x29); // TIMEN -- deliberately unsupported (see RpcRequestReader's own javadoc:
                          // mssql-jdbc's setTime() actually sends legacy DATETIMN 0x6f instead, a
                          // separate, not-yet-covered gap)

        IOException e = assertThrows(IOException.class, () -> RpcRequestReader.read(out.toByteArray()));
        assertTrue(e.getMessage().contains("unsupported"));
    }

    /**
     * Golden bytes captured live from a real mssql-jdbc {@code PreparedStatement.setDate(1,
     * Date.valueOf("2026-09-04"))} call: no TYPE_INFO byte at all for DATEN, just {@code 03 16 4A
     * 0B} -- length 3, then the date as a little-endian day count from 0001-01-01
     * (0x0B4A16 = 739862 days after 0001-01-01 is genuinely 2026-09-04).
     */
    @Test
    void dateNParamMatchesRealMssqlJdbcWireBytesForSeptemberFourthTwentyTwentySix() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x28); // DATEN
        out.write(0x03); // length
        out.write(0x16); out.write(0x4A); out.write(0x0B); // date, little-endian

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(java.sql.Date.valueOf("2026-09-04"), request.params().get(0).value());
    }

    /**
     * Golden bytes captured live from a real mssql-jdbc {@code PreparedStatement.setObject(1,
     * UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"))} call: TYPE_INFO {@code 10}
     * (MaxLen=16), value {@code 10} (length=16) then 16 bytes in real .NET GUID wire layout --
     * each of the first three fields byte-reversed from UUID's own string form, the last 8 bytes
     * (Data4) untouched.
     */
    @Test
    void guidNParamMatchesRealMssqlJdbcWireBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x24); // GUIDN
        out.write(0x10); // MaxLen = 16
        out.write(0x10); // value length = 16
        for (int b : new int[] {0x78, 0x56, 0x34, 0x12, 0xab, 0x90, 0xef, 0xcd,
                0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef}) {
            out.write(b);
        }

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(java.util.UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"),
                request.params().get(0).value());
    }

    @Test
    void guidNNullIsDecodedAsNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x24);
        out.write(0x10);
        out.write(0x00); // length 0 -> NULL

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(0).value());
    }

    @Test
    void dateNNullIsDecodedAsNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x28);
        out.write(0x00); // length 0 -> NULL

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(0).value());
    }

    /**
     * Golden bytes captured live from a real mssql-jdbc {@code PreparedStatement.setTimestamp}
     * call at its default scale (7): TYPE_INFO {@code 07} (Scale=7), value {@code 08 D0 DB 49 AD
     * 5C 16 4A 0B} -- length 8 (5-byte time field for scale 5-7, + 3-byte date field), time units
     * are 100ns increments since midnight, date is the SAME DATEN-shaped field as above.
     */
    @Test
    void dateTime2NParamMatchesRealMssqlJdbcWireBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x2A); // DATETIME2N
        out.write(0x07); // scale
        out.write(0x08); // length: 5-byte time + 3-byte date
        out.write(0xD0); out.write(0xDB); out.write(0x49); out.write(0xAD); out.write(0x5C); // time
        out.write(0x16); out.write(0x4A); out.write(0x0B); // date

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        Object value = request.params().get(0).value();
        assertTrue(value instanceof java.sql.Timestamp);
        // Real captured value's date component is the same 2026-09-04 DATEN's own golden test
        // decodes -- confirms both share the identical date-field decode path.
        java.sql.Timestamp ts = (java.sql.Timestamp) value;
        assertEquals(java.sql.Date.valueOf("2026-09-04").toLocalDate(), ts.toLocalDateTime().toLocalDate());
    }

    @Test
    void dateTime2NNullIsDecodedAsNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x2A);
        out.write(0x07);
        out.write(0x00); // length 0 -> NULL

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(0).value());
    }

    /**
     * Golden bytes captured live from a real mssql-jdbc {@code PreparedStatement.setBigDecimal(1,
     * new BigDecimal("12.34"))} call (via a temporary debug probe, since removed): TYPE_INFO
     * {@code 11 26 02} (MaxLen=17, Precision=38, Scale=2), value {@code 03 01 D2 04} (length 3 = 1
     * sign byte + 2 magnitude bytes, sign=positive, unscaled magnitude little-endian 0x04D2=1234).
     */
    @Test
    void decimalNParamMatchesRealMssqlJdbcWireBytesForOnePointTwoThreeFour() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x6A); // DECIMALN
        out.write(0x11); // MaxLen
        out.write(0x26); // Precision
        out.write(0x02); // Scale
        out.write(0x03); // value length: 1 sign byte + 2 magnitude bytes
        out.write(0x01); // sign: positive
        out.write(0xD2); out.write(0x04); // unscaled magnitude, little-endian: 0x04D2 = 1234

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(new java.math.BigDecimal("12.34"), request.params().get(0).value());
    }

    @Test
    void decimalNNegativeValueDecodesWithCorrectSign() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x6C); // NUMERICN -- same wire shape as DECIMALN
        out.write(0x11);
        out.write(0x26);
        out.write(0x02);
        out.write(0x03);
        out.write(0x00); // sign: negative
        out.write(0xD2); out.write(0x04);

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(new java.math.BigDecimal("-12.34"), request.params().get(0).value());
    }

    @Test
    void decimalNNullIsDecodedAsNull() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, "@P0");
        out.write(0x6A);
        out.write(0x11); out.write(0x26); out.write(0x02);
        out.write(0x00); // value length 0 -> NULL

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertNull(request.params().get(0).value());
    }

    @Test
    void byRefOutputParameterIsDecodedNotRefused() throws IOException {
        // sp_prepare's real shape: @handle is declared OUTPUT (BY_REF) but the client still sends
        // real value bytes for it (NULL here, since the caller doesn't know the handle yet) that
        // must be consumed just like an input param's, or the stream desyncs for every param after.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        String name = "@handle";
        out.write(name.length());
        out.writeBytes(name.getBytes(StandardCharsets.UTF_16LE));
        out.write(0x01); // StatusFlags: BY_REF bit set
        out.write(0x26); // INTN
        out.write(4); // declared max length
        out.write(0); // actual length 0 -> NULL

        // a plain input param right after it, to prove the stream stayed in sync
        intNParam(out, "@P0", 7L);

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals(2, request.params().size());
        assertTrue(request.params().get(0).output(), "BY_REF status flag must be decoded as output=true");
        assertNull(request.params().get(0).value());
        assertEquals("@handle", request.params().get(0).name());
        assertEquals(7L, request.params().get(1).value(), "param after the BY_REF one must decode correctly");
        assertTrue(!request.params().get(1).output());
    }

    @Test
    void truncatedPayloadIsRefusedNotOutOfBounds() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        writeParamHeader(out, null);
        out.write(0xE7);
        u16(out, 4000);
        // missing collation bytes and the value entirely

        assertThrows(IOException.class, () -> RpcRequestReader.read(out.toByteArray()));
    }

    @Test
    void namedProcCallByStringNameAlsoRecognizedNotOnlyNumericProcId() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        String procName = "sp_executesql";
        u16(out, procName.length());
        out.writeBytes(procName.getBytes(StandardCharsets.UTF_16LE));
        u16(out, 0);
        shortVarcharParam(out, null, true, "SELECT 1");

        RpcRequestReader.RpcRequest request = RpcRequestReader.read(out.toByteArray());

        assertEquals("sp_executesql", request.procName());
        assertEquals(0, request.procId());
    }
}
