package com.polygres.wire.mssqlwire.frontend;

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
 * understanding of the spec (see {@code com.polygres.wire.mssqlwire.MssqlJdbcIntegrationTest} for
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
        out.write(0x6C); // NUMERICNTYPE -- deliberately unsupported

        IOException e = assertThrows(IOException.class, () -> RpcRequestReader.read(out.toByteArray()));
        assertTrue(e.getMessage().contains("unsupported"));
    }

    @Test
    void byRefOutputParameterIsRefused() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        noAllHeaders(out);
        procIdSpExecuteSql(out);
        out.write(0); // no name
        out.write(0x01); // StatusFlags: BY_REF bit set

        IOException e = assertThrows(IOException.class, () -> RpcRequestReader.read(out.toByteArray()));
        assertTrue(e.getMessage().contains("BY_REF"));
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
