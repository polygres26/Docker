package com.nexagres.wire.mywire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Golden-byte coverage for {@link MySqlBinaryProtocol}, hand-building synthetic
 * {@code COM_STMT_EXECUTE} parameter blocks and binary rows field-by-field to the exact wire
 * shape -- same discipline as {@code orawire.ttc.ExecuteRequestReaderTest} and
 * {@code mssqlwire.frontend.RpcRequestReaderTest}. See {@link MySqlJdbcIntegrationTest} for the
 * real-client complement.
 */
class MySqlBinaryProtocolTest {

    @Test
    void countPlaceholdersSkipsQuestionMarksInsideStringLiterals() {
        assertEquals(2, MySqlBinaryProtocol.countPlaceholders("SELECT * FROM t WHERE a = ? AND b = ?"));
        assertEquals(1, MySqlBinaryProtocol.countPlaceholders("SELECT * FROM t WHERE a = '?' AND b = ?"));
        assertEquals(0, MySqlBinaryProtocol.countPlaceholders("SELECT '???' FROM t"));
        assertEquals(1, MySqlBinaryProtocol.countPlaceholders("SELECT \"a?b\" , ? FROM t"));
    }

    @Test
    void paramNullBitmapIsPlainZeroBasedNotOffsetLikeRowNullBitmap() {
        // param 0 = not null, param 1 = null, param 2 = not null -> bit pattern 0b010 = 0x02
        byte[] data = {0x02};
        int[] pos = {0};
        boolean[] isNull = MySqlBinaryProtocol.readParamNullBitmap(data, pos, 3);
        assertEquals(new boolean[] {false, true, false}.length, isNull.length);
        assertEquals(false, isNull[0]);
        assertEquals(true, isNull[1]);
        assertEquals(false, isNull[2]);
        assertEquals(1, pos[0], "must advance exactly (numParams+7)/8 bytes");
    }

    @Test
    void decodesTinyShortLongLonglongInSignedTwosComplement() throws IOException {
        byte[] data = {(byte) 0xFF}; // TINY: -1
        assertEquals(-1L, MySqlBinaryProtocol.readValue(data, new int[] {0}, MySqlBinaryProtocol.TYPE_TINY));

        byte[] shortData = {(byte) 0xFF, (byte) 0xFF}; // SHORT: -1
        assertEquals(-1L, MySqlBinaryProtocol.readValue(shortData, new int[] {0}, MySqlBinaryProtocol.TYPE_SHORT));

        byte[] longData = {0x2A, 0x00, 0x00, 0x00}; // LONG: 42
        assertEquals(42L, MySqlBinaryProtocol.readValue(longData, new int[] {0}, MySqlBinaryProtocol.TYPE_LONG));

        byte[] longlongData = {(byte) 0xD6, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}; // LONGLONG: -42
        assertEquals(-42L, MySqlBinaryProtocol.readValue(longlongData, new int[] {0}, MySqlBinaryProtocol.TYPE_LONGLONG));
    }

    @Test
    void decodesDoubleAndFloatAsIeee754LittleEndian() throws IOException {
        byte[] doubleBytes = toLeBytes(Double.doubleToLongBits(3.14), 8);
        assertEquals(3.14, (Double) MySqlBinaryProtocol.readValue(doubleBytes, new int[] {0}, MySqlBinaryProtocol.TYPE_DOUBLE), 0.0001);

        byte[] floatBytes = toLeBytes(Float.floatToIntBits(2.5f) & 0xFFFFFFFFL, 4);
        assertEquals(2.5, (Double) MySqlBinaryProtocol.readValue(floatBytes, new int[] {0}, MySqlBinaryProtocol.TYPE_FLOAT), 0.0001);
    }

    @Test
    void decodesVarStringViaLengthEncodedString() throws IOException {
        byte[] data = {5, 'h', 'e', 'l', 'l', 'o'};
        assertEquals("hello", MySqlBinaryProtocol.readValue(data, new int[] {0}, MySqlBinaryProtocol.TYPE_VAR_STRING));
    }

    @Test
    void decodesDateTimeAtEveryRealWireLength() throws IOException {
        assertEquals("0000-00-00", MySqlBinaryProtocol.readValue(new byte[] {0}, new int[] {0}, MySqlBinaryProtocol.TYPE_DATE));

        // length=4: year(u16 LE)=2024, month=3, day=15
        byte[] dateOnly = {4, (byte) 0xE8, 0x07, 3, 15};
        assertEquals("2024-03-15", MySqlBinaryProtocol.readValue(dateOnly, new int[] {0}, MySqlBinaryProtocol.TYPE_DATE));

        // length=7: + hour=13, minute=45, second=30
        byte[] dateTime = {7, (byte) 0xE8, 0x07, 3, 15, 13, 45, 30};
        assertEquals("2024-03-15 13:45:30",
                MySqlBinaryProtocol.readValue(dateTime, new int[] {0}, MySqlBinaryProtocol.TYPE_DATETIME));

        // length=11: + microseconds(u32 LE)=123456
        byte[] micros = toLeBytes(123456, 4);
        byte[] full = new byte[12];
        full[0] = 11;
        System.arraycopy(dateTime, 1, full, 1, 7);
        System.arraycopy(micros, 0, full, 8, 4);
        assertEquals("2024-03-15 13:45:30.123456",
                MySqlBinaryProtocol.readValue(full, new int[] {0}, MySqlBinaryProtocol.TYPE_TIMESTAMP));
    }

    @Test
    void unsupportedTypeIsRefusedRatherThanMisdecoded() {
        IOException e = assertThrows(IOException.class,
                () -> MySqlBinaryProtocol.readValue(new byte[] {0}, new int[] {0}, 0xf7 /* SET -- deliberately unsupported */));
        assertTrue(e.getMessage().contains("unsupported"));
    }

    @Test
    void encodeRowNullBitmapIsOffsetByTwoBitsNotZeroBased() {
        // 3 columns: col0=null, col1="x", col2=null -> bits at index (0+2)=2 and (2+2)=4 set
        byte[] row = MySqlBinaryProtocol.encodeRow(Arrays.asList(null, "x", null));
        // header(1) + bitmap((3+9)/8=1 byte) + lenenc "x"(2 bytes)
        assertEquals(0x00, row[0]);
        int bitmap = row[1] & 0xFF;
        assertEquals((1 << 2) | (1 << 4), bitmap);
        assertEquals(4, row.length);
        assertEquals(1, row[2]); // length-encoded length of "x"
        assertEquals('x', row[3]);
    }

    @Test
    void decodeExecuteParamsRoundTripsAMixedParameterSetWithNewTypesBound() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0b00000010); // null-bitmap: param 1 is null, params 0 and 2 are not
        out.write(1); // new_params_bound_flag = 1
        out.write(MySqlBinaryProtocol.TYPE_LONG); out.write(0); // param 0: LONG, signed
        out.write(MySqlBinaryProtocol.TYPE_VAR_STRING); out.write(0); // param 1: VAR_STRING (but NULL, no value bytes)
        out.write(MySqlBinaryProtocol.TYPE_VAR_STRING); out.write(0); // param 2: VAR_STRING
        out.write(new byte[] {42, 0, 0, 0}); // param 0 value: 42
        // param 1 has no value bytes -- it's null
        byte[] str = "hi".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.write(2);
        out.writeBytes(str); // param 2 value: "hi"

        int[] cachedTypes = {-1, 0, 0};
        List<Object> values = MySqlBinaryProtocol.decodeExecuteParams(out.toByteArray(), new int[] {0}, 3, cachedTypes);

        assertEquals(42L, values.get(0));
        assertNull(values.get(1));
        assertEquals("hi", values.get(2));
        assertArrayEquals(new int[] {MySqlBinaryProtocol.TYPE_LONG, MySqlBinaryProtocol.TYPE_VAR_STRING,
                MySqlBinaryProtocol.TYPE_VAR_STRING}, cachedTypes, "types must be cached for a later EXECUTE that reuses them");
    }

    @Test
    void decodeExecuteParamsReusesCachedTypesWhenNewParamsBoundFlagIsZero() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0); // null-bitmap: nothing null
        out.write(0); // new_params_bound_flag = 0 -- reuse cached types
        out.write(new byte[] {7, 0, 0, 0}); // value for the one param, as a LONG (from cache)

        int[] cachedTypes = {MySqlBinaryProtocol.TYPE_LONG};
        List<Object> values = MySqlBinaryProtocol.decodeExecuteParams(out.toByteArray(), new int[] {0}, 1, cachedTypes);

        assertEquals(7L, values.get(0));
    }

    private static byte[] toLeBytes(long v, int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
        return b;
    }
}
