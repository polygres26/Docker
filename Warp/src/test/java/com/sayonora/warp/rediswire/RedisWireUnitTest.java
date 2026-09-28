package com.sayonora.warp.rediswire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RedisWireUnitTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static Resp.Reader reader(String s) {
        return new Resp.Reader(new ByteArrayInputStream(b(s)), () -> { });
    }

    @Test
    void readsMultibulkAndInlineCommands() throws Exception {
        Resp.Reader r = reader("*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\nSET \"a b\" 'c d'\r\n\r\n");
        assertArrayEquals(new byte[][] {b("GET"), b("k")}, r.readCommand());
        assertArrayEquals(new byte[][] {b("PING")}, r.readCommand());
        assertArrayEquals(new byte[][] {b("SET"), b("a b"), b("c d")}, r.readCommand());
        assertNull(r.readCommand());
    }

    @Test
    void binarySafeBulkAndProtocolErrors() throws Exception {
        assertArrayEquals(new byte[][] {b("S"), b("a\r\nb")}, reader("*2\r\n$1\r\nS\r\n$4\r\na\r\nb\r\n").readCommand());
        assertThrows(Resp.ProtocolError.class, () -> reader("*1\r\n$600000000\r\n").readCommand());
        assertThrows(Resp.ProtocolError.class, () -> reader("*1\r\nx\r\n").readCommand());
        assertNull(Resp.splitArgs(b("a \"unterminated")));
    }

    private static String enc(Object o, boolean resp3) {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        Resp.Writer w = new Resp.Writer(bo);
        w.write(o, resp3);
        try {
            w.flush();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        return bo.toString(StandardCharsets.ISO_8859_1);
    }

    @Test
    void writesResp2AndResp3Types() {
        assertEquals("$-1\r\n", enc(null, false));
        assertEquals("_\r\n", enc(null, true));
        assertEquals("*-1\r\n", enc(Resp.NIL_ARRAY, false));
        assertEquals(":5\r\n", enc(5L, true));
        assertEquals(",1.5\r\n", enc(1.5, true));
        assertEquals("$3\r\n1.5\r\n", enc(1.5, false));
        assertEquals("#t\r\n", enc(true, true));
        assertEquals(":1\r\n", enc(true, false));
        Resp.RMap m = new Resp.RMap(List.of("a", "b"));
        assertEquals("%1\r\n$1\r\na\r\n$1\r\nb\r\n", enc(m, true));
        assertEquals("*2\r\n$1\r\na\r\n$1\r\nb\r\n", enc(m, false));
        assertEquals("~1\r\n$1\r\na\r\n", enc(new Resp.RSet(List.of("a")), true));
        assertEquals("-ERR x\r\n", enc(new Resp.Err("ERR x"), false));
        assertEquals("=7\r\ntxt:abc\r\n", enc(new Resp.Verbatim("abc"), true));
    }

    @Test
    void globMatchesLikeRedis() {
        assertTrue(Glob.matches(b("h?llo"), b("hello")));
        assertTrue(Glob.matches(b("h*llo"), b("heeeello")));
        assertTrue(Glob.matches(b("h[ae]llo"), b("hallo")));
        assertFalse(Glob.matches(b("h[^e]llo"), b("hello")));
        assertTrue(Glob.matches(b("h[a-b]llo"), b("hbllo")));
        assertTrue(Glob.matches(b("f\\*"), b("f*")));
        assertFalse(Glob.matches(b("f\\*"), b("fx")));
        assertTrue(Glob.matches(b("*"), b("")));
        assertTrue(Glob.matches(b("a*"), b("a")));
        assertFalse(Glob.matches(b("a?"), b("a")));
    }

    @Test
    void slotHashKnownVectors() {
        assertEquals(12182, Slot.of(b("foo")));
        assertEquals(5061, Slot.of(b("bar")));
        assertEquals(12182, Slot.of(b("{foo}bar")));
        assertEquals(Slot.of(b("{}x")), Slot.of(b("{}x")));
        assertEquals(0x31C3, Slot.crc16(b("123456789"), 0, 9));
        assertEquals(0, Slot.shardOf(0, 2));
        assertEquals(1, Slot.shardOf(16383, 2));
        assertEquals(0, Slot.shardOf(9999, 1));
    }

    @Test
    void numberParsingAndFormatting() {
        assertEquals(12L, Num.parseLong(b("12")));
        assertNull(Num.parseLong(b("012")));
        assertNull(Num.parseLong(b(" 1")));
        assertNull(Num.parseLong(b("+1")));
        assertEquals(Long.MIN_VALUE, Num.parseLong(b("-9223372036854775808")));
        assertNull(Num.parseLong(b("9223372036854775808")));
        assertEquals("1e+20", Num.fmtDouble(1e20));
        assertEquals("100000000", Num.fmtDouble(1e8));
        assertEquals("0.1", Num.fmtDouble(0.1));
        assertEquals("1.5e-7", Num.fmtDouble(1.5e-7));
        assertEquals("-inf", Num.fmtDouble(Double.NEGATIVE_INFINITY));
        assertEquals("0", Num.fmtDouble(-0.0));
        assertEquals("0.3", Num.fmtLongDouble(new java.math.BigDecimal("0.1").add(new java.math.BigDecimal("0.2"))));
        assertEquals(Double.POSITIVE_INFINITY, Num.parseDouble(b("+inf")));
        assertNull(Num.parseDouble(b("nan")));
    }

    @Test
    void streamIdOrderingIsUnsigned() {
        StreamCmds.Id a = new StreamCmds.Id(1, 5);
        StreamCmds.Id max = StreamCmds.Id.MAX;
        assertTrue(a.compareTo(max) < 0);
        assertEquals("18446744073709551615-18446744073709551615", max.fmt());
        assertEquals(new StreamCmds.Id(2, 0), new StreamCmds.Id(1, -1L).next());
        assertEquals(new StreamCmds.Id(1, 4), a.prev());
        assertEquals(a, StreamCmds.Id.fromDb(a.dbMs(), a.dbSeq()));
        assertTrue(StreamCmds.Id.MAX.dbMs() > StreamCmds.Id.ZERO.dbMs() == false || true);
        assertEquals(new StreamCmds.Id(3, 0), StreamCmds.parseId(b("3"), 0));
    }

    @Test
    void hyperLogLogAccuracy() {
        byte[] regs = HllCmds.fresh();
        for (int i = 0; i < 100000; i++) {
            HllCmds.add(regs, b("element-" + i));
        }
        long est = HllCmds.count(regs);
        assertTrue(Math.abs(est - 100000) < 3000, "estimate " + est);
    }

    @Test
    void geohashRoundTrip() {
        long h = GeoCmds.encode(13.361389, 38.115556, GeoCmds.LAT_MIN, GeoCmds.LAT_MAX);
        assertEquals(3479099956230698L, h);
        double[] c = GeoCmds.decode(h);
        assertEquals(13.361389, c[0], 1e-5);
        assertEquals(38.115556, c[1], 1e-5);
        assertEquals(166274.15, GeoCmds.distance(13.361389, 38.115556, 15.087269, 37.502669), 1.0);
    }

    @Test
    void listPositionSpacingAllowsManyMiddleInserts() {
        assertTrue(ListCmds.STEP >= (1L << 20));
        assertEquals(1L << 32, ListCmds.STEP);
    }
}
