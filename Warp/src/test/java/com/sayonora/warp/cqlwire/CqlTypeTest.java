package com.sayonora.warp.cqlwire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CqlTypeTest {

    private static CqlType t(String s) {
        return Parser.parseType(s, "ks", (a, b) -> null);
    }

    @Test
    void typeNamesRoundTrip() {
        for (String s : List.of("int", "text", "map<text, frozen<list<int>>>", "frozen<set<uuid>>", "tuple<int, text>", "list<timestamp>")) {
            CqlType x = t(s);
            assertEquals(x, t(x.qualified()));
        }
        assertEquals("frozen<tuple<int, text>>", t("tuple<int,text>").cql());
        assertEquals("map<text, int>", t("map<text,int>").cql());
        assertEquals("text", t("varchar").cql());
    }

    @Test
    void serializationRoundTripsEveryType() throws Exception {
        Object[][] cases = {
            {"int", -5}, {"bigint", Long.MIN_VALUE}, {"smallint", (short) -300}, {"tinyint", (byte) -7}, {"boolean", true},
            {"double", -0.0}, {"float", 1.25f}, {"text", "héllo"}, {"ascii", "abc"}, {"blob", new byte[] {0, 1, (byte) 0xff}},
            {"varint", new BigInteger("-123456789012345678901234567890")}, {"decimal", new BigDecimal("-0.000123456789")},
            {"uuid", UUID.randomUUID()}, {"timeuuid", UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3")},
            {"inet", InetAddress.getByName("::1")}, {"date", -25000}, {"time", 86_399_999_999_999L}, {"timestamp", -1L},
            {"duration", new long[] {14, 3, 14706007000000L}},
        };
        for (Object[] c : cases) {
            CqlType type = t((String) c[0]);
            byte[] b = type.serialize(c[1]);
            assertEquals(0, type.compare(c[1], type.deserialize(b)), (String) c[0]);
        }
        CqlType m = t("map<text, frozen<list<int>>>");
        TreeMap<Object, Object> map = new TreeMap<>(CqlType.TEXT);
        map.put("b", new ArrayList<Object>(List.of(1, 2)));
        map.put("a", new ArrayList<Object>());
        assertEquals(0, m.compare(map, m.deserialize(m.serialize(map))));
        CqlType tup = t("tuple<int, text, boolean>");
        Object[] v = {1, null, true};
        assertArrayEquals(v, (Object[]) tup.deserialize(tup.serialize(v)));
    }

    @Test
    void vintDurations() {
        long[][] ds = {{0, 0, 0}, {1, 2, 3}, {-1, -2, -3}, {Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE}, {0, 0, 1_000_000_000L}};
        CqlType dur = t("duration");
        for (long[] d : ds) {
            assertArrayEquals(d, (long[]) dur.deserialize(dur.serialize(d)));
        }
        assertArrayEquals(new long[] {14, 3, 14706007000000L}, CqlType.parseDuration("1y2mo3d4h5m6s7ms"));
        assertArrayEquals(new long[] {14, 3, 14706000000000L}, CqlType.parseDuration("P1Y2M3DT4H5M6S"));
        assertEquals("1y2mo3d4h5m6s7ms", CqlType.formatDuration(new long[] {14, 3, 14706007000000L}));
    }

    /** The order-preserving encoding must sort exactly like the type's own comparator, ascending and descending. */
    @Test
    void keyEncodingIsOrderPreserving() throws Exception {
        Random rnd = new Random(42);
        record C(String type, java.util.function.Supplier<Object> gen) {
        }
        List<C> cs = List.of(
            new C("int", rnd::nextInt), new C("bigint", rnd::nextLong), new C("smallint", () -> (short) rnd.nextInt()),
            new C("tinyint", () -> (byte) rnd.nextInt()), new C("boolean", rnd::nextBoolean),
            new C("double", () -> rnd.nextInt(10) == 0 ? (rnd.nextBoolean() ? Double.NaN : -0.0) : (rnd.nextDouble() - 0.5) * Math.pow(10, rnd.nextInt(600) - 300)),
            new C("float", () -> (rnd.nextFloat() - 0.5f) * (float) Math.pow(10, rnd.nextInt(60) - 30)),
            new C("varint", () -> new BigInteger(rnd.nextInt(200), rnd).multiply(rnd.nextBoolean() ? BigInteger.ONE : BigInteger.ONE.negate())),
            new C("decimal", () -> new BigDecimal(new BigInteger(rnd.nextInt(80), rnd), rnd.nextInt(40) - 20).multiply(rnd.nextBoolean() ? BigDecimal.ONE : BigDecimal.ONE.negate())),
            new C("text", () -> randomText(rnd)), new C("blob", () -> {
                byte[] b = new byte[rnd.nextInt(6)];
                for (int i = 0; i < b.length; i++) {
                    b[i] = (byte) (rnd.nextInt(4) == 0 ? 0 : rnd.nextInt());
                }
                return b;
            }),
            new C("timeuuid", () -> Eval.timeUuid(rnd.nextInt(1_000_000_000), rnd.nextLong())),
            new C("date", rnd::nextInt), new C("timestamp", rnd::nextLong),
            new C("inet", () -> {
                try {
                    byte[] b = new byte[rnd.nextBoolean() ? 4 : 16];
                    rnd.nextBytes(b);
                    return InetAddress.getByAddress(b);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }),
            new C("frozen<list<int>>", () -> {
                List<Object> l = new ArrayList<>();
                for (int i = rnd.nextInt(4); i > 0; i--) {
                    l.add(rnd.nextInt(5) - 2);
                }
                return l;
            }),
            new C("frozen<tuple<int, text>>", () -> new Object[] {rnd.nextInt(3) == 0 ? null : rnd.nextInt(3), rnd.nextInt(3) == 0 ? null : randomText(rnd)}),
            new C("frozen<set<text>>", () -> {
                TreeSet<Object> s = new TreeSet<>(CqlType.TEXT);
                for (int i = rnd.nextInt(4); i > 0; i--) {
                    s.add(randomText(rnd));
                }
                return s;
            }));
        for (C c : cs) {
            CqlType type = t(c.type());
            for (int round = 0; round < 300; round++) {
                Object a = c.gen().get(), b = c.gen().get();
                int cmp = Integer.signum(type.compare(a, b));
                int enc = Integer.signum(Arrays.compareUnsigned(type.keyEncode(a, false), type.keyEncode(b, false)));
                int dec = Integer.signum(Arrays.compareUnsigned(type.keyEncode(a, true), type.keyEncode(b, true)));
                assertEquals(cmp, enc, c.type() + ": " + type.format(a) + " vs " + type.format(b));
                assertEquals(-cmp, dec, c.type() + " desc: " + type.format(a) + " vs " + type.format(b));
            }
        }
    }

    private static String randomText(Random rnd) {
        StringBuilder b = new StringBuilder();
        for (int i = rnd.nextInt(5); i > 0; i--) {
            b.append("a\u0000bé世".charAt(rnd.nextInt(5)));
        }
        return b.toString();
    }

    @Test
    void keyEncodingIsPrefixFreeSoRangesAreExact() {
        CqlType text = CqlType.TEXT;
        byte[] a = text.keyEncode("ab", false), ab = text.keyEncode("abc", false);
        assertTrue(Arrays.compareUnsigned(a, ab) < 0);
        // succ() of a prefix bounds exactly the keys that start with it
        byte[] prefix = a;
        byte[] hi = Rows.succ(prefix);
        assertTrue(Arrays.compareUnsigned(ab, hi) >= 0, "the encoding of 'abc' must not start with the encoding of 'ab'");
        assertEquals(null, Rows.succ(new byte[] {(byte) 0xff, (byte) 0xff}));
        assertArrayEquals(new byte[] {2}, Rows.succ(new byte[] {1, (byte) 0xff}));
    }

    @Test
    void timestampAndDateParsing() {
        assertEquals(1583020800000L, CqlType.parseTimestamp("2020-03-01"));
        assertEquals(1583020800000L, CqlType.parseTimestamp("2020-03-01T00:00:00Z"));
        assertEquals(1583020800123L, CqlType.parseTimestamp("2020-03-01 00:00:00.123+0000"));
        assertEquals(1583020800000L - 7200_000L, CqlType.parseTimestamp("2020-03-01 00:00:00+0200"));
        assertEquals(1600000000123L, CqlType.parseTimestamp("1600000000123"));
        assertThrows(CqlError.class, () -> CqlType.parseTimestamp("2020-13-01"));
        assertEquals(0, CqlType.parseDate("1970-01-01"));
        assertEquals(43_200_000_000_000L, CqlType.parseTime("12:00:00"));
        assertThrows(CqlError.class, () -> CqlType.parseTime("24:00:00"));
    }
}
