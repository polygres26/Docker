package com.sayonora.warp.cqlwire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.cqlwire.Ast.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WireAndEvalTest {

    @Test
    void wirePrimitivesRoundTrip() {
        Wire.Out o = new Wire.Out().u8(7).u16(65535).i32(-2).i64(Long.MIN_VALUE).string("hé").longString("long").bytes(null).bytes(new byte[] {1, 2})
                .shortBytes(new byte[] {9});
        Wire.In in = new Wire.In(o.toBytes());
        assertEquals(7, in.u8());
        assertEquals(65535, in.u16());
        assertEquals(-2, in.i32());
        assertEquals(Long.MIN_VALUE, in.i64());
        assertEquals("hé", in.string());
        assertEquals("long", in.longString());
        assertNull(in.bytes());
        assertArrayEquals(new byte[] {1, 2}, in.bytes());
        assertArrayEquals(new byte[] {9}, in.shortBytes());
        assertThrows(CqlError.class, () -> in.raw(1));
    }

    @Test
    void valueMarkersNullAndUnset() {
        Wire.In in = new Wire.In(new Wire.Out().i32(-1).i32(-2).i32(1).u8(5).toBytes());
        assertNull(in.value());
        assertTrue(in.value() == Wire.UNSET);
        assertArrayEquals(new byte[] {5}, in.value());
    }

    @Test
    void errorBodies() {
        CqlError e = CqlError.exists("ks", "t");
        Wire.In in = new Wire.In(CqlSession.errorBody(e));
        assertEquals(CqlError.ALREADY_EXISTS, in.i32());
        in.string();
        assertEquals("ks", in.string());
        assertEquals("t", in.string());
        CqlError u = new CqlError(CqlError.UNPREPARED, "x");
        u.id = new byte[] {1, 2};
        in = new Wire.In(CqlSession.errorBody(u));
        in.i32();
        in.string();
        assertArrayEquals(new byte[] {1, 2}, in.shortBytes());
        // unavailable carries consistency, required and alive
        in = new Wire.In(CqlSession.errorBody(new CqlError(CqlError.UNAVAILABLE, "x")));
        in.i32();
        in.string();
        assertEquals(1, in.u16());
        assertEquals(1, in.i32());
        assertEquals(0, in.i32());
    }

    private static final Eval.Binds NONE = Eval.Binds.NONE;

    private static Object coerce(String cql, String type) {
        Insert i = (Insert) Parser.parse("INSERT INTO t (a) VALUES (" + cql + ")");
        return Eval.coerce(i.values().get(0), Parser.parseType(type, "ks", (a, b) -> null), NONE, "a");
    }

    @Test
    void literalCoercion() {
        assertEquals(5, coerce("5", "int"));
        assertEquals(5L, coerce("5", "bigint"));
        assertEquals(new BigDecimal("1.50"), coerce("1.50", "decimal"));
        assertEquals(1.5, coerce("1.5", "double"));
        assertEquals("x", coerce("'x'", "text"));
        assertEquals(UUID.fromString("11111111-1111-1111-1111-111111111111"), coerce("11111111-1111-1111-1111-111111111111", "uuid"));
        assertEquals(1583020800000L, coerce("'2020-03-01'", "timestamp"));
        assertEquals(Double.valueOf(-0.0), coerce("-0.0", "double"));
        assertNull(coerce("null", "int"));
        assertThrows(CqlError.class, () -> coerce("2147483648", "int"));
        assertThrows(CqlError.class, () -> coerce("'x'", "int"));
        assertThrows(CqlError.class, () -> coerce("1.5", "int"));
        assertThrows(CqlError.class, () -> coerce("'é'", "ascii"));
        assertEquals(CqlError.INVALID, assertThrows(CqlError.class, () -> coerce("[1]", "set<int>")).code);
    }

    @Test
    void collectionAndTupleLiterals() {
        Object l = coerce("[3, 1, 3]", "list<int>");
        assertEquals(List.of(3, 1, 3), l);
        assertEquals(List.of(1, 3), List.copyOf((java.util.Collection<?>) coerce("{3, 1, 3}", "set<int>")));
        assertEquals(1, ((java.util.Map<?, ?>) coerce("{'a': 1}", "map<text, int>")).size());
        assertArrayEquals(new Object[] {1, null}, (Object[]) coerce("(1, null)", "tuple<int, text>"));
    }

    @Test
    void jsonRoundTrip() {
        CqlType t = Parser.parseType("map<text, frozen<list<int>>>", "ks", (a, b) -> null);
        Object v = Eval.fromJson(com.google.gson.JsonParser.parseString("{\"b\": [2], \"a\": []}"), t);
        assertEquals("{\"a\": [], \"b\": [2]}", Eval.toJson(v, t));
        assertEquals("\"2020-03-01 00:00:00.000Z\"", Eval.toJson(1583020800000L, CqlType.TIMESTAMP));
        assertEquals("\"NaN\"", Eval.toJson(Double.NaN, CqlType.DOUBLE));
        assertEquals("\"a\\\"b\\n\"", Eval.toJson("a\"b\n", CqlType.TEXT));
    }

    @Test
    void arithmeticFollowsCqlTypes() {
        Eval.TV r = Eval.arith('+', new Eval.TV(1, CqlType.INT), new Eval.TV(2L, CqlType.BIGINT));
        assertEquals(3L, r.v());
        assertEquals(CqlType.BIGINT, r.t());
        assertEquals(2, Eval.arith('/', new Eval.TV(5, CqlType.INT), new Eval.TV(2, CqlType.INT)).v());
        CqlError e = assertThrows(CqlError.class, () -> Eval.arith('/', new Eval.TV(5, CqlType.INT), new Eval.TV(0, CqlType.INT)));
        assertEquals(CqlError.FUNCTION_FAILURE, e.code);
    }

    @Test
    void timeuuidBounds() {
        UUID min = Eval.timeUuid(1_000_000, 0x8080808080808080L);
        UUID max = Eval.timeUuid(1_000_000, 0x7f7f7f7f7f7f7f7fL, 9999);
        assertEquals(1, min.version());
        assertEquals(1_000_000, Eval.uuidMillis(min));
        assertEquals(1_000_000, Eval.uuidMillis(max));
        assertTrue(CqlType.TIMEUUID.compare(min, max) < 0);
    }

    @Test
    void schemaSpecsRoundTrip() {
        Schema.Table t = new Schema.Table("ks", "t", UUID.randomUUID(), List.of(
                new Schema.Column("p", CqlType.INT, Schema.Kind.PARTITION_KEY, 0, false),
                new Schema.Column("c", CqlType.TEXT, Schema.Kind.CLUSTERING, 0, true),
                new Schema.Column("s", Parser.parseType("set<uuid>", "ks", (a, b) -> null), Schema.Kind.REGULAR, -1, false)),
                new java.util.LinkedHashMap<>(java.util.Map.of("comment", "x")), List.of(), null);
        List<String[]> rows = List.of(new String[] {"keyspace", "ks", "ks", Schema.keyspaceSpec("ks", java.util.Map.of("class", "SimpleStrategy"), true)},
                new String[] {"table", "ks", "t", Schema.tableSpec(t)});
        Schema s = Schema.load(1, rows);
        Schema.Table back = s.table("ks", "t");
        assertEquals(t.id, back.id);
        assertEquals(3, back.columns.size());
        assertTrue(back.clustering.get(0).desc);
        assertEquals("set<uuid>", back.col("s").type.cql());
        assertEquals("x", back.options.get("comment"));
    }

    @Test
    void clusteringValuesRoundTripThroughTheirSerializedForm() {
        Schema.Table t = new Schema.Table("ks", "t", UUID.randomUUID(), List.of(
                new Schema.Column("p", CqlType.INT, Schema.Kind.PARTITION_KEY, 0, false),
                new Schema.Column("c1", CqlType.DECIMAL, Schema.Kind.CLUSTERING, 0, false),
                new Schema.Column("c2", CqlType.UUID_T, Schema.Kind.CLUSTERING, 1, true)), new java.util.LinkedHashMap<>(), List.of(), null);
        Object[] ck = {new BigDecimal("1.50"), UUID.randomUUID()};
        Object[] back = t.clusteringFromValues(t.clusteringValues(ck));
        assertEquals(0, ((BigDecimal) ck[0]).compareTo((BigDecimal) back[0]));
        assertEquals(ck[1], back[1]);
        BigInteger unused = BigInteger.ONE;
        assertTrue(unused.signum() > 0);
    }
}
