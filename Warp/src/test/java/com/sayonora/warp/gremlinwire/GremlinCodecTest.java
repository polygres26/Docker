package com.sayonora.warp.gremlinwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** GraphSON 3.0 / untyped / 2.0 and GraphBinary 1.0 encodings of the value model. */
class GremlinCodecTest {

    private static G.Vertex vertex() {
        G.Vertex v = new G.Vertex(1L, "person");
        Mutations.applyVertexProp(v, G.Cardinality.list, "name", "marko", List.of());
        Mutations.applyVertexProp(v, G.Cardinality.list, "age", 29, List.of());
        return v;
    }

    @Test
    void graphSonTypedNumbersAndCollections() {
        assertEquals(JsonParser.parseString("{\"@type\":\"g:Int32\",\"@value\":1}"), GraphSon.V3.write(1));
        assertEquals(JsonParser.parseString("{\"@type\":\"g:Int64\",\"@value\":1}"), GraphSon.V3.write(1L));
        assertEquals(JsonParser.parseString("{\"@type\":\"g:Double\",\"@value\":0.5}"), GraphSon.V3.write(0.5d));
        assertEquals(JsonParser.parseString("{\"@type\":\"g:Float\",\"@value\":0.5}"), GraphSon.V3.write(0.5f));
        assertEquals(JsonParser.parseString("{\"@type\":\"gx:BigDecimal\",\"@value\":1.5}"), GraphSon.V3.write(new BigDecimal("1.5")));
        assertEquals(JsonParser.parseString("{\"@type\":\"g:List\",\"@value\":[\"a\",{\"@type\":\"g:Int32\",\"@value\":2}]}"), GraphSon.V3.write(List.of("a", 2)));
        Map<Object, Object> m = new LinkedHashMap<>();
        m.put(G.T.id, 1L);
        m.put("k", "v");
        assertEquals(JsonParser.parseString("{\"@type\":\"g:Map\",\"@value\":[{\"@type\":\"g:T\",\"@value\":\"id\"},{\"@type\":\"g:Int64\",\"@value\":1},\"k\",\"v\"]}"),
                GraphSon.V3.write(m));
        // untyped: plain JSON with T tokens as strings
        assertEquals(JsonParser.parseString("{\"id\":1,\"k\":\"v\"}"), GraphSon.UNTYPED.write(m));
        // GraphSON 2.0: typed numbers, plain collections
        assertEquals(JsonParser.parseString("[{\"@type\":\"g:Int32\",\"@value\":1}]"), GraphSon.V2.write(List.of(1)));
    }

    @Test
    void graphSonVertexEdgePathRoundShape() {
        JsonElement v = GraphSon.V3.write(vertex());
        assertEquals("g:Vertex", v.getAsJsonObject().get("@type").getAsString());
        assertTrue(v.toString().contains("\"label\":\"name\""));
        G.Edge e = new G.Edge(7L, "knows", 1L, "person", 2L, "person");
        e.props.put("weight", 0.5d);
        String js = GraphSon.V3.write(e).toString();
        assertTrue(js.contains("\"inVLabel\":\"person\"") && js.contains("\"g:Property\""));
        G.Path p = new G.Path(new ArrayList<>(List.of("a", 1)), new ArrayList<>(List.of(Set.of("x"), Set.of())));
        assertTrue(GraphSon.V3.write(p).toString().contains("\"g:Path\""));
    }

    @Test
    void graphSonReadsBytecodeAndPredicates() {
        String bc = "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"has\",\"age\",{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"gt\",\"value\":"
                + "{\"@type\":\"g:Int32\",\"@value\":28}}}],[\"order\"],[\"by\",\"age\",{\"@type\":\"g:Order\",\"@value\":\"desc\"}]]}}";
        G.Bytecode b = (G.Bytecode) GraphSon.V3.read(JsonParser.parseString(bc));
        assertEquals(4, b.steps.size());
        G.P pred = (G.P) b.steps.get(1)[2];
        assertEquals("gt", pred.op);
        assertEquals(28, pred.value());
        assertEquals(G.Order.desc, b.steps.get(3)[2]);
    }

    private static Object roundTrip(Object v) {
        GraphBinary.Out o = new GraphBinary.Out();
        GraphBinary.write(o, v);
        ByteBuffer buf = ByteBuffer.wrap(o.toBytes());
        Object r = GraphBinary.read(buf);
        assertEquals(0, buf.remaining(), "unread bytes for " + v);
        return r;
    }

    @Test
    void graphBinaryRoundTripsScalarsAndCollections() {
        UUID u = UUID.randomUUID();
        for (Object v : new Object[] {1, 1L, 1.5d, 1.5f, "héllo", true, (short) 3, (byte) 4, new BigDecimal("12.345"), new BigInteger("123456789012345678901234567890"),
                u, new Date(1234567890123L), 'c'}) {
            assertEquals(v, roundTrip(v));
        }
        assertEquals(List.of(1, "a", List.of(2L)), roundTrip(List.of(1, "a", List.of(2L))));
        Set<Object> s = new LinkedHashSet<>(List.of("x", "y"));
        assertEquals(s, roundTrip(s));
        Map<Object, Object> m = new LinkedHashMap<>();
        m.put("a", 1);
        m.put(G.T.id, 2L);
        assertEquals(m, roundTrip(m));
        assertEquals(null, roundTrip(null));
    }

    @Test
    void graphBinaryElementsAndRequests() {
        G.Vertex v = (G.Vertex) roundTrip(vertex());
        assertEquals(1L, v.id);
        assertEquals("person", v.label);
        G.Edge e = (G.Edge) roundTrip(new G.Edge(7L, "knows", 1L, "person", 2L, "software"));
        assertEquals(2L, e.inId);
        assertEquals("software", e.inLabel);
        // a bytecode request as the drivers write it: V().has('age', gt(28))
        GraphBinary.Out o = new GraphBinary.Out();
        o.i8(0x81).i64(1).i64(2).str("bytecode").str("traversal").i32(2);
        GraphBinary.write(o, "gremlin");
        o.i8(GraphBinary.BYTECODE).i8(0).i32(2);
        o.str("V").i32(0);
        o.str("has").i32(2);
        GraphBinary.write(o, "age");
        o.i8(GraphBinary.P).i8(0).str("gt").i32(1);
        GraphBinary.write(o, 28);
        o.i32(0);
        GraphBinary.write(o, "aliases");
        Map<Object, Object> al = new LinkedHashMap<>();
        al.put("g", "g");
        GraphBinary.write(o, al);
        GraphBinary.Request r = GraphBinary.readRequest(ByteBuffer.wrap(o.toBytes()));
        assertEquals("bytecode", r.op);
        assertEquals("traversal", r.processor);
        G.Bytecode bc = (G.Bytecode) r.args.get("gremlin");
        assertEquals("gt", ((G.P) bc.steps.get(1)[2]).op);
        assertEquals(Map.of("g", "g"), r.args.get("aliases"));
    }

    @Test
    void responsesCarryStatusAndResults() {
        byte[] b = GraphBinary.response(UUID.randomUUID(), 200, "", new LinkedHashMap<>(), new LinkedHashMap<>(), List.of(1, 2), true, false);
        ByteBuffer buf = ByteBuffer.wrap(b);
        assertEquals(0x81, buf.get() & 0xff);
        assertEquals(0, buf.get()); // requestId present
        buf.getLong();
        buf.getLong();
        assertEquals(200, buf.getInt());
    }
}
