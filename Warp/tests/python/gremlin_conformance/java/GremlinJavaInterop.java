import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;

/**
 * Talks to a Gremlin Server (the reference, or Warp) with the REAL TinkerPop Java message serializers (gremlin-util from the reference
 * image: GraphBinaryMessageSerializerV1 and GraphSONMessageSerializerV3) over a JDK WebSocket, sends script and bytecode requests, decodes the
 * responses with those serializers and prints one JSON line per check with the status code and a normalised rendering of every result.
 * (The Java gremlin-driver itself is not part of the reference image; this is the driver's serialization layer.)
 */
public class GremlinJavaInterop {

    static final class Sink implements WebSocket.Listener {
        final LinkedBlockingQueue<byte[]> in = new LinkedBlockingQueue<>();
        final java.io.ByteArrayOutputStream part = new java.io.ByteArrayOutputStream();

        public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            part.write(b, 0, b.length);
            if (last) {
                in.add(part.toByteArray());
                part.reset();
            }
            ws.request(1);
            return null;
        }

        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            part.writeBytes(data.toString().getBytes(StandardCharsets.UTF_8));
            if (last) {
                in.add(part.toByteArray());
                part.reset();
            }
            ws.request(1);
            return null;
        }
    }

    static String norm(Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof Traverser<?> t) {
            return norm(t.get());
        }
        if (o instanceof Vertex v) {
            List<String> props = new ArrayList<>();
            v.properties().forEachRemaining((VertexProperty<?> p) -> props.add(p.key() + "=" + norm(p.value())));
            java.util.Collections.sort(props);
            return "v[" + v.id() + "," + v.label() + "," + props + "]";
        }
        if (o instanceof Edge e) {
            List<String> props = new ArrayList<>();
            e.properties().forEachRemaining(p -> props.add(p.key() + "=" + norm(p.value())));
            java.util.Collections.sort(props);
            return "e[" + e.id() + "," + e.label() + "," + e.outVertex().id() + "->" + e.inVertex().id() + "," + props + "]";
        }
        if (o instanceof Path p) {
            List<String> l = new ArrayList<>();
            p.objects().forEach(x -> l.add(norm(x)));
            return "path" + l;
        }
        if (o instanceof Tree<?> t) {
            TreeMap<String, String> m = new TreeMap<>();
            ((Map<?, ?>) t).forEach((k, v) -> m.put(norm(k), norm(v)));
            return "tree" + m;
        }
        if (o instanceof Map<?, ?> m) {
            TreeMap<String, String> s = new TreeMap<>();
            m.forEach((k, v) -> s.put(norm(k), norm(v)));
            return s.toString();
        }
        if (o instanceof java.util.Set<?> st) {
            List<String> l = new ArrayList<>();
            st.forEach(x -> l.add(norm(x)));
            java.util.Collections.sort(l);
            return "set" + l;
        }
        if (o instanceof Iterable<?> it) {
            List<String> l = new ArrayList<>();
            it.forEach(x -> l.add(norm(x)));
            return l.toString();
        }
        if (o instanceof java.util.Date d) {
            return "Date:" + d.getTime();
        }
        return o.getClass().getSimpleName() + ":" + o;
    }

    /** One request: op, processor and arguments (the values are written by the reference's own GraphBinary / GraphSON writers). */
    record Req(String op, String processor, Map<String, Object> args) {
    }

    interface Case {
        Req make() throws Exception;
    }

    // The 3.8 Java RequestMessage serializers write the new (HTTP-era) request format, which this server generation does not read over
    // WebSocket; the op/processor/args envelope the drivers still send is written here, the VALUES by the real Java writers.
    static byte[] binaryRequest(Req r) throws Exception {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x81);
        UUID id = UUID.randomUUID();
        buf.writeLong(id.getMostSignificantBits());
        buf.writeLong(id.getLeastSignificantBits());
        for (String s : new String[] {r.op(), r.processor()}) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(b.length);
            buf.writeBytes(b);
        }
        buf.writeInt(r.args().size());
        GraphBinaryWriter w = new GraphBinaryWriter();
        Buffer nb = new NettyBufferFactory().create(buf);
        for (Map.Entry<String, Object> e : r.args().entrySet()) {
            w.write(e.getKey(), nb);
            w.write(e.getValue(), nb);
        }
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        return out;
    }

    static byte[] jsonRequest(Req r) throws Exception {
        org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper m = GraphSONMapper.build().version(GraphSONVersion.V3_0).create().createMapper();
        StringBuilder sb = new StringBuilder("{\"requestId\":{\"@type\":\"g:UUID\",\"@value\":\"" + UUID.randomUUID() + "\"},\"op\":" + jsonStr(r.op()) + ",\"processor\":"
                + jsonStr(r.processor()) + ",\"args\":{");
        boolean first = true;
        for (Map.Entry<String, Object> e : r.args().entrySet()) {
            sb.append(first ? "" : ",").append(jsonStr(e.getKey())).append(":").append(m.writeValueAsString(e.getValue()));
            first = false;
        }
        return sb.append("}}").toString().getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] argv) throws Exception {
        String url = argv[0];
        GraphTraversalSource g = AnonymousTraversalSource.traversal().withEmbedded(EmptyGraph.instance());
        Map<String, Case> cases = new LinkedHashMap<>();
        String[][] scripts = {
                {"count", "g.V().count()"},
                {"names", "g.V().has('name','marko').out('knows').values('name')"},
                {"valueMap", "g.V(1).valueMap(true)"},
                {"elementMap", "g.V(1).elementMap()"},
                {"vertices", "g.V().hasLabel('person').order().by('age')"},
                {"edges", "g.E().hasLabel('knows')"},
                {"weights", "g.E().values('weight')"},
                {"path", "g.V(1).outE().inV().path().by('name').by('weight').by('name')"},
                {"pathElements", "g.V(1).out().path()"},
                {"group", "g.V().group().by(label).by('name')"},
                {"groupCount", "g.V().groupCount().by(label)"},
                {"project", "g.V().hasLabel('person').order().by('age',desc).project('n','a').by('name').by('age')"},
                {"mean", "g.V().values('age').mean()"},
                {"sum", "g.V().values('age').sum()"},
                {"fold", "g.V().values('age').fold()"},
                {"tree", "g.V(1).out().tree()"},
                {"float", "1.5f"},
                {"long", "2L"},
                {"list", "[1,2,3]"},
                {"map", "[a:1,b:'x']"},
                {"date", "new Date(0)"},
                {"uuid", "UUID.fromString('11111111-1111-1111-1111-111111111111')"},
                {"bigdecimal", "1.5"},
                {"null", "null"},
                {"empty", "g.V().hasLabel('none')"},
                {"cap", "g.V().hasLabel('person').aggregate('x').cap('x')"},
                {"properties", "g.V(1).properties()"},
                {"vertexProperty", "g.V(1).properties('name').next()"},
                {"select", "g.V().hasLabel('person').as('a').out('created').as('b').select('a','b').by('name')"},
                {"error", "g.V().foo()"},
        };
        for (String[] s : scripts) {
            cases.put("eval:" + s[0], () -> new Req("eval", "", new LinkedHashMap<>(Map.of("gremlin", s[1], "language", "gremlin-groovy"))));
        }
        Map<String, Bytecode> bcs = new LinkedHashMap<>();
        bcs.put("names", g.V().has("name", "marko").out("knows").values("name").asAdmin().getBytecode());
        bcs.put("valueMap", g.V(1).valueMap(true).asAdmin().getBytecode());
        bcs.put("ordered", g.V().hasLabel("person").order().by("age", Order.desc).values("name").asAdmin().getBytecode());
        bcs.put("between", g.V().has("age", P.between(27, 33)).values("name").asAdmin().getBytecode());
        bcs.put("within", g.V().has("name", P.within("marko", "josh")).count().asAdmin().getBytecode());
        bcs.put("repeat", g.V(1).repeat(__.out()).times(2).path().by("name").asAdmin().getBytecode());
        bcs.put("group", g.V().group().by(T.label).by(__.count()).asAdmin().getBytecode());
        bcs.put("project", g.V().hasLabel("person").project("n", "c").by("name").by(__.out().count()).asAdmin().getBytecode());
        bcs.put("edge", g.E().hasLabel("knows").has("weight", P.gt(0.5d)).asAdmin().getBytecode());
        bcs.put("sack", g.withSack(1.0d).V(1).outE().sack(org.apache.tinkerpop.gremlin.process.traversal.Operator.mult).by("weight").inV().sack().asAdmin().getBytecode());
        bcs.put("choose", g.V().choose(__.hasLabel("person"), __.values("name"), __.values("lang")).asAdmin().getBytecode());
        bcs.put("unbulk", g.V().label().asAdmin().getBytecode());
        bcs.put("coalesce", g.V().coalesce(__.values("age"), __.constant(-1)).sum().asAdmin().getBytecode());
        bcs.put("unknown", g.V().asAdmin().getBytecode());
        for (Map.Entry<String, Bytecode> e : bcs.entrySet()) {
            cases.put("bytecode:" + e.getKey(), () -> {
                Map<String, Object> a = new LinkedHashMap<>();
                a.put("gremlin", e.getValue());
                a.put("aliases", Map.of("g", "g"));
                return new Req("bytecode", "traversal", a);
            });
        }
        AbstractMessageSerializer<?>[] sers = {new GraphBinaryMessageSerializerV1(), new GraphSONMessageSerializerV3()};
        Sink sink = new Sink();
        WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create(url), sink).get(10, TimeUnit.SECONDS);
        ws.request(1);
        for (AbstractMessageSerializer<?> ser : sers) {
            String mime = ser.mimeTypesSupported()[0];
            for (Map.Entry<String, Case> c : cases.entrySet()) {
                StringBuilder out = new StringBuilder();
                try {
                    Req req = c.getValue().make();
                    byte[] mb = mime.getBytes(StandardCharsets.UTF_8);
                    byte[] bb = ser instanceof GraphBinaryMessageSerializerV1 ? binaryRequest(req) : jsonRequest(req);
                    ByteBuffer frame = ByteBuffer.allocate(1 + mb.length + bb.length);
                    frame.put((byte) mb.length).put(mb).put(bb).flip();
                    ws.sendBinary(frame, true).get(10, TimeUnit.SECONDS);
                    List<String> results = new ArrayList<>();
                    int code;
                    String msg = "";
                    while (true) {
                        byte[] resp = sink.in.poll(30, TimeUnit.SECONDS);
                        if (resp == null) {
                            throw new IllegalStateException("timeout");
                        }
                        ResponseMessage rm = ser instanceof GraphBinaryMessageSerializerV1 gb ? gb.deserializeResponse(Unpooled.wrappedBuffer(resp))
                                : ((GraphSONMessageSerializerV3) ser).deserializeResponse(Unpooled.wrappedBuffer(resp));
                        code = rm.getStatus().getCode().getValue();
                        msg = rm.getStatus().getMessage();
                        Object data = rm.getResult().getData();
                        if (data instanceof List<?> l) {
                            for (Object o : l) {
                                if (o instanceof Traverser<?> t) {
                                    for (long i = 0; i < t.bulk(); i++) {
                                        results.add(norm(t.get()));
                                    }
                                } else {
                                    results.add(norm(o));
                                }
                            }
                        }
                        if (code != 206) {
                            break;
                        }
                    }
                    boolean unordered = !c.getKey().contains("ordered") && !c.getKey().equals("eval:vertices");
                    if (unordered) {
                        java.util.Collections.sort(results);
                    }
                    out.append("{\"ser\":\"").append(mime).append("\",\"case\":\"").append(c.getKey()).append("\",\"code\":").append(code).append(",\"message\":").append(code >= 400 ? jsonStr(msg) : "\"\"").append(",\"results\":")
                            .append(jsonList(results)).append("}");
                } catch (Exception ex) {
                    out.append("{\"ser\":\"").append(mime).append("\",\"case\":\"").append(c.getKey()).append("\",\"exception\":").append(jsonStr(ex.toString())).append("}");
                }
                System.out.println(out);
            }
        }
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
        System.exit(0);
    }

    static String jsonStr(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char ch : s.toCharArray()) {
            if (ch == '"' || ch == '\\') {
                sb.append('\\').append(ch);
            } else if (ch < 32) {
                sb.append(String.format("\\u%04x", (int) ch));
            } else {
                sb.append(ch);
            }
        }
        return sb.append('"').toString();
    }

    static String jsonList(List<String> l) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < l.size(); i++) {
            sb.append(i > 0 ? "," : "").append(jsonStr(l.get(i)));
        }
        return sb.append("]").toString();
    }
}
