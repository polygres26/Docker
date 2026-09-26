package com.sayonora.wire.gremlinwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The WebSocket and HTTP endpoints over a heap graph, driven with the JDK's own clients. */
class GremlinWireServerTest {

    private GremlinWireServer server;
    private int port;

    @BeforeEach
    void start() throws Exception {
        server = new GremlinWireServer(0, new MemGraph(), null, null, () -> "test");
        server.start();
        port = server.port();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private static final class Ws implements WebSocket.Listener {
        final LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
        private final StringBuilder partial = new StringBuilder();

        @Override
        public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            partial.append(new String(b, StandardCharsets.UTF_8));
            if (last) {
                messages.add(partial.toString());
                partial.setLength(0);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                messages.add(partial.toString());
                partial.setLength(0);
            }
            ws.request(1);
            return null;
        }
    }

    private JsonObject eval(WebSocket ws, Ws l, String script, String extraArgs) throws Exception {
        String mime = "application/json";
        String msg = "{\"requestId\":\"" + UUID.randomUUID() + "\",\"op\":\"eval\",\"processor\":\"\",\"args\":{\"gremlin\":\"" + script.replace("\"", "\\\"")
                + "\",\"language\":\"gremlin-groovy\"" + extraArgs + "}}";
        byte[] m = mime.getBytes(StandardCharsets.UTF_8);
        byte[] body = msg.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + m.length + body.length);
        buf.put((byte) m.length).put(m).put(body).flip();
        ws.sendBinary(buf, true).get(5, TimeUnit.SECONDS);
        return JsonParser.parseString(l.messages.poll(10, TimeUnit.SECONDS)).getAsJsonObject();
    }

    @Test
    void evalOverWebSocketWithChunking() throws Exception {
        Ws l = new Ws();
        WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI.create("ws://localhost:" + port + "/gremlin"), l).get(5, TimeUnit.SECONDS);
        ws.request(1);
        JsonObject r = eval(ws, l, "1+1", "");
        assertEquals(200, r.getAsJsonObject("status").get("code").getAsInt());
        assertEquals("g:Int32", r.getAsJsonObject("result").getAsJsonObject("data").getAsJsonArray("@value").get(0).getAsJsonObject().get("@type").getAsString());
        for (int i = 0; i < 5; i++) {
            eval(ws, l, "g.addV('n').property(T.id," + (i + 1) + "L)", "");
        }
        JsonObject first = eval(ws, l, "g.V().id()", ",\"batchSize\":2");
        assertEquals(206, first.getAsJsonObject("status").get("code").getAsInt());
        JsonObject second = JsonParser.parseString(l.messages.poll(5, TimeUnit.SECONDS)).getAsJsonObject();
        assertEquals(206, second.getAsJsonObject("status").get("code").getAsInt());
        JsonObject third = JsonParser.parseString(l.messages.poll(5, TimeUnit.SECONDS)).getAsJsonObject();
        assertEquals(200, third.getAsJsonObject("status").get("code").getAsInt());
        assertEquals(204, eval(ws, l, "g.V().hasLabel('none')", "").getAsJsonObject("status").get("code").getAsInt());
        assertEquals(597, eval(ws, l, "g.V().foo()", "").getAsJsonObject("status").get("code").getAsInt());
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye");
    }

    @Test
    void httpEndpoint() throws Exception {
        HttpClient c = HttpClient.newHttpClient();
        HttpResponse<String> r = c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"gremlin\":\"[1,2,3].sum()\"}")).header("Content-Type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode());
        assertEquals(6, JsonParser.parseString(r.body()).getAsJsonObject().getAsJsonObject("result").getAsJsonArray("data").get(0).getAsInt());
        HttpResponse<String> bad = c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/")).POST(HttpRequest.BodyPublishers.ofString("{bad"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(400, bad.statusCode());
        HttpResponse<String> err = c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"gremlin\":\"g.V().foo()\"}")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(500, err.statusCode());
        assertTrue(err.body().contains("\"message\""));
        HttpResponse<String> empty = c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/?gremlin=g.V().count()")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, empty.statusCode());
        assertEquals(List.of(0), List.of(JsonParser.parseString(empty.body()).getAsJsonObject().getAsJsonObject("result").getAsJsonArray("data").get(0).getAsInt()));
    }
}
