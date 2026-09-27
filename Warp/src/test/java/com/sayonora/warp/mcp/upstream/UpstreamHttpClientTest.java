package com.sayonora.warp.mcp.upstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link UpstreamHttpClient} (JSON-RPC-over-HTTP relay + error translation) against a
 * minimal, hand-rolled JSON-RPC server -- NOT the real fake-upstream Python fixture (that is
 * covered end-to-end by tests/python/test_mcp_gateway.py against a real Warp process); this is the
 * fast, dependency-free Java-side unit coverage for the transport itself.
 */
class UpstreamHttpClientTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/mcp", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JsonObject req = JsonParser.parseString(body).getAsJsonObject();
            String method = req.get("method").getAsString();
            JsonObject resp = new JsonObject();
            resp.addProperty("jsonrpc", "2.0");
            resp.add("id", req.get("id"));
            switch (method) {
                case "initialize" -> {
                    JsonObject result = new JsonObject();
                    result.addProperty("protocolVersion", "2025-06-18");
                    resp.add("result", result);
                }
                case "tools/list" -> {
                    JsonObject result = new JsonObject();
                    com.google.gson.JsonArray tools = new com.google.gson.JsonArray();
                    JsonObject t = new JsonObject();
                    t.addProperty("name", "get-weather");
                    t.addProperty("description", "Gets the weather");
                    tools.add(t);
                    result.add("tools", tools);
                    resp.add("result", result);
                }
                case "tools/call" -> {
                    JsonObject params = req.getAsJsonObject("params");
                    if ("boom".equals(params.get("name").getAsString())) {
                        JsonObject error = new JsonObject();
                        error.addProperty("code", -32010);
                        error.addProperty("message", "boom failed upstream-side");
                        resp.add("error", error);
                    } else {
                        JsonObject result = new JsonObject();
                        result.addProperty("isError", false);
                        com.google.gson.JsonArray content = new com.google.gson.JsonArray();
                        JsonObject c = new JsonObject();
                        c.addProperty("type", "text");
                        c.addProperty("text", "echo:" + params.get("arguments").getAsJsonObject().get("q").getAsString());
                        content.add(c);
                        result.add("content", content);
                        resp.add("result", result);
                    }
                }
                default -> {
                    JsonObject error = new JsonObject();
                    error.addProperty("code", -32601);
                    error.addProperty("message", "method not found");
                    resp.add("error", error);
                }
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Mcp-Session-Id", "sess-abc");
            byte[] out = resp.toString().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private McpUpstream upstream() {
        Instant now = Instant.now();
        return new McpUpstream("mup_1", "Fake", "fake", "http://127.0.0.1:" + port + "/mcp",
                McpUpstream.Transport.STREAMABLE_HTTP, McpUpstream.AuthMode.NONE, true, false,
                null, null, null, null, null, null, null, null, null, "unknown", null, 0, now, now);
    }

    @Test
    void listToolsReturnsUpstreamDefsVerbatim() throws Exception {
        UpstreamHttpClient client = new UpstreamHttpClient(upstream(), Duration.ofSeconds(5));
        client.initialize();
        var tools = client.listTools();
        assertEquals(1, tools.size());
        assertEquals("get-weather", tools.get(0).getAsJsonObject().get("name").getAsString());
    }

    @Test
    void callToolRelaysResultVerbatim() throws Exception {
        UpstreamHttpClient client = new UpstreamHttpClient(upstream(), Duration.ofSeconds(5));
        client.initialize();
        JsonObject args = new JsonObject();
        args.addProperty("q", "hello");
        JsonObject result = client.callTool("get-weather", args);
        assertEquals("echo:hello", result.getAsJsonArray("content").get(0).getAsJsonObject().get("text").getAsString());
    }

    @Test
    void callToolTranslatesUpstreamJsonRpcErrorIntoException() throws Exception {
        UpstreamHttpClient client = new UpstreamHttpClient(upstream(), Duration.ofSeconds(5));
        client.initialize();
        UpstreamRpcException e = assertThrows(UpstreamRpcException.class,
                () -> client.callTool("boom", new JsonObject()));
        assertEquals(-32010, e.code());
        assertTrue(e.getMessage().contains("boom failed upstream-side"));
    }

    @Test
    void gatewayMergesNamespacedToolsAndRoutesCallToTheRightUpstream() throws Exception {
        McpUpstreamGateway gw = new McpUpstreamGateway(java.time.Clock.systemUTC());
        McpUpstream u = upstream();
        gw.load(McpUpstream.serialize(List.of(u)));
        var namespaced = gw.namespacedToolsFor(List.of(u.id()));
        assertEquals(1, namespaced.size());
        assertEquals("fake_get_weather", namespaced.get(0).get("name").getAsString());
        assertTrue(namespaced.get(0).get("description").getAsString().contains("via MCP upstream"));

        McpUpstreamGateway.CallTarget target = gw.resolve("fake_get_weather", List.of(u.id()));
        assertEquals("get-weather", target.originalToolName());
        // get-weather carries no annotations.readOnlyHint from the fake server above, so it must
        // resolve as NOT read-only (governance treats unknown-safety upstream tools as mutating).
        assertEquals(false, target.readOnlyHint());
        JsonObject args = new JsonObject();
        args.addProperty("q", "world");
        JsonObject result = gw.callTool(target, args);
        assertEquals("echo:world", result.getAsJsonArray("content").get(0).getAsJsonObject().get("text").getAsString());
    }
}
