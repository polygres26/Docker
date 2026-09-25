package com.sayonora.wire.a2a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.testsupport.RealPostgres;
import com.sayonora.wire.testsupport.WarpProcess;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof of the real A2A (Agent2Agent) frontend -- real Warp subprocess, real
 * disposable Postgres, a real A2A JSON-RPC {@code message/send} call, and a real (local, scripted)
 * LLM endpoint standing in for the drafter/judge (same harness pattern as {@code
 * com.sayonora.wire.mcp.Nl2SqlIntegrationTest}, MCP's own equivalent).
 *
 * <p>Proves three things: (1) the Agent Card at {@code /.well-known/agent-card.json} is real,
 * fetchable JSON describing this agent's one real skill; (2) a plain-English {@code message/send}
 * request gets back a completed Task whose artifact contains the REAL query result, not a canned
 * response; (3) A2A traffic goes through the exact same governed pipeline MCP's {@code
 * query_natural_language} tool does -- a judge-proposed write is refused deterministically, same
 * as {@code Nl2SqlIntegrationTest.aJudgeProposedWriteIsRefusedDeterministically} proves for MCP.
 */
class A2AMessageSendIntegrationTest {

    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;
        private final String draftReply;
        private final String judgeReply;

        FakeLlmServer(String draftReply, String judgeReply) throws Exception {
            this.draftReply = draftReply;
            this.judgeReply = judgeReply;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                String reply = requestBody.contains("SQL judge reviewing") ? judgeReply : draftReply;
                String escaped = reply.replace("\\", "\\\\").replace("\"", "\\\"");
                byte[] bytes = ("{\"choices\":[{\"message\":{\"content\":\"" + escaped + "\"}}]}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("content-type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            });
            server.start();
        }

        int port() {
            return server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static HttpResponse<String> a2aMessageSend(int a2aPort, String question) throws Exception {
        JsonObject part = new JsonObject();
        part.addProperty("kind", "text");
        part.addProperty("text", question);
        com.google.gson.JsonArray parts = new com.google.gson.JsonArray();
        parts.add(part);
        JsonObject message = new JsonObject();
        message.addProperty("role", "user");
        message.addProperty("messageId", "test-message-1");
        message.add("parts", parts);
        JsonObject params = new JsonObject();
        params.add("message", message);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "message/send");
        req.add("params", params);
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + a2aPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return http.send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void agentCardIsRealAndDescribesTheOneRealSkill() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("a2a", "WARP_A2A_PORT")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            HttpClient http = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder(
                            URI.create("http://localhost:" + warp.port("a2a") + "/.well-known/agent-card.json"))
                    .timeout(Duration.ofSeconds(5)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, resp.statusCode());
            JsonObject card = JsonParser.parseString(resp.body()).getAsJsonObject();
            assertEquals("Warp", card.get("name").getAsString());
            assertTrue(card.has("skills") && card.getAsJsonArray("skills").size() == 1,
                    "expected exactly the one real skill this agent offers -- got: " + resp.body());
            assertEquals("query_database", card.getAsJsonArray("skills").get(0).getAsJsonObject().get("id").getAsString());
            assertTrue(card.getAsJsonObject("capabilities").get("streaming").getAsBoolean() == false,
                    "streaming must be advertised as unsupported, not silently ignored");
        }
    }

    @Test
    void messageSendReturnsARealQueryResultThroughTheGovernedPipeline() throws Exception {
        String judgeReply = "{\"corrected\":false,\"sql\":\"SELECT count(*) FROM orders\",\"reasoning\":\"looks right\"}";
        try (FakeLlmServer llm = new FakeLlmServer("SELECT count(*) FROM orders", judgeReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("a2a", "WARP_A2A_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-a2a-model")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99), (2, 44.50), (3, 10.00)");
            }

            HttpResponse<String> resp = a2aMessageSend(warp.port("a2a"), "how many orders are there?");
            assertEquals(200, resp.statusCode());
            JsonObject envelope = JsonParser.parseString(resp.body()).getAsJsonObject();
            JsonObject task = envelope.getAsJsonObject("result");
            assertEquals("completed", task.getAsJsonObject("status").get("state").getAsString(),
                    "expected a completed task -- got: " + resp.body());
            String artifactText = task.getAsJsonArray("artifacts").get(0).getAsJsonObject()
                    .getAsJsonArray("parts").get(0).getAsJsonObject().get("text").getAsString();
            assertTrue(artifactText.contains("3"),
                    "expected the real count (3) from the real query in the returned artifact -- got: " + artifactText);
        }
    }

    @Test
    void aJudgeProposedWriteIsRefusedDeterministicallyThroughA2ATooToo() throws Exception {
        String judgeReply = "{\"corrected\":true,\"sql\":\"DELETE FROM orders\",\"reasoning\":\"cleaning up\"}";
        try (FakeLlmServer llm = new FakeLlmServer("SELECT count(*) FROM orders", judgeReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("a2a", "WARP_A2A_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-a2a-model")
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99)");
            }

            HttpResponse<String> resp = a2aMessageSend(warp.port("a2a"), "delete all the orders");
            assertEquals(200, resp.statusCode());
            JsonObject envelope = JsonParser.parseString(resp.body()).getAsJsonObject();
            JsonObject task = envelope.getAsJsonObject("result");
            assertEquals("failed", task.getAsJsonObject("status").get("state").getAsString(),
                    "expected the deterministic read-only refusal to surface as a failed task -- got: " + resp.body());

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement();
                    var rs = st.executeQuery("SELECT count(*) FROM orders")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "the judge's proposed DELETE must never have actually run");
            }
        }
    }
}
