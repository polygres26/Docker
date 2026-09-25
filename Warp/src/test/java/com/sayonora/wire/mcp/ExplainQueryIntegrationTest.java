package com.sayonora.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sayonora.wire.testsupport.WarpProcess;
import com.sayonora.wire.testsupport.RealPostgres;
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
 * End-to-end proof of the MCP {@code explain_query} tool -- real Warp subprocess, real
 * disposable Postgres, real MCP JSON-RPC calls, and a real (local, scripted) LLM endpoint. No
 * mocks. The safest of this series' MCP tools (pure narration of a fact Postgres itself computed),
 * so the two things proved are correspondingly simple: (1) a real SELECT gets a real EXPLAIN plan
 * back, narrated by the LLM; (2) a non-SELECT is refused before anything runs -- since
 * {@code analyze=true} genuinely executes the statement, this tool's read-only gate matters just
 * as much as {@code query_natural_language}'s does.
 */
class ExplainQueryIntegrationTest {

    private static final String ADMIN_TOKEN = "explain-test-token";

    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;

        FakeLlmServer(String reply) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
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

    private static HttpResponse<String> mcpCall(int mcpPort, JsonObject arguments, String toolName) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", 1);
        req.addProperty("method", "tools/call");
        req.add("params", params);
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return http.send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aRealSelectGetsARealPlanNarratedByTheLlm() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("This scans the orders table sequentially since there is no matching index.");
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-explain-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99)");
            }

            JsonObject args = new JsonObject();
            args.addProperty("sql", "SELECT * FROM orders");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), args, "explain_query");
            assertEquals(200, resp.statusCode());
            // A real Postgres EXPLAIN (FORMAT JSON) plan always names a "Plan" node.
            assertTrue(resp.body().contains("\\\"Plan\\\"") || resp.body().contains("Plan"),
                    "expected a real EXPLAIN plan in the response -- got: " + resp.body());
            assertTrue(resp.body().contains("sequentially") || resp.body().contains("index"),
                    "expected the LLM's narration in the response -- got: " + resp.body());
        }
    }

    @Test
    void aWriteStatementIsRefusedBeforeAnythingRuns() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("irrelevant");
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-explain-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99)");
            }

            JsonObject args = new JsonObject();
            args.addProperty("sql", "DELETE FROM orders");
            args.addProperty("analyze", "true");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), args, "explain_query");
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("only accepts a read-only SELECT"),
                    "expected the deterministic refusal -- got: " + resp.body());

            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement();
                    var rs = st.executeQuery("SELECT count(*) FROM orders")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "the DELETE must never have actually run");
            }
        }
    }
}
