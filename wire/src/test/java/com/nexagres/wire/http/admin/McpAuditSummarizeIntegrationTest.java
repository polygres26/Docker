package com.nexagres.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.testsupport.PolyWireProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that a real MCP {@code tools/call} actually reaches the audit log (a real gap
 * before this feature -- confirmed by grep before writing any of this: nothing in {@code
 * com.nexagres.wire.mcp} ever touched {@code AuditLog}) and that
 * {@code POST /api/mcp-audit/summarize} turns those real events into a plain-English summary via
 * a real (local, scripted) LLM endpoint -- same discipline as the other three drafting/narration
 * features. Real Polywire subprocess, real disposable Postgres, real MCP JSON-RPC calls over
 * HTTP, no mocks.
 */
class McpAuditSummarizeIntegrationTest {

    private static final String ADMIN_TOKEN = "mcp-audit-test-token";

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

    private static HttpResponse<String> mcpCall(int mcpPort, String method, JsonObject params, int id) throws Exception {
        JsonObject req = new JsonObject();
        req.addProperty("jsonrpc", "2.0");
        req.addProperty("id", id);
        req.addProperty("method", method);
        req.add("params", params);
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create("http://localhost:" + mcpPort + "/"))
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(req.toString()))
                .build();
        return http.send(httpReq, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonObject toolCallParams(String toolName, JsonObject arguments) {
        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        return params;
    }

    @Test
    void aRealMcpToolCallReachesTheAuditLogAndCanBeSummarized() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("The client ran a successful read and one failing query against a missing table.");
                RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "POLYWIRE_MCP_PORT")
                        .env("POLYWIRE_LLM_PROVIDER", "custom")
                        .env("POLYWIRE_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("POLYWIRE_LLM_MODEL", "test-mcp-model")
                        .env("POLYWIRE_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            int mcpPort = polywire.port("mcp");

            // One real successful tool call...
            JsonObject okArgs = new JsonObject();
            okArgs.addProperty("sql", "SELECT 1");
            HttpResponse<String> okResp = mcpCall(mcpPort, "tools/call", toolCallParams("execute_sql", okArgs), 1);
            assertEquals(200, okResp.statusCode());

            // ...and one real failure, against a table that genuinely doesn't exist.
            JsonObject failArgs = new JsonObject();
            failArgs.addProperty("sql", "SELECT * FROM this_table_genuinely_does_not_exist_anywhere");
            HttpResponse<String> failResp = mcpCall(mcpPort, "tools/call", toolCallParams("execute_sql", failArgs), 2);
            assertEquals(200, failResp.statusCode());

            // Real audit trail: GET /api/audit must show both MCP_TOOL_CALLED events now --
            // proof the gap (MCP traffic never reaching AuditLog) is actually closed.
            HttpClient http = HttpClient.newHttpClient();
            HttpRequest auditReq = HttpRequest.newBuilder(URI.create("http://localhost:" + polywire.metricsPort() + "/api/audit?limit=50"))
                    .header("Authorization", "Bearer " + ADMIN_TOKEN)
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();
            HttpResponse<String> auditResp = http.send(auditReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, auditResp.statusCode());
            assertTrue(auditResp.body().contains("\"MCP_TOOL_CALLED\""),
                    "expected at least one real MCP_TOOL_CALLED audit event -- got: " + auditResp.body());
            assertTrue(auditResp.body().contains("\"success\":\"false\""),
                    "expected the failing tool call to be recorded as a failure -- got: " + auditResp.body());

            // Now the actual feature: summarize those real events via the real (fake) LLM.
            HttpRequest summarizeReq = HttpRequest.newBuilder(URI.create("http://localhost:" + polywire.metricsPort() + "/api/mcp-audit/summarize"))
                    .header("Authorization", "Bearer " + ADMIN_TOKEN)
                    .header("content-type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            HttpResponse<String> summarizeResp = http.send(summarizeReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, summarizeResp.statusCode(), "summarize request body: " + summarizeResp.body());
            JsonObject summarizeBody = JsonParser.parseString(summarizeResp.body()).getAsJsonObject();
            assertEquals("The client ran a successful read and one failing query against a missing table.",
                    summarizeBody.get("summary").getAsString());
            assertTrue(summarizeBody.get("eventCount").getAsInt() >= 2,
                    "expected at least the two real tool calls in the summarized event count");
        }
    }
}
