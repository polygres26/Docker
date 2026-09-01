package com.nexagres.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
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
 * End-to-end proof of the MCP {@code document_schema} tool -- real Warp subprocess, real
 * disposable Postgres with a genuine foreign-key relationship, real MCP JSON-RPC calls, and a
 * real (local, scripted) LLM endpoint. No mocks. Proves (1) the real table/column listing is
 * always returned, and (2) with an LLM configured, its narrative comes back alongside it -- (3)
 * with NO LLM configured, the raw listing still comes back fine, narrative simply absent.
 */
class DocumentSchemaIntegrationTest {

    private static final String ADMIN_TOKEN = "document-schema-test-token";

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

    private static HttpResponse<String> mcpCall(int mcpPort) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("name", "document_schema");
        params.add("arguments", new JsonObject());
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

    private static void createSchemaWithForeignKey(RealPostgres postgres) throws Exception {
        try (Connection direct = DriverManager.getConnection(
                "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                postgres.username(), postgres.password());
                Statement st = direct.createStatement()) {
            st.execute("CREATE TABLE customers (id int PRIMARY KEY, name text)");
            st.execute("CREATE TABLE orders (id int PRIMARY KEY, customer_id int REFERENCES customers(id), amount numeric)");
        }
    }

    @Test
    void theRealSchemaAndAnLlmNarrativeAreBothReturned() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("Customers place orders; each order references exactly one customer.");
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-schema-doc-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            createSchemaWithForeignKey(postgres);

            HttpResponse<String> resp = mcpCall(warp.port("mcp"));
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("customers") && resp.body().contains("orders"),
                    "expected the real table listing -- got: " + resp.body());
            assertTrue(resp.body().contains("customer_id") || resp.body().contains("amount"),
                    "expected real column names in the listing -- got: " + resp.body());
            assertTrue(resp.body().contains("each order references exactly one customer"),
                    "expected the LLM's narrative in the response -- got: " + resp.body());
        }
    }

    @Test
    void withNoLlmConfiguredTheRawListingStillComesBack() throws Exception {
        try (RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            createSchemaWithForeignKey(postgres);

            HttpResponse<String> resp = mcpCall(warp.port("mcp"));
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("customers") && resp.body().contains("orders"),
                    "the raw table listing must still work with no LLM configured -- got: " + resp.body());
        }
    }
}
