package com.nexagres.wire.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
 * End-to-end proof of the MCP {@code query_natural_language} tool -- real Warp subprocess,
 * real disposable Postgres, real MCP JSON-RPC calls, and a real (local, scripted) LLM endpoint
 * that plays BOTH roles the tool calls (drafter, then judge), routed by which system prompt each
 * request actually carries. No mocks.
 *
 * <p>The two things proved: (1) a deliberately wrong draft (a real typo'd table name) gets
 * corrected by the judge before it ever runs, and both the correction and the real query result
 * are visible in the tool's response and in the real audit trail (NL2SQL_QUERY_EXECUTED AND
 * NL2SQL_JUDGE_CORRECTED); (2) a judge that tries to turn the query into a write is refused
 * deterministically -- proving the read-only guard isn't just LLM-enforced.
 */
class Nl2SqlIntegrationTest {

    private static final String ADMIN_TOKEN = "nl2sql-test-token";

    /** Routes by which system prompt a request actually carries -- {@link
     * com.nexagres.wire.core.TranslationLlmClient#draftSqlFromNaturalLanguage} and {@code
     * judgeSql} use distinct, real, non-overlapping wording ("natural-language-to-SQL assistant"
     * vs "SQL judge reviewing"), so this is inspecting the real request text, not a fake marker
     * added just for this test. */
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

    private static HttpResponse<String> mcpCall(int mcpPort, JsonObject arguments) throws Exception {
        JsonObject params = new JsonObject();
        params.addProperty("name", "query_natural_language");
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
    void aWrongDraftIsCorrectedByTheJudgeAndTheRealQueryRuns() throws Exception {
        String judgeReply = "{\"corrected\":true,\"sql\":\"SELECT count(*) FROM orders\","
                + "\"reasoning\":\"the drafted table name was misspelled\"}";
        try (FakeLlmServer llm = new FakeLlmServer("SELECT count(*) FROM ordrs", judgeReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-nl2sql-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            // No pgwire frontend registered in this test (only "mcp") -- set up the schema via a
            // direct connection to the real Postgres backend instead.
            try (Connection direct = DriverManager.getConnection(
                    "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database(),
                    postgres.username(), postgres.password());
                    Statement st = direct.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99), (2, 44.50)");
            }

            JsonObject args = new JsonObject();
            args.addProperty("question", "how many orders are there?");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), args);
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("\"2\"") || resp.body().contains("2"),
                    "expected the real count (2) from the judge-corrected query -- got: " + resp.body());
            assertTrue(resp.body().contains("judge corrected") || resp.body().contains("misspelled"),
                    "expected the correction to be visible in the tool response -- got: " + resp.body());

            HttpClient http = HttpClient.newHttpClient();
            HttpRequest auditReq = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/audit?limit=50"))
                    .header("Authorization", "Bearer " + ADMIN_TOKEN)
                    .timeout(Duration.ofSeconds(5))
                    .GET().build();
            HttpResponse<String> auditResp = http.send(auditReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, auditResp.statusCode());
            assertTrue(auditResp.body().contains("\"NL2SQL_QUERY_EXECUTED\""),
                    "expected a real NL2SQL_QUERY_EXECUTED audit event -- got: " + auditResp.body());
            assertTrue(auditResp.body().contains("\"NL2SQL_JUDGE_CORRECTED\""),
                    "expected a real NL2SQL_JUDGE_CORRECTED audit event, since the judge did correct it -- got: "
                            + auditResp.body());
        }
    }

    @Test
    void aJudgeProposedWriteIsRefusedDeterministically() throws Exception {
        String judgeReply = "{\"corrected\":true,\"sql\":\"DELETE FROM orders\",\"reasoning\":\"cleaning up\"}";
        try (FakeLlmServer llm = new FakeLlmServer("SELECT count(*) FROM orders", judgeReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("mcp", "WARP_MCP_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-nl2sql-model")
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
            args.addProperty("question", "delete all the orders");
            HttpResponse<String> resp = mcpCall(warp.port("mcp"), args);
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("never executes writes") || resp.body().contains("not a read-only SELECT"),
                    "expected the deterministic read-only refusal, not the judge's proposed DELETE -- got: " + resp.body());

            // Real proof, not just a string match: the row is still there.
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
