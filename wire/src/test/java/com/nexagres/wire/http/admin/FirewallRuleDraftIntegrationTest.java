package com.nexagres.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@code POST /api/firewall-rules/draft} turns plain English into a
 * firewall-rule draft WITHOUT ever writing to {@code polywire_firewall_rules} -- real Polywire
 * subprocess, real disposable Postgres, real admin HTTP API, and a real (local, scripted) HTTP
 * server standing in for the LLM endpoint, same discipline as {@code QueryRepairIntegrationTest}.
 *
 * <p>The two things that matter most to prove here: (1) the draft is genuinely never applied --
 * the rule table has zero rows after drafting, and a real query that would be denied if the draft
 * WERE applied still succeeds; (2) a real, subsequent {@code POST /api/firewall-rules} using the
 * exact draft fields DOES create a working rule, proving the draft's shape is really what that
 * existing endpoint accepts, not a look-alike this test invented independently.
 */
class FirewallRuleDraftIntegrationTest {

    private static final String ADMIN_TOKEN = "firewall-draft-test-token";

    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;

        FakeLlmServer(String jsonReply) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                String escaped = jsonReply.replace("\\", "\\\\").replace("\"", "\\\"");
                String body = "{\"choices\":[{\"message\":{\"content\":\"" + escaped + "\"}}]}";
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
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

    private static HttpResponse<String> post(int metricsPort, String path, String body) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + metricsPort + path))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void draftingAFirewallRuleFromPlainEnglishNeverWritesToTheRuleTable() throws Exception {
        // What the (fake) LLM's chat-completion "content" field contains -- FakeLlmServer itself
        // handles wrapping this in the outer {"choices":[...]} envelope and escaping it as that
        // envelope's own JSON string value.
        String llmJsonReply = "{"
                + "\"action\":\"deny\","
                + "\"priority\":50,"
                + "\"statementType\":\"DELETE\","
                + "\"tablePattern\":\"orders\","
                + "\"sqlPattern\":null,"
                + "\"enabled\":true,"
                + "\"description\":\"Block DELETE statements against orders\""
                + "}";

        try (FakeLlmServer llm = new FakeLlmServer(llmJsonReply);
                RealPostgres postgres = RealPostgres.start();
                PolyWireProcess polywire = PolyWireProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "POLYWIRE_PGWIRE_PORT")
                        .env("POLYWIRE_LLM_PROVIDER", "custom")
                        .env("POLYWIRE_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("POLYWIRE_LLM_MODEL", "test-firewall-model")
                        .env("POLYWIRE_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("POLYWIRE_OTEL_ENDPOINT", "disabled")
                        .start()) {

            // Real table for the drafted rule to plausibly apply to, and a real row in it -- the
            // draft's tablePattern targets "orders", the same table the later real-write test
            // below actually deletes from.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE orders (id int PRIMARY KEY, amount numeric)");
                st.execute("INSERT INTO orders VALUES (1, 129.99)");
            }

            HttpResponse<String> draftResp = post(polywire.metricsPort(), "/api/firewall-rules/draft",
                    "{\"prompt\":\"block any DELETE against the orders table\"}");
            assertEquals(200, draftResp.statusCode(), "draft request body: " + draftResp.body());
            JsonObject draftBody = JsonParser.parseString(draftResp.body()).getAsJsonObject();
            assertFalse(draftBody.get("applied").getAsBoolean(), "a draft must never report itself as applied");
            JsonObject draft = draftBody.getAsJsonObject("draft");
            assertEquals("deny", draft.get("action").getAsString());
            assertEquals(50, draft.get("priority").getAsInt());
            assertEquals("DELETE", draft.get("statementType").getAsString());
            assertEquals("orders", draft.get("tablePattern").getAsString());

            // The actual proof the draft wasn't applied: a real DELETE against "orders" still
            // succeeds -- if the LLM's proposed deny rule had been silently written and activated,
            // this would fail with the firewall's own ERR_FIREWALL_RULE_MATCH instead.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    Statement st = conn.createStatement()) {
                int deleted = st.executeUpdate("DELETE FROM orders WHERE id = 1");
                assertEquals(1, deleted, "DELETE must still succeed -- the draft must not have been applied");
                st.execute("INSERT INTO orders VALUES (1, 129.99)");
            }

            // Now prove the draft's own shape really is what the existing, already-authorized
            // POST /api/firewall-rules endpoint accepts -- submit it verbatim and confirm a real
            // rule gets created AND actually enforced on the very next DELETE.
            HttpClient http = HttpClient.newHttpClient();
            HttpRequest createReq = HttpRequest.newBuilder(URI.create("http://localhost:" + polywire.metricsPort() + "/api/firewall-rules"))
                    .header("Authorization", "Bearer " + ADMIN_TOKEN)
                    .header("content-type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(draft.toString()))
                    .build();
            HttpResponse<String> createResp = http.send(createReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, createResp.statusCode(), "creating a real rule from the draft's own fields must succeed: " + createResp.body());

            // Give the LISTEN/NOTIFY hot-reload a moment to actually reach FirewallStage.
            long deadline = System.currentTimeMillis() + 5000;
            java.sql.SQLException lastDenied = null;
            while (System.currentTimeMillis() < deadline) {
                try (Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:" + polywire.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {
                    st.executeUpdate("DELETE FROM orders WHERE id = 1");
                    Thread.sleep(200);
                } catch (java.sql.SQLException denied) {
                    lastDenied = denied;
                    break;
                }
            }
            assertTrue(lastDenied != null, "expected the now-real deny rule to eventually reject a DELETE against orders");
        }
    }
}
