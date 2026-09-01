package com.nexagres.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
 * End-to-end proof that {@code POST /api/qos-suggestions/draft} proposes a QoS rate-limit change
 * WITHOUT ever writing to {@code warp_config} -- real Warp subprocess, real disposable
 * Postgres, real admin HTTP API, real (local, scripted) LLM endpoint, same discipline as
 * {@code FirewallRuleDraftIntegrationTest}.
 *
 * <p>The two things proved: (1) the draft is genuinely never applied -- {@code GET /api/config}
 * still shows the original QoS config right after drafting; (2) the draft's own
 * {@code qosClassLimitsIfApplied} field really is what {@code PUT /api/config}'s
 * {@code qosClassLimits} field accepts, by submitting it verbatim and reading it back unchanged.
 */
class QosSuggestionDraftIntegrationTest {

    private static final String ADMIN_TOKEN = "qos-draft-test-token";

    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;

        FakeLlmServer(String jsonReply) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                String escaped = jsonReply.replace("\\", "\\\\").replace("\"", "\\\"");
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

    private static HttpResponse<String> call(String method, int metricsPort, String path, String body) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://localhost:" + metricsPort + path))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .header("content-type", "application/json")
                .timeout(Duration.ofSeconds(10));
        builder = "GET".equals(method) ? builder.GET()
                : builder.method(method, HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void draftingAQosTuningChangeNeverWritesToConfig() throws Exception {
        String llmJsonReply = "{"
                + "\"target\":\"write\","
                + "\"ratePerSecond\":80.0,"
                + "\"burstCapacity\":160.0,"
                + "\"maxWaitMillis\":250,"
                + "\"rationale\":\"the default backend has headroom for more write throughput\""
                + "}";

        try (FakeLlmServer llm = new FakeLlmServer(llmJsonReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-qos-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            // Some real backend load for the draft's context to describe.
            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    Statement st = conn.createStatement()) {
                st.execute("SELECT 1");
                st.execute("SELECT 1");
            }

            HttpResponse<String> configBefore = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(200, configBefore.statusCode());

            HttpResponse<String> draftResp = call("POST", warp.metricsPort(), "/api/qos-suggestions/draft", "{}");
            assertEquals(200, draftResp.statusCode(), "draft request body: " + draftResp.body());
            JsonObject draftBody = JsonParser.parseString(draftResp.body()).getAsJsonObject();
            assertFalse(draftBody.get("applied").getAsBoolean(), "a draft must never report itself as applied");
            JsonObject draft = draftBody.getAsJsonObject("draft");
            assertEquals("write", draft.get("target").getAsString());
            assertEquals(80.0, draft.get("ratePerSecond").getAsDouble());
            String qosClassLimitsIfApplied = draftBody.get("qosClassLimitsIfApplied").getAsString();
            assertEquals("write:80.0:160.0:250", qosClassLimitsIfApplied);

            // Proof #1: config genuinely unchanged right after drafting.
            HttpResponse<String> configAfterDraft = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(configBefore.body(), configAfterDraft.body(),
                    "drafting a QoS suggestion must not have touched warp_config at all");

            // Proof #2: the draft's own field really is what PUT /api/config's qosClassLimits accepts.
            JsonObject putBody = new JsonObject();
            putBody.addProperty("qosClassLimits", qosClassLimitsIfApplied);
            HttpResponse<String> putResp = call("PUT", warp.metricsPort(), "/api/config", putBody.toString());
            assertEquals(200, putResp.statusCode(), "applying the draft's own field via PUT /api/config must succeed: " + putResp.body());

            HttpResponse<String> configAfterApply = call("GET", warp.metricsPort(), "/api/config", null);
            JsonObject finalConfig = JsonParser.parseString(configAfterApply.body()).getAsJsonObject();
            assertEquals(qosClassLimitsIfApplied, finalConfig.get("qosClassLimits").getAsString(),
                    "the applied config must read back exactly the string the draft proposed");
        }
    }
}
