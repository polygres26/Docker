package com.nexagres.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nexagres.wire.rollup.RollupConfig;
import com.nexagres.wire.rollup.RollupDefinition;
import com.nexagres.wire.testsupport.WarpProcess;
import com.nexagres.wire.testsupport.RealPostgres;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@code POST /api/rollup-suggestions/draft} proposes a real, valid rollup
 * definition WITHOUT ever writing to {@code warp_config} -- real Warp subprocess, real
 * disposable Postgres, real admin HTTP API, real (local, scripted) LLM endpoint. Same discipline
 * as {@code QosSuggestionDraftIntegrationTest}, whose two proofs this mirrors: (1) config is
 * genuinely unchanged right after drafting, and (2) the draft's own
 * {@code rollupDefinitionsYamlIfApplied} field, applied via the real {@code PUT /api/config},
 * reads back exactly as applied -- and, checked directly with the real
 * {@code RollupConfig.parse}, is actually a valid, complete rollup definition.
 */
class RollupSuggestionDraftIntegrationTest {

    private static final String ADMIN_TOKEN = "rollup-draft-test-token";

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
    void draftingARollupSuggestionNeverWritesToConfigAndProducesAValidDefinition() throws Exception {
        String llmJsonReply = "{"
                + "\"name\":\"orders_by_day\","
                + "\"backend\":\"default\","
                + "\"sourceTable\":\"orders\","
                + "\"groupBy\":[\"order_date\"],"
                + "\"aggregations\":[\"SUM(amount) AS total_amount\",\"COUNT(*) AS order_count\"],"
                + "\"refreshIntervalMinutes\":15,"
                + "\"maxStalenessMinutes\":30,"
                + "\"rationale\":\"this GROUP BY shape runs frequently and is expensive\""
                + "}";

        try (FakeLlmServer llm = new FakeLlmServer(llmJsonReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-rollup-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            HttpResponse<String> configBefore = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(200, configBefore.statusCode());

            HttpResponse<String> draftResp = call("POST", warp.metricsPort(), "/api/rollup-suggestions/draft", "{}");
            assertEquals(200, draftResp.statusCode(), "draft request body: " + draftResp.body());
            JsonObject draftBody = JsonParser.parseString(draftResp.body()).getAsJsonObject();
            assertFalse(draftBody.get("applied").getAsBoolean(), "a draft must never report itself as applied");
            String yamlIfApplied = draftBody.get("rollupDefinitionsYamlIfApplied").getAsString();
            assertTrue(yamlIfApplied.contains("orders_by_day"), "expected the drafted rollup name in the candidate YAML");

            // Proof #1: config genuinely unchanged right after drafting.
            HttpResponse<String> configAfterDraft = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(configBefore.body(), configAfterDraft.body(),
                    "drafting a rollup suggestion must not have touched warp_config at all");

            // Proof #2: the candidate YAML is really a valid, complete rollup definition -- checked
            // directly with the real parser, not just "the string looks plausible".
            List<RollupDefinition> parsed = RollupConfig.parse(yamlIfApplied);
            assertEquals(1, parsed.size());
            RollupDefinition def = parsed.get(0);
            assertEquals("orders_by_day", def.name());
            assertEquals("orders", def.sourceTable());
            assertEquals(List.of("order_date"), def.groupByColumns());
            assertEquals(15, def.refreshIntervalMinutes());
            assertEquals(30, def.maxStalenessMinutes());

            // Proof #3: applying it via the real PUT /api/config reads back exactly as applied.
            JsonObject putBody = new JsonObject();
            putBody.addProperty("rollupDefinitionsYaml", yamlIfApplied);
            HttpResponse<String> putResp = call("PUT", warp.metricsPort(), "/api/config", putBody.toString());
            assertEquals(200, putResp.statusCode(), "applying the draft's own field via PUT /api/config must succeed: " + putResp.body());

            HttpResponse<String> configAfterApply = call("GET", warp.metricsPort(), "/api/config", null);
            JsonObject finalConfig = JsonParser.parseString(configAfterApply.body()).getAsJsonObject();
            assertEquals(yamlIfApplied, finalConfig.get("rollupDefinitionsYaml").getAsString(),
                    "the applied config must read back exactly the YAML the draft proposed");
        }
    }
}
