package com.nexagres.wire.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@code POST /api/router-suggestions/draft} proposes a real table-sharding
 * rule WITHOUT ever writing to {@code warp_config}, and -- the safety property specific to
 * this draft endpoint -- REFUSES a proposal that names a backend that doesn't actually exist, even
 * though the LLM was given the real list. Real Warp subprocess (with a genuine second backend,
 * "shard1", registered alongside "default"), real disposable Postgres, real admin HTTP calls, real
 * (local, scripted) LLM endpoint. No mocks.
 */
class RouterSuggestionDraftIntegrationTest {

    private static final String ADMIN_TOKEN = "router-draft-test-token";

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

    private static WarpProcess.Builder baseBuilder(RealPostgres postgres, FakeLlmServer llm) {
        String jdbcUrl = "jdbc:postgresql://" + postgres.host() + ":" + postgres.port() + "/" + postgres.database();
        String backendEntry = jdbcUrl + "|" + postgres.username() + "|" + postgres.password();
        return WarpProcess.builder()
                .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                // BackendRegistry.fromConfig only auto-registers the implicit WARP_* target as
                // "default" when WARP_BACKENDS is UNSET entirely -- setting it at all (to add
                // "shard1") means "default" must be listed explicitly too, or nothing routes to it
                // (confirmed live: sqswire's own catalog connection threw "no default backend
                // registered" and the whole process never became ready, before this was added).
                // Both point at the same real Postgres instance here -- fine for proving the
                // registry has two real, distinct, resolvable names; the sharding math doesn't
                // care that they happen to share a database.
                .env("WARP_BACKENDS", "default=" + backendEntry + ";shard1=" + backendEntry)
                .env("WARP_LLM_PROVIDER", "custom")
                .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                .env("WARP_LLM_MODEL", "test-router-model")
                .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("WARP_OTEL_ENDPOINT", "disabled");
    }

    @Test
    void draftingATableShardSuggestionNeverWritesToConfig() throws Exception {
        String llmJsonReply = "{"
                + "\"table\":\"orders\","
                + "\"shardColumn\":\"customer_id\","
                + "\"backends\":[\"default\",\"shard1\"],"
                + "\"rationale\":\"default is carrying disproportionate write load\""
                + "}";

        try (FakeLlmServer llm = new FakeLlmServer(llmJsonReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = baseBuilder(postgres, llm).start()) {

            HttpResponse<String> configBefore = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(200, configBefore.statusCode());

            HttpResponse<String> draftResp = call("POST", warp.metricsPort(), "/api/router-suggestions/draft", "{}");
            assertEquals(200, draftResp.statusCode(), "draft request body: " + draftResp.body());
            JsonObject draftBody = JsonParser.parseString(draftResp.body()).getAsJsonObject();
            assertFalse(draftBody.get("applied").getAsBoolean(), "a draft must never report itself as applied");
            String candidate = draftBody.get("routerTableShardsIfApplied").getAsString();
            assertEquals("orders:hash:customer_id:default,shard1", candidate);

            // Proof #1: config genuinely unchanged right after drafting.
            HttpResponse<String> configAfterDraft = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(configBefore.body(), configAfterDraft.body(),
                    "drafting a router suggestion must not have touched warp_config at all");

            // Proof #2: applying it via the real PUT /api/config reads back exactly as applied.
            JsonObject putBody = new JsonObject();
            putBody.addProperty("routerTableShards", candidate);
            HttpResponse<String> putResp = call("PUT", warp.metricsPort(), "/api/config", putBody.toString());
            assertEquals(200, putResp.statusCode(), "applying the draft's own field via PUT /api/config must succeed: " + putResp.body());

            HttpResponse<String> configAfterApply = call("GET", warp.metricsPort(), "/api/config", null);
            JsonObject finalConfig = JsonParser.parseString(configAfterApply.body()).getAsJsonObject();
            assertEquals(candidate, finalConfig.get("routerTableShards").getAsString(),
                    "the applied config must read back exactly what the draft proposed");
        }
    }

    @Test
    void aHallucinatedBackendNameIsRejectedEvenThoughTheRealListWasGiven() throws Exception {
        String llmJsonReply = "{"
                + "\"table\":\"orders\","
                + "\"shardColumn\":\"customer_id\","
                + "\"backends\":[\"default\",\"ghost_shard_that_does_not_exist\"],"
                + "\"rationale\":\"spreading load\""
                + "}";

        try (FakeLlmServer llm = new FakeLlmServer(llmJsonReply);
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = baseBuilder(postgres, llm).start()) {

            HttpResponse<String> configBefore = call("GET", warp.metricsPort(), "/api/config", null);

            HttpResponse<String> draftResp = call("POST", warp.metricsPort(), "/api/router-suggestions/draft", "{}");
            assertEquals(502, draftResp.statusCode(),
                    "a proposal naming a backend that doesn't exist must be rejected, not silently accepted");
            assertTrue(draftResp.body().contains("ghost_shard_that_does_not_exist") || draftResp.body().contains("real configured list"),
                    "expected a clear error naming the problem -- got: " + draftResp.body());

            HttpResponse<String> configAfter = call("GET", warp.metricsPort(), "/api/config", null);
            assertEquals(configBefore.body(), configAfter.body(), "a rejected draft must not have touched config either");
        }
    }
}
