package com.nexagres.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that {@link QueryRepairStage} actually repairs a real Postgres rejection, not
 * just a unit-level check of its retry logic -- a real Warp subprocess, a real disposable
 * Postgres, real pgwire/JDBC, and a real (local, scripted, not a mock object) HTTP server
 * standing in for the LLM endpoint, since {@code TranslationLlmClient} talks any OpenAI-chat-
 * completions-shaped HTTP endpoint and there is no free, deterministic, offline real LLM to point
 * a CI run at. Same "no mocks" discipline as the rest of this suite -- this is a real HTTP server
 * receiving a real HTTP request and returning a real (if scripted) HTTP response, the same
 * category of realism {@code RealPostgres} provides for the database side, not a stand-in object
 * for {@code TranslationLlmClient} itself.
 *
 * <p>Deliberately provokes a same-dialect failure (a plain pgwire client, no Oracle/MySQL
 * involved) so {@link DialectTranslationStage}'s own, separate LLM fallback -- which only ever
 * fires on a dialect MISMATCH -- structurally cannot be what fixes this; only
 * {@link QueryRepairStage}, reacting to the real SQLSTATE Postgres returned, can.
 */
class QueryRepairIntegrationTest {

    private static final String ADMIN_TOKEN = "query-repair-test-token";

    /** A local OpenAI-chat-completions-shaped HTTP server that always answers with the same
     * canned repaired SQL, regardless of what it's asked -- realistic enough to exercise
     * {@link TranslationLlmClient}'s real HTTP request/response handling end to end, without
     * depending on a real network call to an actual LLM provider. */
    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicInteger requestCount = new AtomicInteger();

        FakeLlmServer(String repairedSql) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                requestCount.incrementAndGet();
                String escaped = repairedSql.replace("\\", "\\\\").replace("\"", "\\\"");
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

        int requestCount() {
            return requestCount.get();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static String metricsSummary(WarpProcess warp) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + warp.metricsPort() + "/api/metrics/summary"))
                .header("Authorization", "Bearer " + ADMIN_TOKEN)
                .timeout(Duration.ofSeconds(5))
                .GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    @Test
    void anUndefinedFunctionErrorIsRepairedAndReExecutedSuccessfully() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("SELECT COALESCE(1, 2) AS result");
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_QUERY_REPAIR_ENABLED", "true")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-repair-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    PreparedStatement ps = conn.prepareStatement(
                            // NVL is a real Oracle builtin, not part of plain Postgres -- a genuine
                            // 42883 undefined_function against RealPostgres's own vanilla instance
                            // (no pg_oracle extension installed here), reached via a PLAIN pgwire
                            // client so there is no dialect mismatch for DialectTranslationStage's
                            // own LLM fallback to ever act on.
                            "SELECT NVL(1, 2) AS result")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "the repaired query must return one row");
                    assertEquals(1, rs.getInt("result"),
                            "expected COALESCE(1, 2) = 1 (the LLM's repaired SQL, not NVL's own -- which "
                                    + "was never actually run against this vanilla Postgres)");
                }
            }

            assertEquals(1, llm.requestCount(), "expected exactly one LLM call -- one repair attempt per statement");

            String summary = metricsSummary(warp);
            assertTrue(summary.contains("\"queryRepair\":{\"attempted\":1,\"repaired\":1}"),
                    "expected the admin metrics endpoint to show exactly one attempted and one "
                            + "successful repair -- got: " + summary);
        }
    }

    @Test
    void aGenuinelyMissingTableIsNotRetriedThroughTheLlmAtAll() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("SELECT 1");
                RealPostgres postgres = RealPostgres.start();
                WarpProcess warp = WarpProcess.builder()
                        .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                        .frontend("pgwire", "WARP_PGWIRE_PORT")
                        .env("WARP_QUERY_REPAIR_ENABLED", "true")
                        .env("WARP_LLM_PROVIDER", "custom")
                        .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                        .env("WARP_LLM_MODEL", "test-repair-model")
                        .env("WARP_ADMIN_TOKEN", ADMIN_TOKEN)
                        .env("WARP_OTEL_ENDPOINT", "disabled")
                        .start()) {

            try (Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + warp.port("pgwire") + "/postgres", postgres.username(), postgres.password());
                    Statement st = conn.createStatement()) {
                // 42P01 undefined_table -- deliberately excluded from QueryRepairStage's
                // REPAIRABLE_SQLSTATES, since no LLM rewrite can make a genuinely nonexistent table
                // exist. Confirms the exclusion actually works end to end, not just in a unit test
                // of the constant itself.
                SQLException thrown = assertThrows(SQLException.class,
                        () -> st.executeQuery("SELECT * FROM this_table_genuinely_does_not_exist_anywhere"));
                assertEquals("42P01", thrown.getSQLState());
            }

            assertEquals(0, llm.requestCount(),
                    "a genuinely missing table must never trigger an LLM repair attempt");

            String summary = metricsSummary(warp);
            assertFalse(summary.contains("\"queryRepair\":{\"attempted\":1"),
                    "expected zero repair attempts recorded -- got: " + summary);
        }
    }
}
