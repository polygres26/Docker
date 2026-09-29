package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.testsupport.RealPostgres;
import com.sayonora.warp.testsupport.WarpProcess;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real, live, end-to-end proof that {@link DialectTranslationStage}'s own LLM fallback -- the path
 * taken when {@link DialectTranslations}'s deterministic AST/regex rewriter cannot handle a
 * statement and returns {@code null} -- actually fires, actually applies the LLM's translated SQL,
 * and is genuinely cached (a second identical statement does NOT call the LLM a second time).
 *
 * <p>Real, confirmed gap this closes (2026-09-29, found while surveying test coverage for "every
 * feature needs to be tested and documented"): this exact path had ZERO test coverage anywhere in
 * this suite. {@code QueryRepairIntegrationTest} already uses a real {@code FakeLlmServer} (the
 * same technique reused here) but its own javadoc explicitly says it "deliberately provokes a
 * same-dialect failure ... so DialectTranslationStage's own, separate LLM fallback -- which only
 * ever fires on a dialect MISMATCH -- structurally cannot be what fixes this" -- i.e. that test
 * exists specifically to rule OUT this code path, not exercise it. No other test drives it either.
 *
 * <p>Uses Oracle's {@code CONNECT BY} hierarchical-query syntax as the deliberately untranslatable
 * construct -- {@code DialectTranslationsTest.connectByIsNotSilentlyPassedThroughUnchanged} already
 * proves the deterministic rewriter returns {@code null} for it (Postgres has no equivalent
 * syntax), which is exactly what makes {@code DialectTranslationStage} fall through to the LLM.
 */
class DialectTranslationLlmFallbackIntegrationTest {

    /** Same technique as {@code QueryRepairIntegrationTest.FakeLlmServer} -- a real, local,
     * scripted HTTP server standing in for an OpenAI-chat-completions-shaped LLM endpoint. */
    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicInteger requestCount = new AtomicInteger();

        FakeLlmServer(String translatedSql) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                requestCount.incrementAndGet();
                String escaped = translatedSql.replace("\\", "\\\\").replace("\"", "\\\"");
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

    @Test
    @Timeout(120)
    void connectByFallsThroughToTheLlmAndTheResultIsCachedNotReRequested() throws Exception {
        // The LLM's scripted response stands in for a real CONNECT BY -> Postgres translation
        // (e.g. a recursive CTE) -- what matters for this test is proving the LLM's OWN returned
        // SQL is what actually executes against the real backend, not re-implementing a faithful
        // hierarchical-query translation.
        String llmTranslatedSql = "SELECT id, val FROM tree_it ORDER BY id";

        try (FakeLlmServer llm = new FakeLlmServer(llmTranslatedSql);
                RealPostgres postgres = RealPostgres.start()) {

            try (Connection setup = DriverManager.getConnection(postgres.jdbcUrl(), postgres.username(), postgres.password());
                    Statement stmt = setup.createStatement()) {
                stmt.execute("CREATE TABLE tree_it (id INT PRIMARY KEY, parent_id INT, val TEXT)");
                stmt.execute("INSERT INTO tree_it VALUES (1, NULL, 'root')");
                stmt.execute("INSERT INTO tree_it VALUES (2, 1, 'child-a')");
                stmt.execute("INSERT INTO tree_it VALUES (3, 1, 'child-b')");
            }

            try (WarpProcess warp = WarpProcess.builder()
                    .pgBackend(postgres.host(), postgres.port(), postgres.database(), postgres.username(), postgres.password())
                    .frontend("orawire", "WARP_ORAWIRE_PORT")
                    .env("WARP_LLM_BASE_URL", "http://127.0.0.1:" + llm.port() + "/v1")
                    .env("WARP_OTEL_ENDPOINT", "disabled")
                    .start()) {

                String url = "jdbc:oracle:thin:@//localhost:" + warp.port("orawire") + "/anything";
                try (Connection conn = DriverManager.getConnection(url, postgres.username(), postgres.password());
                        Statement st = conn.createStatement()) {

                    String connectByQuery = "SELECT id, val FROM tree_it CONNECT BY PRIOR id = parent_id "
                            + "START WITH parent_id IS NULL";

                    // First execution: the deterministic rewriter must return null for CONNECT BY
                    // (already proven at the unit level by DialectTranslationsTest), forcing
                    // DialectTranslationStage to call the LLM -- this is the first-ever live proof
                    // that call actually happens and its result is genuinely applied.
                    List<String> firstRun = new ArrayList<>();
                    try (ResultSet rs = st.executeQuery(connectByQuery)) {
                        while (rs.next()) {
                            firstRun.add(rs.getInt(1) + ":" + rs.getString(2));
                        }
                    }
                    assertEquals(List.of("1:root", "2:child-a", "3:child-b"), firstRun,
                            "the LLM's own scripted translation must be what actually executed -- "
                                    + "these exact rows only come back if 'SELECT id, val FROM "
                                    + "tree_it ORDER BY id' (the LLM's response) really ran, not "
                                    + "the original untranslatable CONNECT BY text");
                    assertEquals(1, llm.requestCount(),
                            "the LLM must have been called exactly once for this statement");

                    // Second, IDENTICAL execution: must be served from TranslationCache, not
                    // re-sent to the LLM -- proves the cache-hit path is genuinely wired into
                    // DialectTranslationStage's LLM fallback, not just the deterministic-rewriter
                    // path.
                    List<String> secondRun = new ArrayList<>();
                    try (ResultSet rs = st.executeQuery(connectByQuery)) {
                        while (rs.next()) {
                            secondRun.add(rs.getInt(1) + ":" + rs.getString(2));
                        }
                    }
                    assertEquals(firstRun, secondRun);
                    assertEquals(1, llm.requestCount(),
                            "an identical statement must be served from the translation cache -- "
                                    + "the LLM must NOT be called a second time");
                }
            }
        }
    }
}
