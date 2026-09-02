package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Real (no mocks) end-to-end test of {@link AnomalyDetectionScheduler} against a REAL {@link
 * SqlMetricsCollector}/{@link StatsCollectorStage} pair -- these are pure in-process computation
 * with no I/O of their own (unlike the LLM narration step, which does get a real local HTTP
 * server, same as {@code QueryRepairIntegrationTest}), so a full Warp-subprocess integration
 * test would mostly exercise unrelated networking plumbing rather than this class's own logic.
 * Uses {@link AnomalyDetectionScheduler#forTesting}/{@code runCycle()} (package-private, same
 * reasoning {@code StatisticsScheduler} gives its own {@code runCycle()}) to drive cycles
 * deterministically instead of waiting on real minute-granularity scheduling.
 */
class AnomalyDetectionSchedulerTest {

    private static final class FakeLlmServer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicInteger requestCount = new AtomicInteger();
        private final String reply;

        FakeLlmServer(String reply) throws Exception {
            this.reply = reply;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/chat/completions", exchange -> {
                requestCount.incrementAndGet();
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

        int requestCount() {
            return requestCount.get();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static void recordOps(SqlMetricsCollector metrics, String protocol, int count) {
        for (int i = 0; i < count; i++) {
            metrics.recordOperation(protocol, "default", SqlMetricsCollector.StatementKind.READ,
                    protocol + ".op", 1_000_000L);
        }
    }

    @Test
    void aRealTrafficSpikeIsDetectedAndNarratedByTheRealLlmEndpoint() throws Exception {
        try (FakeLlmServer llm = new FakeLlmServer("mysql traffic just jumped well above its usual rate")) {
            SqlMetricsCollector metrics = new SqlMetricsCollector();
            StatsCollectorStage statsStage = new StatsCollectorStage(null, metrics);
            TranslationLlmClient llmClient = new TranslationLlmClient(null, "http://127.0.0.1:" + llm.port() + "/v1", "test-model");
            AnomalyDetectionScheduler scheduler = AnomalyDetectionScheduler.forTesting(statsStage, () -> llmClient, 3.0, 0.5);

            // Cycle 1: seeds lastCounts, no comparison possible yet.
            recordOps(metrics, "mysql", 2);
            scheduler.runCycle();
            assertTrue(scheduler.recentNotes(10).isEmpty(), "the very first cycle has nothing to compare against yet");

            // Cycle 2: establishes a low baseline rate (~2 ops in ~1s, floor-clamped elapsed time).
            recordOps(metrics, "mysql", 2);
            scheduler.runCycle();
            assertTrue(scheduler.recentNotes(10).isEmpty(), "a steady, unchanged rate must not be flagged");

            // Cycle 3: a real spike -- 40 more ops against a baseline of ~2/s is a real >=3x ratio.
            recordOps(metrics, "mysql", 40);
            scheduler.runCycle();

            List<AnomalyDetectionScheduler.AnomalyNote> notes = scheduler.recentNotes(10);
            assertEquals(1, notes.size(), "expected exactly one anomaly recorded for the spike cycle");
            AnomalyDetectionScheduler.AnomalyNote note = notes.get(0);
            assertEquals("mysql", note.protocol());
            assertTrue(note.ratio() >= 3.0, "expected a real >=3x ratio, got " + note.ratio());
            assertEquals("mysql traffic just jumped well above its usual rate", note.narrative());
            assertEquals(1, llm.requestCount(), "expected exactly one LLM call, for the one real anomaly");

            // Cycle 4: back to baseline-ish traffic -- must not re-flag every cycle forever.
            recordOps(metrics, "mysql", 2);
            scheduler.runCycle();
            assertEquals(1, scheduler.recentNotes(10).size(),
                    "a return to normal traffic must not add a second anomaly note");
        }
    }

    @Test
    void withNoLlmConfiguredTheRawAnomalyIsStillRecorded() {
        SqlMetricsCollector metrics = new SqlMetricsCollector();
        StatsCollectorStage statsStage = new StatsCollectorStage(null, metrics);
        AnomalyDetectionScheduler scheduler = AnomalyDetectionScheduler.forTesting(statsStage, () -> null, 3.0, 0.5);

        recordOps(metrics, "oracle", 2);
        scheduler.runCycle();
        recordOps(metrics, "oracle", 2);
        scheduler.runCycle();
        recordOps(metrics, "oracle", 40);
        scheduler.runCycle();

        List<AnomalyDetectionScheduler.AnomalyNote> notes = scheduler.recentNotes(10);
        assertEquals(1, notes.size());
        assertEquals(null, notes.get(0).narrative(), "no LLM configured must still record the numeric anomaly, narrative null");
    }
}
