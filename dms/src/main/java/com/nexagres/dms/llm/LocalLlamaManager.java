package com.nexagres.dms.llm;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-wide {@link LocalLlamaProcess} manager -- one running sidecar per distinct model path,
 * reused across calls, keyed by {@code modelPath}. Originally a single-process singleton (only one
 * local model at a time), upgraded once PRIMARY and JUDGE became independently configurable: with
 * Judge deliberately meant to run a genuinely different model than Primary (see {@link
 * LlmJudge}'s javadoc), a single-process design would stop and restart a full model load on
 * *every* summarize call as the two roles alternated -- correct but needlessly slow (each llama.cpp
 * cold start is many seconds). Running both concurrently, each on its own port, means a Judge-
 * enabled summarize call only pays each model's startup cost once, not twice per call.
 *
 * <p>This is the layer {@link LlmProviderFactory} calls for {@link LlmProviderType#LOCAL} --
 * callers never talk to {@link LocalLlamaProcess} directly.
 */
public final class LocalLlamaManager {

    private static final Map<String, LocalLlamaProcess> processes = new ConcurrentHashMap<>();
    private static final Map<String, Integer> ports = new ConcurrentHashMap<>();
    private static final AtomicInteger nextPort = new AtomicInteger(
        Integer.parseInt(System.getenv().getOrDefault("NEXAGRES_LLM_LOCAL_PORT", "8091")));

    private LocalLlamaManager() {}

    /** @return the port the running (or freshly started) local server for this model is listening on */
    public static synchronized int ensureRunning(String serverPath, String modelPath) throws IOException {
        if (serverPath == null || serverPath.isBlank()) {
            throw new IllegalStateException("No llama-server binary configured -- set NEXAGRES_LLM_LOCAL_SERVER_PATH "
                + "or install llama-server on PATH.");
        }
        if (modelPath == null || modelPath.isBlank()) {
            throw new IllegalStateException("No local model path configured -- set it on the LLM configuration page.");
        }

        LocalLlamaProcess existing = processes.get(modelPath);
        if (existing != null && existing.isAlive()) {
            return existing.port();
        }

        int port = ports.computeIfAbsent(modelPath, k -> nextPort.getAndIncrement());
        LocalLlamaProcess started = LocalLlamaProcess.start(serverPath, modelPath, port);
        processes.put(modelPath, started);
        return started.port();
    }
}
