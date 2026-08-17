package com.polygres.advisor.llm;

import java.io.IOException;

/**
 * Process-wide singleton {@link LocalLlamaProcess} manager -- at most one local model running at a
 * time (a laptop-class CPU-only sidecar isn't meant to juggle several), reused across calls as
 * long as the requested model path doesn't change; a changed model path stops the old process and
 * starts the new one. This is the layer {@link LlmProviderFactory} calls for {@link
 * LlmProviderType#LOCAL} -- callers never talk to {@link LocalLlamaProcess} directly.
 */
public final class LocalLlamaManager {

    private static volatile LocalLlamaProcess current;
    private static volatile String currentModelPath;
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("POLYGRES_LLM_LOCAL_PORT", "8091"));

    private LocalLlamaManager() {}

    /** @return the port the running (or freshly started) local server is listening on */
    public static synchronized int ensureRunning(String serverPath, String modelPath) throws IOException {
        if (serverPath == null || serverPath.isBlank()) {
            throw new IllegalStateException("No llama-server binary configured -- set POLYGRES_LLM_LOCAL_SERVER_PATH "
                + "or install llama-server on PATH.");
        }
        if (modelPath == null || modelPath.isBlank()) {
            throw new IllegalStateException("No local model path configured -- set it on the LLM configuration page.");
        }

        if (current != null && current.isAlive() && modelPath.equals(currentModelPath)) {
            return current.port();
        }
        if (current != null) {
            current.close();
        }
        current = LocalLlamaProcess.start(serverPath, modelPath, PORT);
        currentModelPath = modelPath;
        return current.port();
    }
}
