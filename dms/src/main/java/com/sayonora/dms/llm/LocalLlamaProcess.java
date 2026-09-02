package com.sayonora.dms.llm;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a {@code llama-server} (llama.cpp) sidecar process -- CPU-only, no GPU dependency --
 * ported from Omnigate's {@code com.omnigate.assistant.LocalLlamaProcess}
 * (~/Projects/Omnigate) with the same shape: Advisor doesn't bundle a model or the
 * {@code llama-server} binary itself, and doesn't download either at runtime. Both are
 * operator-provided (the server binary via {@code SAYONORA_LLM_LOCAL_SERVER_PATH}, defaulting to
 * whatever {@code llama-server} resolves to on {@code PATH}; the model file via {@link
 * LlmSettings#modelPath} on the LLM configuration page). Bound to {@code 127.0.0.1} only -- never
 * exposed beyond this process.
 */
public final class LocalLlamaProcess implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocalLlamaProcess.class);

    private final int port;
    private final Process process;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private LocalLlamaProcess(int port, Process process) {
        this.port = port;
        this.process = process;
    }

    public int port() {
        return port;
    }

    /** Throws (rather than returning null) if startup fails or doesn't become healthy in time -- {@link LocalLlamaManager} is the layer that decides how to degrade for callers. */
    public static LocalLlamaProcess start(String serverPath, String modelPath, int port) throws IOException {
        List<String> args = new ArrayList<>(List.of(
            serverPath, "-m", modelPath, "--port", String.valueOf(port), "--host", "127.0.0.1",
            "-c", "2048", "-ngl", "0"));
        ProcessBuilder builder = new ProcessBuilder(args);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(java.io.File.createTempFile("sayonora-llama-", ".log")));
        Process process = builder.start();
        LocalLlamaProcess instance = new LocalLlamaProcess(port, process);
        if (!instance.waitUntilHealthy(Duration.ofSeconds(60))) {
            instance.close();
            throw new IOException("llama-server did not become healthy within 60s on port " + port
                + " -- check its log (see stdout redirect path in the process's temp file) for details.");
        }
        log.info("llama-server ready on 127.0.0.1:{} (model: {})", port, modelPath);
        return instance;
    }

    private boolean waitUntilHealthy(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                    .timeout(Duration.ofSeconds(2)).GET().build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) return true;
            } catch (Exception ignored) {
                // still starting up -- retry until the deadline
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    public boolean isAlive() {
        return process.isAlive();
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }
}
