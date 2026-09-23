package com.sayonora.wire.testsupport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * A real, disposable Splunk container ({@code splunk/splunk}), managed via the plain {@code
 * docker} CLI -- same discipline as every other {@code Real*} test helper in this project (see
 * {@link RealPostgres}'s own javadoc for why Testcontainers is deliberately avoided here).
 *
 * <p>{@code splunk/splunk} publishes NO arm64 image (confirmed live: {@code docker manifest
 * inspect splunk/splunk:latest} lists only {@code amd64}) -- on this Apple Silicon host it only
 * runs at all via {@code --platform linux/amd64} QEMU emulation, which is real but genuinely slow
 * (Splunk's own ansible-driven first-boot provisioning, already slow natively, is slower still
 * under emulation) -- budget a generous timeout accordingly.
 *
 * <p>Splunk's own REST API (management port 8089) is HTTPS-only with a self-signed certificate by
 * default; this helper's {@link HttpClient} therefore uses a trust-all {@link SSLContext} -- a real,
 * accepted departure from certificate validation for a disposable, localhost-only test container,
 * never appropriate for anything but this kind of throwaway test fixture.
 */
public final class RealSplunk implements AutoCloseable {

    private static final String IMAGE = "splunk/splunk:latest";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "Warptest123!";

    private final String containerName;
    private final int managementPort;
    private final int hecPort;
    private final HttpClient httpClient;

    private RealSplunk(String containerName, int managementPort, int hecPort) {
        this.containerName = containerName;
        this.managementPort = managementPort;
        this.hecPort = hecPort;
        this.httpClient = trustAllHttpClient();
    }

    public static RealSplunk start() throws IOException, InterruptedException {
        String containerName = "warp-test-splunk-" + System.nanoTime();
        int managementPort = findFreePort();
        int hecPort = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "--platform", "linux/amd64",
                "-p", managementPort + ":8089", "-p", hecPort + ":8088",
                "-e", "SPLUNK_START_ARGS=--accept-license",
                "-e", "SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com",
                "-e", "SPLUNK_PASSWORD=" + PASSWORD,
                "-e", "SPLUNK_HEC_TOKEN=00000000-0000-0000-0000-000000000000",
                IMAGE);
        RealSplunk splunk = new RealSplunk(containerName, managementPort, hecPort);
        splunk.waitUntilReady(Duration.ofMinutes(10)); // slow natively, slower still under amd64 emulation
        return splunk;
    }

    public String endpoint() {
        return "localhost:" + managementPort;
    }

    public String authHeader() {
        return "Basic " + Base64.getEncoder().encodeToString((USERNAME + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    }

    /** Real-indexes {@code event} into Splunk's default {@code main} index via the REST {@code
     * /services/receivers/simple} endpoint (basic-auth, no HEC token dance needed) -- and blocks
     * briefly afterward, since a real Splunk indexing pipeline needs a moment before a search over
     * the indexed data returns it (confirmed live: an immediate search can miss an event indexed a
     * few hundred ms earlier). */
    public void indexEvent(String event) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://" + endpoint() + "/services/receivers/simple?index=main"))
                .header("Authorization", authHeader())
                .POST(HttpRequest.BodyPublishers.ofString(event, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("failed to index event into real Splunk: HTTP " + response.statusCode()
                    + ": " + response.body());
        }
    }

    /** Blocks until a real search over {@code index=main} sees at least {@code expectedCount}
     * events -- real proof the indexing pipeline (not just the receive-and-ack) has caught up,
     * rather than a fixed sleep guess. */
    public void waitForIndexedEvents(String search, int expectedCount, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            int count = countSearchResults(search);
            if (count >= expectedCount) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("real Splunk search \"" + search + "\" did not see " + expectedCount
                + " event(s) within " + timeout);
    }

    private int countSearchResults(String search) throws Exception {
        String form = "search=" + java.net.URLEncoder.encode(
                search.stripLeading().toLowerCase(java.util.Locale.ROOT).startsWith("search") ? search : "search " + search,
                StandardCharsets.UTF_8) + "&output_mode=json&exec_mode=oneshot&count=0";
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://" + endpoint() + "/services/search/jobs"))
                .header("Authorization", authHeader())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() / 100 != 2) {
            return 0;
        }
        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(response.body()).getAsJsonObject();
        return json.has("results") ? json.getAsJsonArray("results").size() : 0;
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpRequest request = HttpRequest.newBuilder(
                                URI.create("https://" + endpoint() + "/services/server/info?output_mode=json"))
                        .header("Authorization", authHeader())
                        .GET().build();
                HttpResponse<String> response = send(request);
                if (response.statusCode() == 200) {
                    return; // real management-port auth succeeded -- Splunk is fully up, not just listening
                }
                lastFailure = new IllegalStateException("HTTP " + response.statusCode() + ": " + response.body());
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(3000);
        }
        throw new IllegalStateException("Splunk container " + containerName + " did not become ready within "
                + timeout, lastFailure);
    }

    private HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpClient trustAllHttpClient() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }}, new SecureRandom());
            return HttpClient.newBuilder().sslContext(sslContext).build();
        } catch (Exception e) {
            throw new IllegalStateException("failed to build trust-all HttpClient for real Splunk test container", e);
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("command failed: " + String.join(" ", command) + "\n" + output);
        }
    }

    @Override
    public void close() {
        try {
            run("docker", "rm", "-f", containerName);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
