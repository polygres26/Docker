package com.sayonora.warp.testsupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Launches a real {@code com.sayonora.warp.server.Main} as a subprocess, pointed at a real
 * Postgres backend (typically a Testcontainers {@code PostgreSQLContainer}), for integration
 * tests that connect through an actual protocol frontend rather than instantiating internal
 * classes directly -- matches this project's own "real infra, no mocks" verification style.
 *
 * <p>Runs the same JVM/classpath the test itself runs under (via {@code java.class.path}), so no
 * separate build step or shaded jar is required before {@code mvn test}.
 */
public final class WarpProcess implements AutoCloseable {

    private static final AtomicInteger PORT_HINT = new AtomicInteger(28000);

    // Same set scripts/run.sh and the Docker ENTRYPOINT use -- embedded Apache Ignite (on the
    // classpath as a dependency, its JDBC driver auto-registered via ServiceLoader) reflectively
    // opens several java.base packages during static init, which the module system blocks by
    // default from Java 17 onward.
    private static final java.util.List<String> ADD_OPENS = java.util.List.of(
            "--add-opens=java.base/jdk.internal.access=ALL-UNNAMED",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
            "--add-opens=java.base/java.math=ALL-UNNAMED",
            "--add-opens=java.base/java.time=ALL-UNNAMED",
            "--add-opens=java.base/java.text=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.sql/java.sql=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");

    private final Process process;
    private final int metricsPort;
    private final Map<String, Integer> ports;

    private WarpProcess(Process process, int metricsPort, Map<String, Integer> ports) {
        this.process = process;
        this.metricsPort = metricsPort;
        this.ports = ports;
    }

    public int port(String name) {
        Integer p = ports.get(name);
        if (p == null) {
            throw new IllegalArgumentException("no port registered for " + name);
        }
        return p;
    }

    /** SIGSTOP: the instance stops running entirely (probes, config reloads, requests) until {@link #resume()}. */
    public void pause() throws java.io.IOException, InterruptedException {
        new ProcessBuilder("kill", "-STOP", String.valueOf(process.pid())).inheritIO().start().waitFor();
    }

    public void resume() throws java.io.IOException, InterruptedException {
        new ProcessBuilder("kill", "-CONT", String.valueOf(process.pid())).inheritIO().start().waitFor();
    }

    public int metricsPort() {
        return metricsPort;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, String> env = new LinkedHashMap<>();
        private final Map<String, Integer> ports = new LinkedHashMap<>();

        public Builder pgBackend(String host, int port, String database, String user, String password) {
            env.put("WARP_HOST", host);
            env.put("WARP_PORT", String.valueOf(port));
            env.put("WARP_DATABASE", database);
            env.put("WARP_USER", user);
            env.put("WARP_PASSWORD", password);
            env.put("WARP_AUTH_USER", user);
            env.put("WARP_AUTH_PASSWORD", password);
            // Default QoS (rate=5/s burst=5, maxWaitMs=0) is tuned for production traffic shaping,
            // not a test client's rapid connection-setup handshake.
            env.put("WARP_QOS_RATE_PER_SEC", "1000");
            env.put("WARP_QOS_BURST", "1000");
            // Never the fixed default gRPC port (7070): another process on the machine may hold it, and a test that wants a specific one
            // sets WARP_GRPC_PORT itself afterwards.
            env.put("WARP_GRPC_PORT", String.valueOf(findFreePort()));
            return this;
        }

        /** Allocates a free port for {@code envVar} (e.g. {@code WARP_PGWIRE_PORT}), registered under {@code name}. */
        public Builder frontend(String name, String envVar) {
            int port = findFreePort();
            env.put(envVar, String.valueOf(port));
            ports.put(name, port);
            return this;
        }

        public Builder env(String key, String value) {
            env.put(key, value);
            return this;
        }

        public WarpProcess start() throws IOException, InterruptedException {
            int metricsPort = findFreePort();
            env.put("WARP_METRICS_PORT", String.valueOf(metricsPort));

            String javaBin = System.getProperty("java.home") + "/bin/java";
            java.util.List<String> command = new java.util.ArrayList<>(java.util.List.of(javaBin));
            command.addAll(ADD_OPENS);
            command.addAll(java.util.List.of("-cp", System.getProperty("java.class.path"), "com.sayonora.warp.server.Main"));
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.environment().putAll(env);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // Drain stdout/stderr on a daemon thread -- an unread pipe fills up and blocks the
            // child process once the OS buffer is full.
            java.util.Deque<String> recent = new java.util.concurrent.ConcurrentLinkedDeque<>();
            java.util.concurrent.atomic.AtomicReference<String> fatal = new java.util.concurrent.atomic.AtomicReference<>();
            Thread drain = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        System.out.println("[warp] " + line);
                        if (!line.startsWith("\tat ")) {
                            recent.addLast(line);
                            while (recent.size() > 12) {
                                recent.pollFirst();
                            }
                        }
                        // Main's startup died (e.g. a port is already bound) but other threads keep the JVM alive
                        if (line.startsWith("Exception in thread \"main\"")) {
                            fatal.compareAndSet(null, line);
                        }
                        // a listener this test asked for refused to start (Warp keeps running without it): no point waiting
                        for (String protocol : ports.keySet()) {
                            if (line.contains("ERROR") && line.contains("failed to start")
                                    && line.toLowerCase(java.util.Locale.ROOT).contains(protocol.toLowerCase(java.util.Locale.ROOT))) {
                                fatal.compareAndSet(null, line);
                            }
                        }
                    }
                } catch (IOException ignored) {
                    // process ended
                }
            }, "warp-process-output");
            drain.setDaemon(true);
            drain.start();

            // WARP_TEST_STARTUP_SECONDS lets a slow or heavily loaded machine (embedded Ignite start-up can take a
            // minute) wait longer than the 30s default without editing tests.
            Duration startup = Duration.ofSeconds(Long.parseLong(
                    System.getenv().getOrDefault("WARP_TEST_STARTUP_SECONDS", "30")));
            try {
                BooleanSupplier healthy = () -> process.isAlive() && fatal.get() == null;
                waitForHttpReady(metricsPort, startup, env.get("WARP_ADMIN_TOKEN"), healthy);
            // /metrics starts early in Main's setup, before every protocol listener thread has
            // necessarily started (each frontend binds on its own thread, in sequence) -- so it
            // alone isn't proof the frontend under test is actually accepting connections yet.
            // Found live: a real ojdbc11 client connecting to orawire (one of the later listeners
            // to start) got ORA-12541/connection-refused even though /metrics was already up.
                for (int port : ports.values()) {
                    waitForTcpReady(port, startup, healthy);
                }
            } catch (IllegalStateException | InterruptedException e) {
                // Never leave a half-started Warp behind: it keeps its ports and CPU after the test is over.
                process.destroyForcibly();
                String why = fatal.get() != null ? " -- Warp's startup failed: " + fatal.get() : "";
                throw new IllegalStateException(e.getMessage() + why + "\nlast Warp output:\n  "
                        + String.join("\n  ", recent), e);
            }
            return new WarpProcess(process, metricsPort, Map.copyOf(ports));
        }

        /** {@code adminToken}, when the caller configured {@code WARP_ADMIN_TOKEN} (e.g. a test
         * that also sets {@code WARP_OAUTH_ISSUER}), is sent as a Bearer credential on the
         * readiness probe itself -- {@code /metrics} sits behind {@code AccessContextResolver}
         * once an OAuth issuer is configured, and an unauthenticated probe would 401 forever
         * rather than ever observing real readiness. Null/blank for every other test, unchanged. */
        private static void waitForHttpReady(int metricsPort, Duration timeout, String adminToken,
                BooleanSupplier healthy) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                if (!healthy.getAsBoolean()) {
                    throw new IllegalStateException("Warp stopped or failed while starting");
                }
                try {
                    HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + metricsPort + "/metrics")
                            .toURL().openConnection();
                    conn.setConnectTimeout(500);
                    conn.setReadTimeout(500);
                    if (adminToken != null && !adminToken.isBlank()) {
                        conn.setRequestProperty("Authorization", "Bearer " + adminToken);
                    }
                    if (conn.getResponseCode() == 200) {
                        return;
                    }
                } catch (IOException notReadyYet) {
                    // fall through to retry
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("Warp did not become ready within " + timeout);
        }

        private static void waitForTcpReady(int port, Duration timeout, BooleanSupplier healthy) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                if (!healthy.getAsBoolean()) {
                    throw new IllegalStateException("Warp stopped or failed while starting (frontend on port " + port + ")");
                }
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(new java.net.InetSocketAddress("localhost", port), 500);
                    return;
                } catch (IOException notReadyYet) {
                    // fall through to retry
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("Warp frontend on port " + port + " did not become ready within " + timeout);
        }
    }

    private static int findFreePort() {
        // A monotonically-increasing hint avoids handing out the same just-closed port to two
        // frontends started back-to-back in the same test run (TIME_WAIT can make an
        // immediately-reused ephemeral port from ServerSocket(0) flaky under parallel tests).
        for (int attempt = 0; attempt < 20; attempt++) {
            int candidate = PORT_HINT.getAndIncrement();
            try (java.net.ServerSocket socket = new java.net.ServerSocket(candidate)) {
                return socket.getLocalPort();
            } catch (IOException portTaken) {
                // try the next candidate
            }
        }
        throw new IllegalStateException("could not find a free port after 20 attempts");
    }

    /** Forcibly (SIGKILL, not a graceful shutdown) kills the Warp process itself while it may
     * still have live client sessions -- for tests proving what a real client sees when the
     * SERVER dies out from under it, as opposed to {@link #close}'s graceful teardown (which no
     * real client would ever observe mid-session) or {@code RealPostgres#stop}'s simulated
     * BACKEND outage (a live Warp process still running to translate/forward the error).
     * There is nothing left running to send a graceful in-protocol error frame once this returns
     * -- a real client's own transport-level disconnect detection is what's actually being
     * tested, the same detection a real Oracle/MySQL/SQL Server client already relies on for its
     * own server dying, not something Warp can hand-craft after the fact. */
    public void kill() throws InterruptedException {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
            process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() {
        if (process != null && process.isAlive()) {
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
