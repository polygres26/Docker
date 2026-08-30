package com.nexagres.migration.testsupport;

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

/**
 * Launches a real {@code com.nexagres.wire.server.Main} as a subprocess, pointed at a real
 * Postgres backend (typically a Testcontainers {@code PostgreSQLContainer}), for integration
 * tests that connect through an actual protocol frontend rather than instantiating internal
 * classes directly -- matches this project's own "real infra, no mocks" verification style.
 *
 * <p>Runs the same JVM/classpath the test itself runs under (via {@code java.class.path}), so no
 * separate build step or shaded jar is required before {@code mvn test}.
 */
public final class PolyWireProcess implements AutoCloseable {

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

    private PolyWireProcess(Process process, int metricsPort, Map<String, Integer> ports) {
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
            env.put("POLYWIRE_HOST", host);
            env.put("POLYWIRE_PORT", String.valueOf(port));
            env.put("POLYWIRE_DATABASE", database);
            env.put("POLYWIRE_USER", user);
            env.put("POLYWIRE_PASSWORD", password);
            env.put("POLYWIRE_AUTH_USER", user);
            env.put("POLYWIRE_AUTH_PASSWORD", password);
            // Default QoS (rate=5/s burst=5, maxWaitMs=0) is tuned for production traffic shaping,
            // not a test client's rapid connection-setup handshake.
            env.put("POLYWIRE_QOS_RATE_PER_SEC", "1000");
            env.put("POLYWIRE_QOS_BURST", "1000");
            return this;
        }

        /** Allocates a free port for {@code envVar} (e.g. {@code POLYWIRE_PGWIRE_PORT}), registered under {@code name}. */
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

        public PolyWireProcess start() throws IOException, InterruptedException {
            int metricsPort = findFreePort();
            env.put("POLYWIRE_METRICS_PORT", String.valueOf(metricsPort));

            String javaBin = System.getProperty("java.home") + "/bin/java";
            java.util.List<String> command = new java.util.ArrayList<>(java.util.List.of(javaBin));
            command.addAll(ADD_OPENS);
            command.addAll(java.util.List.of("-cp", System.getProperty("java.class.path"), "com.nexagres.wire.server.Main"));
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.environment().putAll(env);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // Drain stdout/stderr on a daemon thread -- an unread pipe fills up and blocks the
            // child process once the OS buffer is full.
            Thread drain = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        System.out.println("[polywire] " + line);
                    }
                } catch (IOException ignored) {
                    // process ended
                }
            }, "polywire-process-output");
            drain.setDaemon(true);
            drain.start();

            waitForHttpReady(metricsPort, Duration.ofSeconds(30));
            // /metrics starts early in Main's setup, before every protocol listener thread has
            // necessarily started (each frontend binds on its own thread, in sequence) -- so it
            // alone isn't proof the frontend under test is actually accepting connections yet.
            // Found live: a real ojdbc11 client connecting to orawire (one of the later listeners
            // to start) got ORA-12541/connection-refused even though /metrics was already up.
            for (int port : ports.values()) {
                waitForTcpReady(port, Duration.ofSeconds(30));
            }
            return new PolyWireProcess(process, metricsPort, Map.copyOf(ports));
        }

        private static void waitForHttpReady(int metricsPort, Duration timeout) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                try {
                    HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + metricsPort + "/metrics")
                            .toURL().openConnection();
                    conn.setConnectTimeout(500);
                    conn.setReadTimeout(500);
                    if (conn.getResponseCode() == 200) {
                        return;
                    }
                } catch (IOException notReadyYet) {
                    // fall through to retry
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("PolyWire did not become ready within " + timeout);
        }

        private static void waitForTcpReady(int port, Duration timeout) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(new java.net.InetSocketAddress("localhost", port), 500);
                    return;
                } catch (IOException notReadyYet) {
                    // fall through to retry
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("PolyWire frontend on port " + port + " did not become ready within " + timeout);
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

    /** Forcibly (SIGKILL, not a graceful shutdown) kills the PolyWire process itself while it may
     * still have live client sessions -- for tests proving what a real client sees when the
     * SERVER dies out from under it, as opposed to {@link #close}'s graceful teardown (which no
     * real client would ever observe mid-session) or {@code RealPostgres#stop}'s simulated
     * BACKEND outage (a live PolyWire process still running to translate/forward the error).
     * There is nothing left running to send a graceful in-protocol error frame once this returns
     * -- a real client's own transport-level disconnect detection is what's actually being
     * tested, the same detection a real Oracle/MySQL/SQL Server client already relies on for its
     * own server dying, not something PolyWire can hand-craft after the fact. */
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
