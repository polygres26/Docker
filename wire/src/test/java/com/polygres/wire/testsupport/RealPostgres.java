package com.polygres.wire.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A real, disposable Postgres container managed via the plain {@code docker} CLI -- deliberately
 * not the Testcontainers library, whose bundled docker-java client probes with a hardcoded old
 * API version (1.32) that a newer Docker Engine (as shipped by Colima on this host, minimum 1.40)
 * rejects outright. Every other real-infra check in this project already drives Docker via the
 * CLI directly; this does the same thing, just wrapped for JUnit lifecycle use.
 */
public final class RealPostgres implements AutoCloseable {

    private final String containerName;
    private final int port;

    private RealPostgres(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealPostgres start() throws IOException, InterruptedException {
        String containerName = "polywire-test-pg-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName,
                "-p", port + ":5432",
                "-e", "POSTGRES_USER=postgres",
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=postgres",
                "postgres:16-alpine");
        RealPostgres pg = new RealPostgres(containerName, port);
        pg.waitUntilReady(Duration.ofSeconds(30));
        return pg;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    public String database() {
        return "postgres";
    }

    public String username() {
        return "postgres";
    }

    public String password() {
        return "postgres";
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://" + host() + ":" + port + "/" + database();
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        SQLException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection ignored = DriverManager.getConnection(jdbcUrl(), username(), password())) {
                return;
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("Postgres container " + containerName + " did not become ready within "
                + timeout, lastFailure);
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
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
