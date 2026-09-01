package com.nexagres.migration.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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

    private static final String DEFAULT_IMAGE = "postgres:16-alpine";

    public static RealPostgres start() throws IOException, InterruptedException {
        return start(java.util.List.of());
    }

    /** As {@link #start()}, but with {@code postgresql.conf} settings appended as {@code -c
     * key=value} server args -- e.g. {@code "max_prepared_transactions=10"} for XA-recovery tests,
     * which need PREPARE TRANSACTION support that stock Postgres ships disabled (0). */
    public static RealPostgres start(java.util.List<String> postgresConfOverrides) throws IOException, InterruptedException {
        return start(DEFAULT_IMAGE, postgresConfOverrides);
    }

    /** As {@link #start(List)}, but against a custom image instead of stock {@value
     * #DEFAULT_IMAGE} -- e.g. a locally built image with db/pg_oracle already installed, for
     * verifying orawire's pg_oracle-present vs pg_oracle-absent code paths (see
     * PgOracleSupport) against the same real docker/JDBC-driven suite either way, not two
     * different test mechanisms. */
    public static RealPostgres start(String image, java.util.List<String> postgresConfOverrides)
            throws IOException, InterruptedException {
        String containerName = "warp-test-pg-" + System.nanoTime();
        int port = findFreePort();
        List<String> args = new java.util.ArrayList<>(List.of("docker", "run", "-d", "--name", containerName,
                "-p", port + ":5432",
                "-e", "POSTGRES_USER=postgres",
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=postgres",
                image));
        for (String override : postgresConfOverrides) {
            args.add("-c");
            args.add(override);
        }
        run(args.toArray(new String[0]));
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

    /** Stops the container (connection refused, not just slow) without removing it -- for tests
     * that need to simulate a genuine backend outage (health-checker auto-DOWN, failover) rather
     * than just tearing the fixture down. Pair with {@link #resume}. */
    public void stop() throws IOException, InterruptedException {
        run("docker", "stop", containerName);
    }

    /** Restarts a container previously {@link #stop}ped and waits for it to accept connections
     * again -- does NOT re-run init scripts/env vars, so whatever schema/data existed before
     * {@link #stop} is still there. */
    public void resume() throws IOException, InterruptedException {
        run("docker", "start", containerName);
        waitUntilReady(Duration.ofSeconds(30));
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
