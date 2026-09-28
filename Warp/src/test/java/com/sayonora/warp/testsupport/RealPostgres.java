package com.sayonora.warp.testsupport;

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
    // Set only in local-process mode (WARP_TEST_PG_LOCAL=1): a real Postgres server process from the host's own
    // initdb/postgres binaries instead of a container, for machines whose Docker VM is out of disk.
    private Process localProcess;
    private java.nio.file.Path localDir;

    private RealPostgres(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    private static boolean localMode() {
        return "1".equals(System.getenv("WARP_TEST_PG_LOCAL"));
    }

    private static RealPostgres startLocal(java.util.List<String> overrides) throws IOException, InterruptedException {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("warp-localpg-");
        int port = findFreePort();
        java.nio.file.Files.writeString(dir.resolve("pw"), "postgres\n");
        String bin = System.getenv().getOrDefault("WARP_TEST_PG_BIN", "");
        String initdb = bin.isEmpty() ? "initdb" : bin + "/initdb";
        String postgres = bin.isEmpty() ? "postgres" : bin + "/postgres";
        ProcessBuilder init = new ProcessBuilder(initdb, "-D", dir.resolve("data").toString(), "-U", "postgres",
                "--pwfile", dir.resolve("pw").toString(), "-A", "scram-sha-256", "-E", "UTF8", "--locale=C")
                .redirectErrorStream(true);
        init.environment().put("LC_ALL", "en_US.UTF-8");
        Process ip = init.start();
        ip.getInputStream().readAllBytes();
        if (ip.waitFor() != 0) {
            throw new IllegalStateException("initdb failed");
        }
        List<String> cmd = new java.util.ArrayList<>(List.of(postgres, "-D", dir.resolve("data").toString(), "-p",
                String.valueOf(port), "-k", dir.toString(), "-c", "listen_addresses=127.0.0.1", "-c",
                "max_connections=200", "-c", "max_prepared_transactions=10"));
        for (String o : overrides) {
            cmd.add("-c");
            cmd.add(o);
        }
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.environment().put("LC_ALL", "en_US.UTF-8");
        RealPostgres pg = new RealPostgres("local-" + dir.getFileName(), port);
        pg.localProcess = pb.start();
        pg.localDir = dir;
        pg.waitUntilReady(Duration.ofSeconds(30));
        return pg;
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
     * #DEFAULT_IMAGE} -- e.g. a locally built image with Shim/pg_oracle already installed, for
     * verifying orawire's pg_oracle-present vs pg_oracle-absent code paths (see
     * PgOracleSupport) against the same real docker/JDBC-driven suite either way, not two
     * different test mechanisms. */
    public static RealPostgres start(String image, java.util.List<String> postgresConfOverrides)
            throws IOException, InterruptedException {
        if (localMode()) {
            return startLocal(postgresConfOverrides);
        }
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
        if (localProcess != null) {
            try {
                // SIGINT = "fast shutdown" (disconnects clients); destroy()'s SIGTERM would wait for every client
                new ProcessBuilder("kill", "-INT", String.valueOf(localProcess.pid())).start().waitFor();
                if (!localProcess.waitFor(15, TimeUnit.SECONDS)) {
                    localProcess.destroyForcibly();
                }
            } catch (IOException | InterruptedException e) {
                localProcess.destroyForcibly();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
            try (var walk = java.nio.file.Files.walk(localDir)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            } catch (IOException ignored) {
                // best-effort cleanup
            }
            return;
        }
        try {
            run("docker", "rm", "-f", "-v", containerName);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
