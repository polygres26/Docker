package com.nexagres.migration.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A real, disposable MySQL container with binary logging (ROW format) enabled, managed via the
 * plain {@code docker} CLI -- same discipline as every other {@code Real*} test helper in this
 * project. Binlog has to be explicitly enabled and set to ROW format at server startup (neither is
 * MySQL 8's own out-of-the-box default in every distribution) for {@code MySqlSource}'s change-feed
 * path to have anything real to consume -- unlike DynamoDB Local (which cannot do Streams at all,
 * see {@code RealDynamoDb}'s own javadoc), a real {@code mysql:8} container DOES support this, so
 * this connector's CDC path gets full real end-to-end verification, not a fake-client substitute.
 */
public final class RealMySql implements AutoCloseable {

    private static final String DEFAULT_IMAGE = "mysql:8.4";

    private final String containerName;
    private final int port;

    private RealMySql(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealMySql start() throws IOException, InterruptedException {
        String containerName = "polywire-test-mysql-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":3306",
                "-e", "MYSQL_ROOT_PASSWORD=root",
                "-e", "MYSQL_DATABASE=src",
                DEFAULT_IMAGE,
                "--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1", "--gtid-mode=OFF");
        RealMySql mysql = new RealMySql(containerName, port);
        mysql.waitUntilReady(Duration.ofSeconds(60));
        return mysql;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    public String database() {
        return "src";
    }

    public String username() {
        return "root";
    }

    public String password() {
        return "root";
    }

    public String jdbcUrl() {
        return "jdbc:mysql://" + host() + ":" + port + "/" + database()
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectionTimeZone=SERVER";
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        SQLException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl(), username(), password());
                    Statement st = conn.createStatement()) {
                st.execute("SELECT 1");
                return;
            } catch (SQLException e) {
                lastFailure = e;
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("MySQL container " + containerName + " did not become ready within "
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
