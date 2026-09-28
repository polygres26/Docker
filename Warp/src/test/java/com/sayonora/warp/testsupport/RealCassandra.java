package com.sayonora.warp.testsupport;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A real, disposable single-node Cassandra container ({@code cassandra:5}), managed via the plain
 * {@code docker} CLI -- same discipline as every other {@code Real*} test helper in this project
 * (see {@link RealPostgres}'s own javadoc for why Testcontainers is deliberately avoided here).
 *
 * <p>Cassandra's own startup is genuinely slow (bootstrapping the whole storage/gossip/auth stack,
 * confirmed live to take on the order of a minute even for a single node) -- same "budget a
 * generous timeout, poll with a real client" posture {@link RealOracle} already establishes for its
 * own slow-starting real database container. Readiness here is a real {@link CqlSession} connect
 * (which itself only succeeds once the native CQL transport AND the node's own internal gossip/auth
 * setup have both settled), not just a raw port-open check.
 *
 * <p>The stock image's single node always registers itself in datacenter {@code "datacenter1"} /
 * rack {@code "rack1"} (confirmed live via {@code nodetool status}) -- {@link #localDatacenter()}
 * exposes that fixed name for test fixtures that need to supply {@code localDc=} on a
 * {@code cassandra://} backend URL.
 */
public final class RealCassandra implements AutoCloseable {

    private static final String IMAGE = "cassandra:5";
    private static final String LOCAL_DATACENTER = "datacenter1";

    private final String containerName;
    private final int port;

    private RealCassandra(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealCassandra start() throws IOException, InterruptedException {
        String containerName = "warp-test-cassandra-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":9042", IMAGE);
        RealCassandra cassandra = new RealCassandra(containerName, port);
        cassandra.waitUntilReady(Duration.ofSeconds(180)); // real, slow single-node bootstrap
        return cassandra;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    public String contactPoint() {
        return host() + ":" + port;
    }

    public String localDatacenter() {
        return LOCAL_DATACENTER;
    }

    public CqlSession newSession() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host(), port))
                .withLocalDatacenter(LOCAL_DATACENTER)
                .build();
    }

    /** Creates a real keyspace (RF=1, fine for a single-node test container) -- the one-shot
     * fixture-setup helper every real Cassandra connector test needs. */
    public void createKeyspace(String keyspace) {
        try (CqlSession session = newSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                    + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        }
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        RuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (CqlSession session = newSession()) {
                session.execute("SELECT release_version FROM system.local");
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("Cassandra container " + containerName + " did not become ready within "
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
            run("docker", "rm", "-f", "-v", containerName);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }
}
