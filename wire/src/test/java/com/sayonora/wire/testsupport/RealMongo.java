package com.sayonora.wire.testsupport;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.bson.Document;

/**
 * A real, disposable MongoDB container managed via the plain {@code docker} CLI -- deliberately not
 * Testcontainers, for exactly the reason {@link RealPostgres}'s javadoc gives. No auth (a stock
 * {@code mongo} image with no {@code MONGO_INITDB_ROOT_*} env), which is enough to prove the
 * connector's real read path; credential splicing is unit-tested separately.
 */
public final class RealMongo implements AutoCloseable {

    // No mongo image is pinned anywhere else in this repo (mongowire tests talk to Warp itself,
    // not a real mongod); 7.0 is a current, stable LTS-style release line.
    private static final String IMAGE = "mongo:7.0";

    private final String containerName;
    private final int port;

    private RealMongo(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealMongo start() throws IOException, InterruptedException {
        String containerName = "warp-test-mongo-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":27017", IMAGE);
        RealMongo mongo = new RealMongo(containerName, port);
        mongo.waitUntilReady(Duration.ofSeconds(60));
        return mongo;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    /** Base connection string with no database path, e.g. {@code mongodb://localhost:PORT}. */
    public String connectionString() {
        return "mongodb://" + host() + ":" + port;
    }

    public MongoClient client() {
        return MongoClients.create(connectionString() + "/?serverSelectionTimeoutMS=2000");
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        RuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (MongoClient c = client()) {
                c.getDatabase("admin").runCommand(new Document("ping", 1));
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("Mongo container " + containerName + " did not become ready within "
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
