package com.nexagres.wire.testsupport;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.Document;

/**
 * A real, disposable single-node MongoDB replica set, managed via the plain {@code docker} CLI --
 * same discipline as {@link RealPostgres} (not Testcontainers; see that class's own javadoc for
 * why). A single-node REPLICA SET, not a plain standalone server, because change streams need the
 * oplog, which only exists on a replica set -- {@link com.nexagres.wire.mongowire.MongoChangeStreamCdcWorker}
 * and anything else in this project that opens a real change stream needs this, not plain
 * {@code mongo:7}.
 */
public final class RealMongo implements AutoCloseable {

    private static final String DEFAULT_IMAGE = "mongo:7";

    private final String containerName;
    private final int port;

    private RealMongo(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealMongo start() throws IOException, InterruptedException {
        String containerName = "polywire-test-mongo-" + System.nanoTime();
        int port = findFreePort();
        // mongod itself listens on `port` (via --port), not the image's usual 27017, mapped
        // host:port -> container:port with the SAME number on both sides -- required for
        // replSetInitiate's self-validation below to succeed at all: mongod checks the config's
        // member host/port against its OWN actually-bound listening address, and "localhost:X"
        // only matches if X is the port mongod itself is really listening on. A container:27017
        // mapped to an arbitrary free host port (the RealPostgres convention) fails that check
        // with a real InvalidReplicaSetConfig error -- confirmed live, not a hypothetical.
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":" + port, DEFAULT_IMAGE,
                "--replSet", "rs0", "--bind_ip_all", "--port", String.valueOf(port));
        RealMongo mongo = new RealMongo(containerName, port);
        mongo.waitUntilReady(Duration.ofSeconds(30));
        mongo.initiateReplicaSet();
        return mongo;
    }

    public String host() {
        return "localhost";
    }

    public int port() {
        return port;
    }

    public String connectionString() {
        return "mongodb://" + host() + ":" + port + "/?directConnection=true";
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (MongoClient client = MongoClients.create(connectionString())) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                return;
            } catch (Exception e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("Mongo container " + containerName + " did not become ready within "
                + timeout, lastFailure);
    }

    /** {@code replSetInitiate} with an EXPLICIT member host (not the no-arg {@code rs.initiate()}
     * shorthand) -- the container's own self-reported address is only reachable from inside the
     * docker network, not from this JVM on the host, so without pinning {@code host} to the actual
     * externally-mapped {@code localhost:<port>}, every operation after initiation (including the
     * change streams this whole class exists for) would try to reconnect to an address nothing on
     * the host machine can actually reach -- found live, not a hypothetical concern. */
    private void initiateReplicaSet() throws InterruptedException {
        Document config = new Document("_id", "rs0")
                .append("members", List.of(new Document("_id", 0).append("host", "localhost:" + port)));
        Exception lastFailure = null;
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(deadline)) {
            try (MongoClient client = MongoClients.create(connectionString())) {
                client.getDatabase("admin").runCommand(new Document("replSetInitiate", config));
                waitForPrimary();
                return;
            } catch (Exception e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("Mongo replica set " + containerName + " did not initiate", lastFailure);
    }

    private void waitForPrimary() throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(deadline)) {
            try (MongoClient client = MongoClients.create(connectionString())) {
                Document status = client.getDatabase("admin").runCommand(new Document("isMaster", 1));
                if (Boolean.TRUE.equals(status.getBoolean("ismaster"))) {
                    return;
                }
            } catch (Exception ignored) {
                // not elected yet -- keep polling until the deadline
            }
            Thread.sleep(300);
        }
        throw new IllegalStateException("Mongo replica set " + containerName + " never elected a primary");
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
