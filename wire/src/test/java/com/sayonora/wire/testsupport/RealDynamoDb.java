package com.sayonora.wire.testsupport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * A real, disposable DynamoDB Local container ({@code amazon/dynamodb-local}) managed via the plain
 * {@code docker} CLI -- deliberately not Testcontainers, for exactly the reason {@link RealPostgres}'s
 * javadoc gives (its docker-java client's hardcoded 1.32 API probe is rejected by this host's newer
 * Docker Engine). {@code -inMemory -sharedDb}: one shared table namespace regardless of the
 * access key/region a client signs with, so the test's own client and Warp's connector see the
 * same tables.
 */
public final class RealDynamoDb implements AutoCloseable {

    private static final String IMAGE = "amazon/dynamodb-local:latest";

    private final String containerName;
    private final int port;

    private RealDynamoDb(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealDynamoDb start() throws IOException, InterruptedException {
        String containerName = "warp-test-dynamo-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":8000", IMAGE,
                "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
        RealDynamoDb dynamo = new RealDynamoDb(containerName, port);
        dynamo.waitUntilReady(Duration.ofSeconds(60));
        return dynamo;
    }

    public String endpoint() {
        return "http://localhost:" + port;
    }

    public String region() {
        return "us-east-1";
    }

    /** DynamoDB Local accepts any credentials, but the SDK still insists on signing with some. */
    public String accessKeyId() {
        return "fakeAccessKey";
    }

    public String secretAccessKey() {
        return "fakeSecretKey";
    }

    public DynamoDbClient client() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint()))
                .region(Region.of(region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId(), secretAccessKey())))
                .build();
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        RuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (DynamoDbClient c = client()) {
                c.listTables();
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("DynamoDB Local container " + containerName + " did not become ready within "
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
