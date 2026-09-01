package com.nexagres.migration.testsupport;

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
 * A real, disposable DynamoDB Local instance, managed via the plain {@code docker} CLI -- same
 * discipline as every other {@code Real*} test helper in this project (not Testcontainers).
 *
 * <p><b>Known, real limitation, not a bug in this test helper</b>: DynamoDB Local does not
 * implement DynamoDB Streams at all (a documented AWS limitation of the emulator, not something
 * this project can work around) -- only Scan/Query/PutItem/DescribeTable-family operations behave
 * like the real service. Tests exercising {@link com.nexagres.migration.connectors.dynamo.DynamoSource}'s
 * Scan-based snapshot path against this run against real infrastructure exactly like every other
 * connector's tests; its Streams-based change-feed path cannot be verified this way and is instead
 * tested against a hand-written fake {@code DynamoDbStreamsClient} (see {@code
 * DynamoSourceStreamsTest}) -- a real AWS DynamoDB Streams smoke test is a separate, genuinely
 * out-of-CI follow-up.
 */
public final class RealDynamoDb implements AutoCloseable {

    private static final String DEFAULT_IMAGE = "amazon/dynamodb-local:latest";

    private final String containerName;
    private final int port;

    private RealDynamoDb(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealDynamoDb start() throws IOException, InterruptedException {
        String containerName = "warp-test-dynamodb-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":8000", DEFAULT_IMAGE,
                "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
        RealDynamoDb dynamo = new RealDynamoDb(containerName, port);
        dynamo.waitUntilReady(Duration.ofSeconds(30));
        return dynamo;
    }

    public String endpoint() {
        return "http://localhost:" + port;
    }

    /** DynamoDB Local ignores credentials/region entirely but the AWS SDK v2 client still refuses
     * to build without SOME values present -- these are placeholders, never real credentials. */
    public DynamoDbClient newClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build();
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (DynamoDbClient client = newClient()) {
                client.listTables();
                return;
            } catch (Exception e) {
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
