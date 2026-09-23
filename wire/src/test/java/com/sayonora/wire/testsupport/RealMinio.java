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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * A real, disposable MinIO container ({@code minio/minio}) managed via the plain {@code docker}
 * CLI -- deliberately not Testcontainers, same reasoning as {@link RealPostgres}'s javadoc and
 * every other {@code Real*} helper in this package. MinIO is a real, S3-API-compatible object
 * store -- the same live surface ThinkingSense's own {@code S3CompatibleObjectFetcher} was verified
 * against, and the connector's own {@code provider=s3-compatible}/{@code endpoint=}/{@code
 * pathStyleAccess=true} config path exists specifically for it.
 */
public final class RealMinio implements AutoCloseable {

    // Docker Hub's minio/minio now requires authentication to pull (access denied
    // unauthenticated, confirmed live in this environment) -- quay.io/minio/minio:latest is
    // MinIO's own official mirror there, pullable anonymously.
    private static final String IMAGE = "quay.io/minio/minio:latest";
    private static final String ACCESS_KEY = "warptestkey";
    private static final String SECRET_KEY = "warptestsecret";

    private final String containerName;
    private final int port;

    private RealMinio(String containerName, int port) {
        this.containerName = containerName;
        this.port = port;
    }

    public static RealMinio start() throws IOException, InterruptedException {
        String containerName = "warp-test-minio-" + System.nanoTime();
        int port = findFreePort();
        run("docker", "run", "-d", "--name", containerName, "-p", port + ":9000",
                "-e", "MINIO_ROOT_USER=" + ACCESS_KEY, "-e", "MINIO_ROOT_PASSWORD=" + SECRET_KEY,
                IMAGE, "server", "/data");
        RealMinio minio = new RealMinio(containerName, port);
        minio.waitUntilReady(Duration.ofSeconds(60));
        return minio;
    }

    public String endpoint() {
        return "http://localhost:" + port;
    }

    public String accessKeyId() {
        return ACCESS_KEY;
    }

    public String secretAccessKey() {
        return SECRET_KEY;
    }

    public S3Client client() {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build();
    }

    /** Creates {@code bucket} (if absent) and uploads {@code bytes} at {@code key} -- the one-shot
     * fixture-setup helper every real S3 connector test needs (mirrors {@code RealDynamoDb}'s own
     * {@code client()}-based table setup in its own test callers). */
    public void putObject(String bucket, String key, byte[] bytes) {
        try (S3Client c = client()) {
            try {
                c.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            } catch (RuntimeException e) {
                // BucketAlreadyOwnedByYou on a re-used bucket name -- fine, not fatal.
            }
            c.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes));
        }
    }

    private void waitUntilReady(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        RuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (S3Client c = client()) {
                c.listBuckets();
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                Thread.sleep(300);
            }
        }
        throw new IllegalStateException("MinIO container " + containerName + " did not become ready within "
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
