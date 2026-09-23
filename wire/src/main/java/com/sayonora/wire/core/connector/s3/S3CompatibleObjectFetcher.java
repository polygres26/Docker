package com.sayonora.wire.core.connector.s3;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * {@link ObjectFetcher} for AWS S3 and every S3-compatible surface -- ported verbatim (module
 * package only) from the sibling ThinkingSense project's real, MinIO-verified connector. The AWS
 * SDK's own SigV4 request signing plus a custom {@code endpoint} + path-style override is genuinely
 * sufficient for MinIO, Wasabi, OCI Object Storage's S3-compatibility API, and GCS's interoperability
 * API -- all four speak the identical S3 REST wire shape, just at a different endpoint.
 *
 * <p>{@code endpoint}/{@code pathStyleAccess} are no-ops against real AWS S3 when left unset.
 */
final class S3CompatibleObjectFetcher implements ObjectFetcher, AutoCloseable {

    private final S3Client client;
    private final String bucket;

    S3CompatibleObjectFetcher(String bucket, String region, String endpoint, boolean pathStyleAccess,
            String accessKeyId, String secretAccessKey) {
        this.bucket = bucket;
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region == null || region.isBlank() ? "us-east-1" : region))
                .credentialsProvider(credentialsProvider(accessKeyId, secretAccessKey));
        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        if (pathStyleAccess) {
            builder.forcePathStyle(true);
        }
        this.client = builder.build();
    }

    private static AwsCredentialsProvider credentialsProvider(String accessKeyId, String secretAccessKey) {
        if (accessKeyId != null && !accessKeyId.isBlank() && secretAccessKey != null && !secretAccessKey.isBlank()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        // Falls back to the SDK's standard credential chain (env vars, ~/.aws/credentials, instance
        // profile, ...) -- same posture as DynamoSchemaFactory's own credentialsProvider.
        return DefaultCredentialsProvider.create();
    }

    @Override
    public byte[] fetch(String key) {
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
        return client.getObject(request, ResponseTransformer.toBytes()).asByteArray();
    }

    @Override
    public void close() {
        client.close();
    }
}
