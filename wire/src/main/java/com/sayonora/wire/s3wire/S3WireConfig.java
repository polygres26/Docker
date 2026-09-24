package com.sayonora.wire.s3wire;

import com.sayonora.wire.dynamowire.auth.AwsIamCredentialStore;

/**
 * s3wire configuration, read from the environment. s3wire is enabled only when
 * {@code WARP_S3WIRE_BACKEND_BUCKET} is set (it has no backend to serve otherwise).
 *
 * <ul>
 *   <li>{@code WARP_S3WIRE_PORT} -- listen port (default 18020)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_BUCKET} -- the one real backend bucket that holds everything
 *       (required; each client-visible bucket is a key prefix inside it)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_ENDPOINT} -- S3-compatible endpoint, e.g. a MinIO URL
 *       (unset = real AWS S3)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_ACCESS_KEY} / {@code WARP_S3WIRE_BACKEND_SECRET_KEY} -- Warp's
 *       own credentials for the backend (unset = AWS default credential chain)</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_REGION} -- default us-east-1</li>
 *   <li>{@code WARP_S3WIRE_BACKEND_PATH_STYLE} -- default true</li>
 *   <li>{@code WARP_S3WIRE_CREDENTIALS} -- {@code accessKey=secret;accessKey2=secret2} pairs that
 *       clients must SigV4-sign with; falls back to {@code WARP_AWS_IAM_CREDENTIALS}. Required:
 *       s3wire refuses to start without at least one pair rather than silently skipping auth.</li>
 * </ul>
 */
public record S3WireConfig(String endpoint, String region, String accessKey, String secretKey,
        String backendBucket, boolean pathStyle, AwsIamCredentialStore clientCredentials) {

    /** @return null when s3wire is not configured (no backend bucket). */
    public static S3WireConfig fromEnv() {
        String bucket = env("WARP_S3WIRE_BACKEND_BUCKET");
        if (bucket == null) {
            return null;
        }
        String creds = env("WARP_S3WIRE_CREDENTIALS");
        if (creds == null) {
            creds = env("WARP_AWS_IAM_CREDENTIALS");
        }
        AwsIamCredentialStore store = AwsIamCredentialStore.parse(creds);
        if (!store.isEnabled()) {
            throw new IllegalStateException("s3wire requires WARP_S3WIRE_CREDENTIALS "
                    + "(accessKey=secret;...) -- refusing to serve unauthenticated S3");
        }
        String region = env("WARP_S3WIRE_BACKEND_REGION");
        String pathStyle = env("WARP_S3WIRE_BACKEND_PATH_STYLE");
        return new S3WireConfig(env("WARP_S3WIRE_BACKEND_ENDPOINT"), region == null ? "us-east-1" : region,
                env("WARP_S3WIRE_BACKEND_ACCESS_KEY"), env("WARP_S3WIRE_BACKEND_SECRET_KEY"), bucket,
                pathStyle == null || Boolean.parseBoolean(pathStyle), store);
    }

    private static String env(String name) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? null : v.trim();
    }
}
